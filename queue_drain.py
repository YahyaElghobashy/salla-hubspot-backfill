#!/usr/bin/env python3
"""Queue drain (v1.0): reprocess catalog-held orders from the Order Audit Log
workbook's "Queue Log" tab.

Every order the gate held was written to the Queue Log (queue_status=Queued)
and NEVER created in HubSpot. This engine drains that backlog safely:

  1. Reads the Queue Log tab (cfg.spreadsheet_id / cfg.queue_tab), claims rows
     whose status is Queued / Processing (crashed predecessor) / Error with
     attempts left. Duplicate rows of the same order id collapse to one
     primary; twins are marked "Duplicate".
  2. Re-checks the catalog gate for each order (same [M211/M213/M214] searches
     as the backfill, with a per-product cache -- the backlog is dominated by
     a handful of blockers, so ~100 searches cover ~10k orders).
  3. READY orders run through the exact backfill create pipeline (contact
     guardrails, bundles, dedup, created-ledger). The order's ORIGINAL audit
     row is flipped to "Order Approved" (hold reason cleared) -- the sheet's
     15-min auto-verify trigger then fills the verification columns.
  4. BLOCKED orders are left as Queued (attempts+1, notes="blocked: ...") so
     re-running this engine after catalog fixes picks them up -- the engine is
     the retry loop.
  5. Aggregates every blocker into mirror/blocker_matrix.csv: the approval
     list for the client's catalog authority.

Row lifecycle (queue_status): Queued -> Processed | Duplicate | Error | Gone,
or Queued again (still blocked). Nothing is ever deleted.

Idempotent: created-ledger + salla_order_id dedup search + line-item verify
make a re-run of any row a skip, never a duplicate order.

Own namespace so it coexists with live.py (KEEP live running; this engine
yields the HubSpot budget to it) but do NOT run it alongside backfill.py:
drain.log, STOP.drain, drain.lock.

    ./venv/bin/python3 queue_drain.py --scan       # sheet-only blocker matrix (no API)
    ./venv/bin/python3 queue_drain.py --verify     # fetch+gate readiness report, no writes
    ./venv/bin/python3 queue_drain.py --live --max-orders 50   # pilot batch
    ./venv/bin/python3 queue_drain.py --live       # full drain
"""

import argparse
import csv
import json
import logging
import os
import re
import socket
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

import backfill
from backfill import (Config, Engine, GoogleIO, HubSpot, LocalMirror,
                      RelayClient, RelayError, col_letter, dig, ifempty,
                      install_dns_cache, now_str)

log = logging.getLogger("backfill")  # share the engine's logger/format

STOP_FILE = Path("STOP.drain")
LOCK_FILE = Path("drain.lock")
MATRIX_FILE = Path("mirror/blocker_matrix.csv")

# Queue Log tab columns, 0-based (A..N). Matches [M224]/route_held plus the
# two manually added columns M/N.
QC_ENTRY, QC_OID, QC_REF, QC_QUEUED_AT, QC_REASON, QC_ITEMS, QC_STATUS, \
    QC_JSON, QC_PROCESSED_AT, QC_RESULT, QC_ATTEMPTS, QC_NOTES, \
    QC_OLDER, QC_ODATE = range(14)

TERMINAL = {"processed", "duplicate", "gone"}


def load_dotenv(path=".env"):
    """Tiny .env loader (KEY=VALUE lines); never overrides an existing var."""
    try:
        for line in Path(path).read_text().splitlines():
            line = line.strip()
            if line.startswith("export "):
                line = line[len("export "):]
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.strip().strip('"').strip("'")
            os.environ.setdefault(k.strip(), v)
    except FileNotFoundError:
        pass


class SingleInstanceLock:
    """flock on drain.lock: one drain per machine."""

    def __init__(self):
        self._fh = None

    def acquire(self):
        self._fh = open(LOCK_FILE, "w")
        try:
            import fcntl
            fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except ImportError:
            log.warning("fcntl unavailable: no lock protection on this OS")
        except OSError:
            sys.exit("Another queue_drain instance holds drain.lock. "
                     "Refusing to start a second consumer.")
        self._fh.write(f"{os.getpid()}\n")
        self._fh.flush()


# ----------------------------------------------------------------------------
# Sheet I/O for the Queue Log tab (audit workbook), built on GoogleIO internals
# ----------------------------------------------------------------------------

class DrainGoogleIO(GoogleIO):
    """GoogleIO + Queue Log tab access + buffered audit updates.

    _finish_create() calls audit_update() once per created order; at scale
    that is one Sheets write request per order against a ~35/min budget.
    Buffering merges them into one batchUpdate per ~25 rows. The buffer also
    injects the hold-reason clear (col M) whenever a row flips to
    "Order Approved", matching the Apps Script's approve behaviour.
    """

    AUDIT_FLUSH_N = 25

    def __init__(self, cfg, enabled):
        super().__init__(cfg, enabled)
        self._abuf = {}
        self._abuf_lock = threading.Lock()

    # -- buffered audit updates ------------------------------------------------

    def audit_update(self, row_number, values_by_idx, what):
        if not self.enabled or row_number < 0:
            return
        with self._abuf_lock:
            d = self._abuf.setdefault(row_number, {})
            d.update(values_by_idx)
            if values_by_idx.get(11) == "Order Approved":
                d[12] = ""  # clear stale hold reason, same as Approve Held
            need_flush = len(self._abuf) >= self.AUDIT_FLUSH_N
        if need_flush:
            self.flush_audit()

    def flush_audit(self):
        with self._abuf_lock:
            buf, self._abuf = self._abuf, {}
        if not buf:
            return
        data = []
        for row, vals in buf.items():
            idxs = sorted(vals)
            runs, run = [], [idxs[0]]
            for i in idxs[1:]:
                if i == run[-1] + 1:
                    run.append(i)
                else:
                    runs.append(run)
                    run = [i]
            runs.append(run)
            for r in runs:
                data.append({"range": (f"'{self.cfg.audit_tab}'!"
                                       f"{col_letter(r[0])}{row}:"
                                       f"{col_letter(r[-1])}{row}"),
                             "values": [[vals[i] for i in r]]})
        try:
            self._gexec(self.sheets.values().batchUpdate(
                spreadsheetId=self.cfg.spreadsheet_id,
                body={"valueInputOption": "USER_ENTERED", "data": data}),
                f"audit flush x{len(buf)}", self.sheets_rl)
        except Exception as e:
            log.error("audit flush failed for %d row(s): %s", len(buf), e)

    # -- audit order-id -> row map ---------------------------------------------

    def audit_row_map(self):
        """{salla order id: LAST audit sheet row}. One read of column A."""
        resp = self._gexec(self.sheets.values().get(
            spreadsheetId=self.cfg.spreadsheet_id,
            range=f"'{self.cfg.audit_tab}'!A2:A",
            valueRenderOption="UNFORMATTED_VALUE"),
            "audit id map", self.sheets_rl)
        out = {}
        for i, raw in enumerate(resp.get("values", []) or []):
            oid = self._norm_id(raw[0] if raw else "")
            if oid:
                out[oid] = i + 2  # later rows overwrite: last occurrence wins
        return out

    # -- Queue Log read / mark -------------------------------------------------

    def qlog_read(self):
        resp = self._gexec(self.sheets.values().get(
            spreadsheetId=self.cfg.spreadsheet_id,
            range=f"'{self.cfg.queue_tab}'!A2:N",
            valueRenderOption="UNFORMATTED_VALUE"),
            "queue log read", self.sheets_rl)
        out = []
        for i, raw in enumerate(resp.get("values", []) or []):
            raw = list(raw) + [""] * (14 - len(raw))
            try:
                attempts = int(float(raw[QC_ATTEMPTS] or 0))
            except (TypeError, ValueError):
                attempts = 0
            out.append({
                "row": i + 2,
                "order_id": self._norm_id(raw[QC_OID]),
                "reference": self._norm_id(raw[QC_REF]),
                "reason": str(raw[QC_REASON] or ""),
                "items": str(raw[QC_ITEMS] or ""),
                "status": str(raw[QC_STATUS] or "").strip().lower(),
                "attempts": attempts,
                "notes": str(raw[QC_NOTES] or ""),
                "older": str(raw[QC_OLDER] or ""),
            })
        return out

    def qlog_mark_batch(self, marks):
        """marks: list of (row, expect_order_id, {col_idx: value}).
        Verify-then-write like queue_mark: one batchGet of column B for the
        chunk, then one batchUpdate. Rows that moved are skipped loudly."""
        written = 0
        for start in range(0, len(marks), 80):
            chunk = marks[start:start + 80]
            ranges = [f"'{self.cfg.queue_tab}'!B{m[0]}" for m in chunk]
            try:
                resp = self._gexec(self.sheets.values().batchGet(
                    spreadsheetId=self.cfg.spreadsheet_id, ranges=ranges,
                    valueRenderOption="UNFORMATTED_VALUE"),
                    "qlog verify batch", self.sheets_rl)
            except Exception as e:
                log.error("qlog verify batch failed: %s", e)
                continue
            got = [self._norm_id((((vr.get("values") or [[""]])[0]) or [""])[0])
                   for vr in resp.get("valueRanges", [])]
            data = []
            for (row, expect, vals), have in zip(chunk, got):
                if have != str(expect):
                    log.error("QUEUE LOG row %s moved (expected %s, found %s) "
                              "-- write refused", row, expect, have)
                    continue
                idxs = sorted(vals)
                runs, run = [], [idxs[0]]
                for i in idxs[1:]:
                    if i == run[-1] + 1:
                        run.append(i)
                    else:
                        runs.append(run)
                        run = [i]
                runs.append(run)
                for r in runs:
                    data.append({"range": (f"'{self.cfg.queue_tab}'!"
                                           f"{col_letter(r[0])}{row}:"
                                           f"{col_letter(r[-1])}{row}"),
                                 "values": [[vals[i] for i in r]]})
                written += 1
            if data:
                try:
                    self._gexec(self.sheets.values().batchUpdate(
                        spreadsheetId=self.cfg.spreadsheet_id,
                        body={"valueInputOption": "USER_ENTERED", "data": data}),
                        f"qlog mark x{len(data)}", self.sheets_rl)
                except Exception as e:
                    log.error("qlog mark batch failed: %s", e)
        return written


# ----------------------------------------------------------------------------
# The drain engine
# ----------------------------------------------------------------------------

class QueueDrainEngine(Engine):
    """Reprocesses Queue Log rows through the backfill create pipeline.

    Differences from the base engine:
      - intake is the Queue Log tab, not the Salla pagination cursor;
      - the catalog gate result is CACHED per product id (the whole point of
        the backlog is that a few products block thousands of orders);
      - a blocked order is NOT re-queued (route_held suppressed) -- its
        existing row just gets attempts+1 and a blocker note;
      - a created order UPDATES its original audit row (no new arrival row).
    """

    def __init__(self, cfg, relay, hs, gio, mirror, live, workers=None):
        class _NoCursor:
            data = {"status": "drain"}
            status = "drain"
        super().__init__(cfg, _NoCursor(), relay, hs, gio, mirror,
                         live=live, workers=workers)
        # Queue Log provenance column stays whatever the original writer set;
        # audit rows we append (rare fallback) stamp "Yes" like the backfill.
        self._gate_cache = {}
        self._gate_lock = threading.Lock()
        self._audit_rows = {}
        self._blocked = {}     # pid -> {name, why, count, samples}
        self._blocked_lock = threading.Lock()

    # -- cached catalog gate ---------------------------------------------------

    def gate_cached(self, order):
        """gate_unverified_items with a per-product cache. Returns the list of
        unverified items, each with product id + suggested fix."""
        unverified = []
        for item in order.get("items", []) or []:
            if str(item.get("product_type", "")).lower() == "group_products":
                continue
            pid_raw = dig(item, "product.id")
            if not pid_raw:
                # Product deleted in Salla: id-based gate would hold forever.
                # Resolve by SKU against approved legacy records. [M211S]
                sku = str(item.get("sku") or "").strip()
                key = "sku:" + (sku or "-")
                with self._gate_lock:
                    ok = self._gate_cache.get(key)
                if ok is None:
                    ok = bool(sku) and self.hs.gate_search_product_by_sku(
                        [sku, self._legacy_sku(sku)]) > 0
                    with self._gate_lock:
                        self._gate_cache[key] = ok
                if not ok:
                    unverified.append({
                        "id": item.get("id"), "pid": "",
                        "name": item.get("name", ""),
                        "why": ("product deleted in Salla -- create+approve a "
                                "legacy record with hs_sku="
                                + (self._legacy_sku(sku) if sku else "?"))})
                continue
            pid = str(pid_raw)
            with self._gate_lock:
                res = self._gate_cache.get(pid)
            if res is None:
                p = self.hs.gate_search_product_approved(pid)   # [M211]
                te = self.hs.gate_search_template(pid, True)    # [M213]
                ta = self.hs.gate_search_template(pid, False)   # [M214]
                res = (p, te, ta)
                with self._gate_lock:
                    self._gate_cache[pid] = res
            p, te, ta = res
            if (p == 0 and te == 0 and ta == 0) or (te == 0 and ta > 0):
                why = ("activate bundle template (exists but draft/inactive "
                       "or 0 active components)" if ta > 0
                       else "product missing or unapproved -- approve as "
                            "standalone or map as bundle")
                unverified.append({"id": item.get("id"), "pid": pid,
                                   "name": item.get("name", ""), "why": why})
        return unverified

    def note_blocked(self, order, unverified):
        with self._blocked_lock:
            for u in unverified:
                b = self._blocked.setdefault(u["pid"], {
                    "name": u["name"], "why": u["why"], "count": 0,
                    "samples": []})
                b["count"] += 1
                if len(b["samples"]) < 5:
                    b["samples"].append(str(order.get("id")))

    # -- pre-existing resolution (ledger + dedup + LI verify) ------------------

    def resolve_preexisting(self, oid, expected_items):
        """(queue_status, result_note) when the order already exists and is
        complete/authoritative; None to proceed to gate+create."""
        hs_id = self.created_ledger.get(oid)
        if hs_id:
            self._bump("skipped_existing")
            return ("Processed", f"already synced by engine -- HS {hs_id}")
        try:
            hs_id = self.hs.find_order_by_salla_id(oid)
        except Exception as e:
            log.error("pre-existing search failed for %s: %s", oid, e)
            return None  # create path's duplicate-400 guardrail covers it
        if not hs_id:
            return None
        li = self.hs.order_line_item_count(hs_id)
        if li < 0:
            return ("Error", f"HS {hs_id}: line-item verify unavailable -- retry")
        expected = max(1, expected_items)
        if li >= expected:
            self.created_ledger.add(oid, hs_id)
            self._bump("skipped_existing")
            return ("Processed", f"pre-existing HS {hs_id} verified "
                                 f"({li} line items >= {expected} items)")
        self.mirror.error(oid, "partial", f"HS {hs_id} has {li}/{expected} "
                          "line items -- repair via the Repair scenario")
        return ("Error", f"partial pre-existing HS {hs_id}: "
                         f"{li}/{expected} line items -- needs repair")

    # -- one order through gate + create ---------------------------------------

    def drain_one(self, row, order):
        """Returns (queue_status, result_note, attempts_delta)."""
        oid = str(order.get("id"))
        pre = self.resolve_preexisting(oid, len(order.get("items", []) or []))
        if pre:
            return (pre[0], pre[1], 1)
        unverified = self.gate_cached(order)
        if unverified:
            self.note_blocked(order, unverified)
            self._bump("held")
            names = ", ".join(u["name"] for u in unverified)
            log.info("BLOCKED order %s (%d item(s)): %s", oid,
                     len(unverified), names[:160])
            return ("Queued", f"blocked: {names[:180]} @ {now_str()}", 1)
        if not self.live:
            log.info("READY order %s (verify mode -- not creating)", oid)
            return ("Ready", "", 0)
        # ---- create path: archive, audit row, then the engine pipeline ----
        json_text = json.dumps(order, ensure_ascii=False, separators=(",", ":"))
        ref = ifempty(order.get("reference_id"), oid)
        filename = (f"order_RID{ref}_{oid}_{order.get('payment_method','')}_"
                    f"{str(dig(order,'date.date'))[:10]}.json")
        Path(self.cfg.archive_dir).mkdir(parents=True, exist_ok=True)
        (Path(self.cfg.archive_dir) / filename).write_text(
            json_text, encoding="utf-8")
        audit_row = self._audit_rows.get(oid, -1)
        if audit_row < 0:
            # no original audit row (should be rare) -- append an arrival row
            items = order.get("items", []) or []
            add = {0: oid, 1: str(order.get("reference_id", "")),
                   2: dig(order, "date.date"),
                   3: dig(order, "customer.full_name"),
                   4: dig(order, "customer.email"),
                   5: (f"{dig(order,'customer.mobile_code')}"
                       f"{dig(order,'customer.mobile')}"),
                   6: str(dig(order, "amounts.total.amount")),
                   7: dig(order, "amounts.shipping_cost.currency"),
                   8: order.get("payment_method", ""),
                   9: str(len(items)),
                   10: ",".join(str(i.get("name", "")) for i in items),
                   11: "Order Arrived", 29: now_str(), 30: "Yes"}
            audit_row = self.gio.audit_append(add)
            self._audit_rows[oid] = audit_row
        self.route_create(order, audit_row)   # sets self._outcome[oid]
        outcome, hs_ref = self._outcome.pop(oid, ("error", ""))
        if outcome == "created":
            return ("Processed", f"created HS {hs_ref}", 1)
        return ("Error", "create failed -- see drain.log / mirror/errors.csv", 1)

    # -- claim + dedupe ----------------------------------------------------------

    def claimable(self, rows):
        out = []
        for r in rows:
            if not r["order_id"]:
                continue
            s = r["status"]
            if s in ("queued", "processing", ""):
                out.append(r)
            elif s == "error" and r["attempts"] < self.cfg.live_max_attempts:
                out.append(r)
        return out

    @staticmethod
    def split_primaries(claimed):
        primaries, twins, seen = [], [], {}
        for r in claimed:
            oid = r["order_id"]
            if oid in seen:
                twins.append((r, seen[oid]))
            else:
                seen[oid] = r["row"]
                primaries.append(r)
        return primaries, twins

    # -- blocker matrix ----------------------------------------------------------

    def write_matrix(self, extra_note=""):
        MATRIX_FILE.parent.mkdir(parents=True, exist_ok=True)
        rows = sorted(self._blocked.items(), key=lambda kv: -kv[1]["count"])
        with open(MATRIX_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["salla_product_id", "item_name", "blocked_orders",
                        "suggested_action", "sample_order_ids", "checked_at",
                        "note"])
            for pid, b in rows:
                w.writerow([pid, b["name"], b["count"], b["why"],
                            " ".join(b["samples"]), now_str(), extra_note])
        log.info("BLOCKER MATRIX -> %s (%d blocker(s))", MATRIX_FILE, len(rows))
        for pid, b in rows[:20]:
            log.info("BLOCKER %-12s x%-5d %s  [%s]", pid, b["count"],
                     b["name"][:70], b["why"][:60])

    # -- main -------------------------------------------------------------------

    def run_drain(self, max_orders=None):
        t0 = time.monotonic()
        mode = "LIVE" if self.live else "VERIFY (no writes)"
        rows = self.gio.qlog_read()
        claimed = self.claimable(rows)
        primaries, twins = self.split_primaries(claimed)
        log.info("DRAIN start mode=%s rows=%d claimable=%d unique=%d twins=%d "
                 "workers=%d", mode, len(rows), len(claimed), len(primaries),
                 len(twins), self.workers)

        if self.live:
            self._audit_rows = self.gio.audit_row_map()
            log.info("audit map: %d order id(s)", len(self._audit_rows))

        # ledger short-circuit before any fetch: free, local
        marks, to_process = [], []
        for r in primaries:
            hs_id = self.created_ledger.get(r["order_id"])
            if hs_id:
                self._bump("skipped_existing")
                marks.append((r["row"], r["order_id"], {
                    QC_STATUS: "Processed", QC_PROCESSED_AT: now_str(),
                    QC_RESULT: f"already synced by engine -- HS {hs_id}",
                    QC_ATTEMPTS: r["attempts"] + 1}))
            else:
                to_process.append(r)
        if self.live and marks:
            self.gio.qlog_mark_batch(marks)
            log.info("ledger short-circuit settled %d row(s)", len(marks))

        # fetch + process in relay-sized waves so a STOP loses little work
        done_counter = 0
        outcome_by_oid = {}
        wave_size = max(24, self.cfg.relay_batch_size * 4)
        for wstart in range(0, len(to_process), wave_size):
            if self._should_stop():
                log.warning("STOP: leaving remaining rows queued")
                break
            if max_orders is not None and done_counter >= max_orders:
                log.info("Reached --max-orders=%s", max_orders)
                break
            wave = to_process[wstart:wstart + wave_size]
            if max_orders is not None:
                wave = wave[:max(0, max_orders - done_counter)]
            self._yield_to_live()
            self._rates_report()
            ids = [r["order_id"] for r in wave]
            try:
                orders = self.relay.fetch_orders(ids)
            except RelayError as e:
                if self.health is not None:
                    self.health.record_failure(str(e), gio=self.gio)
                log.error("Relay unavailable, halting drain: %s", e)
                break
            if self.health is not None:
                self.health.record_success()

            wave_marks = []
            if self.live:
                ts = now_str()
                self.gio.qlog_mark_batch([
                    (r["row"], r["order_id"],
                     {QC_STATUS: "Processing",
                      QC_NOTES: f"processing since {ts}"})
                    for r in wave if orders.get(r["order_id"])])

            pending = {}
            feed = iter(wave)
            with ThreadPoolExecutor(max_workers=self.workers,
                                    thread_name_prefix="lane") as pool:
                while True:
                    while len(pending) < self.workers and not self._should_stop():
                        r = next(feed, None)
                        if r is None:
                            break
                        order = orders.get(r["order_id"])
                        if not order:
                            att = r["attempts"] + 1
                            gone = att >= self.cfg.live_max_attempts
                            wave_marks.append((r["row"], r["order_id"], {
                                QC_STATUS: "Gone" if gone else "Error",
                                QC_PROCESSED_AT: now_str(),
                                QC_RESULT: ("relay returned nothing after "
                                            f"{att} attempts" if gone
                                            else "relay fetch miss -- retry"),
                                QC_ATTEMPTS: att}))
                            outcome_by_oid[r["order_id"]] = "Gone" if gone else "Error"
                            continue
                        pending[pool.submit(self._drain_safe, r, order)] = r
                        self._in_flight = len(pending)
                    if not pending:
                        break
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for fut in done:
                        r = pending.pop(fut)
                        status, note, att_d = fut.result()
                        outcome_by_oid[r["order_id"]] = status
                        done_counter += 1
                        if status == "Ready":
                            continue  # verify mode: no writes
                        upd = {QC_STATUS: status if status != "Queued" else "Queued",
                               QC_ATTEMPTS: r["attempts"] + att_d}
                        if status == "Processed":
                            upd[QC_PROCESSED_AT] = now_str()
                            upd[QC_RESULT] = note
                            upd[QC_NOTES] = ""
                        elif status == "Queued":
                            upd[QC_NOTES] = note
                        else:
                            upd[QC_RESULT] = note
                        wave_marks.append((r["row"], r["order_id"], upd))
                    self._in_flight = len(pending)
            if self.live and wave_marks:
                self.gio.qlog_mark_batch(wave_marks)
                self.gio.flush_audit()
            done_pct = min(100, int(100 * (wstart + len(wave))
                                    / max(1, len(to_process))))
            log.info("DRAIN progress ~%d%%  created=%d blocked=%d "
                     "skipped_existing=%d errors=%d", done_pct,
                     self.stats.created, self.stats.held,
                     self.stats.skipped_existing, self.stats.errors)

        # twins inherit the primary's outcome
        if self.live and twins:
            tmarks = []
            for r, prow in twins:
                res = outcome_by_oid.get(r["order_id"], "")
                hs_id = self.created_ledger.get(r["order_id"])
                note = f"duplicate of queue row {prow}"
                if hs_id:
                    note += f" -- HS {hs_id}"
                tmarks.append((r["row"], r["order_id"], {
                    QC_STATUS: "Duplicate", QC_PROCESSED_AT: now_str(),
                    QC_RESULT: note}))
            self.gio.qlog_mark_batch(tmarks)
            log.info("marked %d duplicate row(s)", len(tmarks))

        if self.live:
            self.gio.flush_audit()
        self.write_matrix("" if self.live else "verify mode")
        s = self.stats
        log.info("=" * 68)
        log.info("DRAIN SUMMARY mode=%s  %.1f min", mode,
                 (time.monotonic() - t0) / 60)
        log.info("created=%d blocked(still queued)=%d skipped_existing=%d "
                 "errors=%d gate_cache=%d product(s)", s.created, s.held,
                 s.skipped_existing, s.errors, len(self._gate_cache))
        log.info("blocker matrix: %s -- share with the catalog owner, fix, "
                 "re-run this engine", MATRIX_FILE)
        log.info("=" * 68)

    def _drain_safe(self, row, order):
        try:
            return self.drain_one(row, order)
        except Exception as e:
            self._bump("errors")
            log.exception("Order %s failed: %s", row["order_id"], e)
            try:
                self.mirror.error(row["order_id"], "drain", e)
            except Exception:
                pass
            return ("Error", f"unhandled: {e}"[:200], 1)


# ----------------------------------------------------------------------------
# Sheet-only scan (no API): blocker matrix from the stored unverified_items
# ----------------------------------------------------------------------------

def scan_only(gio):
    rows = gio.qlog_read()
    queued = [r for r in rows if r["status"] in ("queued", "processing", "", "error")]
    uniq = {}
    for r in queued:
        uniq.setdefault(r["order_id"], r)
    by_name = defaultdict(lambda: {"count": 0, "samples": []})
    unknown = 0
    for r in uniq.values():
        items = r["items"] or r["reason"]
        if not items:
            unknown += 1
            continue
        for part in re.split(r"\s*,\s*(?=[؀-ۿ])|\s*;\s*", items):
            part = part.strip()
            if not part:
                continue
            b = by_name[part]
            b["count"] += 1
            if len(b["samples"]) < 5:
                b["samples"].append(r["order_id"])
    MATRIX_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(MATRIX_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["salla_product_id", "item_name", "blocked_orders",
                    "suggested_action", "sample_order_ids", "checked_at", "note"])
        for name, b in sorted(by_name.items(), key=lambda kv: -kv[1]["count"]):
            w.writerow(["", name, b["count"],
                        "confirm in validation sheet: approve product or "
                        "activate bundle", " ".join(b["samples"]), now_str(),
                        "name-based scan; run --verify for product-id level"])
    print(f"\nQueue Log: {len(rows)} rows, {len(queued)} claimable, "
          f"{len(uniq)} unique orders, {unknown} with no stored blocker name")
    print(f"Blocker matrix (name-based) -> {MATRIX_FILE}\n")
    for name, b in sorted(by_name.items(), key=lambda kv: -kv[1]["count"])[:20]:
        print(f"  {b['count']:>6}  {name[:90]}")
    print("\nNext: fix catalog per matrix, then `queue_drain.py --verify` "
          "(ID-level readiness) and `--live --max-orders 50` (pilot).")


def main():
    ap = argparse.ArgumentParser(description="Queue Log drain engine (v1.0)")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--scan", action="store_true",
                    help="Sheet-only blocker matrix from stored names (no API)")
    ap.add_argument("--verify", action="store_true",
                    help="Fetch + gate-check every claimable order, write "
                         "nothing; ID-level blocker matrix")
    ap.add_argument("--live", action="store_true",
                    help="Drain for real: create ready orders, mark the sheet")
    ap.add_argument("--max-orders", type=int, default=None,
                    help="Cap processed orders this run (pilot batches)")
    ap.add_argument("--workers", type=int, default=None)
    ap.add_argument("--yes", action="store_true", help="skip the RUN prompt")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    threading.current_thread().name = "main"
    fmt = "%(asctime)s %(levelname)-7s [%(threadName)s] %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=fmt,
                        handlers=[logging.FileHandler("drain.log",
                                                      encoding="utf-8")])
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    console.setFormatter(logging.Formatter(fmt))
    logging.getLogger().addHandler(console)
    logging.getLogger("googleapiclient").setLevel(logging.WARNING)
    socket.setdefaulttimeout(180)
    install_dns_cache()
    load_dotenv()

    cfg = Config.load(args.config)
    if hasattr(backfill, "apply_portal_config"):
        backfill.apply_portal_config(cfg)  # repo build; deploy build bakes IDs

    if not (args.scan or args.verify or args.live):
        print(__doc__)
        sys.exit("Pick a mode: --scan (free), --verify (read-only API), "
                 "or --live (drain).")

    gio = DrainGoogleIO(cfg, enabled=True)

    if args.scan:
        scan_only(gio)
        return

    token = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    secret = os.environ.get("RELAY_SECRET", "")
    if not token or not secret:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN and RELAY_SECRET first (.env).")

    if args.live and not args.yes:
        confirm = input("LIVE DRAIN will create orders in HubSpot and update "
                        "the Queue Log + Audit Log sheets. Type RUN: ")
        if confirm.strip() != "RUN":
            sys.exit("Aborted.")

    SingleInstanceLock().acquire()
    backfill.STOP_FILE = STOP_FILE  # engine STOP checks watch STOP.drain

    mirror = LocalMirror("mirror")
    relay = RelayClient(cfg, secret)
    hs = HubSpot(cfg, token, live=args.live)
    engine = QueueDrainEngine(cfg, relay, hs, gio, mirror, live=args.live,
                              workers=args.workers)
    try:
        import notify
        from relay_health import RelayHealth
        engine.health = RelayHealth(cfg, mirror_dir=mirror.dir, notifier=notify)
    except Exception as e:
        log.warning("outage alerting disabled: %s", e)
    engine.run_drain(max_orders=args.max_orders)


if __name__ == "__main__":
    main()
