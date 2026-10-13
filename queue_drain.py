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
from datetime import datetime
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

# [v2.14.1] A blocked row's note records the ids that held it, so the next
# pass can skip it by id (names are not unique: a relisted Salla product keeps
# its name under a new id). The record goes LAST so the "blocked: <names> @"
# head tools/held_inventory.py reads is unchanged:
#   blocked: <names> @ <YYYY-MM-DD HH:MM:SS> [pids=<id>,<id>/<sku>,sku:<sku>,...]
# [v2.14.1] "<id>/<sku>" is an id with its item's SKU, recorded when that SKU
# did not pass the gate; the row is fetched again once it does.
PIDS_RE = re.compile(r"\[pids=([^\[\]]*)\]\s*$")
TRIED_RE = re.compile(r"(?:@|since) (\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})")
_UNWRITABLE = re.compile(r"[,\[\]\r\n]")

# [v2.14.1] The pre-filter skips a row only for this long after its last try.
# A row's blocker can change without its recorded id ever passing (the listing
# is deleted in Salla and the order now resolves by SKU), so every row is
# fetched again at least this often: ~24 relay fetches an hour for 1,700 rows.
SKIP_MAX_AGE_S = 72 * 3600


def block_key(u):
    """The key a blocker is tallied under: the Salla product id, or the SKU
    for a product deleted in Salla (it has no id)."""
    return u["pid"] or f"sku:{u.get('sku') or '-'}"


def pids_note(unverified, skus=()):
    """The ' [pids=...]' suffix for a blocked row's note; '' when a key cannot
    be written back unambiguously (that row is then simply fetched).
    [v2.14.1] A product id whose item SKU is in `skus` is written as
    "<id>/<sku>"; a SKU that cannot be written back is left off its id."""
    keys = set()
    for u in unverified:
        key = str(block_key(u)).strip()
        if not key or _UNWRITABLE.search(key) or (u["pid"] and "/" in key):
            return ""
        sku = str(u.get("sku") or "").strip()
        if u["pid"] and sku and sku in skus and not _UNWRITABLE.search(sku):
            key = f"{key}/{sku}"
        keys.add(key)
    return f" [pids={','.join(sorted(keys))}]" if keys else ""


def split_key(key):
    """[v2.14.1] A recorded key -> (blocker key, SKU): "<id>/<sku>" gives the
    id and its SKU, "sku:<sku>" itself and its SKU, a bare id (id, '')."""
    if key.startswith("sku:"):
        return key, ("" if key == "sku:-" else key[4:])
    pid, _, sku = key.partition("/")
    return pid, sku


def recorded_keys(notes):
    """The blocker keys a blocked row's note recorded, or None when it has no
    record (a note written before v2.14.1, or not a blocked note)."""
    notes = str(notes or "").strip()
    m = PIDS_RE.search(notes) if notes.startswith("blocked") else None
    keys = frozenset(k.strip() for k in m.group(1).split(",") if k.strip()) if m else None
    return keys or None


def note_names(notes):
    """The blocker names a blocked row's note carries; '' when none."""
    m = re.match(r"blocked:\s*(.*?)\s*@ \d{4}-\d{2}-\d{2}", str(notes or "").strip())
    return m.group(1) if m else ""


def last_tried(notes):
    """When the drain last tried this row ('YYYY-MM-DD HH:MM:SS', from the
    "@ <ts>" of a blocked note or "processing since <ts>"); '' when never."""
    found = TRIED_RE.findall(str(notes or ""))
    return " ".join(found[-1]) if found else ""


def tried_within(notes, seconds, now):
    """[v2.14.1] The drain tried this row less than `seconds` before `now`
    (a now_str() timestamp, the clock the notes are written with). False when
    the note has no readable try."""
    try:
        age = (datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
               - datetime.strptime(last_tried(notes), "%Y-%m-%d %H:%M:%S"))
    except ValueError:
        return False
    return age.total_seconds() < seconds


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
                "queued_at": str(raw[QC_QUEUED_AT] or ""),
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
        self.begin_gate(order)        # [v2.14.1] the line builder reads this decision
        for item in order.get("items", []) or []:
            if str(item.get("product_type", "")).lower() == "group_products":
                continue
            pid_raw = dig(item, "product.id")
            if not pid_raw:
                # Product deleted in Salla: id-based gate would hold forever.
                # Resolve by SKU against approved legacy records. [M211S]
                sku = str(item.get("sku") or "").strip()
                if not self._sku_ok(sku):
                    unverified.append({
                        "id": item.get("id"), "pid": "", "sku": sku,
                        "name": item.get("name", ""), "why": self._deleted_why(sku)})
                continue
            pid = str(pid_raw)
            p, te, raw_ta = self._pid_counts(pid)
            # [v2.14] same stray-record rule as the live gate, applied per order
            # on the cached counts (the verdict keeps its own short cache), so
            # a bundle someone starts building mid-pass holds like it does live.
            # [v2.14.1] Counts the verdict can act on are no older than it.
            ta = self.effective_ta(pid, p, te, raw_ta, item.get("name", ""))
            if ta and self._exemptable(p, te, raw_ta):
                # the verdict did not exempt it, but the counts may predate that
                # verdict (the sentinel can set the record aside in between):
                # read them again, at most once per verdict, before holding
                p, te, raw_ta = self._pid_counts(pid, older_than=self._verdict_at(pid))
                ta = self.effective_ta(pid, p, te, raw_ta, item.get("name", ""))
            if raw_ta and not ta:
                self.note_gate_exempt(order, pid)     # [v2.14.1] for the line builder
            if self._fails(p, te, ta):
                unverified.append({"id": item.get("id"), "pid": pid,
                                   "sku": str(item.get("sku") or "").strip(),
                                   "name": item.get("name", ""), "why": self._why(p, ta)})
        return unverified

    def _sku_ok(self, sku):
        """[M211S] A deleted product's SKU resolves to an approved legacy
        record. Cached for the pass."""
        key = "sku:" + (sku or "-")
        with self._gate_lock:
            ok = self._gate_cache.get(key)
        if ok is None:
            ok = bool(sku) and self.hs.gate_search_product_by_sku(
                [sku, self._legacy_sku(sku)]) > 0
            with self._gate_lock:
                self._gate_cache[key] = ok
        return ok

    def _verdict_at(self, pid):
        """[v2.14.1] When the cached stray verdict for pid was read (0 if none)."""
        hit = (self.__dict__.get("_stray_cache") or {}).get(str(pid))
        return hit[0] if hit else 0

    def _pid_counts(self, pid, older_than=0):
        """(p, te, raw ta) for one product, cached for the pass. Raw, so the
        stray exemption can be decided (and recorded) per order.

        [v2.14.1] Counts of an exemptable shape live only as long as the stray
        verdict they are combined with (STRAY_TTL_S), then they are read again:
        pass-old counts next to a fresh verdict held an approved product's
        orders for the rest of the pass once the catalog sentinel set its stray
        record aside (no record left, but the cached count still had one). One
        lane reads a product at a time, so a product is re-read at most once
        per TTL. A failed re-read keeps the old counts for another TTL; a
        failed first read raises, as before. older_than also re-reads counts
        read before that moment (the verdict they are about to be combined
        with), so counts and verdict never come from different clocks."""
        with self._gate_lock:
            hit = self._gate_cache.get(pid)
        if hit is not None and self._counts_fresh(hit) and hit[3] >= older_than:
            return hit[:3]
        with self._gate_lock:
            lock = self.__dict__.setdefault("_pid_locks", {}).setdefault(pid, threading.Lock())
        with lock:
            with self._gate_lock:
                hit = self._gate_cache.get(pid)
            if hit is not None and self._counts_fresh(hit) and hit[3] >= older_than:
                return hit[:3]                 # another lane read it meanwhile
            try:
                res = (self.hs.gate_search_product_approved(pid),    # [M211]
                       self.hs.gate_search_template(pid, True),      # [M213]
                       self.hs.gate_search_template(pid, False))     # [M214]
            except Exception as e:
                if hit is None:
                    raise
                log.warning("gate re-read of %s failed (%s): keeping its counts", pid, e)
                res = hit[:3]
            with self._gate_lock:
                self._gate_cache[pid] = res + (time.time(),)
        return res

    def _counts_fresh(self, hit):
        """[v2.14.1] Cached (p, te, ta, read_at) still usable: any shape the
        stray exemption cannot touch lasts the pass, an exemptable one lasts
        STRAY_TTL_S."""
        return not self._exemptable(*hit[:3]) or time.time() - hit[3] < self.STRAY_TTL_S

    @staticmethod
    def _fails(p, te, ta):
        """The [M215] hold rule on one product's counts."""
        return (p == 0 and te == 0 and ta == 0) or (te == 0 and ta > 0)

    @staticmethod
    def _why(p, ta):
        if ta > 0 and p:
            # approved product + a bundle record nothing is active in:
            # either a stray made by a listing edit, or a build in progress
            return ("approved product blocked by a bundle record with nothing active: if "
                    "nobody is building this bundle it is a stray from a Salla edit (bundle "
                    "tag/category) -- remove the tag in Salla and set the record aside with "
                    "tools/fix_catalog_blockers.py; if it IS being built, map components and Push")
        if ta > 0:
            return ("activate bundle template (exists but draft/inactive "
                    "or 0 active components)")
        return ("product missing or unapproved -- approve as "
                "standalone or map as bundle")

    def _deleted_why(self, sku):
        return ("product deleted in Salla -- create+approve a legacy record with hs_sku="
                + (self._legacy_sku(sku) if sku else "?"))

    def note_blocked(self, order, unverified):
        """Tally blockers so the matrix names one fix per row.

        Keyed on the product id when there is one, and on the SKU when there is
        not. A deleted Salla product has NO id -- every one of them arrives here
        with pid "" -- so keying on pid alone collapsed every distinct legacy
        SKU into a single bucket that reported the whole count under whichever
        item name happened to land first. That produced a matrix claiming ~631
        orders were held by one product when they were held by dozens, which is
        exactly the number a catalog owner would have been asked to approve.
        """
        with self._blocked_lock:
            for u in unverified:
                key = block_key(u)
                b = self._blocked.setdefault(key, {
                    "name": u["name"], "why": u["why"], "count": 0,
                    "sku": u.get("sku", ""), "pid": u["pid"], "samples": []})
                b["count"] += 1
                if len(b["samples"]) < 5:
                    b["samples"].append(str(order.get("id")))

    def _skip_known_blocked(self, rows):
        """[v2.14] Drop rows that can only be blocked, without fetching them.

        [v2.14.1] Keyed on the ids drain_one recorded in each blocked row's
        note, never on names: two Salla listings can share a name, and a name
        match kept an approved listing's orders skipped (and filed under the
        old listing's id) for as long as the old one stayed blocked. Each
        recorded id (a SKU for a product deleted in Salla) is re-checked now
        with the gate's own rule (HubSpot only, no relay call, so no Make
        credits), and the counts seed the gate cache. A row is skipped only
        when it is Queued, was tried before and EVERY id it recorded still
        fails; a row with no record, or with an id that passes or could not be
        checked, is fetched normally. Skipped rows are carried into this
        pass's matrix under their own ids, so the matrix still counts them.

        [v2.14.1] Also fetched: a row last tried SKIP_MAX_AGE_S ago or more,
        and a row whose recorded id still fails but whose recorded SKU now
        passes. Both cover a listing deleted in Salla after the row was
        recorded: its orders resolve by SKU from then on, and the old id
        would have kept the row skipped (and in the matrix under a product
        nobody can approve) for good."""
        now = now_str()
        marked = []
        for r in rows:
            keys = recorded_keys(r.get("notes"))
            ok = (keys and r.get("status") == "queued" and r.get("attempts", 0) > 0
                  and tried_within(r.get("notes"), SKIP_MAX_AGE_S, now))
            marked.append((r, keys if ok else None))
        wanted = sorted(frozenset().union(*(k for _, k in marked if k)))
        if not wanted:
            return rows
        still = {}
        for key in wanted:
            try:
                why = self._still_blocked(key)
            except Exception as e:
                log.warning("pre-filter re-check of %s failed (%s): fetching its rows", key, e)
                continue
            if why:
                still[key] = why
        if not still:
            return rows
        prev = self._prev_matrix()
        keep, skipped = [], 0
        for r, keys in marked:
            if not keys or not all(k in still for k in keys):
                keep.append(r)
                continue
            skipped += 1
            blocks = {}                  # [v2.14.1] one tally per blocker, SKU or not
            for k in sorted(keys):
                bk, sku = split_key(k)
                if not (blocks.get(bk) or ("", ""))[1]:
                    blocks[bk] = (still[k], sku)
            names = note_names(r.get("notes")) if len(blocks) == 1 else ""
            with self._blocked_lock:
                for k, (why, sku) in blocks.items():
                    m = prev.get(k) or {}
                    by_sku = k.startswith("sku:")
                    rec = self._blocked.setdefault(k, {
                        "name": m.get("item_name") or names, "why": why, "count": 0,
                        "sku": sku if by_sku else (m.get("sku") or sku),
                        "pid": "" if by_sku else k, "samples": []})
                    rec["count"] += 1
                    if len(rec["samples"]) < 5:
                        rec["samples"].append(str(r["order_id"]))
        if skipped:
            self._bump("held", skipped)
            log.info("PRE-FILTER skipped %d row(s) still blocked by %s (no relay fetch)",
                     skipped, ", ".join(sorted(still)))
        return keep

    def _still_blocked(self, key):
        """The gate's suggested fix when this blocker key still fails the
        gate now, '' when it passes. Same rule and same cache as gate_cached,
        never alerting. Raises when a read fails or the stray verdict is
        unknown (it might pass any minute): the caller fetches those rows.
        [v2.14.1] An id recorded with its SKU passes too when that SKU now
        passes the SKU gate (the listing was deleted in Salla)."""
        if key.startswith("sku:"):
            sku = "" if key == "sku:-" else key[4:]
            return "" if self._sku_ok(sku) else self._deleted_why(sku)
        pid, sku = split_key(key)
        p, te, ta = self._pid_counts(pid)
        if self._exemptable(p, te, ta):
            verdict = self._stray_cached(pid)
            if verdict is None:
                raise LookupError("stray verdict unknown")
            if verdict:
                ta = 0
            else:                    # [v2.14.1] counts no older than the verdict, as in gate_cached
                p, te, ta = self._pid_counts(pid, older_than=self._verdict_at(pid))
                if self._exemptable(p, te, ta) and self._stray_cached(pid):
                    ta = 0
        why = self._why(p, ta) if self._fails(p, te, ta) else ""
        return "" if why and sku and self._sku_ok(sku) else why

    def _open_skus(self, unverified):
        """[v2.14.1] The SKUs of blocked Salla products worth recording next to
        their ids: the ones the SKU gate does not pass now (pass-cached, and
        only when the pre-filter is on). When one passes later, the listing was
        most likely deleted in Salla and a legacy record approved, so the row
        is fetched. A SKU that passes already (an approved relisting can carry
        it) could never signal that, and would only get the row fetched every
        pass. A failed check records the SKU: the worst case is one fetch."""
        if not getattr(self.cfg, "drain_skip_known_blocked", False):
            return set()
        out = set()
        for u in unverified:
            sku = str(u.get("sku") or "").strip()
            if not u["pid"] or not sku or sku in out or _UNWRITABLE.search(sku):
                continue
            try:
                if self._sku_ok(sku):
                    continue
            except Exception as e:
                log.warning("SKU check of %s failed (%s): recording it", sku, e)
            out.add(sku)
        return out

    def _prev_matrix(self):
        """Last pass's blocker matrix by blocker key, for the names of rows
        skipped (so never fetched) this pass. Empty when missing or unreadable."""
        out = {}
        try:
            with open(MATRIX_FILE, newline="", encoding="utf-8") as f:
                for m in csv.DictReader(f):
                    if m.get("sku") is None:
                        continue      # the --scan matrix is by name only, no ids
                    out[block_key({"pid": (m.get("salla_product_id") or "").strip(),
                                   "sku": (m.get("sku") or "").strip()})] = m
        except FileNotFoundError:
            pass
        except (OSError, csv.Error) as e:
            log.warning("blocker matrix unreadable, skipped rows carry no name: %s", e)
        return out

    # -- pre-existing resolution (ledger + dedup + LI verify) ------------------

    def resolve_preexisting(self, oid, expected_items):
        """(queue_status, result_note) when the order already exists and is
        complete/authoritative; None to proceed to gate+create."""
        hs_id = self.created_ledger.get(oid)
        if hs_id:
            self._bump("skipped_existing")
            return ("Processed", f"already synced by engine -- HS {hs_id}")
        try:
            hs_id, zid = self.hs.orders_by_salla_id(oid)
        except Exception as e:
            log.error("pre-existing search failed for %s: %s", oid, e)
            return None  # create path's duplicate-400 guardrail covers it
        if zid and not hs_id:
            # [v2.12] held by a Zid-import order: move it off the number and
            # create; otherwise park with the full attempt budget spent so
            # hourly passes stop re-claiming the row
            if self.zid_auto_rekey(oid, zid):
                return None
            return ("Error", self.zid_collision(oid, zid),
                    self.cfg.live_max_attempts)
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
        try:
            return self._drain_one(row, order)
        finally:
            self.end_gate(order)   # [v2.14.1] the gate's decision ends with the order

    def _drain_one(self, row, order):
        oid = str(order.get("id"))
        pre = self.resolve_preexisting(oid, len(order.get("items", []) or []))
        if pre:
            return (pre[0], pre[1], pre[2] if len(pre) > 2 else 1)
        unverified = self.gate_cached(order)
        if unverified:
            self.note_blocked(order, unverified)
            self._bump("held")
            names = ", ".join(u["name"] for u in unverified)
            log.info("BLOCKED order %s (%d item(s)): %s", oid,
                     len(unverified), names[:160])
            # [v2.14.1] the ids go in too: the next pass skips this row by id
            return ("Queued", f"blocked: {names[:180]} @ {now_str()}"
                              f"{pids_note(unverified, self._open_skus(unverified))}", 1)
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
            # [v2.12] mirrored like the engine's arrival, so a -1 row here is
            # replayable by tools/audit_replay.py
            self.mirror.audit_event("arrived_append", audit_row, add, order_id=oid)
            self._audit_rows[oid] = audit_row
        self.route_create(order, audit_row)   # sets self._outcome[oid]
        outcome, hs_ref = self._outcome.pop(oid, ("error", ""))
        if outcome == "created":
            return ("Processed", f"created HS {hs_ref}", 1)
        if outcome == "held" and str(hs_ref).startswith("zid collision"):
            return ("Error", hs_ref, self.cfg.live_max_attempts)   # [v2.12]
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
            w.writerow(["salla_product_id", "sku", "item_name", "blocked_orders",
                        "suggested_action", "sample_order_ids", "checked_at",
                        "note"])
            for key, b in rows:
                w.writerow([b.get("pid", ""), b.get("sku", ""), b["name"],
                            b["count"], b["why"], " ".join(b["samples"]),
                            now_str(), extra_note])
        log.info("BLOCKER MATRIX -> %s (%d blocker(s))", MATRIX_FILE, len(rows))
        for key, b in rows[:20]:
            log.info("BLOCKER %-12s %-20s x%-5d %s", b.get("pid") or "-",
                     b.get("sku") or "-", b["count"], b["name"][:52])

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

        if getattr(self.cfg, "drain_skip_known_blocked", False):
            # [v2.14] During the Oct 2026 hold every pass paid a relay fetch for
            # the same 120 blocked rows (~4k Make credits a day) and never
            # reached the other 1,600. Skip rows that can only be blocked
            # without fetching them. [v2.14.1] The re-check searches wait for
            # live like every other drain search, and the rows waiting longest
            # since their last try go first (never-tried first): lifetime
            # attempts parked the rows old passes retried ~170 times behind
            # every row that can never be skipped, for days.
            self._yield_to_live()
            to_process = self._skip_known_blocked(to_process)
            # '' (never tried) sorts first; stable, so ties keep sheet order
            to_process.sort(key=lambda r: last_tried(r.get("notes")))

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
