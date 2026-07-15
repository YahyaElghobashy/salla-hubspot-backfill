#!/usr/bin/env python3
"""Live order sync (v1.6): the 24/7 companion to backfill.py.

Replaces the Make "Salla | Order Created" scenario. A 2-op Make intake
scenario appends every order.created event as a row in the Live Queue
spreadsheet; this service claims queued rows, fetches the full orders
through the existing relay (shape parity with the tested pipeline), and
runs them through the exact same worker-lane engine the backfill uses —
dedup, catalog gate, contact guardrails, bundles, audit trail, adaptive
pacing, everything.

Durability model (nothing is ever lost, nothing is created twice):
  - intake down / Make credits exhausted -> Make's webhook queue buffers
  - this service down                    -> rows accumulate in the sheet
  - HubSpot search-index lag             -> mirror/created.csv ledger +
    duplicate-400 guardrail make reprocessing idempotent
  - webhook missed entirely              -> hourly relay sweep appends the
    missing ids to the queue (sweep is a producer, never a second consumer)

Row lifecycle: queued -> done | held | gone (Salla 404 terminal) | error
(attempts+1, retried each poll up to live_max_attempts, then loud ledger).
A crash mid-order leaves the row queued; reprocessing is idempotent.

Own namespace so it coexists with a running backfill: live.log, STOP.live,
live.lock, live_state.json, config.live.json.

    ./venv/bin/python3 live.py --init-queue          # one-time: create the queue spreadsheet
    ./venv/bin/python3 live.py --once                # dry run: one poll cycle, plan only
    ./venv/bin/python3 live.py --live                # the 24/7 service (type RUN)
    ./venv/bin/python3 run.py --mode live            # supervised (restart forever)
"""

import argparse
import json
import logging
import os
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timedelta
from pathlib import Path

import backfill
from backfill import (Config, Cursor, Engine, GoogleIO, HubSpot, LocalMirror,
                      RelayClient, dig, now_str)

log = logging.getLogger("backfill")  # share the engine's logger/format

STOP_FILE = Path("STOP.live")
LOCK_FILE = Path("live.lock")
STATE_FILE = Path("live_state.json")
# A poll cycle that drains a large backlog can exceed a couple of minutes, so
# the incumbent's per-cycle heartbeat must be considered fresh well past that.
# Only after ~5 min of total silence does a would-be replacement take over.
HEARTBEAT_STALE_S = 300


class SingleInstanceLock:
    """flock on live.lock: two live services on one machine cannot coexist."""

    def __init__(self):
        self._fh = None

    def acquire(self):
        self._fh = open(LOCK_FILE, "w")
        try:
            import fcntl
            fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except ImportError:
            log.warning("fcntl unavailable (Windows): relying on the sheet "
                        "heartbeat only for single-instance protection")
        except OSError:
            sys.exit("Another live sync instance holds live.lock on this "
                     "machine. Refusing to start a second consumer.")
        self._fh.write(f"{os.getpid()}\n")
        self._fh.flush()


class LiveEngine(Engine):
    """Queue-driven variant of the backfill Engine. Reuses process_order and
    every guardrail; replaces the cursor/page loop with poll->claim->lanes->
    mark. STOP semantics: finish in-flight orders, leave the rest queued."""

    def __init__(self, cfg, relay, hs, gio, mirror, live, workers=None):
        # Engine expects a cursor; live mode has none. A tiny stub keeps the
        # base class untouched.
        class _NoCursor:
            data = {"status": "live"}
            status = "live"
        super().__init__(cfg, _NoCursor(), relay, hs, gio, mirror,
                         live=live, workers=workers)
        self.qsid = cfg.queue_spreadsheet_id
        self.instance_id = f"{socket.gethostname()}-{os.getpid()}"
        self.processed_today = 0
        self._today = datetime.now().date()
        self._active_until = 0.0  # v1.7: live-priority signal cooldown
        # sweep: 0 or >=1440 minutes disables it; otherwise the first sweep
        # runs shortly after start (catches any downtime gap immediately)
        self._sweep_enabled = 0 < cfg.live_sweep_minutes < 1440
        self._next_sweep = time.monotonic() + 60
        self._poll_n = 0
        self._last_trim_day = None
        self._start_row = 2
        self._load_state()

    # -- state pointer (optimization only; restart rescans) -------------------

    def _load_state(self):
        try:
            self._start_row = max(2, int(json.loads(
                STATE_FILE.read_text()).get("start_row", 2)))
        except Exception:
            self._start_row = 2

    def _save_state(self):
        STATE_FILE.write_text(json.dumps({"start_row": self._start_row}))

    # -- heartbeat -------------------------------------------------------------

    @staticmethod
    def _hb_age(cell):
        """(owner, age_seconds) from an 'owner|epoch' heartbeat cell, or None."""
        if not cell or "|" not in cell:
            return None
        owner, ts = cell.rsplit("|", 1)
        try:
            return owner, time.time() - int(float(ts))
        except ValueError:
            return None

    def _heartbeat_ok(self):
        """Single-consumer guard, check-BEFORE-write so the incumbent always
        wins: read J1 first; if a DIFFERENT instance's heartbeat is fresh,
        refuse WITHOUT stamping (no mutual starvation). Otherwise claim
        ownership, write, and re-read to catch a simultaneous-start race
        (TOCTOU) -- if the cell is no longer ours, yield this cycle."""
        try:
            prev = self.gio.queue_read_heartbeat(self.qsid)
        except Exception as e:
            log.warning("heartbeat read failed (%s); claiming anyway -- the "
                        "flock already guards this machine", e)
            return True
        info = self._hb_age(prev)
        if info and info[0] != self.instance_id and info[1] < HEARTBEAT_STALE_S:
            # v1.7: a stale heartbeat from THIS machine is a dead predecessor --
            # the flock we hold proves no other live.py runs here -- so take
            # over immediately instead of waiting out HEARTBEAT_STALE_S. Only a
            # foreign HOSTNAME (a truly separate machine) triggers the refusal.
            my_host = self.instance_id.rsplit("-", 1)[0]
            their_host = info[0].rsplit("-", 1)[0]
            if their_host != my_host:
                log.error("FOREIGN LIVE INSTANCE %s heartbeat %ds ago -- refusing "
                          "to claim (two consumers = duplicate risk). Stop the "
                          "other instance first.", info[0], int(info[1]))
                return False  # do NOT write: let the incumbent keep ownership
            log.info("stale same-machine heartbeat from %s (%ds, dead predecessor) "
                     "-- taking over", info[0], int(info[1]))
        try:
            self.gio.queue_write_heartbeat(self.qsid, self.instance_id)
            back = self._hb_age(self.gio.queue_read_heartbeat(self.qsid))
        except Exception as e:
            log.warning("heartbeat write failed (%s); claiming anyway", e)
            return True
        if back and back[0] != self.instance_id and back[1] < HEARTBEAT_STALE_S:
            log.error("Simultaneous-start race with %s -- yielding this cycle",
                      back[0])
            return False
        return True

    # -- claim + process --------------------------------------------------------

    def _claimable(self, rows, retry_errors):
        out = []
        for r in rows:
            if not r["order_id"]:
                continue
            if r["status"] == "queued":
                out.append(r)
            elif (retry_errors and r["status"] == "error"
                  and r["attempts"] < self.cfg.live_max_attempts):
                out.append(r)
        return out

    def _resolve_preexisting(self, row):
        """Decide a queued row against orders that may already exist.
        Returns (status, note) to mark the row, or None to send it through
        the normal fetch+create path.

        The created-ledger records only FULLY-synced orders (order + all its
        line items), so a ledger hit is authoritative. An order found by
        search but NOT in our ledger is either a crash-partial or one created
        outside this engine (old Make / cutover): it is verified by comparing
        its HubSpot line-item count against the source order's item count
        before being trusted, so a partial is repaired rather than silently
        accepted, and a complete pre-existing order is skipped."""
        oid = row["order_id"]
        hs_id = self.created_ledger.get(oid)
        if hs_id:
            self._bump("skipped_existing")
            log.info("skip existing %s (live: HS %s, synced by this engine)",
                     oid, hs_id)
            return ("done", f"HS {hs_id} (synced by this engine)")
        try:
            hs_id = self.hs.find_order_by_salla_id(oid)
        except Exception as e:
            log.error("pre-existing search failed for %s: %s", oid, e)
            return None  # go to create path; the duplicate-400 guardrail covers dupes
        if not hs_id:
            return None
        li = self.hs.order_line_item_count(hs_id)
        if li < 0:
            return ("error", f"HS {hs_id}: line-item verify unavailable "
                             "(HubSpot error) -- will retry")
        # compare against the source item count (bundles expand one item into
        # several line items, so a complete order has li >= item count)
        expected = 1
        try:
            fetched = self.relay.fetch_orders([oid]).get(oid)
            if fetched is not None:
                expected = len(fetched.get("items", []) or []) or 1
        except Exception as e:
            log.warning("verify fetch failed for %s (%s); using >=1 LI test",
                        oid, e)
        if li >= expected:
            self.created_ledger.add(oid, hs_id)  # verified complete
            self._bump("skipped_existing")
            log.info("skip existing %s (live: HS %s, verified %d LIs >= %d items)",
                     oid, hs_id, li, expected)
            return ("done", f"HS {hs_id} (verified {li} line items)")
        self.mirror.error(oid, "partial",
                          f"HS {hs_id} has {li} line items but source has "
                          f"{expected} -- repair via tools/recover_missing.py")
        self._bump("errors")
        log.error("PARTIAL order %s: HS %s %d line items < %d source items; "
                  "flagged for repair", oid, hs_id, li, expected)
        return ("error", f"partial HS {hs_id}: {li}/{expected} line items")

    def _process_batch(self, claimed, orders, max_orders, done_counter):
        """Run claimed rows through worker lanes (bounded window, same shape
        as the backfill page loop) and mark each row from its outcome."""
        feed = iter(claimed)
        pending = {}
        with ThreadPoolExecutor(max_workers=self.workers,
                                thread_name_prefix="lane") as pool:
            while True:
                while (len(pending) < self.workers and not self._should_stop()
                       and (max_orders is None
                            or done_counter[0] + len(pending) < max_orders)):
                    row = next(feed, None)
                    if row is None:
                        break
                    order = orders.get(row["order_id"])
                    if not order:
                        att = row["attempts"] + 1
                        status = ("gone" if att >= self.cfg.live_max_attempts
                                  else "error")
                        note = (f"relay returned nothing after {att} attempts"
                                if status == "gone" else "relay fetch miss")
                        if status == "gone":
                            log.error("QUEUE order %s marked GONE: %s",
                                      row["order_id"], note)
                        self.gio.queue_mark(self.qsid, row["row"],
                                            row["order_id"], status, att, note)
                        continue
                    pending[pool.submit(self._process_one, row["order_id"],
                                        order)] = row
                    self._in_flight = len(pending)
                if not pending:
                    break
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for fut in done:
                    row = pending.pop(fut)
                    oid = row["order_id"]
                    try:
                        fut.result()
                    except Exception as e:  # belt and braces
                        self.mirror.error(oid, "process", e)
                        self._bump("errors")
                        log.exception("Order %s failed: %s", oid, e)
                    outcome, ref = self._outcome.pop(oid, (None, ""))
                    if outcome == "created":
                        self.gio.queue_mark(self.qsid, row["row"], oid,
                                            "done", row["attempts"], f"HS {ref}")
                        self.processed_today += 1
                    elif outcome == "held":
                        self.gio.queue_mark(self.qsid, row["row"], oid,
                                            "held", row["attempts"],
                                            "catalog gate -- in review queue")
                        self.processed_today += 1
                    else:
                        att = row["attempts"] + 1
                        self.gio.queue_mark(self.qsid, row["row"], oid,
                                            "error", att,
                                            "processing failed -- see live.log "
                                            "and mirror/errors.csv")
                    done_counter[0] += 1
                self._in_flight = len(pending)
                self._rates_report()
        self._in_flight = 0

    # -- sweep (producer) -------------------------------------------------------

    def _sweep(self):
        """Hourly reconciliation: list the recent window via the relay and
        append any order id nobody has seen to the queue. Never processes.
        known_ids is built from a FULL queue read (not the pointer-limited
        view) plus the ledger, so already-settled rows -- including held and
        pre-existing orders not in the ledger -- are not re-enqueued."""
        known_ids = {r["order_id"] for r in self.gio.queue_read(self.qsid, 2)
                     if r["order_id"]}
        start = datetime.now() - timedelta(hours=self.cfg.live_sweep_window_h)
        page, appended = 1, []
        while True:
            path = (f"orders?from_date={start.strftime('%Y-%m-%dT%H:%M:%S')}"
                    f"&per_page={self.cfg.per_page}&format=light&page={page}")
            body = self.relay.get_path(path)
            ids = [str(o.get("id")) for o in body.get("data", []) or []]
            for oid in ids:
                if (oid and oid not in known_ids
                        and not self.created_ledger.get(oid)):
                    appended.append([now_str(), oid, "", "order.created",
                                     "queued", 0, "sweep", ""])
                    known_ids.add(oid)
            total_pages = int(dig(body, "pagination.totalPages", 0) or 0)
            if page >= total_pages:
                break
            page += 1
        if appended:
            self.gio.queue_append_rows(self.qsid, appended)
            log.warning("SWEEP enqueued %d order(s) the webhook missed: %s",
                        len(appended), [r[1] for r in appended][:10])
        else:
            log.info("SWEEP clean: webhook intake caught everything in the "
                     "last %dh", self.cfg.live_sweep_window_h)

    # -- trim -------------------------------------------------------------------

    def _maybe_trim(self, rows):
        today = datetime.now().date()
        if (datetime.now().hour != self.cfg.live_trim_hour
                or self._last_trim_day == today):
            return
        if any(r["status"] == "queued" for r in rows):
            return  # only trim a fully drained queue
        cutoff = (datetime.now() - timedelta(days=self.cfg.live_trim_days))
        def keep(r):
            try:
                return datetime.strptime(str(r["received_at"])[:19],
                                         "%Y-%m-%d %H:%M:%S") >= cutoff
            except ValueError:
                return True  # unparseable -> keep, never guess-delete
        n = self.gio.queue_trim(self.qsid, keep)
        self._last_trim_day = today
        self._start_row = 2
        self._save_state()
        if n:
            log.info("TRIM removed %d terminal row(s) older than %d days",
                     n, self.cfg.live_trim_days)

    # -- main loop ----------------------------------------------------------------

    def run_live(self, max_orders=None, once=False):
        log.info("LIVE SYNC start instance=%s queue=%s poll=%ss workers=%d live=%s",
                 self.instance_id, self.qsid, self.cfg.live_poll_s,
                 self.workers, self.live)
        done_counter = [0]
        while not self._should_stop():
            if datetime.now().date() != self._today:
                self._today = datetime.now().date()
                self.processed_today = 0
            try:
                if not self._heartbeat_ok():
                    time.sleep(self.cfg.live_poll_s)
                    continue
                rows = self.gio.queue_read(self.qsid, start_row=self._start_row)
                # pointer: everything before the first non-terminal row is settled
                for r in rows:
                    if r["status"] in ("done", "gone", "held"):
                        self._start_row = r["row"] + 1
                    else:
                        break
                self._save_state()
                self._poll_n += 1
                # error rows retry on a slower cadence (~3 min) so a broken
                # order does not burn an attempt every poll
                retry_every = max(1, int(180 / max(self.cfg.live_poll_s, 1)))
                retry_errors = retry_every <= 1 or self._poll_n % retry_every == 1
                claimable = self._claimable(rows, retry_errors=retry_errors)
                oldest = min((r["received_at"] for r in claimable), default="")
                age = ""
                try:
                    age = int((datetime.now() - datetime.strptime(
                        str(oldest)[:19], "%Y-%m-%d %H:%M:%S")).total_seconds())
                except ValueError:
                    age = 0
                log.info("QUEUE depth=%d oldest_age=%ss processed_today=%d "
                         "lanes=%d/%d", len(claimable), age,
                         self.processed_today, self._in_flight, self.workers)
                # v1.7: publish an activity signal so the backfill engine yields
                # the shared HubSpot budget to live. Active while there is work
                # plus a 30s cooldown so a bursty queue doesn't flap the backfill.
                nowep = time.time()
                if claimable or self._in_flight:
                    self._active_until = nowep + 30
                try:
                    (Path(self.mirror.dir) / "live_active.json").write_text(json.dumps(
                        {"active": nowep < self._active_until,
                         "depth": len(claimable), "ts": int(nowep)}))
                except Exception as e:
                    log.debug("live_active publish failed: %s", e)
                if claimable:
                    if not self.live:
                        for r in claimable[:20]:
                            log.info("DRY RUN plan: would process queue row %s "
                                     "order %s (attempts=%d source=%s)",
                                     r["row"], r["order_id"], r["attempts"],
                                     r["source"])
                        log.info("DRY RUN: %d claimable row(s), nothing written",
                                 len(claimable))
                        break  # dry run inspects one poll cycle
                    # collapse duplicate ids within the cycle (double webhook
                    # delivery / replay + sweep overlap): exactly ONE row per
                    # order id is processed; the twins inherit its outcome.
                    primaries, twins, seen_ids = [], [], set()
                    for r in claimable:
                        if r["order_id"] in seen_ids:
                            twins.append(r)
                        else:
                            seen_ids.add(r["order_id"])
                            primaries.append(r)
                    # pre-existing resolution first (ledger + LI verify);
                    # marks are batched -- catch-up backlogs (sweeps after
                    # downtime) can be hundreds of skip rows
                    to_fetch, marks = [], []
                    for r in primaries:
                        res = self._resolve_preexisting(r)
                        if res:
                            marks.append((r["row"], r["order_id"],
                                          res[0], r["attempts"], res[1]))
                        else:
                            to_fetch.append(r)
                    self.gio.queue_mark_batch(self.qsid, marks)
                    if to_fetch:
                        orders = self.relay.fetch_orders(
                            [r["order_id"] for r in to_fetch])
                        self._process_batch(to_fetch, orders, max_orders,
                                            done_counter)
                    if twins:
                        tmarks = []
                        for r in twins:
                            hs_id = self.created_ledger.get(r["order_id"])
                            tmarks.append((r["row"], r["order_id"],
                                           "done" if hs_id else "error",
                                           r["attempts"],
                                           f"duplicate of an earlier row"
                                           + (f" -- HS {hs_id}" if hs_id else "")))
                        self.gio.queue_mark_batch(self.qsid, tmarks)
                if max_orders is not None and done_counter[0] >= max_orders:
                    log.info("Reached --max-orders=%s", max_orders)
                    break
                if (self.live and self._sweep_enabled
                        and time.monotonic() >= self._next_sweep):
                    self._next_sweep = time.monotonic() + \
                        self.cfg.live_sweep_minutes * 60
                    try:
                        self._sweep()
                    except Exception as e:
                        log.error("SWEEP failed (will retry next cycle): %s", e)
                self._maybe_trim(rows)
            except Exception as e:
                log.exception("live loop error (backing off 60s): %s", e)
                time.sleep(60)
                continue
            if once:
                log.info("--once: single poll cycle complete")
                break
            time.sleep(self.cfg.live_poll_s)
        s = self.stats
        log.info("=" * 68)
        log.info("LIVE SESSION SUMMARY  live=%s", self.live)
        log.info("created=%d held=%d skipped_existing=%d errors=%d",
                 s.created, s.held, s.skipped_existing, s.errors)
        if s.errors:
            log.error("UNRECOVERED FAILURES: %d -- review %s and the queue "
                      "rows marked error/gone. Nothing was silently dropped.",
                      s.errors, self.mirror.errors)
        log.info("=" * 68)


def main():
    ap = argparse.ArgumentParser(description="Salla live order sync (v1.6)")
    ap.add_argument("--config", default="config.live.json")
    ap.add_argument("--live", action="store_true",
                    help="Perform real writes. Default is a one-cycle dry run.")
    ap.add_argument("--once", action="store_true",
                    help="Run exactly one poll cycle then exit (works with --live)")
    ap.add_argument("--max-orders", type=int, default=None)
    ap.add_argument("--workers", type=int, default=None)
    ap.add_argument("--init-queue", action="store_true",
                    help="Create the Live Queue spreadsheet and exit")
    ap.add_argument("--yes", action="store_true", help="skip the RUN prompt")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    threading.current_thread().name = "main"
    fmt = "%(asctime)s %(levelname)-7s [%(threadName)s] %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=fmt,
                        handlers=[logging.FileHandler("live.log", encoding="utf-8")])
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    console.setFormatter(logging.Formatter(fmt))
    logging.getLogger().addHandler(console)
    logging.getLogger("googleapiclient").setLevel(logging.WARNING)
    socket.setdefaulttimeout(180)

    cfg = Config.load(args.config)
    if hasattr(backfill, "apply_portal_config"):
        backfill.apply_portal_config(cfg)

    token = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    secret = os.environ.get("RELAY_SECRET", "")
    if not token or not secret:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN and RELAY_SECRET first.")

    if args.init_queue:
        gio = GoogleIO(cfg, enabled=True)
        qsid, url = gio.ensure_queue_spreadsheet()
        print(f"Live Queue spreadsheet created:\n  id:  {qsid}\n  url: {url}\n"
              f"Put it in {args.config} as \"queue_spreadsheet_id\" and select "
              f"it in the Make intake scenario's addRow module.")
        return

    if not cfg.queue_spreadsheet_id:
        sys.exit("queue_spreadsheet_id missing in config. Run --init-queue first.")

    if args.live and not args.yes:
        confirm = input("LIVE SYNC will write to HubSpot 24/7. Type RUN: ")
        if confirm.strip() != "RUN":
            sys.exit("Aborted.")

    SingleInstanceLock().acquire()
    backfill.STOP_FILE = STOP_FILE  # engine STOP checks watch STOP.live

    gio = GoogleIO(cfg, enabled=True)   # queue I/O needs Google even in dry run
    mirror = LocalMirror("mirror")
    relay = RelayClient(cfg, secret)
    hs = HubSpot(cfg, token, live=args.live)
    engine = LiveEngine(cfg, relay, hs, gio, mirror, live=args.live,
                        workers=args.workers)
    engine.run_live(max_orders=args.max_orders, once=args.once)


if __name__ == "__main__":
    main()
