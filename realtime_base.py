#!/usr/bin/env python3
"""Shared skeleton for the v2.7 realtime consumers (status relay, customer
sync) -- the queue-tab poll loop, single-instance guards, activity signal and
STOP semantics, factored once so both sub-products stay small and identical
in their operational behavior.

Design inherited from live.py, deliberately:
  * Make is capture-only; a webhook event is durable the moment its row lands
    in the queue tab. The consumer owns retries, idempotency and alerting.
  * Two-layer single-instance guard: flock (same machine) + a per-tab J1
    heartbeat cell (any machine). Two consumers of the SAME tab cannot
    coexist; consumers of different tabs are independent by construction.
  * Realtime precedence: each consumer publishes mirror/live_active_<name>.json
    every poll. The backfill/drain glob live_active*.json and yield the shared
    HubSpot budget whenever ANY realtime stream has work (backfill.py v2.7).
    Consumers never yield to the backfill and never wait for each other.
  * STOP.<name> pauses the consumer without touching the other services.

Subclasses implement handle_row(row) -> (state, note) and may override
claimable() filtering. Everything else is here.
"""

import json
import logging
import os
import socket
import sys
import time
from datetime import datetime
from pathlib import Path

log = logging.getLogger("backfill")

HEARTBEAT_STALE_S = 300


class TabLock:
    """flock on <name>.lock: one consumer per stream per machine."""

    def __init__(self, name):
        self.path = f"{name}.lock"
        self._fh = None

    def acquire(self):
        self._fh = open(self.path, "w")
        try:
            import fcntl
            fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except ImportError:
            log.warning("fcntl unavailable: relying on sheet heartbeat only")
        except OSError:
            sys.exit(f"Another consumer holds {self.path} on this machine. "
                     f"Refusing to start a second instance.")
        self._fh.write(f"{os.getpid()}\n")
        self._fh.flush()


class RealtimeConsumer:
    """Poll -> claim -> handle -> mark loop over one queue tab."""

    #: subclass identity: short name used for lock/STOP/signal/state files
    name = "base"
    #: terminal states a row can rest in (never re-claimed)
    terminal = ("done", "gone", "superseded", "error-final")

    def __init__(self, cfg, hs, gio, tab, live=True):
        self.cfg, self.hs, self.gio = cfg, hs, gio
        self.tab = tab
        self.live = live                      # False = dry-run, no writes
        self.qsid = cfg.queue_spreadsheet_id
        self.instance_id = f"{socket.gethostname()}-{os.getpid()}"
        self.stop_file = Path(f"STOP.{self.name}")
        self.state_file = Path(f"mirror/{self.name}_state.json")
        self.signal_file = Path(f"mirror/live_active_{self.name}.json")
        self._start_row = 2
        self._active_until = 0.0
        self._last_hb = 0.0
        self._load_state()

    # -- state -----------------------------------------------------------------

    def _load_state(self):
        try:
            d = json.loads(self.state_file.read_text())
            self._start_row = max(2, int(d.get("start_row", 2)))
        except (OSError, json.JSONDecodeError, ValueError):
            pass

    def _save_state(self):
        try:
            self.state_file.parent.mkdir(exist_ok=True)
            self.state_file.write_text(json.dumps(
                {"start_row": self._start_row,
                 "ts": datetime.now().isoformat(timespec="seconds")}))
        except OSError as e:
            log.warning("%s state save failed: %s", self.name, e)

    def _should_stop(self):
        return self.stop_file.exists()

    # -- single-instance heartbeat (per tab) -------------------------------------

    def _heartbeat_ok(self):
        now = time.monotonic()
        if now - self._last_hb < 60:
            return True
        raw = self.gio.queue_read_heartbeat(self.qsid, tab=self.tab)
        owner, _, epoch = raw.partition("|")
        try:
            age = time.time() - float(epoch)
        except ValueError:
            age = 1e9
        if owner and owner != self.instance_id and age < HEARTBEAT_STALE_S:
            log.error("FOREIGN %s INSTANCE %s heartbeat %.0fs old -- refusing "
                      "to claim; will re-check", self.name, owner, age)
            return False
        self.gio.queue_write_heartbeat(self.qsid, self.instance_id,
                                       tab=self.tab)
        self._last_hb = now
        return True

    # -- signal ------------------------------------------------------------------

    def _publish_signal(self, depth):
        nowep = time.time()
        if depth:
            self._active_until = nowep + 30
        try:
            self.signal_file.write_text(json.dumps(
                {"active": nowep < self._active_until,
                 "depth": depth, "ts": int(nowep), "stream": self.name}))
        except OSError as e:
            log.debug("%s signal publish failed: %s", self.name, e)

    # -- claim -------------------------------------------------------------------

    def claimable(self, rows, retry_errors):
        """Rows this cycle may process. Mirrors the live engine's semantics:
        queued always; deferred when its not-before time (stored in note as
        'nb=<epoch>') has passed; error only on the slow retry cadence."""
        out = []
        for r in rows:
            st = r["status"]
            if st in self.terminal or st == "held":
                continue
            if st == "queued" or st == "":
                out.append(r)
            elif st == "deferred":
                nb = 0.0
                for tok in str(r.get("note", "")).split():
                    if tok.startswith("nb="):
                        try:
                            nb = float(tok[3:])
                        except ValueError:
                            nb = 0.0
                if time.time() >= nb:
                    out.append(r)
            elif st == "error" and retry_errors:
                out.append(r)
        return out

    # -- subclass hook -----------------------------------------------------------

    def handle_row(self, row):
        """Process one row. Returns (state, note). Must be idempotent."""
        raise NotImplementedError

    # -- main loop ---------------------------------------------------------------

    def run(self, once=False):
        log.info("%s START instance=%s tab=%r poll=%ss live=%s",
                 self.name.upper(), self.instance_id, self.tab,
                 self.cfg.live_poll_s, self.live)
        poll_n = 0
        while not self._should_stop():
            try:
                if not self._heartbeat_ok():
                    time.sleep(self.cfg.live_poll_s)
                    continue
                rows = self.gio.queue_read(self.qsid,
                                           start_row=self._start_row,
                                           tab=self.tab)
                for r in rows:
                    if r["status"] in self.terminal:
                        self._start_row = r["row"] + 1
                    else:
                        break
                self._save_state()
                poll_n += 1
                retry_every = max(1, int(180 / max(self.cfg.live_poll_s, 1)))
                retry_errors = retry_every <= 1 or poll_n % retry_every == 1
                claim = self.claimable(rows, retry_errors)
                self._publish_signal(len(claim))
                if claim:
                    log.info("%s QUEUE depth=%d", self.name.upper(), len(claim))
                # duplicate ids inside one cycle: first row wins, twins inherit
                seen, primaries, twins = set(), [], []
                for r in claim:
                    (twins if r["order_id"] in seen else primaries).append(r)
                    seen.add(r["order_id"])
                outcome = {}
                for r in primaries:
                    if self._should_stop():
                        break
                    try:
                        # Dry-run still runs the real decision: the HubSpot
                        # client skips writes but performs every search, and
                        # the subclasses skip their ledger appends. That makes
                        # a dry pass a genuine rehearsal against live data,
                        # which is what the parallel-validation window needs --
                        # a run that only logged "would handle row" would prove
                        # nothing about the logic.
                        state, note = self.handle_row(r)
                    except Exception as e:
                        log.exception("%s row %s failed: %s",
                                      self.name, r["row"], e)
                        state, note = "error", f"{type(e).__name__}: {e}"[:180]
                    outcome[r["order_id"]] = (state, note)
                    if not self.live:
                        log.info("DRY RUN %s row %s id %s (%s) -> %s | %s",
                                 self.name, r["row"], r["order_id"],
                                 r["event"][:40], state, note[:90])
                        continue
                    self.gio.queue_mark(self.qsid, r["row"], r["order_id"],
                                        state, r["attempts"] + 1, note,
                                        tab=self.tab)
                for r in twins:
                    st, note = outcome.get(r["order_id"], (None, None))
                    if st and self.live:
                        self.gio.queue_mark(self.qsid, r["row"], r["order_id"],
                                            st, r["attempts"], f"twin: {note}",
                                            tab=self.tab)
                if once:
                    break
            except Exception as e:
                log.exception("%s loop error (auto-retried): %s", self.name, e)
            time.sleep(self.cfg.live_poll_s)
        log.info("%s STOP file honored -- exiting cleanly", self.name.upper())
