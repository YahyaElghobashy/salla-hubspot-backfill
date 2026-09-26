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

import contextlib
import json
import logging
import os
import socket
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

log = logging.getLogger("backfill")

HEARTBEAT_STALE_S = 300
TRIM_LOCK = Path("mirror/trim.lock")
TRIM_LOCK_STALE_S = 2 * 3600
# [v2.12] Linux PID_MAX_LIMIT. A larger number in the lock is not a pid, and
# os.kill rejects it with OverflowError rather than an OSError.
PID_MAX = 4194304


class TrimLockHeld(RuntimeError):
    """[v2.12] trim_lock refused: another live process holds TRIM_LOCK."""


def _lock_pid(value):
    """[v2.12] A pid read from the lock, or None. Only ASCII digits count
    (str.isdigit also accepts Arabic-Indic and superscript digits) and only
    up to PID_MAX, so a garbled lock never reaches os.kill. The length check
    comes first: int() refuses a digit string over 4300 characters."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        pid = value
    else:
        head = str(value if value is not None else "").strip()
        if not (head.isascii() and head.isdigit()) or len(head) > len(str(PID_MAX)):
            return None
        pid = int(head)
    return pid if 0 < pid <= PID_MAX else None


def trim_lock_holder():
    """[v2.12] Who holds TRIM_LOCK: {"pid", "tab", "ts", "mtime"} or None when
    there is no lock file. The file holds one line "<pid> <tab> <ts>" (tab
    names contain spaces, ts does not). A v2.11 JSON lock is read too, so a
    lock left by the previous version is still understood. pid is None when
    the file cannot be parsed (for example read between create and write, or
    bytes that are not UTF-8).

    The file is stat'ed before it is read. When the stat worked and the read
    did not, mtime stays the file's real one, so the two-hour rule for an
    unreadable lock can still expire; only a failed stat reports "now"."""
    try:
        mtime = TRIM_LOCK.stat().st_mtime
    except FileNotFoundError:
        return None
    except OSError:
        return {"pid": None, "tab": "", "ts": "", "mtime": time.time()}
    try:
        text = TRIM_LOCK.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None                                   # released after the stat
    except (OSError, UnicodeDecodeError):
        return {"pid": None, "tab": "", "ts": "", "mtime": mtime}
    pid, tab, ts = None, "", ""
    if text.startswith("{"):
        try:
            d = json.loads(text)
            pid = _lock_pid(d.get("pid"))
            tab, ts = str(d.get("tab", "")), str(d.get("ts", ""))
        except (ValueError, TypeError, AttributeError):
            pass
    else:
        head, _, rest = text.partition(" ")
        pid = _lock_pid(head)
        tab, _, ts = rest.rpartition(" ")
    return {"pid": pid, "tab": tab, "ts": ts, "mtime": mtime}


def _pid_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True            # exists, owned by another user
    except (OSError, OverflowError, ValueError):
        return False           # [v2.12] no process can have this pid
    return True


def _pid_started_at(pid):
    """Epoch start time of `pid` from /proc (Linux), or None elsewhere."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        ticks = int(stat.rsplit(")", 1)[1].split()[19])     # field 22, starttime
        btime = next(int(line.split()[1]) for line in
                     Path("/proc/stat").read_text().splitlines()
                     if line.startswith("btime "))
        return btime + ticks / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError, StopIteration):
        return None


def _holder_live(h):
    """A holder is live while its pid runs and that process is the one that
    wrote the lock. A pid that started after the lock was written is a reused
    pid (crash, reboot), not the holder. With no readable pid, fall back to
    the v2.11 age rule so a half-written lock is not taken over."""
    if not isinstance(h["pid"], int) or h["pid"] <= 0:
        return time.time() - h["mtime"] < TRIM_LOCK_STALE_S
    if not _pid_alive(h["pid"]):
        return False
    started = _pid_started_at(h["pid"])
    return started is None or started <= h["mtime"] + 2      # 2 s: btime rounding


@contextlib.contextmanager
def _takeover_guard():
    """flock around a stale-lock check-and-unlink: two processes that find the
    same stale lock cannot remove each other's fresh one. Creation itself is
    O_EXCL and needs no guard."""
    fh = open(TRIM_LOCK.with_name(TRIM_LOCK.name + ".guard"), "a")
    try:
        try:
            import fcntl
            fcntl.flock(fh, fcntl.LOCK_EX)
        except ImportError:
            pass
        yield
    finally:
        fh.close()


def trim_lock_active():
    """[v2.11] True while a queue trim is deleting rows. Tools that hold sheet
    row numbers (drains, repairs, state setters) must not run meanwhile: the
    rows under them move up.

    [v2.12] True while any LIVE holder exists, however long its trim takes.
    A lock whose pid is dead (a crash) is not active; before v2.12 such a
    lock blocked the tools for two hours. The two-hour rule is kept only for
    a lock whose pid cannot be read."""
    h = trim_lock_holder()
    return bool(h) and _holder_live(h)


class trim_lock:
    """Context manager that holds TRIM_LOCK for the duration of a trim.

    [v2.12] Owner-aware. Enter creates the file exclusively and writes
    "<pid> <tab> <ts>". A lock held by a live process is never overwritten:
    enter raises TrimLockHeld (a RuntimeError) and the caller skips its
    trim. A lock left by a dead pid is taken over with a WARNING. Exit
    unlinks the file only while it still holds our pid, so a trim never
    removes a lock that another process took after it."""

    def __init__(self, tab):
        self.tab = tab
        self._held = False

    def __enter__(self):
        TRIM_LOCK.parent.mkdir(exist_ok=True)
        line = f"{os.getpid()} {self.tab} {datetime.now().isoformat(timespec='seconds')}\n"
        for _ in range(3):
            try:
                fd = os.open(TRIM_LOCK, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            except FileExistsError:
                h = trim_lock_holder()
                if h is None:
                    continue                          # released meanwhile: retry
                if _holder_live(h):
                    raise TrimLockHeld(
                        f"trim lock {TRIM_LOCK} is held by pid {h['pid'] or '?'} "
                        f"({h['tab'] or '?'} since {h['ts'] or '?'}); "
                        f"not trimming {self.tab!r}")
                with _takeover_guard():
                    # unlink only the stale lock we judged, never a fresh one
                    # that another process created after taking it over
                    now = trim_lock_holder()
                    if now and (now["pid"], now["mtime"]) == (h["pid"], h["mtime"]):
                        log.warning("TRIM LOCK taking over a stale lock from pid %s "
                                    "(%s since %s): that process is no longer running",
                                    h["pid"] or "?", h["tab"] or "?", h["ts"] or "?")
                        try:
                            TRIM_LOCK.unlink()
                        except FileNotFoundError:
                            pass
                continue
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(line)
            except BaseException:
                # [v2.12] the file is ours (O_EXCL) but has no line: left in
                # place it reads as unparseable and blocks every trim for
                # two hours. Remove it, then let the error through.
                try:
                    TRIM_LOCK.unlink()
                except OSError:
                    pass
                raise
            self._held = True
            return self
        raise TrimLockHeld(f"trim lock {TRIM_LOCK} could not be taken for "
                           f"{self.tab!r} (another trim raced for it)")

    def __exit__(self, *exc):
        if not self._held:
            return False
        self._held = False
        h = trim_lock_holder()
        if h and h["pid"] == os.getpid():
            try:
                TRIM_LOCK.unlink()
            except OSError:
                pass
        elif h:
            log.warning("TRIM LOCK now held by pid %s (%s), not by us; left in place",
                        h["pid"] or "?", h["tab"] or "?")
        return False


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
    #: Collapse repeat rows for the same entity id within one cycle?
    #: TRUE where a second row means a duplicate delivery of the SAME event
    #: (customer.created replayed by the webhook). FALSE where several rows
    #: for one id are genuinely DIFFERENT events that must each be applied in
    #: order (an order moving under_review -> completed). Getting this wrong
    #: silently discards real events, so subclasses declare it explicitly.
    collapse_twins = True
    #: [v2.11] rows deliberately left for a human or a tool. The read cursor
    #: walks past them like terminal rows (before v2.11 one held row pinned
    #: the cursor and every poll re-read the tab from it), but they are never
    #: claimed and never trimmed.
    parked = ("held",)
    #: [v2.11] where an `error` row settles once cfg.realtime_max_attempts is
    #: reached, so a permanent failure cannot pin the cursor forever.
    exhausted_state = "error-final"
    #: [v2.11] True where column H carries the capture payload (Customer
    #: Queue): a non-final outcome then writes its note to column I and keeps
    #: H, and a final outcome replaces H with the note and clears I.
    payload_in_h = False
    #: [v2.11] daily trim: which states may be deleted, the Config attribute
    #: holding the retention in days, and the hour offset from
    #: cfg.realtime_trim_hour (streams trim at different hours).
    trimmable = ("done", "gone", "superseded", "error-final")
    trim_days_attr = ""
    trim_hour_offset = 0

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
        self._last_rewalk_day = None
        self._last_trim_day = None
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
        # A dry rehearsal must never claim the tab: it neither reads the guard
        # nor writes one. Writing it would lock the real consumer out for the
        # full stale window (300s) after every rehearsal, which is exactly the
        # kind of self-inflicted outage this guard exists to prevent.
        if not self.live:
            return True
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

    def on_exhausted(self, row, note):
        """[v2.11] Called once when an `error` row hits the attempt cap.
        Subclasses record it wherever a human will look."""

    # -- v2.11 settle / mark / re-walk / trim -------------------------------------

    def _settle(self, row, state, note):
        """Cap retries: the cursor stops at `error` rows, so an error that
        repeats forever would rebuild exactly the pin v2.11 removes."""
        cap = int(getattr(self.cfg, "realtime_max_attempts", 0) or 0)
        if state == "error" and cap and int(row.get("attempts") or 0) + 1 >= cap:
            note = f"gave up after {int(row.get('attempts') or 0) + 1} attempts: {note}"[:180]
            log.error("%s row %s id %s: %s", self.name, row.get("row"),
                      row.get("order_id"), note)
            try:
                self.on_exhausted(row, note)
            except Exception as e:
                log.warning("%s on_exhausted failed: %s", self.name, e)
            return self.exhausted_state, note
        return state, note

    def _mark(self, row, state, attempts, note):
        """Write a row's outcome. On payload tabs a non-final outcome keeps
        the payload in H (note to I); a final one replaces H and clears I."""
        kw = {"tab": self.tab, "expect_received_at": row.get("received_at")}
        if self.payload_in_h:
            if state in self.terminal:
                kw["clear_col"] = "I"
            else:
                kw["note_col"] = "I"
        return self.gio.queue_mark(self.qsid, row["row"], row["order_id"],
                                   state, attempts, note, **kw)

    def _maybe_rewalk(self):
        """Once a day walk the tab again from row 2, so a row someone puts
        back to `queued` behind the cursor is still picked up. Tied to the
        daily trim: before a tab is trimmed a walk from row 2 is a full read of
        a very large tab."""
        if not getattr(self.cfg, "realtime_trim_enabled", False):
            return
        now = datetime.now()
        hour = int(getattr(self.cfg, "realtime_trim_hour", 4)) + self.trim_hour_offset
        if self._last_rewalk_day == now.date() or now.hour < hour % 24:
            return
        self._last_rewalk_day = now.date()
        if self._start_row != 2:
            log.info("%s daily re-walk from row 2 (cursor was %d)",
                     self.name.upper(), self._start_row)
            self._start_row = 2
            self._save_state()

    def _maybe_trim(self):
        """Daily trim of this stream's own tab (Google caps a workbook at 10M
        cells and these tabs never shrank). The consumer is the only deleter
        of its tab; its cursor is reset to row 2 BEFORE the delete and again
        in `finally`, so an interrupted delete can never leave it pointing
        past rows that moved up. [v2.12] When another live process holds the
        trim lock the trim is skipped for the day (WARNING), never forced."""
        if not getattr(self.cfg, "realtime_trim_enabled", False) or not self.live:
            return
        days = int(getattr(self.cfg, self.trim_days_attr, 0) or 0) if self.trim_days_attr else 0
        now = datetime.now()
        hour = (int(getattr(self.cfg, "realtime_trim_hour", 4)) + self.trim_hour_offset) % 24
        if not days or now.hour != hour or self._last_trim_day == now.date():
            return
        self._last_trim_day = now.date()
        cutoff = now - timedelta(days=days)
        unparseable = [0]

        def keep(r):
            try:
                return datetime.strptime(str(r["received_at"])[:19],
                                         "%Y-%m-%d %H:%M:%S") >= cutoff
            except ValueError:
                unparseable[0] += 1
                return True  # never guess-delete

        self._start_row = 2
        self._save_state()
        n = 0
        try:
            with trim_lock(self.tab):
                n = self.gio.queue_trim(self.qsid, keep, tab=self.tab,
                                        deletable=self.trimmable)
        except TrimLockHeld as e:
            log.warning("%s trim skipped today, nothing deleted: %s", self.name, e)
            return      # [v2.12] no trim ran: no "removed 0" line (finally still resets)
        except Exception as e:
            log.exception("%s trim failed (nothing further deleted): %s", self.name, e)
            return      # the count would be wrong; finally still resets the cursor
        finally:
            self._start_row = 2
            self._save_state()
        log.info("%s TRIM removed %d row(s) older than %d day(s); %d row(s) "
                 "kept because their date could not be read",
                 self.name.upper(), n, days, unparseable[0])

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
                self._maybe_rewalk()
                rows = self.gio.queue_read(self.qsid,
                                           start_row=self._start_row,
                                           tab=self.tab)
                for r in rows:
                    if r["status"] in self.terminal or r["status"] in self.parked:
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
                # -- but only for streams where a repeat id means a repeat of
                # the same event (see collapse_twins).
                seen, primaries, twins = set(), [], []
                for r in claim:
                    if self.collapse_twins and r["order_id"] in seen:
                        twins.append(r)
                    else:
                        primaries.append(r)
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
                    state, note = self._settle(r, state, note)
                    outcome[r["order_id"]] = (state, note)
                    if not self.live:
                        log.info("DRY RUN %s row %s id %s (%s) -> %s | %s",
                                 self.name, r["row"], r["order_id"],
                                 r["event"][:40], state, note[:90])
                        continue
                    self._mark(r, state, r["attempts"] + 1, note)
                for r in twins:
                    st, note = outcome.get(r["order_id"], (None, None))
                    if st and self.live:
                        self._mark(r, st, r["attempts"], f"twin: {note}")
                self._maybe_trim()
                if once:
                    break
            except Exception as e:
                log.exception("%s loop error (auto-retried): %s", self.name, e)
            time.sleep(self.cfg.live_poll_s)
        log.info("%s STOP file honored -- exiting cleanly", self.name.upper())
