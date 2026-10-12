#!/usr/bin/env python3
"""Out-of-band alerting for the Salla -> HubSpot engines (v2.0).

Design constraint that drives everything here: the alert path must NOT depend on
the platform it monitors. The 2026-07-23 outage was a Make.com credit exhaustion
that silenced both relay scenarios for 68.9h -- an alert routed through Make
(e.g. the existing `held_notify_url` webhook) would have been dead too. So the
channels are Slack's own API and direct SMTP, neither of which touches Make.

Two channels, both optional and independent:
  * Slack  -- chat.postMessage with a bot token (NOT an incoming webhook), so a
              "resolved" message can be threaded under the original alert.
  * Email  -- stdlib smtplib. No dependency, no vendor.

Everything degrades gracefully: a missing credential disables that channel with
one INFO line, never an exception. An alerting path that crashes the engine it
is meant to protect would be worse than no alerting at all.

Secrets are read from the environment only (populated from .env by run.py):
  SLACK_BOT_TOKEN, SLACK_CHANNEL_ID
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_FROM, SMTP_TO
Nothing here ever logs a secret value.
"""

import fcntl
import json
import logging
import os
import re
import smtplib
import socket
import ssl
import threading
import time
from contextlib import contextmanager
from email.message import EmailMessage
from pathlib import Path

from backfill import http_request, with_retries, now_str

log = logging.getLogger("backfill")

SLACK_API = "https://slack.com/api/chat.postMessage"
SLACK_REACT = "https://slack.com/api/reactions.add"

# smtplib blocks forever by default. A hanging alert path is worse than a failing
# one -- it wedges the engine loop that called it.
SMTP_TIMEOUT_S = 20

# [v2.14.1] Issue threads post from the live worker lanes, where with_retries'
# backoff cost about 75 s per channel during a Slack outage and held up the
# whole poll. Inside issue_open/issue_update/issue_resolve a post makes at most
# one quick retry with a short timeout; a failed open is retried later by the
# registry, so waiting longer here buys nothing. Every other caller keeps the
# patient defaults. The mode travels in a thread-local, so the post_slack call
# shape (which test fakes mirror) does not change.
ISSUE_POST_TIMEOUT_S = 8
ISSUE_RETRY_PAUSE_S = 2      # longest pause before the one retry
_SLACK_RETRY_STATUSES = (429, 500, 502, 503, 504, 0)
_posting = threading.local()


@contextmanager
def _fast_posts():
    prev = getattr(_posting, "fast", False)
    _posting.fast = True
    try:
        yield
    finally:
        _posting.fast = prev


def _post_fast(go):
    """One try, then one retry only for a throttle or a transient error, and
    only when Slack does not ask for a longer wait than ISSUE_RETRY_PAUSE_S.
    No sleep after the last try (with_retries sleeps even then)."""
    status, headers, text = go()
    if status in _SLACK_RETRY_STATUSES:
        ra = (headers or {}).get("Retry-After")
        wait = float(ra) if ra and str(ra).isdigit() else 1.0
        if wait <= ISSUE_RETRY_PAUSE_S:
            log.warning("slack chat.postMessage got %s, one quick retry", status)
            time.sleep(wait)
            status, headers, text = go()
    return status, headers, text


def _env(name, default=""):
    return (os.environ.get(name) or default).strip()


def slack_channels():
    """All configured channel ids.

    SLACK_CHANNEL_IDS (comma-separated) wins; SLACK_CHANNEL_ID kept for
    backward compatibility. Multiple channels exist because audiences differ:
    e.g. an internal ops channel plus a shared channel with the client -- an
    alert that only reaches the team that already knows is half an alert.
    """
    raw = _env("SLACK_CHANNEL_IDS") or _env("SLACK_CHANNEL_ID")
    return [c.strip() for c in raw.split(",") if c.strip()]


def slack_enabled():
    return bool(_env("SLACK_BOT_TOKEN") and slack_channels())


def email_enabled():
    return bool(_env("SMTP_HOST") and _env("SMTP_FROM") and _env("SMTP_TO"))


def post_slack(text, blocks=None, thread_ts=None, dry_run=False, channel=None,
               reply_broadcast=False):
    """Post to one Slack channel. Returns the message ts (for threading) or None.
    [v2.13] reply_broadcast=True on a thread reply also shows it in the channel.

    Slack returns HTTP 200 even for application errors -- the same trap as Make's
    `200 Accepted`. The JSON `ok` field is the real status and must be checked.
    """
    if not slack_enabled():
        log.debug("slack alert skipped: SLACK_BOT_TOKEN/SLACK_CHANNEL_ID(S) not set")
        return None
    chans = slack_channels()
    payload = {"channel": channel or chans[0], "text": text}
    if blocks:
        payload["blocks"] = blocks
    if thread_ts:
        payload["thread_ts"] = thread_ts
        if reply_broadcast:
            payload["reply_broadcast"] = True
    payload["unfurl_links"] = False      # links stay one line, not preview cards
    if dry_run:
        log.info("DRY RUN slack payload: %s", json.dumps(
            {k: v for k, v in payload.items() if k != "channel"})[:400])
        return None

    fast = getattr(_posting, "fast", False)

    def go():
        return http_request(
            "POST", SLACK_API,
            headers={"Authorization": f"Bearer {_env('SLACK_BOT_TOKEN')}",
                     "Content-Type": "application/json; charset=utf-8"},
            body=json.dumps(payload), timeout=ISSUE_POST_TIMEOUT_S if fast else 20)

    if fast:
        status, _, text_resp = _post_fast(go)
    else:
        status, _, text_resp = with_retries(go, "slack chat.postMessage", retries=3)
    if status != 200:
        log.warning("slack alert HTTP %s: %s", status, text_resp[:200])
        return None
    try:
        parsed = json.loads(text_resp)
    except json.JSONDecodeError:
        log.warning("slack alert returned non JSON: %s", text_resp[:200])
        return None
    if not parsed.get("ok"):
        # Common causes: not_in_channel (invite the bot), channel_not_found,
        # invalid_auth (token rotated). Surface the code, never the token.
        log.warning("slack alert failed: %s", parsed.get("error"))
        return None
    return parsed.get("ts")


def send_email(subject, body, dry_run=False):
    """Send one alert email over SMTP. Returns True on success."""
    if not email_enabled():
        log.debug("email alert skipped: SMTP_HOST/SMTP_FROM/SMTP_TO not set")
        return False
    to_list = [a.strip() for a in _env("SMTP_TO").split(",") if a.strip()]
    msg = EmailMessage()
    # Gmail collapses identical subjects into one thread and buries repeats, so
    # the timestamp keeps each incident visually distinct in the inbox.
    msg["Subject"] = f"{subject} [{now_str()}]"
    msg["From"] = _env("SMTP_FROM")
    msg["To"] = ", ".join(to_list)
    msg.set_content(body)
    if dry_run:
        log.info("DRY RUN email: subject=%r to=%d recipient(s) body=%d chars",
                 msg["Subject"], len(to_list), len(body))
        return False
    host, port = _env("SMTP_HOST"), int(_env("SMTP_PORT", "587") or 587)
    user, password = _env("SMTP_USER"), _env("SMTP_PASSWORD")
    try:
        if port == 465:
            server = smtplib.SMTP_SSL(host, port, timeout=SMTP_TIMEOUT_S,
                                      context=ssl.create_default_context())
        else:
            server = smtplib.SMTP(host, port, timeout=SMTP_TIMEOUT_S)
        with server:
            if port != 465:
                server.starttls(context=ssl.create_default_context())
            if user and password:
                server.login(user, password)
            server.send_message(msg)
        return True
    except smtplib.SMTPAuthenticationError as e:
        # Gmail killed "Less Secure Apps" on 2025-05-01; a raw account password
        # yields 534-5.7.9 here. An App Password is required.
        log.warning("email alert auth failed (%s) -- Gmail/Workspace needs an "
                    "App Password, not the account password", e.smtp_code)
        return False
    except (smtplib.SMTPException, socket.timeout, OSError) as e:
        log.warning("email alert failed: %s", type(e).__name__)
        return False


def send_alert(subject, body, blocks=None, thread_ts=None, dry_run=False):
    """Fan out one alert to every configured channel.

    Slack shape is deliberate: the SUBJECT is the visible channel message -- it
    must read as a complete, understandable sentence on its own, because that is
    all most people see in a notification or on a phone. The full explanation is
    posted as a THREADED REPLY, so the channel stays skimmable but anyone who
    wants the reasoning can open the thread.

    Email has no threads, so it gets subject + body together.

    Never raises: alerting must not be able to break the engine it protects.
    Returns the Slack parent ts.
    """
    ts = None
    try:
        # every configured channel gets the headline + its own detail thread;
        # a failure in one channel must not silence the others
        for ch in (slack_channels() or [None]):
            if ch is None:
                break
            try:
                cts = post_slack(subject, blocks=blocks, thread_ts=thread_ts,
                                 dry_run=dry_run, channel=ch)
                if body and cts:
                    post_slack(body, thread_ts=cts, dry_run=dry_run, channel=ch)
                ts = ts or cts  # first channel's ts kept for callers that thread
            except Exception as e:
                log.warning("slack alert to %s failed: %s", ch, e)
    except Exception as e:
        log.warning("slack alert dispatch error: %s", e)
    try:
        send_email(subject, body, dry_run=dry_run)
    except Exception as e:
        log.warning("email alert dispatch error: %s", e)
    if not slack_enabled() and not email_enabled():
        log.warning("ALERT (no channel configured): %s -- %s", subject, body[:300])
    return ts


def channels_summary():
    """Human-readable channel status for startup logs. No secret values."""
    n = len(slack_channels()) if slack_enabled() else 0
    return (f"slack={'on x' + str(n) + ' channel' + ('s' if n != 1 else '') if n else 'off'} "
            f"email={'on' if email_enabled() else 'off'}")


# ---------------------------------------------------------------------------
# [v2.13] Issue threads: one Slack thread per problem, answered when it clears
# ---------------------------------------------------------------------------
#
# Every problem alert is opened under a stable key ("held:<product>",
# "customer-held:<id>", "status-missing:<order>", "make-queue:<scenario>",
# ...). The parent message and its detail reply are posted once per channel
# and their ts stored in mirror/slack_issues.json. While the problem lasts,
# news goes into the same thread (throttled). When the code that owns the
# problem sees it clear, it resolves the key: a "Resolved" reply lands in the
# same thread (optionally also shown in the channel), the parent gets a
# check-mark reaction, and the record is closed. Several services share the
# file, so every read-modify-write holds an exclusive lock.
#
# [v2.14.1] An open that reached no Slack channel is stored as "failed" with a
# retry_at, never as "open": an open record with no thread swallowed every
# later update and the resolve, so the whole incident stayed invisible. The
# first caller after retry_at posts the thread; until then callers only merge
# their meta.

ISSUES = Path("mirror/slack_issues.json")
ISSUES_LOCK = Path("mirror/slack_issues.lock")
CLOSED_KEEP_S = 30 * 86400
# Covers the longest single step of an open (one channel's two fast posts, or
# the SMTP send); "t" is refreshed before every step, so a slow open across
# several channels is never taken over by a second lane.
OPENING_TTL_S = 180
FAILED_RETRY_S = 300


@contextmanager
def _issues():
    ISSUES.parent.mkdir(parents=True, exist_ok=True)
    with open(ISSUES_LOCK, "a+") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        try:
            try:
                data = json.loads(ISSUES.read_text()) if ISSUES.exists() else {}
            except (ValueError, OSError):
                data = {}
            before = json.dumps(data, sort_keys=True)
            yield data
            now = time.time()
            # a failed open nobody retried for a month is no longer a live problem
            for k in [k for k, v in data.items()
                      if (v.get("state") == "closed" and now - float(v.get("closed_at") or 0) > CLOSED_KEEP_S)
                      or (v.get("state") == "failed" and now - float(v.get("retry_at") or 0) > CLOSED_KEEP_S)]:
                data.pop(k, None)
            if json.dumps(data, sort_keys=True) != before:
                tmp = ISSUES.with_suffix(".tmp")
                tmp.write_text(json.dumps(data, indent=1, ensure_ascii=False, sort_keys=True))
                tmp.replace(ISSUES)
        finally:
            fcntl.flock(lk, fcntl.LOCK_UN)


def _merge_meta(rec, meta):
    if not meta:
        return
    m = rec.setdefault("meta", {})
    for k, v in meta.items():
        if isinstance(v, list):
            have = m.setdefault(k, [])
            for x in v:
                if x not in have:
                    have.append(x)
        else:
            m[k] = v


def add_reaction(channel, ts, name="white_check_mark"):
    """Best effort: needs the reactions:write scope; a missing scope is logged
    once per process and never raised."""
    if not slack_enabled() or not ts:
        return False
    try:
        status, _, body = http_request(
            "POST", SLACK_REACT,
            headers={"Authorization": f"Bearer {_env('SLACK_BOT_TOKEN')}",
                     "Content-Type": "application/json; charset=utf-8"},
            body=json.dumps({"channel": channel, "timestamp": ts, "name": name}),
            timeout=ISSUE_POST_TIMEOUT_S)   # only issue_resolve calls this
        parsed = json.loads(body or "{}")
        if not parsed.get("ok") and parsed.get("error") not in ("already_reacted",):
            if not getattr(add_reaction, "_warned", False):
                log.info("slack reaction skipped: %s", parsed.get("error"))
                add_reaction._warned = True
            return False
        return True
    except Exception as e:
        log.debug("slack reaction failed: %s", e)
        return False


def issue_get(key):
    with _issues() as d:
        rec = d.get(key)
        return json.loads(json.dumps(rec)) if rec else None


def issue_is_open(key):
    rec = issue_get(key)
    return bool(rec and rec.get("state") == "open")


def issues_open(prefix=""):
    with _issues() as d:
        return {k: json.loads(json.dumps(v)) for k, v in d.items()
                if k.startswith(prefix) and v.get("state") == "open"}


def _touch_opening(key):
    """Refresh an in-flight open's clock before its next slow step."""
    with _issues() as d:
        rec = d.get(key)
        if rec and rec.get("state") == "opening":
            rec["t"] = time.time()


def issue_open(key, subject, body, meta=None, dry_run=False, email=True):
    """Open a problem thread. Returns True when a new thread was posted; when
    the key is already open, being opened, or failed and not yet due a retry,
    its meta is merged and nothing is posted. email=False skips the email (for
    a caller that sends its own). When Slack is configured but no channel took
    the post, the key is stored "failed" and False is returned."""
    now = time.time()
    with _issues() as d:
        rec = d.get(key)
        state = rec.get("state") if rec else None
        if state == "open":
            _merge_meta(rec, meta)
            return False
        if state == "opening" and now - float(rec.get("t") or 0) < OPENING_TTL_S:
            _merge_meta(rec, meta)
            return False
        if state == "failed" and now < float(rec.get("retry_at") or 0):
            _merge_meta(rec, meta)
            return False
        if dry_run:
            log.info("DRY RUN issue open %s: %s", key, subject)
            return False
        # a retry keeps what queued up while Slack was failing (the orders
        # waiting on it) and does not email again if the failed try did
        emailed = bool(state == "failed" and rec.get("emailed"))
        d[key] = {"state": "opening", "t": now, "subject": subject}
        if state == "failed" and rec.get("meta"):
            d[key]["meta"] = rec["meta"]
        _merge_meta(d[key], meta)
    threads = {}
    with _fast_posts():
        for i, ch in enumerate(slack_channels() if slack_enabled() else []):
            if i:
                _touch_opening(key)
            try:
                ts = post_slack(subject, channel=ch)
                if ts:
                    threads[ch] = ts
                    if body:
                        post_slack(body, thread_ts=ts, channel=ch)
            except Exception as e:
                log.warning("issue %s: slack post to %s failed: %s", key, ch, e)
    if email and not emailed:
        if email_enabled():
            _touch_opening(key)
        try:
            emailed = bool(send_email(subject, body))
        except Exception as e:
            log.warning("issue %s: email failed: %s", key, e)
    if not slack_enabled() and not email_enabled():
        log.warning("ALERT (no channel configured): %s -- %s", subject, (body or "")[:300])
    # partial success (any channel) is open; none at all is retried later
    failed = slack_enabled() and not threads
    with _issues() as d:
        rec = d.get(key) or {"meta": {}}
        rec.pop("t", None)
        if failed:
            rec.update({"state": "failed", "subject": subject, "emailed": emailed,
                        "retry_at": time.time() + FAILED_RETRY_S})
        else:
            rec.update({"state": "open", "opened_at": now, "last_update": now, "subject": subject,
                        "threads": threads})
            rec.pop("retry_at", None)
            rec.pop("emailed", None)
        d[key] = rec
    if failed:
        log.warning("ISSUE %s: no Slack channel took the thread, retry after %ds: %s",
                    key, FAILED_RETRY_S, subject)
        return False
    log.info("ISSUE opened %s: %s", key, subject)
    return True


def issue_update(key, text, every_s=0, meta=None, dry_run=False, broadcast=False):
    """News for an open thread, at most once per every_s. Returns True when
    posted. broadcast=True also shows the reply in the channel (for news that
    makes the problem more urgent)."""
    now = time.time()
    with _issues() as d:
        rec = d.get(key)
        if not rec or rec.get("state") != "open":
            if rec and rec.get("state") in ("failed", "opening"):
                _merge_meta(rec, meta)   # kept for the thread the open will post
            return False
        _merge_meta(rec, meta)
        if now - float(rec.get("last_update") or 0) < every_s:
            return False
        if dry_run:
            return False
        rec["last_update"] = now
        threads = dict(rec.get("threads") or {})
    with _fast_posts():
        for ch, ts in threads.items():
            try:
                post_slack(text, thread_ts=ts, channel=ch, reply_broadcast=broadcast)
            except Exception as e:
                log.warning("issue %s: update to %s failed: %s", key, ch, e)
    return True


def issue_resolve(key, text, broadcast=False, dry_run=False):
    """Answer the thread that raised `key` and close it. Returns True when an
    open issue was resolved; False (and nothing posted) otherwise."""
    with _issues() as d:
        rec = d.get(key)
        if rec and rec.get("state") == "failed" and not dry_run:
            # no thread was ever posted, so there is nothing to answer; closing
            # it lets the next occurrence open a fresh thread at once
            rec.update({"state": "closed", "closed_at": time.time(), "resolution": text[:300]})
            rec.pop("retry_at", None)
            return False
        if not rec or rec.get("state") != "open":
            return False
        if dry_run:
            log.info("DRY RUN issue resolve %s", key)
            return False
        rec.update({"state": "closed", "closed_at": time.time(), "resolution": text[:300]})
        threads, subject = dict(rec.get("threads") or {}), rec.get("subject", key)
    with _fast_posts():
        for ch, ts in threads.items():
            try:
                post_slack(text, thread_ts=ts, channel=ch, reply_broadcast=broadcast)
                add_reaction(ch, ts)
            except Exception as e:
                log.warning("issue %s: resolve post to %s failed: %s", key, ch, e)
    try:
        # [v2.14.1] the stored subject can tag owners for Slack ("<@U...>");
        # in an email subject that markup is noise
        send_email("Resolved: " + re.sub(r"<@[A-Z0-9]+>\s*", "", subject), text)
    except Exception as e:
        log.warning("issue %s: resolve email failed: %s", key, e)
    log.info("ISSUE resolved %s", key)
    return True
