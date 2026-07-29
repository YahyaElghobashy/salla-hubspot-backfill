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

import json
import logging
import os
import smtplib
import socket
import ssl
from email.message import EmailMessage

from backfill import http_request, with_retries, now_str

log = logging.getLogger("backfill")

SLACK_API = "https://slack.com/api/chat.postMessage"

# smtplib blocks forever by default. A hanging alert path is worse than a failing
# one -- it wedges the engine loop that called it.
SMTP_TIMEOUT_S = 20


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


def post_slack(text, blocks=None, thread_ts=None, dry_run=False, channel=None):
    """Post to one Slack channel. Returns the message ts (for threading) or None.

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
    if dry_run:
        log.info("DRY RUN slack payload: %s", json.dumps(
            {k: v for k, v in payload.items() if k != "channel"})[:400])
        return None

    def go():
        return http_request(
            "POST", SLACK_API,
            headers={"Authorization": f"Bearer {_env('SLACK_BOT_TOKEN')}",
                     "Content-Type": "application/json; charset=utf-8"},
            body=json.dumps(payload), timeout=20)

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
