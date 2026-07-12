#!/usr/bin/env python3
"""
Salla -> HubSpot Order Backfill, Local Engine
=============================================
Faithful local replica of a production Make backfill scenario.
Every property name, filter, association type ID, formula, and route condition in this
file was extracted verbatim from the deployed blueprint export dated 2026-07-04.
Blueprint module IDs are referenced in [M###] comments for traceability.

Data flow per page (mirrors the TIMESLOT design):
  [M500] read cursor  ->  [M300/302] list one page of one 3h slot (via Make relay)
  -> per order: [M310] dedup -> [M304] fetch full (relay, batched) -> [M250/255] archive
  -> [M203] audit row -> [M209..215] verification gate
  -> HELD  route: [M244] audit=Queued, [M222] notify webhook, [M224] queue log row
  -> CREATE route: [M7/12/3/16] contact -> [M2] order -> [M4] synced -> [M240] audit
       -> per item [M100..104] searches -> router [M30]:
          standalone [M110/232/111] | bundle template [M120..130]
          | Salla native [M150..158] | needs_review [M140..142]
  -> [M501] advance cursor (deviation: written AFTER the page completes, see README)

Guardrails: dry run by default, --max-orders, --max-pages, STOP file, sticky overflow
flag, idempotent dedup, client side rate limiting, exponential backoff, atomic cursor
writes, graceful SIGINT, full DEBUG logfile, local CSV mirrors of every sheet write.

Secrets come from the environment, never from config or logs:
  HUBSPOT_ACCESS_TOKEN   HubSpot private app token
  RELAY_SECRET           Shared secret configured in the relay scenario filters
"""

import argparse
import csv
import json
import logging
import os
import random
import re
import signal
import socket
import ssl
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# ----------------------------------------------------------------------------
# Portal-specific identifiers. Every value comes from config.json (the setup
# wizard discovers them from your portal). apply_portal_config() populates
# these module globals before any engine object is constructed.
# ----------------------------------------------------------------------------
OBJ_BUNDLE_TEMPLATE = None    # HubSpot custom object type id, e.g. "2-XXXXXXXXX"
OBJ_BUNDLE          = None
OBJ_COMPONENT       = None
ASSOC_ORDER_CONTACT = 507     # HUBSPOT_DEFINED, standard on every portal
ASSOC_ORDER_LI      = 513     # HUBSPOT_DEFINED, standard on every portal
ASSOC_TPL_BUNDLE    = None    # USER_DEFINED association ids from your portal
ASSOC_BUNDLE_ORDER  = None
ASSOC_BUNDLE_PARENT = None
ASSOC_BUNDLE_COMP   = None
ORDER_PIPELINE_STAGE = None   # default creation stage id
RACE_RETRY_WAIT_S = 5  # [M3-guardrail] wait before the post-failure re-search

# Creation stage follows the order's CURRENT Salla status, keyed on
# status.slug (expanded payloads expose the parent-level status). Unmapped
# slugs fall back to ORDER_PIPELINE_STAGE.
STATUS_STAGE_MAP = {}


def apply_portal_config(cfg):
    """Populate portal-specific module globals from Config. Fails loudly when
    required identifiers are missing so a half-filled config can never write
    to the wrong place."""
    g = globals()
    g["ORDER_PIPELINE_STAGE"] = cfg.default_pipeline_stage
    g["STATUS_STAGE_MAP"] = {str(k).lower(): v for k, v in (cfg.status_stage_map or {}).items()}
    obj = cfg.object_type_ids or {}
    assoc = cfg.assoc_type_ids or {}
    g["OBJ_BUNDLE_TEMPLATE"] = obj.get("bundle_template")
    g["OBJ_BUNDLE"] = obj.get("bundle")
    g["OBJ_COMPONENT"] = obj.get("component")
    g["ASSOC_ORDER_CONTACT"] = int(assoc.get("order_contact", 507))
    g["ASSOC_ORDER_LI"] = int(assoc.get("order_line_item", 513))
    for key, name in (("template_bundle", "ASSOC_TPL_BUNDLE"),
                      ("bundle_order", "ASSOC_BUNDLE_ORDER"),
                      ("bundle_parent_li", "ASSOC_BUNDLE_PARENT"),
                      ("bundle_component_li", "ASSOC_BUNDLE_COMP")):
        g[name] = int(assoc[key]) if assoc.get(key) is not None else None
    if not g["ORDER_PIPELINE_STAGE"]:
        raise SystemExit("config.json incomplete: default_pipeline_stage required. "
                         "Run the setup wizard: python serve.py")
    if cfg.bundles_enabled and not all((g["OBJ_BUNDLE_TEMPLATE"], g["OBJ_BUNDLE"],
                                        g["OBJ_COMPONENT"], g["ASSOC_TPL_BUNDLE"],
                                        g["ASSOC_BUNDLE_ORDER"], g["ASSOC_BUNDLE_PARENT"],
                                        g["ASSOC_BUNDLE_COMP"])):
        raise SystemExit("config.json: bundles_enabled is true but object_type_ids/"
                         "assoc_type_ids are incomplete. Run the wizard (python "
                         "serve.py) or set bundles_enabled to false.")

STOP_FILE = Path("STOP")

log = logging.getLogger("backfill")

# ----------------------------------------------------------------------------
# Small utilities
# ----------------------------------------------------------------------------

def dig(obj, path, default=""):
    """Safe nested getter: dig(order, 'customer.mobile_code')."""
    cur = obj
    for key in path.split("."):
        if isinstance(cur, dict) and key in cur and cur[key] is not None:
            cur = cur[key]
        else:
            return default
    return cur if cur is not None else default


def ifempty(a, b):
    """Make ifempty(): b when a is empty/None."""
    return b if a in (None, "", [], {}) else a


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class AdaptiveLimiter:
    """v1.4: AIMD (additive-increase / multiplicative-decrease) min-interval
    limiter. Thread free — the engine is sequential.

    The rate starts at `start_per_s` and always stays within
    [floor_per_s, ceil_per_s]. The ceiling is the documented provider limit
    scaled by the configured target utilization (default 0.92, i.e. the
    90-95%% band), so even fully "warmed up" the engine leaves headroom for
    live automations sharing the same quota.

    Feedback:
      on_throttle(retry_after)  provider returned 429/quota-exceeded ->
                                rate *= hard_decrease, growth frozen for
                                `cooldown_s` (sized to the provider's window).
      on_result(remaining, cap) provider bucket telemetry (HubSpot headers).
                                Headroom below `soft_floor` means OTHER
                                consumers are eating the shared bucket ->
                                rate *= soft_decrease with a short cooldown.
      on_success()              after `growth_every` consecutive successes
                                outside a cooldown, rate += step_per_s.

    With adaptive=False the rate is pinned at start_per_s (legacy behavior).
    """

    def __init__(self, name, start_per_s, floor_per_s=None, ceil_per_s=None,
                 step_per_s=0.1, growth_every=25, cooldown_s=30.0,
                 hard_decrease=0.5, soft_decrease=0.75, soft_floor=0.15,
                 adaptive=True):
        self.name = name
        self.rate = max(start_per_s, 0.001)
        ceil = ceil_per_s if ceil_per_s is not None else self.rate
        floor = floor_per_s if floor_per_s is not None else self.rate
        # Invariant: floor <= start <= ceil. A ceiling below the start caps the
        # start; a floor above the start would defeat multiplicative decrease
        # (and with floor > ceil even freeze the rate at the ceiling), so the
        # floor is clamped to the effective start.
        self.ceil = max(ceil, 0.001)
        self.rate = min(self.rate, self.ceil)
        self.floor = min(max(floor, 0.001), self.rate)
        if not adaptive:
            self.floor = self.ceil = self.rate
        self.step = step_per_s
        self.growth_every = max(1, growth_every)
        self.cooldown_s = cooldown_s
        self.hard_decrease = hard_decrease
        self.soft_decrease = soft_decrease
        self.soft_floor = soft_floor
        self.adaptive = adaptive
        self._last = 0.0
        self._successes = 0
        self._cooldown_until = 0.0

    @property
    def min_interval(self):
        return 1.0 / self.rate

    def wait(self):
        delta = time.monotonic() - self._last
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self._last = time.monotonic()

    def _set_rate(self, new_rate, reason):
        new_rate = min(self.ceil, max(self.floor, new_rate))
        if abs(new_rate - self.rate) < 1e-9:
            return
        log.info("ADAPT %s %.3f->%.3f/s (%s)", self.name, self.rate, new_rate, reason)
        self.rate = new_rate

    def on_throttle(self, retry_after=None):
        """Provider throttled us (429 / RESOURCE_EXHAUSTED)."""
        if not self.adaptive:
            return
        self._successes = 0
        self._cooldown_until = time.monotonic() + self.cooldown_s
        self._set_rate(self.rate * self.hard_decrease,
                       f"throttled, retry_after={retry_after}")

    def on_result(self, remaining, cap):
        """Success WITH provider bucket telemetry (e.g. HubSpot rate headers)."""
        if not self.adaptive:
            return
        if cap and cap > 0 and (remaining / cap) < self.soft_floor:
            # The shared bucket is nearly drained by all consumers combined
            # (this engine + live automations). Yield gently.
            self._successes = 0
            # extend, never shorten, an in-flight hard-throttle cooldown
            self._cooldown_until = max(self._cooldown_until,
                                       time.monotonic() + min(10.0, self.cooldown_s))
            self._set_rate(self.rate * self.soft_decrease,
                           f"bucket low {remaining}/{cap}")
            return
        self.on_success()

    def on_success(self):
        if not self.adaptive or time.monotonic() < self._cooldown_until:
            return
        self._successes += 1
        if self._successes >= self.growth_every:
            self._successes = 0
            self._set_rate(self.rate + self.step, "recovery")

    def snapshot(self):
        return f"{self.name}={self.rate:.2f}/s"


# Backward-compatible alias: a plain fixed-rate limiter is an AdaptiveLimiter
# with adaptation off.
def RateLimiter(per_second):
    if per_second <= 0:
        per_second = 1e9  # effectively no pacing, matching the old behavior
    return AdaptiveLimiter("fixed", per_second, adaptive=False)


def http_request(method, url, headers=None, body=None, timeout=90):
    """Raw HTTP with structured result. Never logs Authorization headers."""
    data = body.encode("utf-8") if isinstance(body, str) else body
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return resp.status, dict(resp.headers), resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), e.read().decode("utf-8", "replace")
    except Exception as e:  # network layer
        return 0, {}, f"NETWORK_ERROR: {e}"


def with_retries(fn, what, retries=5, retry_statuses=(429, 500, 502, 503, 504, 0),
                 feedback=None):
    """Retry wrapper with exponential backoff and jitter. Honors Retry-After.
    v1.4: `feedback(status, headers)` is invoked on EVERY attempt (including
    throttled ones that this wrapper absorbs), so an AdaptiveLimiter learns
    about 429s even when the call ultimately succeeds."""
    for attempt in range(1, retries + 1):
        status, headers, text = fn()
        if feedback:
            try:
                feedback(status, headers)
            except Exception as e:  # feedback must never break the request path
                log.debug("rate feedback error: %s", e)
        if status not in retry_statuses:
            return status, headers, text
        wait = min(60.0, (2 ** attempt) + random.uniform(0, 1))
        ra = headers.get("Retry-After") if headers else None
        if ra and str(ra).isdigit():
            wait = max(wait, float(ra))
        log.warning("%s got %s (attempt %d/%d), backing off %.1fs",
                    what, status, attempt, retries, wait)
        time.sleep(wait)
    return status, headers, text


# ----------------------------------------------------------------------------
# Configuration and cursor state
# ----------------------------------------------------------------------------

@dataclass
class Config:
    hubspot_base: str = "https://api.hubapi.com"
    relay_url: str = ""
    relay_batch_size: int = 12
    relay_min_interval_s: float = 1.5
    slot_hours: int = 3
    per_page: int = 30
    overflow_pages: int = 18
    google_enabled: bool = False
    spreadsheet_id: str = ""
    audit_tab: str = "Order Audit Log"
    queue_tab: str = "Queue Log"
    drive_folder_id: str = ""
    held_notify_url: str = ""
    record_url_base: str = ""
    archive_dir: str = "archive"
    state_file: str = "cursor.json"
    hs_search_per_s: float = 3.5
    hs_general_per_s: float = 10.0
    sheets_per_min: float = 50.0
    salla_timezone_default: str = "Asia/Riyadh"
    bundles_enabled: bool = True
    default_pipeline_stage: str = ""
    status_stage_map: dict = None
    object_type_ids: dict = None
    assoc_type_ids: dict = None

    # v1.4 adaptive pacing. The *_per_s / *_per_min fields above become the
    # STARTING rates; ceilings are documented provider limits scaled by
    # adaptive_target_util (the 90-95% band). If other engines/integrations
    # share your HubSpot account's 5/s search pool, lower
    # hs_search_limit_per_s to your fair share of it.
    adaptive_enabled: bool = True
    adaptive_target_util: float = 0.92
    hs_search_limit_per_s: float = 5.0        # HubSpot CRM search cap per ACCOUNT
    hs_general_limit_per_10s: float = 190.0   # HubSpot private-app burst (Pro/Ent)
    sheets_limit_per_min: float = 60.0        # Sheets API write requests /min/user
    drive_start_per_min: float = 40.0
    drive_limit_per_min: float = 120.0        # self-imposed; Drive quota is far higher
    relay_floor_interval_s: float = 0.8       # fastest allowed relay call gap

    @staticmethod
    def load(path):
        with open(path) as f:
            raw = json.load(f)
        known = {f.name for f in Config.__dataclass_fields__.values()}
        unknown = set(raw) - known
        if unknown:
            log.warning("config.json: ignoring unknown keys %s", sorted(unknown))
        return Config(**{k: v for k, v in raw.items() if k in known})


class Cursor:
    """Local mirror of Make data store salla_backfill_cursor (key 'current').
    Fields and advance/overflow formula replicate [M501] exactly."""

    def __init__(self, path):
        self.path = Path(path)
        self.data = json.loads(self.path.read_text())

    def save(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2))
        tmp.replace(self.path)

    @property
    def status(self):
        return self.data["status"]

    def slot_window(self, slot_hours):
        """[M300/M302] from_date .. from_date+slot_hours (exclusive)."""
        start = datetime.strptime(self.data["from_date"], "%Y-%m-%dT%H:%M:%S")
        end = start + timedelta(hours=slot_hours)
        return start, end

    def advance(self, total_pages, slot_hours, page_just_done, overflow_limit):
        """Verbatim [M501] formula. Called only after the page fully processed."""
        d = self.data
        overflowed = total_pages > overflow_limit
        sticky = d["status"] == "overflow"
        start = datetime.strptime(d["from_date"], "%Y-%m-%dT%H:%M:%S")
        to_date = datetime.strptime(d["to_date"], "%Y-%m-%d")
        slot_done = page_just_done >= total_pages
        if slot_done:
            nxt = start + timedelta(hours=slot_hours)
            if nxt >= to_date:
                d["status"] = "done_overflow" if (overflowed or sticky) else "done"
            else:
                d["status"] = "overflow" if (overflowed or sticky) else "running"
            d["from_date"] = nxt.strftime("%Y-%m-%dT%H:%M:%S")
            d["next_page"] = 1
        else:
            d["status"] = "overflow" if (overflowed or sticky) else "running"
            d["next_page"] = page_just_done + 1
        d["total_pages"] = total_pages
        self.save()


# ----------------------------------------------------------------------------
# Relay client (Salla via the existing Make connection)
# ----------------------------------------------------------------------------

class RelayError(RuntimeError):
    pass


class RelayClient:
    def __init__(self, cfg: Config, secret: str):
        self.cfg = cfg
        self.secret = secret
        # v1.4: the relay gap adapts between relay_floor_interval_s (fast) and
        # 4x the configured interval (slow). Transient relay responses (Make
        # async ACK under queue pressure, HTTP failures) slow it down; sustained
        # clean responses speed it back up.
        base = 1.0 / max(cfg.relay_min_interval_s, 0.05)
        self._gap = AdaptiveLimiter(
            "relay", base,
            floor_per_s=base / 4.0,
            ceil_per_s=1.0 / max(cfg.relay_floor_interval_s, 0.05),
            step_per_s=0.05, growth_every=8, cooldown_s=20.0,
            adaptive=cfg.adaptive_enabled)

    def _call(self, payload, what):
        body = json.dumps(payload)

        def go():
            return http_request("POST", self.cfg.relay_url,
                                headers={"Content-Type": "application/json"},
                                body=body, timeout=180)

        # v1.3.1: Make sometimes ACKs the webhook asynchronously ("Accepted",
        # plain text, HTTP 200) instead of running WebhookRespond synchronously,
        # under queue pressure. That is transient: retry with backoff before
        # giving up. Retries also cover HTTP-status failures via with_retries.
        last = ""
        for attempt in range(1, 7):
            self._gap.wait()
            status, _, text = with_retries(go, what)
            if status != 200:
                last = f"relay HTTP {status}: {text[:300]}"
            else:
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError:
                    last = f"relay returned non JSON: {text[:200]}"
                else:
                    if parsed.get("ok"):
                        self._gap.on_success()  # v1.4 adaptive feedback
                        return parsed
                    last = f"relay ok=false: {text[:300]}"
            self._gap.on_throttle()  # v1.4: transient response -> widen the gap
            wait = min(45.0, (2 ** attempt) + random.uniform(0, 1))
            log.warning("%s transient relay response (attempt %d/6): %s; retry in %.1fs",
                        what, attempt, last, wait)
            time.sleep(wait)
        raise RelayError(f"{what}: {last}")

    def get_path(self, path):
        """Single Salla GET, e.g. the slot list page. [M300/M302 equivalent]"""
        parsed = self._call({"secret": self.secret, "path": path}, f"relay GET {path[:80]}")
        data = parsed.get("data")
        if not isinstance(data, dict) or "status" not in data:
            raise RelayError(f"relay GET {path[:80]}: unexpected Salla payload")
        return data

    def fetch_orders(self, ids):
        """Batched full order fetch, orders/{id}?expanded=true per id. [M304]"""
        out = {}
        for i in range(0, len(ids), self.cfg.relay_batch_size):
            chunk = [str(x) for x in ids[i:i + self.cfg.relay_batch_size]]
            parsed = self._call({"secret": self.secret, "ids": ",".join(chunk)},
                                f"relay batch fetch x{len(chunk)}")
            for item in parsed.get("batch", []):
                body = item if isinstance(item, dict) else {}
                data = body.get("data")
                if isinstance(data, dict) and data.get("id") is not None:
                    out[str(data["id"])] = data
                else:
                    log.error("Batch fetch element missing data (Salla error inside "
                              "relay run): %s", json.dumps(body)[:200])
        missing = [i for i in map(str, ids) if i not in out]
        if missing:
            log.error("Relay batch did not return orders: %s", missing)
        return out


# ----------------------------------------------------------------------------
# HubSpot client
# ----------------------------------------------------------------------------

class HubSpot:
    def __init__(self, cfg: Config, token: str, live: bool):
        self.cfg = cfg
        self.token = token
        self.live = live
        util = cfg.adaptive_target_util
        # v1.4 adaptive pacing. Documented limits (developers.hubspot.com
        # usage guidelines, verified 2026-07-12):
        #   - CRM search: 5 req/s PER ACCOUNT (shared with every live
        #     integration on the portal), and search responses carry NO rate
        #     headers -- so the search limiter adapts on 429s alone.
        #   - General API: 190 req/10s per private app (Pro/Enterprise);
        #     responses carry X-HubSpot-RateLimit-Max/-Remaining for the
        #     10s window, which the general limiter reads as a shared-bucket
        #     headroom signal.
        self.search_rl = AdaptiveLimiter(
            "hs_search", cfg.hs_search_per_s,
            floor_per_s=1.0,
            ceil_per_s=cfg.hs_search_limit_per_s * util,
            step_per_s=0.1, growth_every=25, cooldown_s=30.0,
            adaptive=cfg.adaptive_enabled)
        self.general_rl = AdaptiveLimiter(
            "hs_general", cfg.hs_general_per_s,
            floor_per_s=3.0,
            ceil_per_s=cfg.hs_general_limit_per_10s / 10.0 * util,
            step_per_s=0.5, growth_every=25, cooldown_s=15.0,
            adaptive=cfg.adaptive_enabled)

    def _rl_feedback(self, limiter, is_search):
        """v1.4: translate HubSpot responses into limiter feedback."""
        def cb(status, headers):
            if status == 429:
                h = {k.lower(): str(v) for k, v in (headers or {}).items()}
                limiter.on_throttle(h.get("retry-after"))
                return
            if not status or status >= 300:
                return  # 5xx/network: the retry layer handles it; not a rate signal
            if is_search:
                limiter.on_success()  # search responses have no rate headers
                return
            h = {k.lower(): str(v) for k, v in (headers or {}).items()}
            try:
                rem = int(h["x-hubspot-ratelimit-remaining"])
                cap = int(h["x-hubspot-ratelimit-max"])
            except (KeyError, ValueError):
                limiter.on_success()
                return
            limiter.on_result(rem, cap)
        return cb

    def _req(self, method, path, body=None, is_search=False, what=""):
        limiter = self.search_rl if is_search else self.general_rl
        limiter.wait()
        url = self.cfg.hubspot_base + path
        headers = {"Authorization": f"Bearer {self.token}",
                   "Content-Type": "application/json"}
        payload = json.dumps(body) if body is not None else None

        def go():
            return http_request(method, url, headers=headers, body=payload)

        status, hdrs, text = with_retries(go, what or f"HS {method} {path}",
                                          feedback=self._rl_feedback(limiter, is_search))
        log.debug("HS %s %s -> %s %s", method, path, status, text[:400])
        try:
            data = json.loads(text) if text else {}
        except json.JSONDecodeError:
            data = {"raw": text}
        return status, data

    # -- reads (always executed, including dry run) ---------------------------

    def search(self, path, body, what):
        status, data = self._req("POST", path, body, is_search=True, what=what)
        if status != 200:
            raise RuntimeError(f"{what}: HubSpot search {status}: {json.dumps(data)[:300]}")
        return data

    def dedup_order_exists(self, salla_order_id):
        """[M310] POST /crm/v3/objects/orders/search on salla_order_id."""
        data = self.search("/crm/v3/objects/orders/search", {
            "filterGroups": [{"filters": [{"propertyName": "salla_order_id",
                                           "operator": "EQ",
                                           "value": str(salla_order_id)}]}],
            "properties": ["hs_object_id"], "limit": 1}, f"dedup {salla_order_id}")
        return int(data.get("total", 0)) > 0

    def search_contact_by_phone(self, mobile_code, mobile):
        """[M7] query on code+mobile, phone EQ mobile OR phone EQ code+mobile."""
        data = self.search("/crm/v3/objects/contacts/search", {
            "query": f"{mobile_code}{mobile}",
            "filterGroups": [
                {"filters": [{"propertyName": "phone", "operator": "EQ", "value": str(mobile)}]},
                {"filters": [{"propertyName": "phone", "operator": "EQ", "value": f"{mobile_code}{mobile}"}]},
            ],
            "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
            "properties": ["phone", "hs_object_id"], "limit": 5}, "contact search")
        results = data.get("results", [])
        return (results[0]["id"], int(data.get("total", 0))) if results else (None, 0)

    def search_contact_retry(self, order):
        """[M3-guardrail v1.1] Post-create-failure re-search. Same dual phone
        match as [M7] plus a salla_customer_id group (OR), since the competing
        customer-create scenario stamps that id. Returns contact id or None."""
        code = dig(order, "customer.mobile_code")
        mobile = dig(order, "customer.mobile")
        data = self.search("/crm/v3/objects/contacts/search", {
            "filterGroups": [
                {"filters": [{"propertyName": "phone", "operator": "EQ", "value": str(mobile)}]},
                {"filters": [{"propertyName": "phone", "operator": "EQ", "value": f"{code}{mobile}"}]},
                {"filters": [{"propertyName": "salla_customer_id", "operator": "EQ",
                              "value": str(dig(order, "customer.id"))}]},
            ],
            "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
            "properties": ["phone", "hs_object_id"], "limit": 5}, "contact retry search")
        results = data.get("results", [])
        return results[0]["id"] if results else None

    def gate_search_product_approved(self, salla_product_id):
        """[M211] products: salla_product_id EQ x AND catalog_approval_status approved."""
        data = self.search("/crm/v3/objects/products/search", {
            "filterGroups": [{"filters": [
                {"propertyName": "salla_product_id", "operator": "EQ",
                 "value": str(ifempty(salla_product_id, 0))},
                {"propertyName": "catalog_approval_status", "operator": "EQ",
                 "value": "approved"}]}],
            "properties": ["hs_object_id"], "limit": 1}, "gate product")
        return int(data.get("total", 0))

    def gate_search_template(self, salla_product_id, eligible_only):
        """[M213]/[M214] bundle template search on the template custom object."""
        if not OBJ_BUNDLE_TEMPLATE:
            return 0
        filters = [{"propertyName": "bundle_template_key", "operator": "EQ",
                    "value": str(ifempty(salla_product_id, 0))}]
        if eligible_only:
            filters += [{"propertyName": "template_status", "operator": "EQ", "value": "active"},
                        {"propertyName": "active_component_count", "operator": "GT", "value": "0"}]
        data = self.search(f"/crm/v3/objects/{OBJ_BUNDLE_TEMPLATE}/search", {
            "filterGroups": [{"filters": filters}],
            "properties": ["hs_object_id"], "limit": 1}, "gate template")
        return int(data.get("total", 0))

    def item_search_product(self, salla_product_id):
        """[M101] product with full properties, no approval filter."""
        data = self.search("/crm/v3/objects/products/search", {
            "filterGroups": [{"filters": [{"propertyName": "salla_product_id",
                                           "operator": "EQ",
                                           "value": str(salla_product_id)}]}],
            "properties": ["hs_object_id", "hs_sku", "name", "salla_product_id"],
            "limit": 1}, "item product")
        return data

    def item_search_template(self, salla_product_id, eligible_only):
        """[M102]/[M104] with full property set on the eligible search."""
        if not OBJ_BUNDLE_TEMPLATE:
            return {"total": 0, "results": []}
        filters = [{"propertyName": "bundle_template_key", "operator": "EQ",
                    "value": str(salla_product_id)}]
        props = ["hs_object_id", "bundle_template_key", "template_status"]
        if eligible_only:
            filters += [{"propertyName": "template_status", "operator": "EQ", "value": "active"},
                        {"propertyName": "active_component_count", "operator": "GT", "value": "0"}]
            props = ["hs_object_id", "bundle_template_key", "bundle_template_name",
                     "bundle_sku", "template_status", "active_component_count",
                     "needs_review_component_count", "component_count"]
        data = self.search(f"/crm/v3/objects/{OBJ_BUNDLE_TEMPLATE}/search", {
            "filterGroups": [{"filters": filters}], "properties": props, "limit": 1},
            "item template")
        return data

    def search_active_components(self, template_key):
        """[M126] components on the component custom object, active, limit 100."""
        if not OBJ_COMPONENT:
            return []
        data = self.search(f"/crm/v3/objects/{OBJ_COMPONENT}/search", {
            "filterGroups": [{"filters": [
                {"propertyName": "bundle_template_key", "operator": "EQ", "value": str(template_key)},
                {"propertyName": "component_status", "operator": "EQ", "value": "active"}]}],
            "properties": ["component_key", "component_product_sku",
                           "component_salla_product_id", "component_hubspot_product_id",
                           "component_product_name_snapshot", "quantity_in_bundle",
                           "component_code"],
            "limit": 100}, "components")
        return data.get("results", [])

    # -- writes (skipped in dry run) ------------------------------------------

    def _write(self, method, path, body, what):
        if not self.live:
            log.info("DRY RUN skip write: %s %s %s", what, method, path)
            return 200, {"id": f"DRY-{what.replace(' ', '_')}"}
        status, data = self._req(method, path, body, what=what)
        return status, data

    def create_contact(self, order):
        """[M3] upsert branch: only runs when the phone search found nothing,
        so semantically this is a create with the exact property set."""
        c = order.get("customer", {})
        props = {
            "incorrect_email": dig(order, "customer.email"),
            "firstname": dig(order, "customer.first_name"),
            "lastname": dig(order, "customer.last_name"),
            "main_phone_number": f"{dig(order,'customer.mobile_code')}{dig(order,'customer.mobile')}",
            "city": dig(order, "customer.city"),
            "hs_country_region_code": dig(order, "customer.country_code"),
            "salla_customer_id": str(c.get("id", "")),
            "salla_customer_admin_link": dig(order, "customer.urls.admin"),
            "lifecyclestage": "customer",
            "phone": f"{dig(order,'customer.mobile_code')}{dig(order,'customer.mobile')}",
        }
        status, data = self._write("POST", "/crm/v3/objects/contacts",
                                   {"properties": props}, "create contact")
        if status not in (200, 201):
            log.error("Contact create failed (%s): %s", status, json.dumps(data)[:300])
            return None
        return data.get("id")

    def create_order(self, order, customer_id, tz_default):
        """[M2] createAnOrder with the verbatim property map and assoc 507."""
        ref = ifempty(order.get("reference_id"), order.get("id"))
        date_raw = dig(order, "date.date")[:19]
        tz = dig(order, "date.timezone", tz_default) or tz_default
        try:
            created_ms = str(int(datetime.strptime(date_raw, "%Y-%m-%d %H:%M:%S")
                                 .replace(tzinfo=ZoneInfo(tz)).timestamp() * 1000))
        except Exception:
            created_ms = ""
        discounts = order.get("amounts", {}).get("discounts", []) or []
        props = {
            "hs_tax": str(dig(order, "amounts.tax.amount.amount")),
            "salla_store": "Salla",
            "hs_order_name": f"RID{ref} | Salla | {dig(order,'customer.first_name')} | PO: {order.get('payment_method','')}",
            "hs_total_price": str(dig(order, "amounts.total.amount")),
            "salla_order_id": str(order.get("id", "")),
            "hs_source_store": "Salla",
            "salla_order_url": dig(order, "urls.admin"),
            "hs_currency_code": dig(order, "amounts.shipping_cost.currency"),
            "hs_shipping_cost": str(dig(order, "amounts.shipping_cost.amount")),
            "hs_discount_codes": ", ".join(str(d.get("code", "")) for d in discounts),
            "hs_pipeline_stage": STATUS_STAGE_MAP.get(
                str(dig(order, "status.slug")).lower(), ORDER_PIPELINE_STAGE),  # v1.3
            "hs_subtotal_price": str(dig(order, "amounts.sub_total.amount")),
            "hs_external_order_id": str(order.get("id", "")),
            "hs_external_order_url": dig(order, "urls.admin"),
            "hs_fulfillment_status": dig(order, "status.name"),
            "salla_order_reference": str(ref),
            "hs_billing_address_city": dig(order, "customer.city"),
            "hs_billing_address_name": f"{dig(order,'customer.first_name')} {dig(order,'customer.last_name')}",
            "hs_billing_address_email": dig(order, "customer.email"),
            "hs_billing_address_phone": str(dig(order, "customer.mobile")),
            "hs_external_created_date": created_ms,
            "hs_external_order_status": dig(order, "status.name"),
            "hs_shipping_address_city": dig(order, "customer.city"),
            "hs_shipping_address_phone": str(dig(order, "customer.mobile")),
            "hs_billing_address_country": dig(order, "customer.country"),
            "hs_buyer_accepts_marketing": True,
            "hs_billing_address_lastname": dig(order, "customer.last_name"),
            "hs_shipping_address_country": dig(order, "customer.country"),
            "hs_billing_address_firstname": dig(order, "customer.first_name"),
            "hs_payment_processing_method": order.get("payment_method", ""),
        }
        body = {"properties": props}
        if customer_id:
            body["associations"] = [{"to": {"id": str(customer_id)},
                                     "types": [{"associationCategory": "HUBSPOT_DEFINED",
                                                "associationTypeId": ASSOC_ORDER_CONTACT}]}]
        status, data = self._write("POST", "/crm/v3/objects/orders", body, "create order")
        if status not in (200, 201):
            log.error("Order create failed (%s): %s", status, json.dumps(data)[:400])
            return None
        return data.get("id")

    def patch_order(self, order_id, props, what):
        return self._write("PATCH", f"/crm/v3/objects/orders/{order_id}",
                           {"properties": props}, what)

    def create_line_item(self, props, what):
        status, data = self._write("POST", "/crm/v3/objects/line_items",
                                   {"properties": props}, what)
        if status not in (200, 201):
            log.error("%s failed (%s): %s", what, status, json.dumps(data)[:300])
            return None
        return data.get("id")

    def patch_line_item(self, li_id, props, what):
        return self._write("PATCH", f"/crm/v3/objects/line_items/{li_id}",
                           {"properties": props}, what)

    def associate(self, from_type, to_type, from_id, to_id, type_id, category, what):
        """[M111 etc] v4 batch/create with a single input, exactly as deployed."""
        path = f"/crm/v4/associations/{from_type}/{to_type}/batch/create"
        body = {"inputs": [{"from": {"id": str(from_id)}, "to": {"id": str(to_id)},
                            "types": [{"associationCategory": category,
                                       "associationTypeId": type_id}]}]}
        status, data = self._write("POST", path, body, what)
        if status not in (200, 201):
            log.warning("%s failed (%s): %s", what, status, json.dumps(data)[:200])


# ----------------------------------------------------------------------------
# Google Sheets and Drive sinks (live) with local mirror always on
# ----------------------------------------------------------------------------

AUDIT_WIDTH = 31  # columns A..AE (0..30)


def col_letter(idx0):
    idx = idx0 + 1
    s = ""
    while idx:
        idx, r = divmod(idx - 1, 26)
        s = chr(65 + r) + s
    return s


class GoogleIO:
    """values.append / values.batchUpdate on the audit workbook plus Drive uploads.
    Uses user OAuth (credentials.json + cached token.json). Disabled by --no-google
    or dry run; the local mirror below records every intended write regardless."""

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]

    def __init__(self, cfg: Config, enabled: bool):
        self.cfg = cfg
        self.enabled = enabled
        self.sheets = None
        self.drive = None
        util = cfg.adaptive_target_util
        # v1.4 adaptive pacing. Documented quotas (verified 2026-07-12):
        #   - Sheets API v4: 60 write requests/min/user (fixed ~60s refill
        #     window, no Retry-After header) -> ceiling 0.92*60 = ~55/min,
        #     65s cooldown after a 429 so the window can actually refill.
        #   - Drive API v3 (unit model, May 2026): 325,000 units/min/user,
        #     files.create = 50 units -> quota allows ~6,500 uploads/min.
        #     The ceiling below is self-imposed; upload latency, not quota,
        #     is Drive's real constraint. Drive no longer shares the Sheets
        #     limiter (pre-v1.4 it needlessly did).
        self.sheets_rl = AdaptiveLimiter(
            "sheets", cfg.sheets_per_min / 60.0,
            floor_per_s=20.0 / 60.0,
            ceil_per_s=cfg.sheets_limit_per_min / 60.0 * util,
            step_per_s=1.0 / 60.0, growth_every=20, cooldown_s=65.0,
            adaptive=cfg.adaptive_enabled)
        self.drive_rl = AdaptiveLimiter(
            "drive", cfg.drive_start_per_min / 60.0,
            floor_per_s=10.0 / 60.0,
            ceil_per_s=cfg.drive_limit_per_min / 60.0,
            step_per_s=5.0 / 60.0, growth_every=10, cooldown_s=65.0,
            adaptive=cfg.adaptive_enabled)
        if enabled:
            self._auth()

    def _gexec(self, request, what, limiter):
        """v1.4: execute a googleapiclient request with quota-aware retries.
        429s (and rate-flavored 403s) feed the AdaptiveLimiter and retry with
        truncated exponential backoff per Google's documented recommendation;
        anything else propagates to the caller's existing error semantics."""
        from googleapiclient.errors import HttpError
        # 6 attempts: cumulative backoff (~2+4+8+16+32s) outlasts the Sheets
        # fixed 60s quota window, so a write survives a window drained by
        # other consumers of the same Google user.
        for attempt in range(1, 7):
            limiter.wait()
            try:
                out = request.execute()
                limiter.on_success()
                return out
            except HttpError as e:
                code = getattr(getattr(e, "resp", None), "status", None)
                throttled = code == 429 or (
                    code == 403 and "ratelimitexceeded" in str(e).lower())
                if not throttled:
                    raise
                limiter.on_throttle()  # the limiter learns even on the last attempt
                if attempt == 6:
                    raise
                wait = min(64.0, (2 ** attempt) + random.uniform(0, 1))
                log.warning("%s got %s (attempt %d/6), backing off %.1fs",
                            what, code, attempt, wait)
                time.sleep(wait)

    def _auth(self):
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
        creds = None
        if Path("token.json").exists():
            creds = Credentials.from_authorized_user_file("token.json", self.SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file("credentials.json", self.SCOPES)
                creds = flow.run_local_server(port=0)
            Path("token.json").write_text(creds.to_json())
        self.sheets = build("sheets", "v4", credentials=creds).spreadsheets()
        self.drive = build("drive", "v3", credentials=creds)
        log.info("Google authenticated (Sheets + Drive)")

    def drive_upload_json(self, filename, json_text):
        """[M255] upload the raw serialized order into the archive folder."""
        if not self.enabled:
            return ""
        log.debug("PHASE drive upload %s", filename)  # v1.2 observability
        from googleapiclient.http import MediaInMemoryUpload
        try:
            media = MediaInMemoryUpload(json_text.encode("utf-8"),
                                        mimetype="application/json")
            f = self._gexec(self.drive.files().create(
                body={"name": filename, "parents": [self.cfg.drive_folder_id]},
                media_body=media, fields="id, webViewLink",
                supportsAllDrives=True), "drive upload", self.drive_rl)
            return f.get("webViewLink", "")
        except Exception as e:  # [oe255] Ignore semantics: continue without link
            log.error("Drive upload failed for %s: %s", filename, e)
            return ""

    def audit_append(self, values_by_idx):
        """[M203] append one sparse row, return the sheet row number for updates."""
        row = [""] * AUDIT_WIDTH
        for idx, val in values_by_idx.items():
            row[idx] = val
        if not self.enabled:
            return -1
        log.debug("PHASE sheet append")  # v1.2 observability
        try:
            resp = self._gexec(self.sheets.values().append(
                spreadsheetId=self.cfg.spreadsheet_id,
                range=f"'{self.cfg.audit_tab}'!A:AE",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": [row]}), "audit append", self.sheets_rl)
            rng = resp.get("updates", {}).get("updatedRange", "")
            m = re.search(r"![A-Z]+(\d+)", rng)
            return int(m.group(1)) if m else -1
        except Exception as e:  # [oe203] fallback row semantics
            log.error("Audit append failed, writing fallback row: %s", e)
            fb = dict(values_by_idx)
            fb[27] = "JSON exceeded limit - use Order Ops > Backfill JSON from Salla"
            try:
                row = [""] * AUDIT_WIDTH
                for idx, val in fb.items():
                    row[idx] = val
                resp = self._gexec(self.sheets.values().append(
                    spreadsheetId=self.cfg.spreadsheet_id,
                    range=f"'{self.cfg.audit_tab}'!A:AE",
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [row]}), "audit fallback append", self.sheets_rl)
                rng = resp.get("updates", {}).get("updatedRange", "")
                m = re.search(r"![A-Z]+(\d+)", rng)
                return int(m.group(1)) if m else -1
            except Exception as e2:
                log.error("Audit fallback append also failed: %s", e2)
                return -1

    def audit_update(self, row_number, values_by_idx, what):
        """[M240]/[M244] sparse cell updates on the audit row, contiguous runs
        grouped so untouched columns are never blanked."""
        if not self.enabled or row_number < 0:
            return
        log.debug("PHASE sheet update %s", what)  # v1.2 observability
        idxs = sorted(values_by_idx)
        runs, run = [], [idxs[0]]
        for i in idxs[1:]:
            if i == run[-1] + 1:
                run.append(i)
            else:
                runs.append(run)
                run = [i]
        runs.append(run)
        data = []
        for r in runs:
            rng = (f"'{self.cfg.audit_tab}'!{col_letter(r[0])}{row_number}:"
                   f"{col_letter(r[-1])}{row_number}")
            data.append({"range": rng,
                         "values": [[values_by_idx[i] for i in r]]})
        try:
            self._gexec(self.sheets.values().batchUpdate(
                spreadsheetId=self.cfg.spreadsheet_id,
                body={"valueInputOption": "USER_ENTERED", "data": data}),
                f"audit update {what}", self.sheets_rl)
        except Exception as e:  # [oe240 Resume / oe244 Ignore]
            log.error("Audit update (%s) failed on row %s: %s", what, row_number, e)

    def queue_append(self, values_by_idx):
        """[M224] Queue Log row."""
        if not self.enabled:
            return
        row = [""] * 14
        for idx, val in values_by_idx.items():
            row[idx] = val
        try:
            self._gexec(self.sheets.values().append(
                spreadsheetId=self.cfg.spreadsheet_id,
                range=f"'{self.cfg.queue_tab}'!A:N",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": [row]}), "queue append", self.sheets_rl)
        except Exception as e:
            log.error("Queue Log append failed: %s", e)


class LocalMirror:
    """CSV mirrors of every sheet write plus an error ledger. Always on: this is
    the reconciliation and debugging backbone independent of Google state."""

    def __init__(self, outdir):
        self.dir = Path(outdir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.audit = self.dir / "audit_mirror.csv"
        self.queue = self.dir / "queue_mirror.csv"
        self.errors = self.dir / "errors.csv"
        for p, hdr in ((self.audit, ["ts", "event", "sheet_row"] + [f"c{i}" for i in range(AUDIT_WIDTH)]),
                       (self.queue, ["ts"] + [f"c{i}" for i in range(14)]),
                       (self.errors, ["ts", "salla_order_id", "stage", "detail"])):
            if not p.exists():
                with open(p, "w", newline="") as f:
                    csv.writer(f).writerow(hdr)

    def _write(self, path, row):
        with open(path, "a", newline="") as f:
            csv.writer(f).writerow(row)

    def audit_event(self, event, sheet_row, values_by_idx):
        row = [""] * AUDIT_WIDTH
        for i, v in values_by_idx.items():
            row[i] = v
        self._write(self.audit, [now_str(), event, sheet_row] + row)

    def queue_event(self, values_by_idx):
        row = [""] * 14
        for i, v in values_by_idx.items():
            row[i] = v
        self._write(self.queue, [now_str()] + row)

    def error(self, salla_order_id, stage, detail):
        self._write(self.errors, [now_str(), salla_order_id, stage, str(detail)[:500]])


# ----------------------------------------------------------------------------
# Order processing (routes replicated from router 216 and router 30)
# ----------------------------------------------------------------------------

@dataclass
class Stats:
    pages: int = 0
    scanned: int = 0
    skipped_existing: int = 0
    created: int = 0
    held: int = 0
    li_standalone: int = 0
    li_bundle_parent: int = 0
    li_component: int = 0
    li_needs_review: int = 0
    errors: int = 0


class Engine:
    def __init__(self, cfg, cursor, relay, hs, gio, mirror, live):
        self.cfg, self.cursor, self.relay = cfg, cursor, relay
        self.hs, self.gio, self.mirror = hs, gio, mirror
        self.live = live
        self.stats = Stats()
        self.stop = False
        self._next_rate_report = 0.0  # v1.4
        signal.signal(signal.SIGINT, self._sigint)

    def _rates_report(self):
        """v1.4: periodic one-line snapshot of every adaptive rate."""
        if time.monotonic() < self._next_rate_report:
            return
        self._next_rate_report = time.monotonic() + 60.0
        log.info("RATES hs_search=%.2f/s hs_general=%.2f/s sheets=%.1f/min "
                 "drive=%.1f/min relay_gap=%.2fs",
                 self.hs.search_rl.rate, self.hs.general_rl.rate,
                 self.gio.sheets_rl.rate * 60.0, self.gio.drive_rl.rate * 60.0,
                 1.0 / self.relay._gap.rate)

    def _sigint(self, *_):
        log.warning("SIGINT received: finishing current order, then stopping")
        self.stop = True

    def _should_stop(self):
        if self.stop:
            return True
        if STOP_FILE.exists():
            log.warning("STOP file present: halting gracefully")
            return True
        return False

    # -- verification gate [M209..M215] ---------------------------------------

    def gate_unverified_items(self, order):
        """Returns the list of unverified items per the [M215] filter:
        (not group_products AND p==0 AND te==0 AND ta==0) OR
        (not group_products AND te==0 AND ta>0)"""
        unverified = []
        for item in order.get("items", []) or []:
            ptype = str(item.get("product_type", ""))
            if ptype.lower() == "group_products":
                continue
            pid = dig(item, "product.id")
            p = self.hs.gate_search_product_approved(pid)      # [M211]
            te = self.hs.gate_search_template(pid, True)       # [M213]
            ta = self.hs.gate_search_template(pid, False)      # [M214]
            if (p == 0 and te == 0 and ta == 0) or (te == 0 and ta > 0):
                unverified.append({"id": item.get("id"), "name": item.get("name", "")})
        return unverified

    # -- HELD route [M244/M222/M224] ------------------------------------------

    def route_held(self, order, audit_row, unverified):
        self.stats.held += 1
        upd = {11: "Held for Review", 12: "Items not yet approved in catalog",
               13: "FALSE", 19: "N/A", 20: "N/A", 21: "N/A", 22: "N/A", 23: "N/A",
               24: "Queued", 25: now_str()}
        self.mirror.audit_event("queued_update", audit_row, upd)
        if self.live:
            self.gio.audit_update(audit_row, upd, "queued")
        names = ", ".join(u["name"] for u in unverified)
        qrow = {0: now_str(), 1: str(order.get("id")), 2: str(order.get("reference_id", "")),
                3: now_str(), 4: names, 5: names, 6: "Queued", 12: "Yes",
                13: dig(order, "customer.created_at.date")}
        self.mirror.queue_event(qrow)
        if self.live:
            self.gio.queue_append(qrow)
            # [M222] notify webhook, stop on http error replicated as log only
            if self.cfg.held_notify_url:
                body = json.dumps({"order_id": order.get("id"),
                                   "reference_id": str(order.get("reference_id", ""))})
                status, _, text = http_request(
                    "POST", self.cfg.held_notify_url,
                    headers={"Content-Type": "application/json"}, body=body)
                if status != 200:
                    log.warning("Held notify webhook returned %s: %s", status, text[:200])
        log.info("HELD order %s (%d unverified item(s): %s)",
                 order.get("id"), len(unverified), names)

    # -- CREATE route [M7..M240 + items] --------------------------------------

    def flag_partial(self, order_id, source, detail):
        """[M142 / oe170-oe197] PATCH last_salla_sync_status=partial + error log."""
        self.hs.patch_order(order_id, {
            "last_salla_sync_status": "partial",
            "sync_error_log": f"[{now_str()}] [{source}] {detail}"[:1000],
        }, "flag partial")

    def route_create(self, order, audit_row):
        oid = str(order.get("id"))
        # [M7] contact search, [M12/M3] create if missing, [M16] merge,
        # [M3-guardrail v1.1] create failure triggers a delayed re-search: the
        # competing customer-create scenario may have created the contact between
        # our search and our create. Mirrors the Resume-route retry in Make.
        try:
            found_id, total = self.hs.search_contact_by_phone(
                dig(order, "customer.mobile_code"), dig(order, "customer.mobile"))
        except Exception as e:  # [oe7 Resume]
            log.error("Contact search failed for %s: %s", oid, e)
            found_id, total = None, 0
        if found_id is None:
            customer_id = self.hs.create_contact(order)
            if not customer_id:
                log.warning("Contact create failed for order %s: re-searching in "
                            "%ss (race guardrail)", oid, RACE_RETRY_WAIT_S)
                time.sleep(RACE_RETRY_WAIT_S)
                try:
                    customer_id = self.hs.search_contact_retry(order)
                except Exception as e:
                    log.error("Guardrail re-search failed for %s: %s", oid, e)
                    customer_id = None
                if customer_id:
                    log.info("Race guardrail resolved contact %s for order %s",
                             customer_id, oid)
        else:
            customer_id = found_id
        if not customer_id:
            # In Make an empty Customer ID fails the order create and oe2 drops the
            # bundle. Same net effect here: no order created, loud local error.
            self.mirror.error(oid, "contact", "contact resolution failed, order not created")
            self.stats.errors += 1
            log.error("Order %s NOT created: contact resolution failed", oid)
            return

        # [M2] order create
        order_id = self.hs.create_order(order, customer_id, self.cfg.salla_timezone_default)
        if not order_id:  # [oe2 Ignore]
            self.mirror.error(oid, "order_create", "createAnOrder failed after retries")
            self.stats.errors += 1
            return
        self.stats.created += 1
        log.info("CREATED order %s -> HubSpot %s (%s contact %s)", oid, order_id,
                 "existing" if total > 0 else "new", customer_id)

        # [M4] sync status
        self.hs.patch_order(order_id, {"last_salla_sync_status": "synced"}, "set synced")

        # [M240] audit processed
        upd = {11: "Order Approved", 13: "TRUE", 14: str(order_id),
               15: f"{self.cfg.record_url_base}/{order_id}",
               16: "New Contact" if total == 0 else "Existing Contact",
               17: str(customer_id), 18: "TRUE",
               19: "Pending Verification", 20: "Pending Verification",
               21: "Pending Verification", 22: "Pending Verification",
               23: "Pending Verification", 24: "Synced", 25: now_str()}
        self.mirror.audit_event("processed_update", audit_row, upd)
        if self.live:
            self.gio.audit_update(audit_row, upd, "processed")

        # [M100] items loop
        for item in order.get("items", []) or []:
            try:
                self.process_item(order, order_id, item)
            except Exception as e:
                self.mirror.error(oid, f"item {item.get('id')}", e)
                self.stats.errors += 1
                log.error("Item %s on order %s raised: %s", item.get("id"), oid, e)

    def process_item(self, order, order_id, item):
        oid = str(order.get("id"))
        pid = dig(item, "product.id")
        ptype = str(item.get("product_type", ""))
        # [M101/M102/M103/M104]
        p = self.hs.item_search_product(pid)
        p_total = int(p.get("total", 0))
        p_first = (p.get("results") or [{}])[0]
        te = self.hs.item_search_template(pid, True)
        te_total = int(te.get("total", 0))
        tpl = (te.get("results") or [{}])[0]  # [M103] first eligible template
        ta_total = int(self.hs.item_search_template(pid, False).get("total", 0))

        price = str(dig(item, "amounts.price_without_tax.amount"))
        common = {
            "hs_sku": item.get("sku", ""), "salla_sku": item.get("sku", ""),
            "hs_url": dig(item, "product.url"),
            "hs_images": dig(item, "product.thumbnail"),
            "quantity": str(item.get("quantity", "")),
            "salla_order_id": oid, "salla_product_id": str(pid),
            "salla_order_item_id": str(item.get("id", "")),
            "hs_line_item_currency_code": item.get("currency", ""),
        }

        # Route 30.2: Salla native bundle [M150..M158]
        if ptype == "group_products":
            self._route_salla_native(order, order_id, item, common, price, p_first)
            return
        # Route 30.1: eligible HubSpot template [M120..M130]
        if te_total > 0:
            self._route_bundle_template(order, order_id, item, common, price, tpl, p_first)
            return
        # Route 30.0: standalone [M110/M232/M111]
        if p_total > 0 and te_total == 0 and ta_total == 0:
            props = dict(common)
            props.update({"name": dig(p_first, "properties.name", item.get("name", "")),
                          "price": price, "sale_context": "standalone_product",
                          "salla_currency": item.get("currency", ""),
                          "reporting_product_key": item.get("sku", ""),
                          "revenue_attribution_method": "standalone_revenue"})
            li = self.hs.create_line_item(props, "LI standalone")
            if not li:
                self.flag_partial(order_id, "Module 110: Create LI standalone", "create failed")
                return
            self.stats.li_standalone += 1
            self.hs.patch_line_item(li, {"hs_product_id": p_first.get("id", "")},
                                    "stamp product on LI")            # [M232]
            self.hs.associate("order", "line_items", order_id, li,
                              ASSOC_ORDER_LI, "HUBSPOT_DEFINED", "assoc order LI")  # [M111]
            return
        # Route 30.3: needs_review fallback [M140/M141/M142]
        if (p_total == 0 and te_total == 0 and ta_total == 0) or (ta_total > 0 and te_total == 0):
            props = dict(common)
            props.update({"name": item.get("name", ""), "price": price,
                          "sale_context": "needs_review",
                          "reporting_product_key": ifempty(item.get("sku", ""), str(pid)),
                          "revenue_attribution_method": "standalone_revenue"})
            li = self.hs.create_line_item(props, "LI needs_review")
            if li:
                self.stats.li_needs_review += 1
                self.hs.associate("order", "line_items", order_id, li,
                                  ASSOC_ORDER_LI, "HUBSPOT_DEFINED", "assoc order LI")
            self.flag_partial(order_id, "Route 4: Needs Review",
                              f"salla_product_id={pid} salla_item_id={item.get('id')} - item "
                              "could not be classified. Check HubSpot for a matching Product "
                              "or Active Bundle Template.")
            log.warning("needs_review item %s on order %s", item.get("id"), oid)

    def _route_bundle_template(self, order, order_id, item, common, price, tpl, p_first):
        oid = str(order.get("id"))
        tprops = tpl.get("properties", {})
        tkey = tprops.get("bundle_template_key", "")
        # [M120] bundle record
        bundle_props = {
            "bundle_instance_key": f"{oid}::{item.get('id')}::{tkey}",
            "bundle_name": f"{tprops.get('bundle_template_name','')} - Order #{oid}",
            "bundle_template_key": tkey,
            "bundle_template_name_snapshot": tprops.get("bundle_template_name", ""),
            "bundle_template_record_id": tpl.get("id", ""),
            "bundle_template_salla_id_snapshot": str(dig(item, "product.id")),
            "bundle_template_sku_snapshot": tprops.get("bundle_sku", ""),
            "bundle_sku": item.get("sku", ""),
            "bundle_product_id_snapshot": str(dig(item, "product.id")),
            "bundle_product_name_snapshot": item.get("name", ""),
            "bundle_quantity": str(item.get("quantity", "")),
            "bundle_unit_price": price,
            "bundle_revenue_attribution_method": "parent_bundle_revenue_only",
            "bundle_status": "purchased",
            "component_count": str(tprops.get("active_component_count", "")),
            "currency": item.get("currency", ""),
            "salla_order_id": oid,
            "salla_order_reference": str(ifempty(order.get("reference_id"), oid)),
            "salla_bundle_id": str(dig(item, "product.id")),
            "source_store": "Salla", "source_system": "Salla",
            "last_sync_status": "synced",
        }
        status, data = self.hs._write("POST", f"/crm/v3/objects/{OBJ_BUNDLE}",
                                      {"properties": bundle_props}, "create bundle")
        if status not in (200, 201):
            self.flag_partial(order_id, "Module 120: Create Bundle record",
                              json.dumps(data)[:200])
            return
        bundle_id = data.get("id")
        self.hs.associate(OBJ_BUNDLE_TEMPLATE, OBJ_BUNDLE, tpl.get("id", ""), bundle_id,
                          ASSOC_TPL_BUNDLE, "USER_DEFINED", "assoc tpl bundle")   # [M121]
        self.hs.associate(OBJ_BUNDLE, "order", bundle_id, order_id,
                          ASSOC_BUNDLE_ORDER, "USER_DEFINED", "assoc bundle order")  # [M122]
        # [M123] parent LI
        props = dict(common)
        props.update({"name": item.get("name", ""), "price": price,
                      "sale_context": "bundle_parent", "is_bundle_parent": True,
                      "bundle_template_key": tkey,
                      "reporting_product_key": tprops.get("bundle_sku", ""),
                      "revenue_attribution_method": "bundle_parent_revenue",
                      "bundle_template_name_snapshot": tprops.get("bundle_template_name", "")})
        parent_li = self.hs.create_line_item(props, "LI bundle parent")
        if not parent_li:
            self.flag_partial(order_id, "Module 123: Create LI bundle parent", "create failed")
            return
        self.stats.li_bundle_parent += 1
        self.hs.patch_line_item(parent_li, {"hs_product_id": p_first.get("id", "")},
                                "stamp parent LI")                                 # [M233]
        self.hs.associate("order", "line_items", order_id, parent_li,
                          ASSOC_ORDER_LI, "HUBSPOT_DEFINED", "assoc order parent")  # [M124]
        self.hs.associate(OBJ_BUNDLE, "line_items", bundle_id, parent_li,
                          ASSOC_BUNDLE_PARENT, "USER_DEFINED", "assoc bundle parent")  # [M125]
        # [M126/M127/M128/M234/M129/M130] components
        for comp in self.hs.search_active_components(tkey):
            cp = comp.get("properties", {})
            try:
                qty = float(item.get("quantity", 1)) * float(cp.get("quantity_in_bundle", 1) or 1)
                qty = int(qty) if qty == int(qty) else qty
            except (TypeError, ValueError):
                qty = item.get("quantity", 1)
            cprops = {
                "name": cp.get("component_product_name_snapshot", ""), "price": "0",
                "hs_sku": cp.get("component_product_sku", ""),
                "salla_sku": cp.get("component_product_sku", ""),
                "quantity": str(qty), "sale_context": "bundle_component",
                "salla_order_id": oid,
                "salla_product_id": str(cp.get("component_salla_product_id", "")),
                "bundle_template_key": tkey, "is_bundle_component": True,
                "salla_order_item_id": f"{item.get('id')}_{cp.get('component_code','')}",
                "reporting_product_key": cp.get("component_product_sku", ""),
                "hs_line_item_currency_code": item.get("currency", ""),
                "revenue_attribution_method": "component_quantity_only",
                "allocated_component_revenue": "0",
                "bundle_template_name_snapshot": tprops.get("bundle_template_name", ""),
            }
            comp_li = self.hs.create_line_item(cprops, "LI component")
            if not comp_li:
                self.flag_partial(order_id, "Module 128: Create LI bundle component",
                                  "create failed")
                continue
            self.stats.li_component += 1
            self.hs.patch_line_item(comp_li,
                                    {"hs_product_id": cp.get("component_hubspot_product_id", "")},
                                    "stamp component LI")                          # [M234]
            self.hs.associate("order", "line_items", order_id, comp_li,
                              ASSOC_ORDER_LI, "HUBSPOT_DEFINED", "assoc order comp")   # [M129]
            self.hs.associate(OBJ_BUNDLE, "line_items", bundle_id, comp_li,
                              ASSOC_BUNDLE_COMP, "USER_DEFINED", "assoc bundle comp")  # [M130]

    def _route_salla_native(self, order, order_id, item, common, price, p_first):
        oid = str(order.get("id"))
        pid = str(dig(item, "product.id"))
        consisted = item.get("consisted_products", []) or []
        # [M150] bundle record from the Salla group product itself
        bundle_props = {
            "bundle_instance_key": f"{oid}::{item.get('id')}::{pid}",
            "bundle_name": f"{item.get('name','')} - Order #{oid}",
            "bundle_template_key": pid,
            "bundle_template_name_snapshot": item.get("name", ""),
            "bundle_template_salla_id_snapshot": pid,
            "bundle_sku": item.get("sku", ""),
            "bundle_product_id_snapshot": pid,
            "bundle_product_name_snapshot": item.get("name", ""),
            "bundle_quantity": str(item.get("quantity", "")),
            "bundle_unit_price": price,
            "bundle_revenue_attribution_method": "parent_bundle_revenue_only",
            "bundle_status": "purchased",
            "component_count": str(len(consisted)),
            "currency": item.get("currency", ""),
            "salla_order_id": oid,
            "salla_order_reference": str(ifempty(order.get("reference_id"), oid)),
            "salla_bundle_id": pid,
            "source_store": "Salla", "source_system": "Salla",
            "last_sync_status": "synced",
        }
        status, data = self.hs._write("POST", f"/crm/v3/objects/{OBJ_BUNDLE}",
                                      {"properties": bundle_props}, "create bundle salla")
        if status not in (200, 201):
            self.flag_partial(order_id, "Module 150: Create Bundle record (Salla-native)",
                              json.dumps(data)[:200])
            return
        bundle_id = data.get("id")
        self.hs.associate(OBJ_BUNDLE, "order", bundle_id, order_id,
                          ASSOC_BUNDLE_ORDER, "USER_DEFINED", "assoc bundle order")  # [M151]
        # [M152] parent LI
        props = dict(common)
        props.update({"name": item.get("name", ""), "price": price,
                      "sale_context": "bundle_parent", "is_bundle_parent": True,
                      "bundle_template_key": pid,
                      "reporting_product_key": ifempty(item.get("sku", ""), pid),
                      "revenue_attribution_method": "bundle_parent_revenue",
                      "bundle_template_name_snapshot": item.get("name", "")})
        parent_li = self.hs.create_line_item(props, "LI bundle parent salla")
        if not parent_li:
            self.flag_partial(order_id, "Module 152: Create LI (bundle parent, Salla)",
                              "create failed")
            return
        self.stats.li_bundle_parent += 1
        self.hs.patch_line_item(parent_li, {"hs_product_id": p_first.get("id", "")},
                                "stamp parent LI salla")                           # [M235]
        self.hs.associate("order", "line_items", order_id, parent_li,
                          ASSOC_ORDER_LI, "HUBSPOT_DEFINED", "assoc order parent")  # [M153]
        self.hs.associate(OBJ_BUNDLE, "line_items", bundle_id, parent_li,
                          ASSOC_BUNDLE_PARENT, "USER_DEFINED", "assoc bundle parent")  # [M154]
        # [M155..M158] components straight from the Salla payload
        for cp in consisted:
            try:
                qty = float(item.get("quantity", 1)) * float(cp.get("quantity_in_group", 1) or 1)
                qty = int(qty) if qty == int(qty) else qty
            except (TypeError, ValueError):
                qty = item.get("quantity", 1)
            cprops = {
                "name": cp.get("name", ""), "price": "0",
                "hs_sku": cp.get("sku", ""), "salla_sku": cp.get("sku", ""),
                "hs_url": cp.get("url", ""), "hs_images": cp.get("thumbnail", ""),
                "quantity": str(qty), "sale_context": "bundle_component",
                "salla_order_id": oid, "salla_product_id": str(cp.get("id", "")),
                "bundle_template_key": pid, "is_bundle_component": True,
                "salla_order_item_id": f"{item.get('id')}_{cp.get('id','')}",
                "reporting_product_key": cp.get("sku", ""),
                "parent_salla_bundle_id": pid,
                "hs_line_item_currency_code": cp.get("currency", item.get("currency", "")),
                "revenue_attribution_method": "component_quantity_only",
                "allocated_component_revenue": "0",
                "bundle_template_name_snapshot": item.get("name", ""),
            }
            comp_li = self.hs.create_line_item(cprops, "LI component salla")
            if not comp_li:
                self.flag_partial(order_id, "Module 156: Create LI (bundle component, Salla)",
                                  "create failed")
                continue
            self.stats.li_component += 1
            self.hs.associate("order", "line_items", order_id, comp_li,
                              ASSOC_ORDER_LI, "HUBSPOT_DEFINED", "assoc order comp")   # [M157]
            self.hs.associate(OBJ_BUNDLE, "line_items", bundle_id, comp_li,
                              ASSOC_BUNDLE_COMP, "USER_DEFINED", "assoc bundle comp")  # [M158]

    # -- one order end to end ---------------------------------------------------

    def process_order(self, order):
        oid = str(order.get("id"))
        self._rates_report()  # v1.4
        log.info("ORDER begin %s ref %s items=%d", oid,
                 ifempty(order.get("reference_id"), oid),
                 len(order.get("items", []) or []))  # v1.2 observability, log only
        # [M250] serialize (compact) and archive locally
        json_text = json.dumps(order, ensure_ascii=False, separators=(",", ":"))
        ref = ifempty(order.get("reference_id"), order.get("id"))
        filename = (f"order_RID{ref}_{order.get('id')}_"
                    f"{order.get('payment_method','')}_"
                    f"{str(dig(order,'date.date'))[:10]}.json")
        Path(self.cfg.archive_dir).mkdir(parents=True, exist_ok=True)
        (Path(self.cfg.archive_dir) / filename).write_text(json_text, encoding="utf-8")
        # [M255] Drive upload (live only)
        link = self.gio.drive_upload_json(filename, json_text) if self.live else ""
        # [M203] audit row
        items = order.get("items", []) or []
        add = {0: oid, 1: str(order.get("reference_id", "")),
               2: dig(order, "date.date"),
               3: dig(order, "customer.full_name"),
               4: dig(order, "customer.email"),
               5: f"{dig(order,'customer.mobile_code')}{dig(order,'customer.mobile')}",
               6: str(dig(order, "amounts.total.amount")),
               7: dig(order, "amounts.shipping_cost.currency"),
               8: order.get("payment_method", ""),
               9: str(len(items)),
               10: ",".join(str(i.get("name", "")) for i in items),
               11: "Order Arrived",
               27: link if link else "Drive upload failed",
               29: now_str(), 30: "Yes"}
        audit_row = self.gio.audit_append(add) if self.live else -1
        self.mirror.audit_event("arrived_append", audit_row, add)

        # gate [M209..M215] then router [M216]
        unverified = self.gate_unverified_items(order)
        if not self.live:
            plan = "HELD" if unverified else "CREATE"
            routes = []
            if not unverified:
                for item in items:
                    pid = dig(item, "product.id")
                    if str(item.get("product_type", "")) == "group_products":
                        routes.append(f"{item.get('id')}=salla_native({len(item.get('consisted_products') or [])} comps)")
                        continue
                    te = int(self.hs.item_search_template(pid, True).get("total", 0))
                    ta = int(self.hs.item_search_template(pid, False).get("total", 0))
                    p = int(self.hs.item_search_product(pid).get("total", 0))
                    if te > 0:
                        routes.append(f"{item.get('id')}=bundle_template")
                    elif p > 0 and ta == 0:
                        routes.append(f"{item.get('id')}=standalone")
                    else:
                        routes.append(f"{item.get('id')}=needs_review")
            log.info("DRY RUN plan for order %s (ref %s): %s%s", oid, ref, plan,
                     f" [{'; '.join(routes)}]" if routes else
                     f" unverified={[u['name'] for u in unverified]}")
            return
        if unverified:
            self.route_held(order, audit_row, unverified)
        else:
            self.route_create(order, audit_row)

    # -- main loop ---------------------------------------------------------------

    def run(self, max_pages=None, max_orders=None):
        t0 = time.monotonic()
        orders_done = 0
        while True:
            if self._should_stop():
                break
            if self.cursor.status in ("done", "done_overflow"):  # [M300 filter]
                log.info("Cursor status is %s: nothing to do", self.cursor.status)
                break
            if max_pages is not None and self.stats.pages >= max_pages:
                log.info("Reached --max-pages=%s", max_pages)
                break
            start, end = self.cursor.slot_window(self.cfg.slot_hours)
            page = int(self.cursor.data["next_page"])
            path = (f"orders?from_date={start.strftime('%Y-%m-%dT%H:%M:%S')}"
                    f"&to_date={end.strftime('%Y-%m-%dT%H:%M:%S')}"
                    f"&per_page={self.cfg.per_page}&format=light&page={page}")
            log.info("PAGE slot %s -> %s page %s", start, end, page)
            self._rates_report()  # v1.4
            body = self.relay.get_path(path)                    # [M302]
            total_pages = int(dig(body, "pagination.totalPages", 0) or 0)
            ids = [str(o.get("id")) for o in body.get("data", []) or []]
            self.stats.pages += 1
            log.info("Slot reports totalPages=%s, page has %s order(s)", total_pages, len(ids))
            if total_pages > self.cfg.overflow_pages:
                log.error("OVERFLOW: slot %s reports %s pages (limit %s). Sticky flag set.",
                          start, total_pages, self.cfg.overflow_pages)

            new_ids = []
            for oid in ids:                                     # [M313/M310]
                if self._should_stop():
                    break
                if self.hs.dedup_order_exists(oid):
                    self.stats.skipped_existing += 1
                    log.debug("skip existing %s", oid)
                else:
                    new_ids.append(oid)
            self.stats.scanned += len(ids)

            orders = self.relay.fetch_orders(new_ids) if new_ids else {}  # [M304]
            for oid in new_ids:
                if self._should_stop():
                    break
                if max_orders is not None and orders_done >= max_orders:
                    break
                order = orders.get(oid)
                if not order:
                    self.mirror.error(oid, "fetch", "full order missing from relay batch")
                    self.stats.errors += 1
                    continue
                try:
                    self.process_order(order)
                except Exception as e:
                    self.mirror.error(oid, "process", e)
                    self.stats.errors += 1
                    log.exception("Order %s failed: %s", oid, e)
                orders_done += 1

            hit_order_cap = max_orders is not None and orders_done >= max_orders
            if self._should_stop() or hit_order_cap:
                log.warning("Halting BEFORE cursor advance: page %s of slot %s will be "
                            "rescanned on resume (dedup makes that free).", page, start)
                break
            if self.live:                                       # [M501]
                self.cursor.advance(total_pages, self.cfg.slot_hours, page,
                                    self.cfg.overflow_pages)
                log.info("Cursor advanced: %s", json.dumps(self.cursor.data))
            else:
                log.info("DRY RUN: cursor NOT advanced")
                break  # a dry run inspects exactly one page

        dur = time.monotonic() - t0
        s = self.stats
        log.info("=" * 68)
        log.info("RUN SUMMARY  duration=%.1f min  live=%s", dur / 60, self.live)
        log.info("pages=%d scanned=%d skipped_existing=%d created=%d held=%d",
                 s.pages, s.scanned, s.skipped_existing, s.created, s.held)
        log.info("line items: standalone=%d bundle_parent=%d component=%d needs_review=%d",
                 s.li_standalone, s.li_bundle_parent, s.li_component, s.li_needs_review)
        log.info("errors=%d (see %s)", s.errors, self.mirror.errors)
        log.info("cursor: %s", json.dumps(self.cursor.data))
        log.info("=" * 68)


# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------

def setup_logging(verbose):
    fmt = "%(asctime)s %(levelname)-7s %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=fmt,
                        handlers=[logging.FileHandler("backfill.log", encoding="utf-8")])
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(logging.Formatter(fmt))
    logging.getLogger().addHandler(console)
    logging.getLogger("googleapiclient").setLevel(logging.WARNING)
    logging.getLogger("google_auth_oauthlib").setLevel(logging.WARNING)


def main():
    ap = argparse.ArgumentParser(description="Salla -> HubSpot local backfill engine")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--live", action="store_true",
                    help="Perform real writes. Without this flag the run is a dry run.")
    ap.add_argument("--max-orders", type=int, default=None,
                    help="Stop after processing N new orders (test gate)")
    ap.add_argument("--max-pages", type=int, default=None)
    ap.add_argument("--no-google", action="store_true",
                    help="Skip Drive and Sheets writes even in live mode (local mirrors only)")
    ap.add_argument("--status", action="store_true", help="Print cursor state and exit")
    ap.add_argument("--verbose", action="store_true", help="DEBUG on console")
    args = ap.parse_args()

    setup_logging(args.verbose)
    # v1.2.1: googleapiclient/httplib2 defaults to no socket timeout; a stalled
    # Drive/Sheets socket would hang the engine forever. Global floor:
    socket.setdefaulttimeout(180)
    cfg = Config.load(args.config)
    apply_portal_config(cfg)
    cursor = Cursor(cfg.state_file)
    if args.status:
        print(json.dumps(cursor.data, indent=2))
        return

    token = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    secret = os.environ.get("RELAY_SECRET", "")
    if not token or not secret:
        sys.exit("Set HUBSPOT_ACCESS_TOKEN and RELAY_SECRET in the environment first.")

    if args.live:
        confirm = input(f"LIVE RUN against your HubSpot portal "
                        f"(max_orders={args.max_orders}, max_pages={args.max_pages}). "
                        f"Type RUN to proceed: ")
        if confirm.strip() != "RUN":
            sys.exit("Aborted.")

    google_enabled = args.live and not args.no_google and cfg.google_enabled
    gio = GoogleIO(cfg, enabled=google_enabled)
    mirror = LocalMirror("mirror")
    relay = RelayClient(cfg, secret)
    hs = HubSpot(cfg, token, live=args.live)
    engine = Engine(cfg, cursor, relay, hs, gio, mirror, live=args.live)
    engine.run(max_pages=args.max_pages, max_orders=args.max_orders)


if __name__ == "__main__":
    main()
