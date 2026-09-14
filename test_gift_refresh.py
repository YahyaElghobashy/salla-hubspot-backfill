#!/usr/bin/env python3
"""Unit tests for the v2.9 gift address refresh (gift_refresh.py).

Everything runs offline against fakes. The behaviors under test are the ones
the incident depends on: the working-set search converging by property, one
exact PATCH per confirmation, honest terminal states (the boolean never lies),
alert-once across restarts, degraded payloads never deciding anything, and the
module writing nothing at all in dry runs. All fixtures are synthetic.
"""
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from backfill import Config, gift_address_unconfirmed, gift_props
import gift_refresh
from gift_refresh import (GiftLedger, GiftState, _confirm_patch, run_cycle)


def _chdir_tmp(test):
    tmp = tempfile.TemporaryDirectory()
    test.addCleanup(tmp.cleanup)
    old = os.getcwd()
    os.chdir(tmp.name)
    test.addCleanup(os.chdir, old)
    Path("mirror").mkdir()
    return tmp.name


def _cfg(**over):
    cfg = Config()
    cfg.gift_refresh_enabled = True
    cfg.gift_refresh_batch = 48
    cfg.gift_refresh_search_limit = 100
    cfg.gift_stale_terminal_days = 120
    cfg.gift_hydrate_fail_max = 3
    cfg.record_url_base = "https://example.test/orders"
    for k, v in over.items():
        setattr(cfg, k, v)
    return cfg


def _payload(sid, confirmed=True, expiry="2099-01-01 00:00:00", gift=True,
             full=True, key_absent=False):
    if not gift:
        return {"id": int(sid), "type": "order", "source": "store",
                "status": {"slug": "completed"},
                "amounts": {"total": {"amount": 10}},
                "customer": {"first_name": "Sara"}}
    p = {"id": int(sid), "type": "gift", "source": "buy_as_gift",
         "receiver": {"name": "نور ❤️", "phone": "+966500000001",
                      "email": "", "notify": False},
         "gift": {"text": "هدية", "expiry_date": expiry},
         "urls": {"gift_confirmation": "https://clarahair.test/gifts/TOKEN"}}
    if full:
        p["status"] = {"slug": "completed"}
        p["amounts"] = {"total": {"amount": 10}}
    if confirmed:
        if not key_absent:
            p["address_incomplete"] = False
        p["shipping"] = {"address": {"city": "جدة", "country": "SA",
                                     "phone": "+966500000002"}}
    else:
        if not key_absent:
            p["address_incomplete"] = True
    return p


class FakeHS:
    """Stores order property state and answers the working-set search from it,
    so a PATCH genuinely removes an order from the next page -- the property
    convergence the module relies on."""

    def __init__(self, orders=None):
        # salla_id -> {"id": hs_id, "props": {...}}
        self.orders = dict(orders or {})
        self.searches = []
        self.patches = []
        self.fail_patch_for = set()   # hs ids that reject with HTTP 400

    def search(self, path, body, what):
        self.searches.append((path, body))
        rows = []
        for sid, o in sorted(self.orders.items(),
                             key=lambda kv: kv[1]["props"].get("hs_createdate", "")):
            p = o["props"]
            if (p.get("is_gift_order") == "true"
                    and p.get("gift_address_incomplete") == "true"
                    and not p.get("gift_address_state")):
                rows.append({"id": o["id"], "properties": dict(p)})
        limit = body.get("limit") or len(rows)
        return {"total": len(rows), "results": rows[:limit]}

    def update_order(self, hs_id, props, what):
        self.patches.append((str(hs_id), dict(props)))
        if str(hs_id) in self.fail_patch_for:
            return 400, {"message": "PROPERTY_VALIDATION"}
        for o in self.orders.values():
            if str(o["id"]) == str(hs_id):
                o["props"].update(props)
        return 200, {"id": str(hs_id)}


class FakeRelay:
    def __init__(self, payloads=None, error=False):
        self.payloads = dict(payloads or {})
        self.error = error
        self.calls = []

    def fetch_orders(self, ids):
        if self.error:
            raise gift_refresh.RelayError("relay down")
        self.calls.append(list(ids))
        return {str(i): self.payloads[str(i)] for i in ids
                if str(i) in self.payloads}


def _order(sid, hs_id=None, created="2026-09-01", expiry="",
           state="", incomplete="true"):
    return (str(sid), {"id": hs_id or f"HS{sid}", "props": {
        "salla_order_id": str(sid),
        "salla_order_reference": f"R{sid}",
        "hs_createdate": f"{created}T00:00:00Z",
        "gift_confirmation_expiry": expiry,
        "gift_confirmation_url": "https://clarahair.test/gifts/TOKEN",
        "gift_receiver_name": "نور ❤️",
        "is_gift_order": "true",
        "gift_address_incomplete": incomplete,
        "gift_address_state": state,
    }})


def _run(hs, relay, cfg=None, live=True):
    ledger = GiftLedger()
    state = GiftState()
    with mock.patch.object(gift_refresh, "send_alert") as alert:
        metrics = run_cycle(cfg or _cfg(), hs, relay, ledger, state, live=live)
    return metrics, ledger, state, alert


class SearchScoping(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_working_set_filters_sort_limit(self):
        hs = FakeHS(dict([_order(1)]))
        _run(hs, FakeRelay({"1": _payload(1)}))
        _, body = hs.searches[0]
        filters = body["filterGroups"][0]["filters"]
        by_prop = {f["propertyName"]: f for f in filters}
        self.assertEqual(by_prop["is_gift_order"]["value"], "true")
        self.assertEqual(by_prop["gift_address_incomplete"]["value"], "true")
        self.assertEqual(by_prop["gift_address_state"]["operator"],
                         "NOT_HAS_PROPERTY")
        self.assertEqual(len(body["filterGroups"]), 1)  # AND, not OR
        self.assertEqual(body["sorts"][0]["direction"], "ASCENDING")
        self.assertEqual(body["limit"], 100)


class ConfirmedOrders(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_clears_and_rebakes(self):
        hs = FakeHS(dict([_order(1)]))
        m, ledger, _, alert = _run(hs, FakeRelay({"1": _payload(1)}))
        self.assertEqual(len(hs.patches), 1)
        hs_id, props = hs.patches[0]
        self.assertEqual(hs_id, "HS1")
        self.assertEqual(props["gift_address_incomplete"], "false")
        self.assertEqual(props["gift_address_state"], "confirmed")
        self.assertEqual(props["gift_receiver_name"], "نور ❤️")
        self.assertEqual(props["hs_shipping_address_city"], "جدة")
        self.assertEqual(props["hs_shipping_address_country"], "SA")
        self.assertEqual(props["hs_shipping_address_phone"], "+966500000002")
        self.assertNotIn("hs_billing_address_city", props)
        self.assertEqual(ledger.outcome["1"], "cleared")
        alert.assert_not_called()

    def test_patch_derives_from_gift_props(self):
        pay = _payload(1)
        base = gift_props(pay)
        patch = _confirm_patch(pay)
        for k, v in base.items():
            if k != "gift_address_incomplete":
                self.assertEqual(patch[k], v)

    def test_empty_values_never_blank_existing(self):
        self.assertNotIn("gift_receiver_email", _confirm_patch(_payload(1)))

    def test_key_absent_with_address_is_confirmed(self):
        hs = FakeHS(dict([_order(1)]))
        _run(hs, FakeRelay({"1": _payload(1, key_absent=True)}))
        self.assertEqual(hs.patches[0][1]["gift_address_state"], "confirmed")

    def test_receiver_phone_swap_last_write_wins(self):
        pay = _payload(1)
        pay["receiver"]["phone"] = "+966500000009"
        pay["shipping"]["address"].pop("phone")
        patch = _confirm_patch(pay)
        self.assertEqual(patch["gift_receiver_phone"], "+966500000009")
        self.assertEqual(patch["hs_shipping_address_phone"], "+966500000009")

    def test_shipping_phone_priority_matches_live_shape(self):
        # live-verified: shipping.address carries no phone; the delivery
        # contact is shipping.receiver.phone (falls back to the gift phone)
        pay = _payload(1)
        pay["shipping"]["address"].pop("phone")
        pay["shipping"]["receiver"] = {"name": "x", "phone": "966500000003"}
        patch = _confirm_patch(pay)
        self.assertEqual(patch["hs_shipping_address_phone"], "966500000003")

    def test_cleared_exits_next_search(self):
        hs = FakeHS(dict([_order(1)]))
        relay = FakeRelay({"1": _payload(1)})
        _run(hs, relay)
        m2, _, _, _ = _run(hs, relay)
        self.assertEqual(len(hs.patches), 1)  # no second patch
        self.assertEqual(m2["pending"], 0)


class PendingAndExpired(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_still_unexpired_is_noop(self):
        hs = FakeHS(dict([_order(1)]))
        m, ledger, _, alert = _run(
            hs, FakeRelay({"1": _payload(1, confirmed=False)}))
        self.assertEqual(hs.patches, [])
        self.assertEqual(ledger.outcome, {})
        alert.assert_not_called()
        self.assertEqual(m["pending"], 1)

    def test_expired_goes_terminal_bool_untouched(self):
        hs = FakeHS(dict([_order(1)]))
        m, ledger, _, alert = _run(hs, FakeRelay(
            {"1": _payload(1, confirmed=False, expiry="2026-01-01 00:00:00")}))
        self.assertEqual(len(hs.patches), 1)
        self.assertEqual(hs.patches[0][1],
                         {"gift_address_state": "expired_unconfirmed"})
        self.assertEqual(
            hs.orders["1"]["props"]["gift_address_incomplete"], "true")
        self.assertEqual(ledger.outcome["1"], "expired")
        self.assertEqual(alert.call_count, 1)
        subject = alert.call_args[0][0]
        self.assertNotIn("+966", subject)          # no phones in the headline
        self.assertIn("TOKEN", alert.call_args[0][1])  # url in the thread

    def test_expiry_aggregates_to_one_alert(self):
        orders = dict([_order(i) for i in (1, 2, 3)])
        pays = {str(i): _payload(i, confirmed=False,
                                 expiry="2026-01-01 00:00:00")
                for i in (1, 2, 3)}
        hs = FakeHS(orders)
        _, _, _, alert = _run(hs, FakeRelay(pays))
        self.assertEqual(alert.call_count, 1)
        self.assertIn("3 gift orders", alert.call_args[0][0])

    def test_alert_once_across_restart_and_index_lag(self):
        hs = FakeHS(dict([_order(1)]))
        relay = FakeRelay(
            {"1": _payload(1, confirmed=False, expiry="2026-01-01 00:00:00")})
        _run(hs, relay)
        # simulate index lag: force the patched order back into the page
        hs.orders["1"]["props"]["gift_address_state"] = ""
        _, _, _, alert2 = _run(hs, relay)   # fresh ledger over same mirror
        alert2.assert_not_called()
        self.assertEqual(len(hs.patches), 1)      # not re-patched
        self.assertEqual(len(relay.calls), 1)     # not re-hydrated

    def test_missing_expiry_ages_out(self):
        hs = FakeHS(dict([_order(1, created="2026-01-01")]))
        pay = _payload(1, confirmed=False)
        del pay["gift"]["expiry_date"]
        _, ledger, _, _ = _run(hs, FakeRelay({"1": pay}))
        self.assertEqual(ledger.outcome["1"], "expired")

    def test_missing_expiry_young_stays_pending(self):
        hs = FakeHS(dict([_order(1, created="2026-09-10")]))
        pay = _payload(1, confirmed=False)
        del pay["gift"]["expiry_date"]
        m, ledger, _, _ = _run(hs, FakeRelay({"1": pay}))
        self.assertEqual(ledger.outcome, {})
        self.assertEqual(m["pending"], 1)


class RecheckRotation(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_never_checked_orders_beat_sticky_pendings(self):
        cfg = _cfg(gift_refresh_batch=1)
        # order 1 is older (page front) but stays pending; order 2 never checked
        hs = FakeHS(dict([_order(1, created="2026-01-05"),
                          _order(2, created="2026-06-01")]))
        pays = {"1": _payload(1, confirmed=False), "2": _payload(2)}
        relay = FakeRelay(pays)
        l = GiftLedger(); s = GiftState()
        with mock.patch.object(gift_refresh, "send_alert"):
            run_cycle(cfg, hs, relay, l, s, live=True)   # checks 1, pending
            run_cycle(cfg, hs, relay, l, s, live=True)   # must pick 2, not 1
        self.assertEqual(l.outcome.get("2"), "cleared")
        self.assertEqual(relay.calls[0], ["1"])
        self.assertEqual(relay.calls[1], ["2"])


class DegradedPayloads(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_fragment_never_decides(self):
        hs = FakeHS(dict([_order(1, expiry="2026-01-01")]))
        frag = _payload(1, confirmed=True, full=False)
        m, ledger, _, alert = _run(hs, FakeRelay({"1": frag}))
        self.assertEqual(hs.patches, [])
        self.assertEqual(ledger.outcome, {})
        alert.assert_not_called()

    def test_id_mismatch_is_not_full(self):
        hs = FakeHS(dict([_order(1)]))
        wrong = _payload(2)
        wrong["id"] = 2
        m, ledger, _, _ = _run(hs, FakeRelay({"1": wrong}))
        self.assertEqual(hs.patches, [])

    def test_fetch_miss_retries_then_parks(self):
        cfg = _cfg(gift_hydrate_fail_max=2)
        hs = FakeHS(dict([_order(1)]))
        relay = FakeRelay({})     # never returns the order
        l1 = GiftLedger(); s1 = GiftState()
        with mock.patch.object(gift_refresh, "send_alert"):
            run_cycle(cfg, hs, relay, l1, s1, live=True)
        self.assertEqual(l1.outcome, {})
        l2 = GiftLedger(); s2 = GiftState()   # fresh process, same mirror
        with mock.patch.object(gift_refresh, "send_alert"):
            run_cycle(cfg, hs, relay, l2, s2, live=True)
        self.assertEqual(l2.outcome["1"], "unfetchable")
        self.assertEqual(hs.patches, [])      # parked, never patched

    def test_not_gift_parks_without_writes(self):
        hs = FakeHS(dict([_order(1)]))
        _, ledger, _, alert = _run(hs, FakeRelay({"1": _payload(1, gift=False)}))
        self.assertEqual(hs.patches, [])
        self.assertEqual(ledger.outcome["1"], "not_gift")
        alert.assert_not_called()

    def test_relay_error_aborts_cleanly(self):
        hs = FakeHS(dict([_order(1)]))
        m, ledger, _, alert = _run(hs, FakeRelay(error=True))
        self.assertEqual(hs.patches, [])
        self.assertEqual(ledger.outcome, {})
        alert.assert_not_called()


class PatchFailures(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_failure_keeps_pending_no_ledger(self):
        hs = FakeHS(dict([_order(1)]))
        hs.fail_patch_for.add("HS1")
        _, ledger, _, _ = _run(hs, FakeRelay({"1": _payload(1)}))
        self.assertEqual(ledger.outcome, {})   # stays in the working set

    def test_persistent_failure_alerts_once(self):
        cfg = _cfg(gift_hydrate_fail_max=2)
        hs = FakeHS(dict([_order(1)]))
        hs.fail_patch_for.add("HS1")
        relay = FakeRelay({"1": _payload(1)})
        calls = 0
        for _ in range(4):
            l = GiftLedger(); s = GiftState()
            with mock.patch.object(gift_refresh, "send_alert") as a:
                run_cycle(cfg, hs, relay, l, s, live=True)
            calls += a.call_count
        self.assertEqual(calls, 1)


class DryRun(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_dry_never_writes_locally(self):
        hs = FakeHS(dict([
            _order(1),
            _order(2, expiry="2026-01-01"),
        ]))
        pays = {"1": _payload(1),
                "2": _payload(2, confirmed=False, expiry="2026-01-01 00:00:00")}
        _, ledger, state, alert = _run(hs, FakeRelay(pays), live=False)
        self.assertEqual(ledger.outcome, {})
        self.assertFalse(Path("mirror/gift_refreshed.csv").exists())
        self.assertFalse(Path("mirror/gift_refresh_state.json").exists())
        alert.assert_not_called()


class DisabledConfig(unittest.TestCase):
    def test_default_config_is_inert(self):
        self.assertFalse(Config().gift_refresh_enabled)

    def test_main_exits_before_any_client(self):
        tmp = _chdir_tmp(self)
        Path("config.json").write_text(json.dumps({}))
        with mock.patch.object(gift_refresh, "HubSpot") as hs_cls, \
             mock.patch.object(gift_refresh, "RelayClient") as rc_cls, \
             mock.patch("sys.argv", ["gift_refresh.py", "--once", "--live"]):
            gift_refresh.main()
        hs_cls.assert_not_called()
        rc_cls.assert_not_called()


class StateAndDigest(unittest.TestCase):
    def setUp(self):
        self.tmp = _chdir_tmp(self)

    def test_state_file_written_live(self):
        hs = FakeHS(dict([_order(1)]))
        _run(hs, FakeRelay({"1": _payload(1)}))
        d = json.loads(Path("mirror/gift_refresh_state.json").read_text())
        self.assertEqual(d["cleared_total"], 1)
        self.assertTrue(d["enabled"])
        self.assertIn("ts", d)

    def test_digest_reads_state(self):
        import report_digest
        hs = FakeHS(dict([_order(1), _order(2, expiry="2026-01-01")]))
        pays = {"1": _payload(1, confirmed=False),
                "2": _payload(2, confirmed=False, expiry="2026-01-01 00:00:00")}
        _run(hs, FakeRelay(pays))
        with mock.patch.object(report_digest, "ROOT", Path(self.tmp)):
            gw = report_digest._gift_watch()
        self.assertFalse(gw["stale"])
        self.assertEqual(gw["pending"], 1)
        self.assertEqual(gw["expired_total"], 1)

    def test_digest_stale_state_says_not_recorded(self):
        import report_digest
        Path("mirror/gift_refresh_state.json").write_text(json.dumps(
            {"enabled": True, "ts": "2026-01-01 00:00:00", "pending": 7}))
        with mock.patch.object(report_digest, "ROOT", Path(self.tmp)):
            gw = report_digest._gift_watch()
        self.assertTrue(gw["stale"])

    def test_digest_disabled_state_is_silent(self):
        import report_digest
        Path("mirror/gift_refresh_state.json").write_text(json.dumps(
            {"enabled": False, "ts": "2026-09-14 00:00:00", "pending": 7}))
        with mock.patch.object(report_digest, "ROOT", Path(self.tmp)):
            self.assertIsNone(report_digest._gift_watch())


class CreationPolarity(unittest.TestCase):
    """The creation-time flag now derives from evidence, not key presence."""

    def test_truthy_key(self):
        self.assertTrue(gift_address_unconfirmed({"address_incomplete": True}))

    def test_explicit_false_trusted(self):
        self.assertFalse(gift_address_unconfirmed({"address_incomplete": False}))

    def test_absent_key_no_address_is_unconfirmed(self):
        self.assertTrue(gift_address_unconfirmed({"id": 1, "type": "gift"}))
        self.assertEqual(
            gift_props({"id": 1, "type": "gift"})["gift_address_incomplete"],
            "true")

    def test_absent_key_with_address_is_confirmed(self):
        pay = _payload(1, key_absent=True)
        self.assertFalse(gift_address_unconfirmed(pay))

    def test_shipments_variant(self):
        self.assertFalse(gift_address_unconfirmed(
            {"shipments": [{"address": {"city": "جدة"}}]}))
        self.assertTrue(gift_address_unconfirmed(
            {"shipments": [{"id": 5, "status": "created"}]}))

    def test_empty_address_block_is_unconfirmed(self):
        self.assertTrue(gift_address_unconfirmed(
            {"shipping": {"address": {"city": "", "country": None}}}))


class Stress(unittest.TestCase):
    def setUp(self):
        _chdir_tmp(self)

    def test_backlog_converges_bounded(self):
        cfg = _cfg()
        orders, pays = {}, {}
        for i in range(500):   # the watch backlog
            sid, o = _order(1000 + i, created=f"2026-{(i % 8) + 1:02d}-01")
            orders[sid] = o
            if i % 10 == 9:    # 10% expired unanswered
                pays[sid] = _payload(1000 + i, confirmed=False,
                                     expiry="2026-01-01 00:00:00")
            else:
                pays[sid] = _payload(1000 + i)
        for i in range(76):    # confirmed-at-creation orders: must never be touched
            sid, o = _order(2000 + i, incomplete="false")
            orders[sid] = o
        hs = FakeHS(orders)
        relay = FakeRelay(pays)
        t0 = time.time()
        cycles = alerts = 0
        l = GiftLedger(); s = GiftState()
        while cycles < 20:
            with mock.patch.object(gift_refresh, "send_alert") as a:
                m = run_cycle(cfg, hs, relay, l, s, live=True)
            alerts_this = a.call_count
            self.assertLessEqual(alerts_this, 1)
            alerts += alerts_this
            cycles += 1
            if m["pending"] == 0:
                break
        self.assertLessEqual(cycles, -(-500 // cfg.gift_refresh_batch) + 1)
        self.assertLess(time.time() - t0, 10)
        self.assertEqual(len(l.outcome), 500)
        touched = {hs.orders[k]["id"] for k, _ in
                   [(sid, o) for sid, o in hs.orders.items()
                    if sid.startswith("2")]}
        patched_ids = {hid for hid, _ in hs.patches}
        self.assertFalse(touched & patched_ids)   # flag=false orders untouched


if __name__ == "__main__":
    unittest.main(verbosity=1)
