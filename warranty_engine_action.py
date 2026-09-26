# ── ClaraHair warranty engine ────────────────────────────────────────────────
# Runs as a HubSpot custom-coded workflow action on the Order object, on the
# workflow "Warranty · Activate on Delivery".
#
# One warranty record per warranted device UNIT on the delivered order. A line
# item with quantity 3 produces units u1, u2, u3, each its own record, so a
# customer whose second unit fails has a record that can be claimed or voided
# without touching the first (serial_number and claim_count live per unit).
# A HubSpot workflow cannot iterate associated records, which is why this is code
# and not a branch: an order with two devices must produce two warranties, and a
# device inside a bundle must produce one of its own.
#
# Requires a workflow secret WARRANTY_ENGINE_TOKEN (private-app token with
# crm.objects.orders.read, crm.objects.line_items.read, crm.objects.products.read,
# crm.objects.custom.write, crm.objects.contacts.read).
#
# The double-brace placeholders below are filled by 04_build_activation_workflow.py
# from warranty_object.json, so no id is ever hardcoded in two places.

import os
import datetime
import requests

TOKEN = os.getenv("WARRANTY_ENGINE_TOKEN")
BASE = "https://api.hubapi.com"
HDR = {"Authorization": "Bearer " + str(TOKEN), "Content-Type": "application/json"}

WARRANTY_OBJ = "2-252148104"
PIPELINE_ID = "4064600283"
STAGE_ACTIVE = "5913821399"
STAGE_EXPIRED = "5913821401"
ASSOC_ORDER = 113
ASSOC_ORDER_CAT = "USER_DEFINED"
ASSOC_LINE_ITEM = 117
ASSOC_LINE_ITEM_CAT = "USER_DEFINED"
ASSOC_CONTACT = 111
ASSOC_CONTACT_CAT = "USER_DEFINED"

ORDER_OBJ = "0-123"
LINE_ITEM_OBJ = "0-8"
PRODUCT_OBJ = "0-7"
CONTACT_OBJ = "0-1"

LI_PROPS = ["salla_product_id", "salla_sku", "salla_order_item_id", "quantity",
            "sale_context", "name", "bundle_template_name_snapshot",
            "hs_product_id", "hs_sku", "product_class", "warranty_months"]

# Line item contexts that must never produce a warranty. bundle_parent carries the
# BUNDLE sku rather than a real product - its device components are separate line
# items and get their own warranties.
SKIP_CONTEXTS = ("bundle_parent", "needs_review")


def _get(path, params=None):
    return requests.get(BASE + path, headers=HDR, params=params or {}, timeout=30)


def _post(path, body):
    return requests.post(BASE + path, headers=HDR, json=body, timeout=30)


def add_months(iso_date, months):
    """'2026-08-19' + 24 -> '2028-08-19', clamping to the last valid day of the month."""
    y, m, d = int(iso_date[0:4]), int(iso_date[5:7]), int(iso_date[8:10])
    total = (m - 1) + int(months)
    y += total // 12
    m = total % 12 + 1
    if m == 2:
        last = 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28
    elif m in (4, 6, 9, 11):
        last = 30
    else:
        last = 31
    return "%04d-%02d-%02d" % (y, m, min(d, last))


def main(event):
    order_id = str(event["object"]["objectId"])

    # ── 1. the order ────────────────────────────────────────────────────────
    r = _get("/crm/v3/objects/%s/%s" % (ORDER_OBJ, order_id), {
        "properties": "salla_order_id,salla_order_reference,delivery_date,hs_lastmodifieddate,"
                      "hs_source_store"})
    if r.status_code != 200:
        return {"outputFields": {"status": "order_fetch_failed",
                                 "detail": r.text[:200], "warranties_created": 0}}
    op = r.json().get("properties") or {}
    # v2.12 (2026-09-27): an imported Zid order never earns warranties from a live
    # stage change. Its history is settled; the backfill already covered the ones in
    # scope. Before this guard, a Salla status event landing on a Zid order that
    # shared its number minted phantom records (22 found, see zid_rekey.py).
    if (op.get("hs_source_store") or "").strip() == "Zid":
        return {"outputFields": {"status": "zid_order_skipped",
                                 "detail": "imported Zid order; no live warranties",
                                 "warranties_created": 0}}
    salla_order_id = op.get("salla_order_id") or order_id
    order_ref = op.get("salla_order_reference") or ""

    # delivery_date is written by a separate workflow enrolling on this same stage-change
    # event, so ordering between the two is not guaranteed and it may still be empty on
    # the first read. hs_lastmodifieddate is that same moment, so it is a safe stand-in
    # and removes the race rather than papering over it with a delay.
    start = (op.get("delivery_date") or op.get("hs_lastmodifieddate") or "")[:10]
    if len(start) != 10:
        return {"outputFields": {"status": "no_start_date", "warranties_created": 0}}

    # ── 2. line items on the order ──────────────────────────────────────────
    # Paginate: the v4 associations endpoint caps a page at 500, and silently returning
    # only the first page would drop line items on a very large order.
    li_ids, after = [], None
    while True:
        params = {"limit": 500}
        if after:
            params["after"] = after
        r = _get("/crm/v4/objects/%s/%s/associations/%s" % (ORDER_OBJ, order_id, LINE_ITEM_OBJ),
                 params)
        if r.status_code != 200:
            break
        body = r.json()
        li_ids += [x["toObjectId"] for x in (body.get("results") or [])]
        after = ((body.get("paging") or {}).get("next") or {}).get("after")
        if not after:
            break
    if not li_ids:
        return {"outputFields": {"status": "no_line_items", "warranties_created": 0}}

    items = []
    for i in range(0, len(li_ids), 100):
        rr = _post("/crm/v3/objects/%s/batch/read" % LINE_ITEM_OBJ,
                   {"properties": LI_PROPS,
                    "inputs": [{"id": str(x)} for x in li_ids[i:i + 100]]})
        if rr.status_code == 200:
            items += rr.json().get("results") or []

    candidates = []
    for it in items:
        p = it.get("properties") or {}
        if p.get("sale_context") in SKIP_CONTEXTS:
            continue
        # Any ONE of these identifies the product. Measured across all 290,484 line
        # items: hs_product_id set on 249,251, salla_product_id on 284,387, and
        # NEITHER on 0. Requiring salla_product_id alone silently dropped every
        # legacy LGCY- line item, which is the bug this replaces.
        if not (p.get("salla_product_id") or p.get("hs_product_id")
                or p.get("salla_sku") or p.get("hs_sku")):
            continue
        candidates.append(it)
    if not candidates:
        return {"outputFields": {"status": "no_candidate_items", "warranties_created": 0}}

    # ── 3. resolve class + term ────────────────────────────────────
    # The LINE ITEM is consulted first. HubSpot copies product properties onto a line
    # item at creation time, so anything created after the catalogue was tagged already
    # carries product_class and warranty_months, snapshotting the term as it stood when
    # the customer bought. That is the value we want, not whatever the product says
    # today. The catalogue is a fallback for line items created before tagging.
    by_salla, by_pid, by_sku = {}, {}, {}
    PROD_PROPS = ["salla_product_id", "hs_sku", "product_class", "warranty_months", "name"]

    def _search(prop, values, into):
        for i in range(0, len(values), 100):
            rr = _post("/crm/v3/objects/%s/search" % PRODUCT_OBJ, {
                "limit": 100, "properties": PROD_PROPS,
                "filterGroups": [{"filters": [{"propertyName": prop, "operator": "IN",
                                               "values": values[i:i + 100]}]}]})
            if rr.status_code == 200:
                for pr in rr.json().get("results") or []:
                    pp = pr.get("properties") or {}
                    k = pp.get(prop)
                    if k and str(k) not in into:
                        into[str(k)] = pp

    def _vals(getter):
        out = set()
        for it in candidates:
            pr = it["properties"]
            if pr.get("product_class") and pr.get("warranty_months") not in (None, ""):
                continue  # line item is already self-sufficient
            v = getter(pr)
            if v:
                out.add(str(v))
        return sorted(out)

    sids = _vals(lambda pr: pr.get("salla_product_id"))
    pids = _vals(lambda pr: pr.get("hs_product_id"))
    skus = _vals(lambda pr: pr.get("salla_sku") or pr.get("hs_sku"))
    if sids:
        _search("salla_product_id", sids, by_salla)
    if skus:
        _search("hs_sku", skus, by_sku)
    if pids:
        for i in range(0, len(pids), 100):
            rr = _post("/crm/v3/objects/%s/batch/read" % PRODUCT_OBJ, {
                "properties": PROD_PROPS,
                "inputs": [{"id": x} for x in pids[i:i + 100]]})
            if rr.status_code == 200:
                for pr in rr.json().get("results") or []:
                    by_pid[str(pr.get("id"))] = pr.get("properties") or {}

    def _meta(pr):
        """Line item wins; product fills any gap. None means 'not classifiable'."""
        cls = pr.get("product_class")
        months = pr.get("warranty_months")
        src = "line_item" if cls else None
        if not cls or months in (None, ""):
            for key, table in ((pr.get("salla_product_id"), by_salla),
                               (pr.get("hs_product_id"), by_pid),
                               (pr.get("salla_sku") or pr.get("hs_sku"), by_sku)):
                if key and str(key) in table:
                    pp = table[str(key)]
                    if not cls:
                        cls = pp.get("product_class")
                        src = src or "product"
                    if months in (None, ""):
                        months = pp.get("warranty_months")
                    break
        if not cls:
            return None
        return {"product_class": cls, "warranty_months": months, "source": src or "product"}

    # ── 4. build one payload per device line ────────────────────────────────
    # Compare against the REAL current date, not the delivery date. Using `start` made
    # `end >= today` always true, so the Expired branch was unreachable and a backfilled
    # order delivered years ago would have been created as live cover.
    today = datetime.datetime.utcnow().date().isoformat()
    payloads, line_of_key, skipped_no_term, bad_term = [], {}, [], []

    def _ident(pr, itm):
        return (pr.get("salla_sku") or pr.get("hs_sku") or pr.get("salla_product_id")
                or pr.get("hs_product_id") or itm["id"])
    for it in candidates:
        p = it["properties"]
        meta = _meta(p)
        if not meta or meta.get("product_class") != "device":
            continue
        months = meta.get("warranty_months")
        if months in (None, ""):
            # No term set on the product. Do not invent one - a wrong expiry date is
            # worse than a missing warranty, and this surfaces as a countable output.
            skipped_no_term.append(_ident(p, it))
            continue
        # A term must be a positive whole number of months within a believable range.
        # 0 would expire cover on the day it started; a negative or absurd value is a
        # data-entry error and must not reach a customer-facing expiry date.
        try:
            months = int(float(months))
        except Exception:
            bad_term.append(_ident(p, it))
            continue
        if months < 1 or months > 600:
            bad_term.append(_ident(p, it))
            continue
        end = add_months(start, months)
        item_id = p.get("salla_order_item_id") or it["id"]
        name = p.get("name") or "Device"
        try:
            qty = int(float(p.get("quantity") or 1))
        except Exception:
            qty = 1
        if qty < 1:
            qty = 1
        # A quantity beyond any plausible retail order is a data error, and 40
        # records minted from one bad cell would be worse than none. Largest
        # real value measured across the 1.59M-item corpus: 30.
        if qty > 40:
            bad_term.append(_ident(p, it))
            continue
        # One record per physical unit. The key extends the line-item key with
        # a unit ordinal (u1..uN), so quantity-1 items keep one record and a
        # re-delivered or re-enrolled order upserts onto the same units rather
        # than duplicating them. quantity stays on the record as 1 per unit;
        # unit_count preserves what the line item carried at sale.
        for unit in range(1, qty + 1):
            key = "%s:%s:u%d" % (salla_order_id, item_id, unit)
            unit_tag = " - unit %d/%d" % (unit, qty) if qty > 1 else ""
            payloads.append({
                "idProperty": "warranty_key",
                "id": key,
                "properties": {
                    "warranty_key": key,
                    "warranty_name": "%s - order %s%s" % (
                        name[:70], order_ref or salla_order_id, unit_tag),
                    "device_salla_product_id": p.get("salla_product_id") or "",
                    "device_sku": p.get("salla_sku") or p.get("hs_sku") or "",
                    "device_name": name,
                    "quantity": 1,
                    "unit_index": unit,
                    "unit_count": qty,
                    "origin": "live_engine",
                    "warranty_start_date": start,
                    "warranty_end_date": end,
                    "warranty_months_snapshot": months,
                    "sale_context": p.get("sale_context") or "standalone_product",
                    "bundle_name_snapshot": p.get("bundle_template_name_snapshot") or "",
                    "salla_order_id": salla_order_id,
                    "salla_order_item_id": str(item_id),
                    "order_reference": order_ref,
                    "hs_pipeline": PIPELINE_ID,
                    "hs_pipeline_stage": STAGE_ACTIVE if end >= today else STAGE_EXPIRED,
                }})
            line_of_key[key] = it["id"]

    if not payloads:
        return {"outputFields": {"status": "no_devices", "warranties_created": 0,
                                 "skipped_no_term": (",".join(skipped_no_term + bad_term))[:200]}}

    # ── 5. upsert - idempotent on warranty_key ──────────────────────────────
    # Re-delivery, Auto-Repair re-passes, workflow re-enrolment and any future
    # backfill all converge on the same record rather than duplicating it.
    # Two line items on one order can collide on the same key if the source data
    # repeats an order-item id. HubSpot rejects a whole batch containing duplicate ids,
    # so collapse them here: same key means the same covered item.
    seen_keys, deduped = set(), []
    for pl in payloads:
        if pl["id"] in seen_keys:
            continue
        seen_keys.add(pl["id"])
        deduped.append(pl)
    payloads = deduped

    # batch/upsert accepts at most 100 inputs per call
    created = []
    for i in range(0, len(payloads), 100):
        rr = _post("/crm/v3/objects/%s/batch/upsert" % WARRANTY_OBJ,
                   {"inputs": payloads[i:i + 100]})
        if rr.status_code >= 300:
            return {"outputFields": {"status": "upsert_failed", "detail": rr.text[:300],
                                     "warranties_created": len(created)}}
        created += rr.json().get("results") or []

    # ── 6. associate: order, covered line item, contact ─────────────────────
    contact_ids = []
    rc = _get("/crm/v4/objects/%s/%s/associations/%s" % (ORDER_OBJ, order_id, CONTACT_OBJ),
              {"limit": 10})
    if rc.status_code == 200:
        contact_ids = [x["toObjectId"] for x in (rc.json().get("results") or [])]

    def assoc(to_obj, pairs, type_id, category):
        # association batch/create also caps at 100 inputs per call
        for i in range(0, len(pairs), 100):
            chunk = pairs[i:i + 100]
            if not chunk:
                continue
            _post("/crm/v4/associations/%s/%s/batch/create" % (WARRANTY_OBJ, to_obj),
                  {"inputs": [{"from": {"id": str(a)}, "to": {"id": str(b)},
                               "types": [{"associationCategory": category,
                                          "associationTypeId": type_id}]}
                              for a, b in chunk]})

    order_pairs, li_pairs, contact_pairs = [], [], []
    for res in created:
        wid = res.get("id")
        key = (res.get("properties") or {}).get("warranty_key")
        if not wid:
            continue
        order_pairs.append((wid, order_id))
        if key in line_of_key:
            li_pairs.append((wid, line_of_key[key]))
        for cid in contact_ids:
            contact_pairs.append((wid, cid))

    assoc(ORDER_OBJ, order_pairs, ASSOC_ORDER, ASSOC_ORDER_CAT)
    assoc(LINE_ITEM_OBJ, li_pairs, ASSOC_LINE_ITEM, ASSOC_LINE_ITEM_CAT)
    assoc(CONTACT_OBJ, contact_pairs, ASSOC_CONTACT, ASSOC_CONTACT_CAT)

    return {"outputFields": {
        "status": "ok",
        "warranties_created": len(created),
        "warranty_start_date": start,
        "skipped_no_term": (",".join(skipped_no_term + bad_term))[:200],
    }}
