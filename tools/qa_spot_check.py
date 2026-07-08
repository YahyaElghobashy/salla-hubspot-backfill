#!/usr/bin/env python3
"""QA spot check: deep-verify a random sample of engine-created orders against
HubSpot and the local archive ground truth. Read-only. Run:
    set -a; source secrets.env; set +a; ./venv/bin/python3 tools/qa_spot_check.py [N]
"""
import csv, glob, json, os, random, ssl, sys, time, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
BASE = "https://api.hubapi.com"
TOK = os.environ["HUBSPOT_ACCESS_TOKEN"]
CTX = ssl.create_default_context()
N = int(sys.argv[1]) if len(sys.argv) > 1 else 15


def http(method, url, body=None):
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body else None,
                                 method=method,
                                 headers={"Authorization": f"Bearer {TOK}",
                                          "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60, context=CTX) as r:
        return json.loads(r.read())


def search(obj, filters, props, limit=2):
    return http("POST", f"{BASE}/crm/v3/objects/{obj}/search",
                {"filterGroups": [{"filters": filters}], "properties": props, "limit": limit})


rows = list(csv.reader(open("mirror/audit_mirror.csv")))
hdr = rows[0]
arrived = {}
queued_ids = set()
for r in rows[1:]:
    d = dict(zip(hdr, r))
    if d["event"] == "arrived_append" and d["c0"]:
        arrived[d["sheet_row"]] = d["c0"]
    if d["event"] == "queued_update":
        queued_ids.add(d["sheet_row"])
held_orders = {arrived[sr] for sr in queued_ids if sr in arrived}
created_ids = [oid for sr, oid in arrived.items() if sr not in queued_ids and sr != "-1"]

sample = random.sample(created_ids, min(N, len(created_ids)))
fails, checked = [], 0

for oid in sample:
    checked += 1
    arch = glob.glob(f"archive/*_{oid}_*.json")
    if not arch:
        fails.append(f"{oid}: no archive file"); continue
    src = json.load(open(arch[0]))
    ref = src.get("reference_id") or src.get("id")
    res = search("orders", [{"propertyName": "salla_order_id", "operator": "EQ", "value": oid}],
                 ["hs_order_name", "hs_pipeline_stage", "last_salla_sync_status",
                  "hs_total_price", "hs_currency_code", "salla_order_reference"])
    time.sleep(0.35)
    if res.get("total") != 1:
        fails.append(f"{oid}: dedup integrity total={res.get('total')} (expect exactly 1)"); continue
    p = res["results"][0]["properties"]; hsid = res["results"][0]["id"]
    if not p["hs_order_name"].startswith(f"RID{ref} | Salla | "):
        fails.append(f"{oid}: name format {p['hs_order_name']!r}")
    cfg = json.load(open("config.json"))
    valid_stages = {cfg.get("default_pipeline_stage")} | set((cfg.get("status_stage_map") or {}).values())
    if p["hs_pipeline_stage"] not in valid_stages:
        fails.append(f"{oid}: unexpected stage {p['hs_pipeline_stage']}")
    if p["last_salla_sync_status"] != "synced":
        fails.append(f"{oid}: sync status {p['last_salla_sync_status']!r}")
    want_total = str(src.get("amounts", {}).get("total", {}).get("amount"))
    if str(p["hs_total_price"]) not in (want_total, want_total + ".0", f"{float(want_total):g}" if want_total.replace('.','',1).isdigit() else want_total):
        try:
            if abs(float(p["hs_total_price"]) - float(want_total)) > 0.01:
                fails.append(f"{oid}: total {p['hs_total_price']} != archive {want_total}")
        except (TypeError, ValueError):
            fails.append(f"{oid}: total unparseable {p['hs_total_price']!r} vs {want_total!r}")
    full = http("GET", f"{BASE}/crm/v3/objects/orders/{hsid}?associations=contacts,line_items")
    n_contacts = len(full.get("associations", {}).get("contacts", {}).get("results", []))
    n_lis = len(full.get("associations", {}).get("line items", {}).get("results", []))
    n_items = len(src.get("items", []) or [])
    if n_contacts < 1:
        fails.append(f"{oid}: no contact associated")
    if n_lis < n_items:
        fails.append(f"{oid}: LIs {n_lis} < salla items {n_items}")

for hoid in held_orders:
    res = search("orders", [{"propertyName": "salla_order_id", "operator": "EQ", "value": hoid}],
                 ["hs_object_id"])
    if res.get("total") != 0:
        fails.append(f"HELD {hoid}: present in HubSpot (should be absent)")
    else:
        print(f"held {hoid}: correctly absent from HubSpot")

print(f"\nQA SPOT CHECK: {checked} created orders sampled, {len(held_orders)} held verified")
if fails:
    print("FAILURES:")
    for f in fails:
        print(" -", f)
    sys.exit(1)
print("ALL CHECKS PASS")
