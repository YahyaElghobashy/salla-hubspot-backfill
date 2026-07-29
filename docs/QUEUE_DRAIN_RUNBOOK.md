# Queue Drain Runbook — Held Orders Release

Drains the **Queue Log** tab of your Order Audit Log workbook (orders the
catalog gate held; they do **NOT** exist in HubSpot until drained).
Engine: `queue_drain.py` (v1.0). Run it from your deployment directory, the
same one that holds `config.json` and `.env`.

## Ground rules

- **Live sync stays ON.** The drain reads `mirror/live_active.json` and yields
  the HubSpot budget to it automatically.
- **Backfill stays PAUSED while draining** (both compete for the same search
  budget and the same Queue Log). Stop it first, note the cursor page it
  reached, and resume afterwards with `python run.py --live --yes`.
- Nothing is ever deleted from the sheet. Row states (col G):
  `Queued` (still blocked / not yet tried) → `Processing` → `Processed`,
  `Duplicate`, `Error` (retried next run, up to `live_max_attempts`), `Gone`
  (Salla 404). Blocked rows stay `Queued` with `notes` = blocker + timestamp.
- Idempotent by design: created-ledger + `salla_order_id` dedup search +
  line-item verify. Re-running is always safe; a crash mid-run loses nothing.
- Stop switch: `touch STOP.drain` (graceful: finishes in-flight, rest stays
  queued). Single-instance: `drain.lock`.

## The cycle (repeat until drained)

```bash
cd /path/to/your/deployment

# 1. FREE, sheet-only: blocker matrix from stored names (no API calls)
python queue_drain.py --scan

# 2. Read-only API: fetch + gate-check every claimable order.
#    Produces mirror/blocker_matrix.csv at product-ID level + READY counts.
python queue_drain.py --verify

# 3. Send mirror/blocker_matrix.csv to the catalog owner for approval.
#    Fixes happen in the validation sheet:
#      - product missing/unapproved  -> Products > Approve as Standalone
#                                       (or map + Push as a bundle)
#      - bundle draft/inactive/0-comp -> map components + Bundles > Push to HubSpot
#      - wrong tagging in Salla       -> retag, let Product sync recreate, then approve
#
# 4. Pilot, then full drain:
python queue_drain.py --live --max-orders 50
#    -> spot-check ~10 created orders in HubSpot (contact assoc, line items,
#       bundle components) + their flipped audit rows, then:
python queue_drain.py --live
#
# 5. Re-run after each catalog fix wave. Blocked rows retry automatically.
```

`--live` asks for a typed `RUN` confirmation; pass `--yes` to skip it
(the web UI does).

## What a live run writes

- HubSpot: contact (upsert by phone) + order + line items + bundle records +
  associations — the exact backfill pipeline, `last_salla_sync_status=synced`.
- Queue Log row: `Processed` + `processed_at` + `processing_result`
  (`created HS <id>` / `pre-existing HS <id> verified` / `already synced`),
  `attempts`+1. Duplicate rows of the same order → `Duplicate` + pointer.
- Audit Log row (the order's ORIGINAL row): `Order Approved`, hold reason
  cleared, HS id + URL, contact columns, verification columns set to
  `Pending Verification` — the workbook's auto-verify trigger (or
  Order Ops > Check Unverified) fills them. Same contract as the Apps Script
  "Approve Held", so the sheet tooling stays consistent.
- Local: `archive/order_<id>.json`, `mirror/created.csv`, `drain.log`.

## Verification & reconciliation (after the backlog drains)

1. `queue_drain.py --scan` → remaining Queued should equal the still-blocked
   set only; `Error`/`Gone` rows need eyeballing (`mirror/errors.csv`).
2. Audit workbook: Order Ops > Check Unverified (or wait for auto-verify),
   then Show Counts Summary — the Held count should drop accordingly.
3. Counts: `wc -l mirror/created.csv` vs Processed rows in the sheet.
4. Residual truly-stuck rows: mark manually, or leave `Error` (the engine
   stops retrying at `live_max_attempts`).

## Costs / limits

- Make ops: roughly 1 relay call per 12 orders (batched fetch) — small next to
  the credit budget; `credit_watch` alerts stay live throughout.
- HubSpot search: adaptive limiter under the account cap, yields to live sync.
- Sheets: all marks and audit updates are batched (tens of rows per request).
- Gate searches are cached per product id, and a backlog is normally dominated
  by a small number of distinct blockers, so re-runs are cheap.

## Known data quirks

- The Queue Log's "order Date" column (N) actually holds the **customer
  created-at** date (`route_held` writes `customer.created_at.date`) — which is
  why values can reach back years before the store's first order. Do not read
  it as the order date.
- `queued_reason` / `unverified_items` are blank on rows written by the Make
  gate (live holds); they are filled on engine-written rows. `--scan` therefore
  reports some rows as "no stored blocker name"; `--verify` resolves those.
- If a manual pilot pass marked rows `Processed` before this engine existed,
  those rows are already terminal and will be skipped.
