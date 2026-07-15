# Live-sync cutover runbook

Zero-loss, zero-duplicate migration of live order syncing from the Make
scenario to the local engine's live mode. Read fully before touching Make.

## Why the ordering below matters

- the old scenario has **no dedup** — it must never run in parallel with the engine
  processing the same orders.
- Our engine dedups by `salla_order_id`, but HubSpot's **search index is
  eventually consistent** (fresh orders take up to minutes to become
  searchable). The engine carries a local created-ledger + a duplicate-400
  guardrail, and the runbook still inserts settle time for belt and braces.
- Make hooks buffer events from the moment the hook exists, even while the
  scenario is OFF — that is what makes the staged cutover lossless.

## One-time preparation (no production impact)

1. `./venv/bin/python3 live.py --init-queue` → creates the "Salla Live
   Queue" spreadsheet; put its id into `config.live.json`
   (`queue_spreadsheet_id`).
2. Make → Scenarios → Import Blueprint → `docs/Salla_Live_Order_Intake.blueprint.json`.
   On import: create a **new** webhook on the existing Salla connection,
   select the "Salla Live Queue" spreadsheet + "Live Queue" tab. In scenario
   settings enable **Allow storing of incomplete executions** (the retry
   handler needs it) and turn on email notifications for incomplete runs.
3. **Hook coexistence test (critical, do this in a low-traffic window):**
   turn the intake scenario ON while the old scenario is still ON, then place/observe one
   real order. Confirm BOTH: the old scenario executed AND a queue row appeared.
   - If the old scenario stopped firing, Salla replaced the subscription: turn intake
     OFF, verify the old scenario fires again, and use plan B at cutover — detach hook
     the existing hook from the old scenario and attach it to the intake scenario in the same
     minute (rollback = reattach).

## Cutover (≈30 min, any time after the test passes)

1. Optional but recommended: `touch STOP` to pause the backfill engine for
   the window (resume after step 6; dedup makes the re-scan free).
2. Intake scenario **ON** (it may already be on from the test). Queue rows
   accumulate; the engine is not consuming yet.
3. the old scenario **OFF**. In Make, wait until its queued/incomplete executions fully
   drain (History shows no running/pending).
4. Wait **10–15 minutes** (HubSpot search-index settle for the old scenario's last
   creations).
5. Start the live engine — GUI Run panel → "LIVE SYNC 24/7" → type RUN, or:
   `./venv/bin/python3 run.py --mode live --live` (supervised, restarts
   forever). It drains the backlog: ledger+dedup skip everything the old scenario
   already created; everything newer is created by the engine.
6. Watch the Dashboard (Engine → Live sync): queue depth should fall to 0
   and stay there; `SWEEP clean` lines confirm the webhook is catching
   everything. Remove `STOP` to resume the backfill.

**Rollback** any time before step 5: the old scenario back ON (its hook was never
touched; nothing was lost). After step 5: stop the live engine
(`STOP.live` / GUI), the old scenario back ON — the engine's ledger prevents any
double-processing if you later switch forward again.

## After cutover

- Salla webhook outage / missed events → the hourly sweep enqueues the
  missing ids (`SWEEP enqueued N` in live.log — investigate if frequent).
- Make credits exhausted → intake stops running; Make's webhook queue
  buffers (up to ~10k events) and replays on renewal. The relay also stalls
  (fetches fail; rows wait as `error` retries) — everything is lossless,
  it just waits. Long outage recovery: run the backfill engine over the gap
  window afterwards.
- Engine host dies → rows accumulate in the sheet; restart consumes them.
  Two instances can never both claim (flock + sheet heartbeat).
- **Flag to the client (their scenarios, our note):** "Order delivery
  status Update" still has the completed→Returned duplicate-filter bug and
  the unrouted `restored` slug; additionally, for orders whose status
  changes within ~2 minutes of creation it may search before the order is
  indexed — recommend a 3–5 min sleep + one re-search in its not-found
  branch. The engine already creates such orders with the post-transition
  status, so the missed update is usually a no-op.

## Credit economics

the old scenario: ~25–40 ops per order. Intake: **2 ops** per order (trigger + append)
+ ~1–2 relay ops amortized per fetched batch + hourly sweep (~30–60
ops/day). At ~600 orders/day: ≈ **1.3–1.5k ops/day vs ~15–20k** — the same
~92–94% cut the backfill achieved.
