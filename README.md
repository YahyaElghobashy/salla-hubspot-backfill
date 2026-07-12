# Salla → HubSpot Order Backfill

A local, auditable engine that backfills historical **Salla** store orders into
**HubSpot** — contacts, orders, line items, and (optionally) product-bundle
custom objects — at a tiny fraction of the automation-platform cost that
usually makes large backfills impractical.

Born from a real production migration: tens of thousands of orders, a Make
scenario burning ~36 credits per order, and a plan that replaced it with this
engine at ~2.3 credits per order (~94% cheaper) while keeping a complete audit
trail. The engine has processed real stores end to end with per-slot
reconciliation against Salla's own counts.

## How it works

```
             ┌──────────────────────────── your machine ────────────────────────────┐
Salla API ── Make "relay" (5 modules, reuses your existing Salla OAuth connection)  │
             │        │                                                             │
             │  cursor.json ── slot-by-slot sweep ── dedup vs HubSpot ── create:    │
             │        │        (3h windows)          (salla_order_id)   contact →   │
             │        │                                                order →      │
             │  backfill.log · mirror/*.csv · archive/*.json           line items → │
             │  (full audit trail, always on)                          bundles      │
             └───────────────────────────────────────────────────────── HubSpot API ┘
```

- **Relay, not OAuth surgery:** Salla's Merchant API is OAuth-app-only. Instead
  of new credentials, a 5-module Make scenario (blueprint included) proxies
  read-only Salla calls through the Salla connection you already have. A few
  Make operations per batch instead of dozens per order.
- **Resumable by construction:** the cursor advances only after a page fully
  processes. Kill it, reboot, resume — deduplication makes rescans free and
  duplicates impossible (every order is checked by `salla_order_id` before any
  write).
- **Guardrails everywhere:** dry-run by default, typed `RUN` confirmation for
  live mode, `--max-orders` test gates, graceful STOP file, client-side rate
  limiting (HubSpot search cap shared with your live automations), exponential
  backoff, a contact-create race guardrail (concurrent live flows can win the
  race between search and create — the engine waits, re-searches, and adopts
  the winner), status-aware pipeline staging, and an unrecovered-failure
  ledger that stays empty or gets loud.

## Quick start

```bash
git clone https://github.com/YahyaElghobashy/salla-hubspot-backfill.git
cd salla-hubspot-backfill
python3 -m venv venv
./venv/bin/pip install -r requirements.txt     # Windows: venv\Scripts\pip install -r requirements.txt
./venv/bin/python serve.py                     # Windows: venv\Scripts\python serve.py
```

`serve.py` opens the **web UI** (any modern browser, macOS / Windows / Linux):

1. **Setup wizard** — paste your HubSpot private-app token and relay secret
   (stored only in a local `.env`), test both live, let it pull your order
   pipeline stages and your store's status list, click your status→stage
   mapping together, set the backfill window, save.
2. **Run tab** — dry run first (writes nothing, prints a would-create plan),
   then a capped live test (e.g. 2 orders), then the full run. Graceful stop
   any time.
3. **Dashboard tab** — live phase per order (dedup → archive → catalog gate →
   contact → order → line items → associations), counters, rate, recent feed.
4. **Logs tab** — raw log tail plus the real-failure ledger.

Prefer terminals? Everything works without the UI:

```bash
./venv/bin/python run.py --dry --max-orders 2      # dry run gate
./venv/bin/python run.py --live --max-orders 2     # live gate (type RUN)
./venv/bin/python run.py --live                    # full run, supervised + keep-awake
./venv/bin/python dashboard.py                     # rich TUI dashboard in a second terminal
```

`run.py` is the cross-platform supervisor: keeps the machine awake
(macOS `caffeinate` / Windows `SetThreadExecutionState` / Linux
`systemd-inhibit`), relaunches the engine if it's killed abnormally, and stops
cleanly on the STOP file or when the cursor reaches `done`.

## The Make relay (one-time, ~5 minutes)

1. Open `relay/relay_blueprint.template.json`, replace
   `RELAY_SECRET_REPLACE_ME` (2 places) with a long random string.
2. Make → Scenarios → Create → Import Blueprint → pick the file. Create the
   webhook when prompted; select **your existing Salla connection** on both
   Salla modules.
3. Turn it ON, copy the webhook URL into the wizard. Idle cost is zero; it
   only spends operations when the engine calls it.

## What gets written where

| Destination | Content |
|---|---|
| HubSpot | contacts (searched by phone first, race-guarded), orders (30+ properties incl. totals, payment, status-mapped pipeline stage), line items with product stamps, optional bundle records with full associations |
| Local `archive/` | the raw Salla JSON of every processed order |
| Local `mirror/` | CSV mirror of every audit/queue row + `errors.csv`, the unrecovered-failure ledger |
| Google Sheets / Drive (optional) | live audit rows and JSON archive uploads, if you enable them in the wizard |

**Privacy note:** `archive/`, `mirror/`, and logs contain customer data. They
are gitignored; keep them local.

## Adaptive rate pacing (v1.4)

The engine paces itself per destination with an AIMD controller (additive
increase, multiplicative decrease — the same family TCP uses), targeting
**90–95% of each provider's documented limit** and never more:

| Bucket | Documented limit | Default ceiling (×0.92) | Feedback signals |
|---|---|---|---|
| HubSpot search | 5 req/s **per account** | 4.6/s | 429s only (search responses carry no rate headers) |
| HubSpot general | 190 req/10s per private app (Pro/Ent) | 17.5/s | 429s + `X-HubSpot-RateLimit-Max/-Remaining` (10s window) |
| Sheets writes | 60 req/min/user (fixed ~60s window) | 55/min | 429s, 65s cooldown so the window refills |
| Drive uploads | 325k units/min/user (create = 50 units) | 120/min (self-imposed) | 429/403 rate errors |
| Make relay | webhook 300 req/10s; scenario latency dominates | gap ≥ `relay_floor_interval_s` | transient/async-ACK responses widen the gap |

How it behaves: rates start at your configured `*_per_s` / `*_per_min`
values, climb slowly after sustained clean streaks, halve immediately on a
429 (then cool down before growing again), and yield gently when HubSpot's
shared-bucket headers show other integrations draining the same pool. Every
change is logged as an `ADAPT` line and a `RATES` snapshot appears about
once a minute (both dashboards display it live).

Sharing the portal with live automations? The 5/s search pool is
account-wide — set `hs_search_limit_per_s` to your fair share of it (e.g.
`3.0` if a live integration needs the rest). Set `adaptive_enabled: false`
to pin every rate at its starting value (pre-v1.4 behavior).

## Verification tools

- `tools/qa_spot_check.py N` — random-sample deep verification of created
  orders against HubSpot and the local archive (dedup integrity, naming,
  stage, totals, contact + line-item associations).
- `tools/retro_status.py` — retroactive stage sweep: re-stages already-created
  orders from their current Salla status (batched 100/call), with sample-first
  and dry-run modes. See `--help`.

## FAQ

**Is this a server?** No. Outbound HTTPS only. Close the lid and it stops
cleanly; run again and it resumes.

**Can it duplicate orders?** Every order is looked up by `salla_order_id`
before any write, and the cursor never skips work. Kill -9 mid-page costs a
few read-only skips on resume, nothing else.

**What about my live automations?** The default HubSpot search pacing
(3.5/s of the account-wide 5/s) deliberately leaves headroom for them.

**Windows?** Yes — pure Python 3.9+, no shell scripts in the critical path.

## License

MIT
