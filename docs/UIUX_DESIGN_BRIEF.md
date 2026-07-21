# UI/UX Design Brief — Salla → HubSpot Sync Engine

> **To:** UI/UX Expert & Art Director
> **From:** Engineering
> **Subject:** Full GUI overhaul for a data synchronization engine expanding beyond orders

---

## What this product is

A locally-hosted synchronization engine that moves e-commerce data from **Salla** (a storefront platform popular in the Middle East) into **HubSpot CRM**. It runs on the operator's own machine or a small cloud VM — not a SaaS product, not a browser extension. Think of it as a control tower for a continuous data pipeline between two systems that don't natively talk to each other.

**Two engines, one codebase:**

- **Backfill** — sweeps a historical date window (months or years of past orders) and creates every missing record in HubSpot. Resumable, killable, restartable. A bounded job that eventually finishes.
- **Live sync** — a 24/7 service that polls a queue sheet every 5 seconds, picks up new orders the moment they land, and processes them through the same pipeline. Runs forever.

Both can run **simultaneously**, coordinating over a shared API rate budget so live orders always get priority while the backfill keeps grinding in the background.

**What it writes to HubSpot today:** contacts (deduplicated), orders (with 30+ properties), line items (with product links), product-bundle custom objects, and all the associations between them — plus a full audit trail to Google Sheets and local CSV mirrors.

**Where it's expanding next:**
- Standalone **contact sync** (create/update contacts independent of orders)
- **Order delivery & shipment status** relay (Salla status changes pushed to HubSpot pipeline stages in real time)
- **Custom object sync** beyond bundles
- **Multi-store** support (one dashboard, multiple Salla stores → one or multiple HubSpot portals)

The GUI must scale to accommodate these new sync streams without collapsing into chaos. That's the core design challenge.

---

## Who uses it

**Primary persona:** a CRM operations manager or technical integrations lead at an e-commerce company. They are comfortable with dashboards, spreadsheets, and CRM configuration — but they are not developers. They will not read raw log files to understand what happened; the UI must tell them.

**What they need from the GUI:**

1. **Trust** — the engine processes thousands of records autonomously. The operator needs to glance at the screen and know: is it working? is it healthy? is anything stuck? A running engine should radiate confidence, not anxiety.
2. **Diagnosis** — when something is held or errored, they need to trace that specific record's journey from intake to terminal state, step by step, without touching a terminal.
3. **Control** — setup, launch, pause, stop, reconfigure. A first-time user should be able to go from zero to a successful dry run in under 10 minutes with the wizard alone.
4. **Awareness** — they may be managing backfill + live sync + (soon) contact sync + status relay all at once. They need a unified view of what's running, what's yielding, what's idle, and where attention is needed.

---

## What the current UI displays (the bedrock — all of this must survive)

The existing UI is a single-page app with three tabs. Everything below is information the operator already relies on. Your redesign must preserve all of it — but you are free to restructure, recompose, and reimagine how it's presented.

### Setup & run control
- A 5-step guided wizard: (1) HubSpot token + relay secret, (2) Salla relay webhook, (3) Google Sheets/Drive credentials, (4) pipeline stage mapping, (5) date window + pacing configuration
- A readiness checklist strip (green/amber/red chips showing what's configured)
- Run panel: mode selector (dry / live / live-sync 24/7), max-orders test gate, a `RUN` confirmation for live mode, start/pause/stop controls
- "Recommended first flight" guidance card

### Live operational dashboard
- **Engine status**: two status pills (backfill + live sync), each with a live state label (creating / scanning / watching / working / stopped)
- **Live queue**: queued depth, oldest event age, processed-today count
- **All-time metrics strip**: lifetime orders created, held, run count, engine hours, best rate, error events
- **Current period**: window progress bar, cursor position, session created/held counts, live rate
- **Current slot**: the exact time-slot and page being processed, with a page progress bar
- **Worker lane cards**: one card per concurrent unit of work, color-coded — backfill lanes in sage green, live-order cards in ocean blue-green, live sorted first. Each shows order ID, current phase, lane assignment, item count, and age. A "scanning" dashed-border card appears when the backfill is skipping already-synced orders (so a working engine never looks idle)
- **Adaptive pacing bars**: one bar per rate limiter (HubSpot search, HubSpot general, Sheets writes, Drive uploads, relay gap), showing current rate vs the 90-95% ceiling. The bars deepen in color as utilization rises — **a full bar is the goal** (full utilization), not a warning. A "full utilization" badge appears when all limiters are near ceiling
- **Throughput sparkline**: a continuous orders/minute line chart with hover tooltips
- **Completions feed**: the most recent created/held results, source-tagged (backfill vs live)

### Activity & audit
- **Unrecovered failure ledger** — front and center, the list that must stay empty. Distinguished from recoverable log errors
- **Run history table** — every backfill run ever, parsed from the log: window, mode, duration, created/held/skipped/errors, rate. Sortable by any column
- **Event log** — every engine action as a human-readable sentence, filterable by type (created / held / deduplicated / errors / pacing / system) and source (backfill / live / both), with free-text search, newest/oldest toggle
- **Per-order drill-down** — click any order event to expand a step-by-step check trace: dedup result, contact resolution, order creation, line items, bundles, associations, with per-step status icons and timestamps
- **Raw log tail** — one click away, for when the operator (or a developer helping them) needs the actual log lines

---

## Technical concepts the design must make legible

These are not implementation details to hide — they are the product's value proposition. The operator should understand them intuitively through the UI without reading documentation.

- **Adaptive pacing (AIMD)**: the engine speeds up gradually and slows down sharply to stay just under API rate limits. Visualize this as the engine "breathing" — expanding toward the ceiling, contracting on throttle. Full utilization is success, not danger.
- **Live-priority coordination**: when both engines run, the backfill yields its speed budget to live orders. The operator should see this happening — the backfill slowing down is a feature, not a problem.
- **Worker lanes**: parallel processing slots. Each lane carries one order at a time. The operator should see what each lane is doing and which lanes are busy vs idle.
- **Idempotency**: the guarantee that no record is ever created twice, even after crashes or restarts. This should be a quiet confidence signal, not a noisy feature.
- **Catalog gate**: orders can be "held" because a product isn't approved in HubSpot yet. Held is a healthy, intentional state — visually distinct from errors.
- **Queue lifecycle**: every record transitions through states (queued → processing → done/held/error/gone). The operator should be able to see where records are in this lifecycle at a glance.

---

## Design principles

1. **Operational confidence over information density.** The default view should answer "is everything okay?" in under 2 seconds. Detail is one click away, never zero clicks away.
2. **Debuggability as a first-class feature.** Any record, at any point in time, should be traceable from intake to outcome. The per-order drill-down is not a developer tool — it's the primary diagnostic interface.
3. **Progressive disclosure.** Setup wizard → dry run → capped live test → full run. Dashboard summary → lane detail → order trace → raw log. The UI teaches the product through its structure.
4. **Held ≠ broken, full ≠ danger.** The visual language must distinguish healthy-but-paused states (held orders, yielding backfill, full pacing bars) from actual problems (errors, stale engines, unrecovered failures). Most dashboards train users to fear amber/yellow — this one should use it for "intentionally parked."
5. **Scales to N sync streams.** Today: orders (backfill + live). Tomorrow: contacts, delivery status, custom objects. The navigation and layout pattern must accommodate new streams without a redesign. Think: a sync-stream-aware shell, not a tab-per-feature architecture.
6. **Dark and light, both native.** The current UI supports `prefers-color-scheme`. The redesign should treat both themes as first-class, not as an afterthought invert.

---

## What I'm asking you to envision

I am **not** asking you to reskin the current UI or move boxes around. I'm asking you to step back and think about:

- **Information architecture**: how should sync streams, setup, monitoring, and audit relate to each other in navigation? What's the right shell pattern for a product that will have 4-6 sync types?
- **Spatial composition**: what deserves persistent screen real estate vs. on-demand panels? How do you balance the "everything's fine" glance with the "trace this one order" drill-down?
- **Visual language**: color as meaning (not decoration), typography hierarchy, the semiotics of pacing bars and lane cards, how to make "full utilization" feel triumphant and "held" feel intentional
- **Motion and state**: how do live-updating elements (SSE streams, lane cards appearing/disappearing, pacing bars breathing) feel? What earns animation and what should be still?
- **The 10-minute first run**: a new user opens this for the first time. How does the UI guide them from "I have credentials" to "I just watched my first 2 orders sync" without external documentation?

---

## Deliverables I need from you

1. **Your vision document** — information architecture, navigation model, layout strategy, visual direction (color system, type scale, spacing rhythm, component vocabulary). Written so I can evaluate the thinking, not just the pictures.

2. **Key screen compositions** — at minimum: (a) the unified dashboard with 2+ sync streams running, (b) the setup/onboarding flow, (c) the order trace drill-down, (d) an error/attention state. Wireframe fidelity is fine; annotate the why behind layout choices.

3. **Prompts for Claude Designer** — write 2-4 self-contained prompts I can paste into Claude's artifact/designer mode to generate high-fidelity mockups of your key screens. Each prompt should specify: layout structure, color tokens, type hierarchy, component states, and what data to show. Be specific enough that the output is recognizably your design, not a generic dashboard.

4. **Prompts for Google Stitch** — write 2-3 prompts I can use in Google Stitch to generate interactive prototypes. Focus on the flows that matter most: (a) first-time setup wizard, (b) monitoring a running dual-engine with a live-priority yield event, (c) diagnosing a held order from the dashboard down to the per-step trace. Specify the interaction states and transitions.

5. **A conversation plan** — after you produce the above, I'll bring the Claude Designer outputs and Stitch prototypes back to you. Tell me what feedback loop you want: what to evaluate in the mockups, what to test in the prototypes, and what questions the first round should answer before we go to implementation.

---

## Reference material

- [`docs/ARCHITECTURE.md`](ARCHITECTURE.md) — full engineering deep-dive with diagrams, the relay architecture, both engines, pacing, idempotency, coordination, efficiency calculation, and GCP deployment
- [`README.md`](../README.md) — user-facing documentation with current screenshots
- [`docs/img/dashboard-consolidated.png`](img/dashboard-consolidated.png) — current consolidated dashboard screenshot
- [`docs/img/activity-lanes.png`](img/activity-lanes.png) — current color-coded lane cards closeup
- The current UI source is a single `index.html` (933 lines) — functional, responsive, dark/light aware, but built by engineers for engineers. Your job is to make it built for operators.
