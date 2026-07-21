# Quiet Fleet — UI/UX Vision for the Salla → HubSpot Sync Engine

> **Response to:** [`UIUX_DESIGN_BRIEF.md`](UIUX_DESIGN_BRIEF.md)
> **From:** Design
> **Status:** Round 1 — vision, key screens, generation prompts, and the feedback loop
> **Companion reading:** [`ARCHITECTURE.md`](ARCHITECTURE.md) (engine mechanics), current UI screenshots in [`img/`](img/)

---

## 0 · How this direction was chosen

Three complete, deliberately incompatible visions were developed in parallel and judged
comparatively through three lenses (operator trust + first run · scale to N streams ·
feasibility + bedrock preservation):

| Vision | Stance | Trust | Scale | Feasibility | Total |
|---|---|---|---|---|---|
| **Calm Console** | radical progressive disclosure; one health sentence; color scarcity | **8.5** | 7 | **8.5** | **24.0** |
| **Stream Platform** | Vercel/Linear-style shell; streams as first-class objects | 7.5 | **9** | 7 | 23.5 |
| Mission Control | living pipeline map; position encodes progress | 6.5 | 5.5 | 5.5 | 17.5 |

A near-tie with **complementary strengths**: Calm Console wins how the product *feels*
(the <2-second glance, alarm authority, gentlest onboarding); Stream Platform wins how the
product *scales* (the brief's stated core challenge — 4–6 sync streams without redesign).
Each judge who ranked one first grafted the other's core mechanic onto it. Mission Control
loses as a shell but contributes the strongest individual moves (the beacon word, the
still-red doctrine, the celebrated empty state, the visible AIMD ceiling, the parking
apron, ghost-pipeline onboarding).

**The final direction — "Quiet Fleet" — is therefore: Stream Platform's skeleton wearing
Calm Console's skin, with Mission Control's best organs.** A stream-object shell that can
absorb the sixth stream for free, rendered with the restraint that makes an operator trust
it for eight hours a day.

---

## 1 · Vision document

### 1.1 Philosophy

Sync streams are the product's only real noun. The shell is built around them the way
Vercel is built around projects: every stream — orders-backfill, orders-live, and the
future contacts, status-relay, custom-objects, per-store variants — gets an **identical
anatomy** (Overview / Activity / Settings), a health verdict, and a place in one
aggregating **Fleet** home. The operator learns one grammar once; the sixth stream costs
zero new design.

On top of that skeleton, composure is the trust mechanism. The default screen leads with
**one true sentence** about system health and a handful of numbers. Every detail exists;
every detail earns its click. A screen that never shouts is a system that never panics —
and for an operator who cannot read logs, that composure *is* the product.

### 1.2 Information architecture & navigation

```
┌────────────────────────────────────────────────────────────────────┐
│ TOP STRIP   ● NOMINAL   "All 2 streams healthy · live queue 0 ·    │
│             backfill 62% of March"        ◌ live 2s ago   ☾  ⌘K    │
├──────────┬─────────────────────────────────────────────────────────┤
│ RAIL     │                                                         │
│ ▸ Fleet  │                THE ACTIVE SURFACE                       │
│ STREAMS  │                                                         │
│  ● Orders · Backfill      (status dot + micro-metric per entry)    │
│  ● Orders · Live                                                   │
│  ○ Contacts          — future streams appear here, same anatomy    │
│  ○ Status Relay                                                    │
│ ▸ Ledger (•)  ← the app's ONLY persistent red, absent when empty   │
│ ▸ Audit                                                            │
│ ▸ Setup ✓     ← evaporates to a checkmark once configured          │
└──────────┴─────────────────────────────────────────────────────────┘
```

**Five surfaces, one rail — never a tab per feature:**

1. **Fleet** (home, default): the aggregated monitoring view. §2.1.
2. **Streams**: one rail entry per stream object. Under multi-store, the section groups
   into a collapsible tree (`Store name ▸ 5 streams`) — N stores × M streams is
   hierarchy, not sprawl. Every stream page has the same three sub-tabs:
   - **Overview** — that stream's dashboard (same zone grammar as Fleet, filtered to one
     stream) **plus its run-control action bar** (mode, max-orders gate, type-RUN
     confirmation, start/pause/stop). Control is a property of the stream, not of the app.
   - **Activity** — that stream's filtered events, runs, drill-downs.
   - **Settings** — that stream's slice of config (window+pacing for backfill; poll
     cadence for live; stage mapping for status-relay).
3. **Ledger**: the unrecovered-failure ledger as a first-class surface. Its rail badge is
   the only persistent red in the product — *absent*, not zeroed, when empty.
4. **Audit**: cross-stream event log, run-history table, raw log tail behind a `Raw`
   toggle (both `backfill.log` and `live.log` — closing today's gap where the live tail
   is unreachable).
5. **Setup**: the 5-step wizard, readiness chips, credentials, "Add a stream". Once a
   stream is configured its Setup entry collapses to a `configured ✓` row — configuration
   is a hallway you pass through, not a room you live in.

**Global affordances:**
- **The beacon** (top-left): one word — `NOMINAL / ATTENTION / FAULT` — an O(1) glance
  token that stays 2-second-readable even at 12 streams, leading the health sentence
  which inherently grows with N.
- **⌘K command palette**: jump to any stream, any surface — and **paste any order ID**
  to open its trace directly. The palette is the universal "where is this record?" answer.
- **Focus, don't navigate**: first click on a Fleet stream tile *filters Fleet in place*
  (lane rail, feed, budget bar all narrow to that stream); second click navigates to the
  stream page. Cross-stream triage without page-hopping.
- **Fleet-level pause**: one guarded "pause everything" control on Fleet — the global
  emergency vantage the per-stream action bars would otherwise sacrifice.

### 1.3 Layout strategy — persistent vs on-demand

| Persistent (zero clicks) | One click away | Two clicks away |
|---|---|---|
| Beacon + health sentence | Stream Overview (full detail) | Per-step raw payloads in a trace |
| Stream tile row (pill, hero number, sparkline) | Order trace drawer | Raw log tail |
| Shared Budget Commons (the yield bar) | Per-limiter pacing bars (collapsible row) | Run-history row detail |
| Unified lane rail (all streams' work) | All-time strip + period/slot detail | |
| Whisper feed (one line per stream, expandable) | Full interleaved feed | |
| Ledger seal ("0 unrecovered ●") | The Ledger surface | |

The 2-second glance protocol: **beacon word → tile pills → ledger seal.** Three fixed
positions, all above the fold, none requiring reading more than one word each. The health
sentence is the 10-second version of the same answer.

### 1.4 Visual direction

#### Color — semantic tokens (light / dark)

Color is meaning; nothing is decorated. Two constitutional rules:
**red never animates** and **red has a physical monopoly** (Ledger badge, affected tile
edge, sentence replacement — nowhere else, ever). One red pixel = act now.

| Token | Light | Dark | Role |
|---|---|---|---|
| `bg` | `#fafafa` | `#101113` | app background |
| `panel` | `#ffffff` | `#191b1e` | cards, drawers |
| `ink` | `#141414` | `#ececec` | text |
| `muted` | `#757272` | `#8f9296` | secondary text |
| `line` | `#ebebeb` | `#2a2d31` | borders |
| `sage-500 / tint` | `#5ea380` / `#e4f3e9` | `#6fbf93` / `#1d2b23` | backfill stream · success · triumph |
| `ocean-500 / tint` | `#1f7d86` / `#dcf0f1` | `#3fb3bc` / `#14282b` | live stream · live-priority (always sorts first) |
| `gold-500 / tint` | `#e2b13c` / `#faf3df` | `#e8bd58` / `#2e2716` | **parked only** (held orders) — stamp-like seal, never a warning triangle |
| `amber-600 / tint` | `#d97706` / `#fdf0e0` | `#f59e0b` / `#2e2210` | **degraded/stale only** — split from gold so parked and warning can never share a hue |
| `red-500 / tint` | `#f55157` / `#fdeaea` | `#ff6b70` / `#2e1618` | unrecovered failure only |
| `rose-400` | `#d789ad` | `#e39fc0` | pacing / coordination events |
| `violet-500` | `#8b7ec8` | `#a79bdf` | *reserved:* contacts stream |
| `copper-500` | `#c88a5e` | `#d9a077` | *reserved:* status-relay stream |
| `slate-500` | `#5e8ac8` | `#7ba3dd` | *reserved:* custom-objects stream |

The amber/gold split is the palette-level enforcement of **held ≠ broken**: gold always
travels with the word "parked/held" plus a release condition; amber always names a
degradation with a verb.

#### Type scale

| Size / weight | Use |
|---|---|
| 28px / 700 | the beacon word; the leading verdict word of the health sentence |
| 24px / 300 | the health sentence body — light weight, editorial calm |
| 22px / 650 tabular | hero metrics on tiles and strips |
| 15px / 600 | card titles |
| 13px / 500 tabular | data cells, ages, counts (fixed-width so SSE updates never jitter) |
| 13px / 400 | body, event sentences, trace outcomes |
| 11px / 600 caps +0.08em | eyebrows — zone labels, trace stage names |
| mono 12px | order IDs, raw log, payload disclosures only |

#### Spacing & shape

4px base grid · 20px card padding · 24px inter-card · 40px zone gaps · 16px card radius ·
8px chip/bar radius. The quiet-luxury heritage (soft shadows, tabular numerals,
letter-spaced eyebrows) is kept intact — this is an evolution of the existing
quiet-luxury language, not a replacement.

#### Component vocabulary & semiotics

- **Stream tile** — the stream's passport page: status pill top-left, one hero number
  center (live: queue depth + oldest age; backfill: window % + cursor), 40-point
  micro-sparkline footer, gold `held n` chip when applicable. Identical anatomy for every
  current and future stream.
- **Pacing bar** — a rounded track whose fill *breathes* toward a right-edge goal tick.
  Fill in stream color. At the tick, a `✦` appears and holds — **fullness drawn as
  arrival, not pressure; no red zone exists on any bar.** During yield, the AIMD ceiling
  tick itself visibly slides down (4.6 → 0.6/s) — the operator watches the ceiling move,
  not just usage drop.
- **Shared Budget Commons** — the signature element: one stacked horizontal bar for the
  shared HubSpot search budget, segmented by stream color. Live's ocean segment grows as
  backfill's sage segment compresses, annotated inline `yielding to live · 4.6→0.6/s`.
  Yield is watched, not inferred — the backfill slowing down reads as courtesy.
- **Lane card** — a small ticket: 3px left edge-stripe in stream color, phase as eyebrow,
  order ID as title, items + age as muted tabular footer. Exists only while carrying an
  order. New lanes enter from the left, completed lanes exit right — direction narrates
  the pipeline. The dashed **scanning ghost card** (slow 2s dash-march) appears when the
  backfill is skip-heavy, so a working engine never looks idle.
- **Held seal** — a circular gold seal with a parking glyph, always adjacent to its
  reason and its release condition ("will auto-drain when the catalog activates this
  bundle"). Deliberately stamp-like; never triangular.
- **Idempotency mark** — `✓✓` in quiet sage on dedup-skipped completions, hover copy
  "already synced — never twice." The never-twice guarantee as punctuation, not a page.
- **The celebrated empty state** — the Ledger's zero is a designed hero moment:
  `Failure ledger: 0 · nothing needs you.` Serenity as a rendered artifact.

### 1.5 Motion doctrine

Motion only where data moves; chrome never moves.

| Animates | How |
|---|---|
| Pacing bars | width eases 700ms — at AIMD's natural cadence this *is* the breathing; no artificial pulse |
| Yield moment | one 900ms choreographed segment-handoff on the Commons + rose annotation, shown once, then decays into the feed as a still event |
| Lane cards | enter left 200ms fade+rise · exit right with a 300ms sage edge-flash |
| Sparklines | shift one point per second — the system's visible heartbeat |
| Numerals | 150ms opacity crossfade in fixed tabular slots — zero layout shift, no count-up |
| Drawers | 240ms ease-out slide |

**Never animates:** status pills (state changes snap — they must read as facts), the
health sentence, table rows, anything red (a motionless red reads as more serious than a
blinking one), theme changes.

`prefers-reduced-motion`: every transition becomes an instant snap, the dash-march stops,
the yield moment is carried entirely by its text annotation. State is never encoded in
motion — only underlined by it.

### 1.6 The attention ladder

| Tier | Devices allowed | Examples |
|---|---|---|
| **Triumph** (sage + ✦) | color, stillness | full bars, yield annotation, idempotency marks |
| **Parked** (gold) | seal + reason + release condition; no badges, no rail presence | held orders |
| **Warning** (amber) | tile sub-badge, sentence clause, freshness dot | SSE stale, queue age over SLA, stopped-but-should-run |
| **Alarm** (red) | Ledger badge, tile edge, sentence replacement — never animated, max one treatment per screen | unrecovered failures, engine crash |

The hierarchy is falsifiable: **if red is visible, something genuinely needs a human; if
it isn't, nothing does.**

### 1.7 Feasibility notes (for engineering)

Every persistent element maps to data the backend already emits: tiles, lanes, pacing,
sparkline, queue, slot — SSE (`lanes[]`, `pacing{}`, `spark[40]`, `counts`, queue fields,
`stale_s`); all-time strip — `/api/runs.totals`; trace — `/api/order/<id>`; probes —
`/api/test/*`. The health sentence and beacon are **client-composed** from SSE fields
(testable pure function; no new backend). The Commons derives from `pacing{cur,ceil}` on
both streams. New backend needs are limited to: a stream/store registry (multi-store,
future), per-source raw-log param wiring (exists, unwired), and nothing else for round 1.

---

## 2 · Key screen compositions

### 2.1 Fleet — two streams running, yield event in progress

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ ● NOMINAL   All 2 streams healthy · live is draining 3 orders ·              │
│             backfill yielding                     ◌ live · 2s ago   ☾   ⌘K   │
├────────┬─────────────────────────────────────────────────────────────────────┤
│ Fleet  │  ORDERS · LIVE               ORDERS · BACKFILL          + Add stream │
│ ────── │ ┌──────────────────┐  ┌──────────────────┐                          │
│ Orders │ │ ● working        │  │ ● creating       │   ← passport tiles:      │
│  ·Live │ │   3 queued       │  │   62% of window  │     pill · hero number   │
│ Orders │ │   oldest 6s      │  │   cursor 03-23   │     sparkline · held chip│
│  ·Bkfl │ │ ▁▂▄▆▅▇  held 2 ⛉ │  │ ▃▄▅▄▆▅  held 11 ⛉│                          │
│ ────── │ └──────────────────┘  └──────────────────┘                          │
│ Ledger │  SHARED HUBSPOT BUDGET ─────────────────────────────── ✦ ceiling    │
│ Audit  │ ▐███ ocean ███▌▐▒ sage ▒▌·············▼(ceiling tick sliding down)  │
│ Setup ✓│  live 3.9/s ↑        backfill 0.6/s ↓   yielding to live · 4.6→0.6  │
│        │                                                                     │
│        │  LANES ──────────────────────────────  FEED ────────────────────    │
│        │ ┃100299001  creating order   ← ocean   ✓ 100288417 → HS ···  bkfl   │
│        │ ┃100299014  contact lookup   ← ocean   ✓ 100299002 → HS ···  live   │
│        │ ┋100288423  line items       ← sage    ✓✓ 100288390 skipped  bkfl   │
│        │ ┋╌scanning╌ skipping synced  ← ghost   ⛉ 100299009 parked    live   │
│        │                                        (whisper: 1 line/stream,     │
│        │  0 unrecovered · nothing needs you ●    expand for full feed)       │
│        │  ─ Details: all-time · period · slot (disclosure) ─                 │
└────────┴─────────────────────────────────────────────────────────────────────┘
```

**Why:** the glance path (beacon → pills → ledger seal) is three fixed positions; the
Commons sits at the visual center because coordination is the product's most impressive
invisible behavior; lanes and feed share a band so "what is happening" and "what just
happened" read as one story; everything below the fold is disclosure.

### 2.2 Setup & onboarding — the 10-minute first flight

```
┌──────────────────────────────────────────────────────────────────┐
│  SET UP: ORDERS SYNC                    readiness ◉◉◉◉◎          │
│ ┌──────────────────────────────┐  ┌───────────────────────────┐  │
│ │ Step 4 of 5 · Pipeline map   │  │   Salla ─── Relay ─┐      │  │
│ │                              │  │                    ▼      │  │
│ │  Salla status → HS stage     │  │   [Engine] ─── HubSpot ✓  │  │
│ │  delivered   → [Shipped ▾]   │  │      │            ▲       │  │
│ │  canceled    → [Closed  ▾]   │  │   Sheets ✓     (edges     │  │
│ │  …                           │  │   Drive  ✓    lighting    │  │
│ │        [ Save mapping ]      │  │   as steps land)          │  │
│ └──────────────────────────────┘  └───────────────────────────┘  │
│                                                                  │
│  FIRST FLIGHT ── after readiness ◉◉◉◉◉ ─────────────────────────│
│  ① Dry run · 2 orders   [ Start ]        ← pre-selected          │
│  ② Live · 2 orders      🔒 unlocks after a clean dry run         │
│  ③ Open the throttle    🔒 unlocks after 2 clean live orders     │
└──────────────────────────────────────────────────────────────────┘
```

**Why:** each wizard step self-verifies with a live probe (`token verified · portal …`,
relay `waiting for first ping…` flips green) so a non-developer watches each credential
*prove itself*. The ghost pipeline paints node-by-node as credentials land — onboarding
builds the operator's mental model and the monitoring vocabulary simultaneously. The
first-flight ladder is **physically locked** (padlock + reason), converting
progressive-disclosure advice into an interface ramp. On the first dry completion, the
trace drawer auto-opens once: the primary diagnostic is taught before anything real ever
runs. Minute 10 ends with two real orders synced and the throttle unlocked.

### 2.3 Order trace — held order, drawer over any surface

```
                                   ┌── TRACE · 100299009 ── open as page ─ ✕ ──┐
                                   │ ORDERS · LIVE   ⛉ PARKED   4 stages · 9s  │
                                   │                                           │
                                   │ ⛉  Parked — catalog gate                  │
                                   │    This product isn't active in HubSpot   │
                                   │    yet. The order will sync automatically │
                                   │    when the catalog releases it.          │
                                   │    Held 14m · View gate rules →           │
                                   │                                           │
                                   │ ▸ IDENTITY   4 steps · all ✓ · 1.2s       │
                                   │ ▾ CATALOG                                 │
                                   │   ✓ product check     found       11:04:07│
                                   │   ! catalog gate      bundle not  11:04:08│
                                   │     active — parked               (+0.3s) │
                                   │ ▸ WRITE      not reached · will resume    │
                                   │ ▸ RECORD     not reached                  │
                                   │                                           │
                                   │ ✓✓ seen before? no — first sync attempt   │
                                   │ ─ View raw log for this order ─           │
                                   └───────────────────────────────────────────┘
```

**Why:** reachable from *every* order ID in the product plus ⌘K paste. The 13 step types
group into four stages (Identity / Catalog / Write / Record); **all-green stages fold**
to a one-line summary so the drawer opens pre-focused on the step that matters. A held
order leads with the gold seal, the plain-language reason, and — critically — **the
future** ("what releases this"), so held reads as paused-mid-journey, not failed. Raw
log: one click, pre-filtered to this order. Diagnosis is three clicks from Fleet, zero
log files.

### 2.4 Attention state — one unrecovered failure

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ ● FAULT    Needs attention: 1 unrecovered failure in Orders · Live           │
│            — order 100299017 failed at line items          [ Open trace ]    │
├────────┬─────────────────────────────────────────────────────────────────────┤
│ Fleet  │  ORDERS · LIVE  ⎡red edge⎤   ORDERS · BACKFILL                      │
│ …      │ │ ● working · 1 failed │    │ ● creating · 62% │   ← everything     │
│ Ledger•│ └──────────────────────┘    └──────────────────┘     else unchanged │
│        │  (Commons, lanes, feed continue calmly below —                      │
│        │   the failure does not repaint the world)                           │
└────────┴─────────────────────────────────────────────────────────────────────┘
```

**Why:** the banner *replaces* the health sentence — calm and alarm are mutually
exclusive states of the same real estate, so red never competes with serenity. Exactly
three red elements (beacon, tile edge, Ledger badge), none animated. The rest of the
dashboard keeps working normally: one failure is one failure, not a crisis theme. The
banner's only affordance is the path to action (the trace, auto-scrolled to the first ✗,
with "retry from this step" when safe).

---

## 3 · Prompts for Claude Designer

> Paste each prompt whole. Each is self-contained. All data is sanitized demo data.

### Prompt 1 — Fleet dashboard (light, yield in progress)

```
Design a desktop web dashboard screen (1440×1024, light theme) for "Quiet Fleet", the
monitoring home of a data-sync engine that mirrors e-commerce orders into a CRM. The
aesthetic is quiet-luxury editorial: background #fafafa, cards #ffffff with 16px radius
and very soft shadows, text #141414, muted #757272, hairlines #ebebeb, 20px card padding,
40px between zones, tabular numerals everywhere.

Layout: a 220px left rail + main column. Rail items top-to-bottom: "Fleet" (active),
section label STREAMS with "Orders · Live" (teal dot) and "Orders · Backfill" (sage dot),
then "Ledger", "Audit", "Setup ✓". No red badge on Ledger (nothing is failing).

Top strip inside main column: a small sage dot + the word "NOMINAL" in 28px/700, followed
by a 24px/300 light sentence: "All 2 streams healthy · live is draining 3 orders ·
backfill yielding." Right side: a tiny freshness dot labeled "live · 2s ago", a theme
toggle, and a ⌘K chip.

Zone 1 — two stream tiles side by side (identical anatomy: status pill top-left, hero
number 22px/650 center, 40-point micro-sparkline footer, small gold chip "held 2 ⛉"):
Tile A "ORDERS · LIVE" pill "● working" in teal #1f7d86 on tint #dcf0f1, hero "3 queued ·
oldest 6s". Tile B "ORDERS · BACKFILL" pill "● creating" in sage #5ea380 on tint #e4f3e9,
hero "62% of window · cursor Mar 23".

Zone 2 — the signature "SHARED HUBSPOT BUDGET" card: one horizontal stacked bar, left
segment teal (labeled "live 3.9/s") visibly larger than the right sage segment (labeled
"backfill 0.6/s"), a thin goal tick near the right edge sliding inward with a small
inline rose #d789ad annotation "yielding to live · 4.6→0.6/s". Below it a collapsed row
hint "5 limiters · all healthy · ✦ full utilization".

Zone 3 — "LANES" list (left 60%): four small lane tickets, each with a 3px colored left
edge-stripe, an 11px/600 uppercase phase eyebrow, a mono order id, and "1 item · 4s"
muted footer. Two teal-striped (phases "creating order", "contact lookup"), one
sage-striped ("line items"), and one dashed-border ghost ticket "scanning — skipping
already-synced orders". Zone 3 right 40% — "FEED": three completion rows "✓ 100288417 →
HS 20xxxxx" with tiny stream chips, one row "✓✓ 100288390 skipped — already synced", one
"⛉ 100299009 parked — catalog gate" in gold #e2b13c.

Bottom: a serene one-line seal "0 unrecovered · nothing needs you ●" in sage, and a
collapsed disclosure "Details: all-time · period · slot".

No red anywhere on this screen. No warning triangles. Gold is calm, never alarming.
```

### Prompt 2 — Setup wizard with ghost pipeline (light)

```
Design a desktop setup screen (1440×1024, light theme, bg #fafafa, cards #ffffff, 16px
radius, text #141414, muted #757272) for a data-sync engine's first-run wizard, step 4
of 5. Two-column layout inside a centered 1100px container.

Left column (55%): a card titled "Step 4 · Pipeline mapping" with an 11px/600 uppercase
eyebrow "SET UP: ORDERS SYNC". Inside, a two-column mapping list: e-commerce statuses
("delivered", "shipped", "canceled", "restored") each with a dropdown mapped to CRM stage
names ("Shipped", "In transit", "Closed", "Reopened"). A primary button "Save mapping"
in sage #5ea380, white text. Above the card, a readiness strip of five circular chips —
four filled sage, one hollow — labeled tersely: token ✓, relay ✓, google ✓, mapping (in
progress), window.

Right column (45%): a "ghost pipeline" illustration card — a minimal node diagram drawn
in light blueprint grey #c9cdd2 dashed strokes: nodes "Salla" → "Relay" → "Engine" →
"HubSpot", with side taps "Sheets" and "Drive". The nodes already configured (Salla,
Relay, HubSpot, Sheets, Drive) are filled in their real colors (teal/sage) with small ✓
marks; the edge being configured right now (Engine → HubSpot stage mapping) is animating
in half-drawn. Caption: "Your pipeline lights up as each step lands."

Bottom band: a "FIRST FLIGHT" card with three staged rows: "① Dry run · 2 orders" with
an enabled sage button "Start"; "② Live · 2 orders" dimmed with a small padlock and the
caption "unlocks after a clean dry run"; "③ Open the throttle" dimmed with padlock and
"unlocks after 2 clean live orders". The locked rows are calm grey, not disabled-red.

Tone: instructional serenity — a wizard that proves each credential works (tiny "token
verified · portal Demo Store" caption under step 1's chip) rather than asking for trust.
```

### Prompt 3 — Order trace drawer, held order (dark)

```
Design a dark-theme desktop screen (1440×1024) showing a right-side drawer (540px) open
over a dimmed dashboard. Dark tokens: bg #101113, panel #191b1e, text #ececec, muted
#8f9296, hairlines #2a2d31, radius 16px. The drawer slides over the content with a soft
shadow; the dashboard behind is visible but 40% dimmed.

Drawer header: mono order id "100299009", a small teal chip "ORDERS · LIVE", a
circular gold seal ⛉ with the word "PARKED" (gold #e8bd58 on tint #2e2716), and meta
"4 stages · 9s". Top-right: "open as page ↗" and ✕.

Hero block: the gold seal enlarged with copy — title "Parked — catalog gate", body "This
product isn't active in HubSpot yet. The order will sync automatically when the catalog
releases it.", meta line "Held 14m · View gate rules →". Calm, stamp-like, zero warning
iconography.

Body: a vertical timeline grouped into four stages with 11px/600 uppercase eyebrows:
"IDENTITY — 4 steps · all ✓ · 1.2s" (collapsed, one line, sage #6fbf93 checkmark);
"CATALOG" (expanded): row "✓ product check — found · 11:04:07", row "! catalog gate —
bundle not yet active — parked · 11:04:08 (+0.3s)" with the ! in gold; "WRITE — not
reached · will resume when released" (collapsed, hollow dots); "RECORD — not reached"
(collapsed). Each expanded row: icon, 13px/400 human sentence, right-aligned tabular
timestamp and duration.

Footer: a quiet sage line "✓✓ seen before? no — first sync attempt", then a hairline,
then a muted link "View raw log for this order".

No red anywhere. The drawer must read as "paused mid-journey", not "failed".
```

### Prompt 4 — Fault state (light)

```
Design a desktop dashboard screen (1440×1024, light theme, bg #fafafa, panels #ffffff,
16px radius, text #141414) for a data-sync monitoring home in its ALARM state — exactly
one unrecovered failure. This screen demonstrates a strict alarm hierarchy: red #f55157
appears in exactly three places and NOWHERE else, and nothing red is animated.

Top strip: a small still red dot + the word "FAULT" in 28px/700, then — replacing the
usual healthy sentence — a banner sentence in 24px/300: "Needs attention: 1 unrecovered
failure in Orders · Live — order 100299017 failed at line items", with one primary
button "Open trace" (ink #141414 button, white text — the button itself is not red).

Left rail: "Fleet" active; under STREAMS "Orders · Live" and "Orders · Backfill";
"Ledger" carries a small red badge "1" (red appearance #2). "Audit", "Setup ✓" normal.

Zone 1: the two stream tiles. "ORDERS · LIVE" tile has a thin red edge glow (red
appearance #3, still, not pulsing) and its pill reads "● working · 1 failed". The
"ORDERS · BACKFILL" tile is completely normal (sage pill "● creating", hero "62%").

Everything below continues calmly and unchanged — the shared budget bar (teal + sage
segments), two lane tickets, a completions feed with ✓ rows and one gold ⛉ parked row.
The design must communicate: one failure is one failure, not a crisis theme. The rest of
the system keeps working and looks like it.

Contrast note: gold ⛉ "parked" chips and amber staleness dots may appear in the feed but
must read as clearly calmer than the three red elements.
```

---

## 4 · Prompts for Google Stitch

> Each prompt describes one interactive flow with explicit states and transitions.

### Stitch 1 — First-time setup wizard (10-minute first flight)

```
Build an interactive prototype of a 5-step setup wizard for a data-sync engine, desktop
web, light theme (bg #fafafa, white cards, 16px radius, sage green #5ea380 primary,
teal #1f7d86 accent).

Screens & states:
1. Empty shell: left rail (Fleet / Streams: two ghost tiles "not configured" / Ledger /
   Audit / Setup) with one hero CTA "Set up your first stream".
2. Wizard step 1 "Connect": two password fields (HubSpot token, relay secret) + "Save &
   test" button → on click, show an inline verification state: spinner 1s → sage caption
   "token verified · portal Demo Store". A readiness chip row (5 hollow circles) at top;
   chip 1 fills on success.
3. Step 2 "Relay": one URL field + a live listener panel "waiting for first ping…" that
   flips to "✓ ping received" (sage) after 2s. Chip 2 fills.
4. Step 3 "Google": file-drop for credentials + probe result "✓ Sheets & Drive reachable".
   Chip 3 fills. Include a "skip — local mirrors only" secondary path.
5. Step 4 "Pipeline mapping": four status→stage dropdown rows, "Save mapping". Chip 4.
6. Step 5 "Window & pacing": date-range picker + a preset "recommended ceiling" toggle.
   Chip 5 fills → auto-advance to the First Flight card.
7. First Flight: three staged rows — "Dry run · 2 orders" (enabled), "Live · 2" (locked,
   padlock + reason), "Open throttle" (locked). Clicking Start on dry → transition to a
   mini dashboard where two lane tickets appear, progress through phases (eyebrow text
   changes: "dedup check" → "contact lookup" → "creating order" → done), then a toast
   "Click any order to see every step we took" → clicking a completed order opens a
   trace drawer with a 4-stage timeline, dry steps tagged "simulated".
8. Returning to First Flight: row ② is now unlocked; clicking it opens a confirmation
   modal requiring the user to literally type "RUN" (input + disabled confirm until the
   text matches) with the consequence line "2 real orders will be written to HubSpot".
9. After live completes: row ③ unlocks; final state celebrates "First flight complete".

Transitions: wizard steps slide horizontally 240ms; chips fill with a 200ms pop; the
locked rows shake subtly (4px, 150ms) if clicked while locked, with a tooltip naming the
unlock condition.
```

### Stitch 2 — Dual-engine monitoring with a live-priority yield event

```
Build an interactive prototype of a monitoring dashboard ("Fleet") for two data-sync
engines sharing one API budget, desktop web, light theme (tokens: bg #fafafa, white
cards, teal #1f7d86 = live stream, sage #5ea380 = backfill stream, rose #d789ad =
coordination annotations, gold #e2b13c = parked).

Initial state (T0 — quiet): beacon "● NOMINAL" + sentence "All 2 streams healthy · live
queue 0 · backfill 78% through its window." Two stream tiles (live: "watching · queue
0"; backfill: "creating · 78%"). A "SHARED HUBSPOT BUDGET" stacked bar: sage segment
dominant (labeled "backfill 4.6/s"), sliver of teal ("live 0/s"). Below: two sage lane
tickets cycling phase text, a feed of ✓ completions.

Trigger (a "simulate new orders" dev button, or a 5s timer): THE YIELD EVENT —
1. Live tile pill flips to "working · 3 queued", queue hero counts 3 → 2 → 1 as lanes
   pick orders up.
2. The budget bar animates over 900ms: teal segment grows to dominant, sage compresses,
   and a rose annotation fades in beside it: "yielding to live · 4.6→0.6/s". A thin
   ceiling tick on the bar slides left.
3. Two teal lane tickets slide in from the left ABOVE the sage ones; one sage ticket
   dims to 60%.
4. The health sentence rewrites: "Live is draining 3 orders · backfill yielding."
5. Feed gains a rose event row: "backfill yielded its budget to live".

Resolution (after the 3 orders complete): teal lanes exit right with a brief sage
edge-flash; the budget bar ramps back in 3 visible discrete steps (0.6 → 1.4 → 2.9 →
4.6/s) each annotated "+reclaiming", teaching the additive-increase rhythm; sentence
returns to quiet; a "✦ full utilization" badge fades in at the bar's goal tick.

Interactions to include: hovering the budget bar shows a tooltip explaining the yield
("live orders get priority; the backfill hands over its speed budget and reclaims it
gradually"); clicking a stream tile once filters lanes+feed to that stream (second click
opens a detail view — can be a stub); clicking any lane ticket or feed row opens a trace
drawer stub.
```

### Stitch 3 — Diagnosing a held order (dashboard → trace → resolution)

```
Build an interactive prototype of a diagnostic flow, desktop web, light theme (white
cards on #fafafa, gold #e2b13c = parked/held, sage #5ea380 = success, teal #1f7d86 =
live stream, red #f55157 used NOWHERE in this flow).

State 1 — Fleet: the live stream tile shows a small gold chip "held 2 ⛉". Everything
else healthy (beacon NOMINAL). Hovering the chip: tooltip "2 orders parked — intentional,
not errors."

State 2 — click the chip → a "Parked orders" panel slides in: a two-row list, each row =
mono order id, held reason ("catalog gate — product not yet active"), age ("14m", "2h"),
and a gold seal icon. Caption at top: "Parked orders wait for a condition, then sync
automatically. Nothing here is broken."

State 3 — click row 1 → the trace drawer slides over (540px): header with order id +
"ORDERS · LIVE" chip + gold "PARKED" seal. Hero: "Parked — catalog gate" + plain-language
body "This product isn't active in HubSpot yet. The order will sync automatically when
the catalog releases it." + "Held 14m · View gate rules →". Timeline: "IDENTITY — 4
steps · all ✓ · 1.2s" (collapsed, expandable on click), "CATALOG" expanded showing
"✓ product check — found" and "! catalog gate — bundle not yet active — parked",
"WRITE — not reached · will resume when released", "RECORD — not reached". Footer link
"View raw log for this order" → opens a mono-font log panel pre-filtered to this id
(stub with 6 fake log lines).

State 4 — resolution simulation: a dev button "Simulate catalog release" → the drawer's
gold seal crossfades to a sage ✓ "Released — syncing now", the WRITE stage rows fill in
one by one (order create ✓, line items ✓, associations ✓), then RECORD fills, and the
header state flips to "✓ SYNCED · HS 20xxxxx". Back on Fleet, the held chip decrements
to "held 1 ⛉" and the feed gains "✓ 100299009 → HS 20xxxxx (released from catalog gate)".

The emotional arc to test: gold must never feel like a warning; the operator should end
the flow understanding that "held" resolves itself and requires (at most) a catalog
approval elsewhere — not a fix in this tool.
```

---

## 5 · Conversation plan — the feedback loop

Bring the Claude Designer mockups and Stitch prototypes back alongside this document.
Round 1 evaluates **direction**, not pixels.

### 5.1 What to evaluate in the static mockups

| Check | Method | Pass looks like |
|---|---|---|
| The 2-second glance | 5-second exposure test: show Prompt-1 output for 5s, ask "is anything wrong? how do you know?" | Answer cites beacon word or ledger seal, not a scan of numbers |
| Alarm authority | Show Prompt-4 (fault) after Prompt-1 (calm) | Viewer's eye goes banner → Ledger badge → tile edge; nothing else reads as alarming |
| Held ≠ broken | Show Prompt-3 (held trace) cold | Viewer describes the order as "waiting/paused", not "failed"; nobody asks "how do I fix it" |
| Commons legibility | Show Prompt-1, ask "what is the wide bar telling you?" | Uncoached viewers say some version of "live is getting priority right now" |
| Theme parity | Compare Prompt-3 (dark) against light screens | Dark reads as designed-for, not inverted; gold/amber/red stay distinguishable |
| Scale stress | Mentally add 4 more tiles + 2 rail entries to Prompt-1 | Nothing must be redesigned — only repeated |

### 5.2 What to test in the Stitch prototypes

- **Stitch 1 (setup):** time-to-first-dry-run for someone who has never seen the product
  (target < 10 min, zero external docs); whether the locked ladder reads as guidance or
  as friction; whether the type-RUN ritual feels like safety or nagging.
- **Stitch 2 (yield):** after watching the yield event once, ask the tester to explain
  what happened in their own words. Pass = "live orders got priority and the backfill
  slowed down on purpose." Also probe: did the backfill's slowdown cause any worry?
- **Stitch 3 (held):** count clicks from Fleet to "I understand why this order is
  waiting and what releases it" (target ≤ 3); ask whether anything in the flow felt like
  an error state.

### 5.3 Questions round 1 must answer before implementation

1. **Commons vs simpler yield viz** — is the stacked-bar metaphor instantly legible, or
   does the simpler "descending ceiling tick on the pacing bar" carry the story better?
   (Both are specced; keep the winner, demote the loser to the Details disclosure.)
2. **Sentence trust** — does the composed health sentence feel authoritative, or do
   operators re-verify it against the tiles every time (which would mean it earns its
   pixels only as a summary, not as the primary signal)?
3. **Rail density** — with 2 streams the stream-object shell adds one layer of
   indirection; does it feel over-built today? If yes, decide whether Fleet can *be* the
   only surface until stream #3 ships (the shell stays, the rail collapses).
4. **Gold reframe** — does "parked" + release-condition copy actually defeat years of
   amber-means-trouble instinct? If not, the fallback is neutralizing held to slate-grey
   with a gold accent only in the trace.
5. **Motion budget** — is the breathing bar + heartbeat sparkline combination calm or
   subtly fatiguing over a long session? (Test with the prototype left running in a
   corner for 20+ minutes, not a 2-minute demo.)

### 5.4 Round 2 and beyond

Round 2 = revised mockups answering §5.3 + a clickable dark-theme pass + the Fleet
filter interaction (focus-don't-navigate). Round 3 = component inventory handoff:
tokens as CSS custom properties (drop-in replacement for the existing `:root` block),
component specs (tile, ticket, seal, bars, drawer) with all states enumerated, and a
migration order (shell → Fleet → trace → wizard) that keeps the current UI shippable at
every step. Implementation should begin only after §5.3's five questions have answers.

---

*Every mechanism this vision visualizes — lanes, pacing ceilings, yield/reclaim, the
queue lifecycle, the 13-step trace, held reasons — already exists in the engine's SSE and
REST payloads today (see §1.7). This is a presentation revolution over a data foundation
that is already in production.*
