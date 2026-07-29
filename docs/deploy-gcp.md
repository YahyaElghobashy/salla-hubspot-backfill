# Running the live sync 24/7 on GCP

The engine is plain Python + outbound HTTPS — a e2-micro (free tier) is
plenty (a modest hourly order rate, bursts of a few hundred after downtime).

## Setup (Debian/Ubuntu VM)

```bash
sudo apt-get update && sudo apt-get install -y python3-venv git
git clone <your-repo> backfill && cd backfill
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
# copy the gitignored payload from the current machine with scp — NEVER git:
#   .env (0600)        HubSpot + relay + Slack + Make API tokens
#   config.json        backfill config   (needed for backfill/drain runs)
#   config.live.json   live-sync + credit-watch config
#   credentials.json   Google OAuth client
#   token.json         Google refresh token (no browser flow needed on the VM)
# and the idempotency state:
#   mirror/created.csv live_state.json  cursor.json
```

## systemd unit

`/etc/systemd/system/salla-live-sync.service`:

```ini
[Unit]
Description=Salla -> HubSpot live order sync
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=backfill
WorkingDirectory=/home/backfill/backfill
ExecStart=/home/backfill/backfill/venv/bin/python3 live.py --live --yes
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

`systemctl enable --now salla-live-sync`. systemd replaces run.py's
restart-forever loop (either works; don't run both). `STOP.live` still
pauses claiming gracefully; `systemctl stop` sends SIGINT → in-flight
orders finish.

## Second unit: the credit watcher

The watcher must outlive the engines — being awake during an outage is its
job — so it gets its own service, `/etc/systemd/system/salla-credit-watch.service`:

```ini
[Unit]
Description=Make.com credit watcher (balance alerts, refill + renewal notices)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=backfill
WorkingDirectory=/home/backfill/backfill
ExecStart=/home/backfill/backfill/venv/bin/python3 credit_watch.py --interval 300
Restart=always
RestartSec=60

[Install]
WantedBy=multi-user.target
```

`systemctl enable --now salla-credit-watch`. It reads `config.live.json` and
`.env` (`MAKE_API_TOKEN`, `SLACK_*`); `STOP.credits` stops it gracefully.

## Bounded jobs: backfill and queue drain — NOT services

Run these by hand (tmux/screen), one at a time, never beside each other:

```bash
./venv/bin/python3 run.py --live --yes            # historical backfill, supervised
./venv/bin/python3 queue_drain.py --live          # release catalog-held orders
```

Both yield the HubSpot budget to live sync automatically, so live stays on.
See `docs/QUEUE_DRAIN_RUNBOOK.md` for the drain's scan → verify → pilot cycle.

## Migration from the local machine (no gap, no double-processing)

1. Stop the local live engine (`STOP.live`, wait for the session summary).
2. Copy `mirror/created.csv` (the idempotency ledger), `live_state.json`,
   `cursor.json` (backfill bookmark) and `mirror/credit_state.json` (alert
   thresholds already fired this cycle) along with the configs.
3. Start the service. The sheet heartbeat also refuses dual claiming for
   90s if you forget step 1.

## Monitoring

- `journalctl -u salla-live-sync -f` or `tail -f live.log`
- the web UI works on the server too: `venv/bin/python serve.py --host 0.0.0.0`
  behind your firewall/IAP of choice (it exposes engine control — do NOT
  leave it on a public IP).
- Watch for: `SWEEP enqueued` (webhook losses), `UNRECOVERED FAILURES`
  banners, `FOREIGN LIVE INSTANCE` (two consumers), queue depth growth in
  the `QUEUE` lines.

## Security posture (applied 2026-07-29)

Principle: the VM does exactly its job — outbound HTTPS to five providers —
and nothing else. Every control below is free and adds zero operational
turbulence.

| Layer | Rule | Why |
|---|---|---|
| Ingress | `deny-all` (prio 1000, tag-scoped) | the engine accepts no inbound traffic, ever |
| Ingress | `allow tcp:22` **only from 35.235.240.0/20** (Google IAP) | SSH exists solely through IAP tunnels; the public IP answers nothing |
| Egress | `allow tcp:443` + `deny all` | HubSpot, Make, Google APIs, Slack, GitHub, apt — all HTTPS; anything else (exfil channels, port-80, SMTP relays) is dead. DNS/NTP use the link-local metadata server, unaffected |
| Legacy holes | `default-allow-ssh/rdp/icmp` **deleted** | network-wide 0.0.0.0/0 allows removed; future VMs don't inherit them |
| Identity | **no service account** on the VM | a compromised box holds zero GCP permissions |
| SSH auth | **OS Login enforced project-wide** | access granted/revoked via IAM roles, every login audited; metadata SSH keys disabled |
| Boot | Shielded VM (secure boot, vTPM, integrity monitoring) | detects boot-level tampering |
| Patching | `unattended-upgrades` + apt over HTTPS | always-on box patches itself |
| Secrets | `.env`/tokens `0600`, never in git | |
| Dashboard | loopback only, via SSH tunnel | it has run controls and no auth — never expose it |

Deliberately **not** done (cost/benefit):
- FQDN egress filtering — needs Cloud NGFW or a proxy; provider IPs rotate, so
  it breaks things for near-zero gain once egress is 443-only on a box with no
  service account
- Dropping the external IP — requires Cloud NAT (~$32/mo, ≈2× the VM) for no
  additional exposure given deny-all ingress; revisit if policy demands it

Owner-side (billing perms required): a **budget alert** on the billing account.
