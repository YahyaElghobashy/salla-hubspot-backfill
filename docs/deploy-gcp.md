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
