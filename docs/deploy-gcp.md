# Running the live sync 24/7 on GCP

The engine is plain Python + outbound HTTPS — a e2-micro (free tier) is
plenty (~25 orders/hour average, bursts of a few hundred after downtime).

## Setup (Debian/Ubuntu VM)

```bash
sudo apt-get update && sudo apt-get install -y python3-venv git
git clone <your-repo> backfill && cd backfill
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
# copy config.live.json, .env (0600), credentials.json, token.json from the
# current machine (scp). token.json carries the Google refresh token, so no
# browser flow is needed on the server.
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

## Migration from the local machine (no gap, no double-processing)

1. Stop the local live engine (`STOP.live`, wait for the session summary).
2. Copy `mirror/created.csv` (the idempotency ledger) and `live_state.json`
   to the server along with the configs.
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
