#!/usr/bin/env python3
"""Launch the local web UI (setup wizard + run control + live dashboard).

    python serve.py            # http://127.0.0.1:8377 opens in your browser
    python serve.py --port 9000 --no-browser
"""
import argparse
import threading
import webbrowser


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8377)
    ap.add_argument("--host", default="127.0.0.1",
                    help="bind address; use 0.0.0.0 to view from other devices on your LAN")
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

    from webui.app import create_app
    app = create_app()
    url = f"http://{'127.0.0.1' if args.host == '0.0.0.0' else args.host}:{args.port}"
    if not args.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    print(f"Salla -> HubSpot backfill UI: {url}")
    app.run(host=args.host, port=args.port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
