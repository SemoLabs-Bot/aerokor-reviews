#!/usr/bin/env python3
"""Create/label an OpenClaw session key and print a Control UI URL.

Why:
- Control UI can pin a session via ?session=<sessionKey>
- But it has no built-in ?label=... query param.
- So we do one-time labeling via `sessions.patch`.

Usage examples:
  python3 scripts/openclaw_work_session.py --label "메타광고(일자별 보고서)" --key work-meta-ads
  python3 scripts/openclaw_work_session.py --label "운영/디버깅" --open
  python3 scripts/openclaw_work_session.py --label "리뷰허브 수집/정규화" --with-token

Notes:
- --with-token prints the tokenized URL; avoid pasting it into public places.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode


DEFAULT_BASE_URL = "http://127.0.0.1:18789/"


def load_gateway_token() -> str:
    cfg_path = Path.home() / ".openclaw" / "openclaw.json"
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    token = cfg.get("gateway", {}).get("auth", {}).get("token")
    if not isinstance(token, str) or not token.strip():
        raise RuntimeError(f"Gateway token missing in {cfg_path}")
    return token.strip()


def run_sessions_patch(key: str, label: str, timeout_ms: int = 8000) -> dict:
    params = {"key": key, "label": label}
    cmd = [
        "openclaw",
        "gateway",
        "call",
        "sessions.patch",
        "--params",
        json.dumps(params, ensure_ascii=False),
        "--timeout",
        str(timeout_ms),
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or p.stdout.strip() or f"command failed: {' '.join(cmd)}")
    try:
        return json.loads(p.stdout)
    except json.JSONDecodeError:
        raise RuntimeError(p.stdout.strip())


def make_default_key() -> str:
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    rand = secrets.token_hex(2)
    return f"work-{ts}-{rand}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True, help="Human label shown in Sessions UI (Korean OK)")
    ap.add_argument("--key", default=None, help="Session key (recommended: work-...). If omitted, auto-generated.")
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL, help=f"Control UI base URL (default {DEFAULT_BASE_URL})")
    ap.add_argument("--with-token", action="store_true", help="Include gateway token in the printed URL")
    ap.add_argument("--open", action="store_true", help="Open the URL (macOS: `open`) after patch")
    args = ap.parse_args()

    key = (args.key or "").strip() or make_default_key()
    label = args.label.strip()

    res = run_sessions_patch(key=key, label=label)
    session_key = res.get("key") or res.get("entry", {}).get("key")
    if not isinstance(session_key, str) or not session_key.strip():
        # Gateway response should include `key`.
        raise RuntimeError(f"Unexpected sessions.patch response (missing key): {res}")

    q = {"session": session_key}
    if args.with_token:
        q["token"] = load_gateway_token()

    url = args.base_url.rstrip("/") + "/" + ("?" + urlencode(q))

    print(session_key)
    print(url)

    if args.open:
        opener = "open" if sys.platform == "darwin" else os.environ.get("BROWSER")
        if opener == "open":
            subprocess.run(["open", url], check=False)
        elif opener:
            subprocess.run([opener, url], check=False)
        else:
            print("(not opened: no opener available)", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
