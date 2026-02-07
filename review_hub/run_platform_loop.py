from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass
from typing import Optional

from review_hub.lock import file_lock

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@dataclass
class LoopConfig:
    lock_path: str
    cmd: str
    max_minutes: int = 55
    sleep_s: float = 2.0
    per_run_timeout_s: int = 1800


def _run(cmd: str, *, timeout_s: int) -> int:
    p = subprocess.run(
        ["bash", "-lc", cmd],
        cwd=WORKSPACE_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=timeout_s,
        check=False,
    )
    return int(p.returncode)


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Run a single platform collector in a tight loop (bounded by time + guarded by a lock)")
    ap.add_argument("--lock", required=True, help="Lock file path (workspace-relative or absolute)")
    ap.add_argument("--cmd", required=True, help="Command to run (bash -lc)")
    ap.add_argument("--max-minutes", type=int, default=55)
    ap.add_argument("--sleep", type=float, default=2.0)
    ap.add_argument("--per-run-timeout", type=int, default=1800)

    args = ap.parse_args(argv)

    lock_path = args.lock
    if not os.path.isabs(lock_path):
        lock_path = os.path.join(WORKSPACE_ROOT, lock_path)

    cfg = LoopConfig(
        lock_path=lock_path,
        cmd=str(args.cmd),
        max_minutes=int(args.max_minutes),
        sleep_s=float(args.sleep),
        per_run_timeout_s=int(args.per_run_timeout),
    )

    deadline = time.time() + cfg.max_minutes * 60

    with file_lock(cfg.lock_path):
        while time.time() < deadline:
            _run(cfg.cmd, timeout_s=cfg.per_run_timeout_s)
            time.sleep(cfg.sleep_s)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
