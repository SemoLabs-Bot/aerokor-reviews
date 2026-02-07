from __future__ import annotations

import os
from contextlib import contextmanager


@contextmanager
def file_lock(path: str):
    """Advisory exclusive lock using flock (macOS/Linux).

    Creates the file if missing.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    f = open(path, "a+", encoding="utf-8")
    try:
        import fcntl

        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            import fcntl

            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            f.close()
        except Exception:
            pass
