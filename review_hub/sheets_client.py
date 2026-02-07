import json
import os
import re
import subprocess
from typing import List, Optional

from review_hub.lock import file_lock


def _run_quiet(cmd: list[str], *, env: dict):
    # Silence gog's human-friendly stdout (e.g., "Updated ...") so upstream JSON parsing doesn't break.
    subprocess.run(cmd, env=env, check=True, stdout=subprocess.DEVNULL)


def _env_account_cmd(account: Optional[str] = None):
    env = os.environ.copy()
    if account:
        env["GOG_ACCOUNT"] = account
    return env


def _sheets_lock_path() -> str:
    # One global lock for all Sheets writes (across sessions)
    return os.path.join(os.path.expanduser("~"), ".openclaw", "locks", "review-hub-sheets.lock")


class GogSheetsClient:
    def __init__(self, *, account: str, spreadsheet_id: str):
        self.account = account
        self.spreadsheet_id = spreadsheet_id

    def get(self, a1_range: str) -> List[List[str]]:
        """Return values for a given A1 range."""
        cmd = [
            "gog",
            "sheets",
            "get",
            self.spreadsheet_id,
            a1_range,
            "--json",
            "--no-input",
        ]
        out = subprocess.check_output(cmd, env=_env_account_cmd(self.account)).decode("utf-8")
        data = json.loads(out)
        return data.get("values") or []

    def update(self, a1_range: str, values_2d: List[List[object]]):
        cmd = [
            "gog",
            "sheets",
            "update",
            self.spreadsheet_id,
            a1_range,
            "--values-json",
            json.dumps(values_2d, ensure_ascii=False),
            "--input",
            "RAW",
            "--no-input",
        ]
        with file_lock(_sheets_lock_path()):
            _run_quiet(cmd, env=_env_account_cmd(self.account))

    def append(self, a1_range: str, values_2d: List[List[object]]):
        """Google Sheets 'append' API (table-heuristic). Use carefully."""
        cmd = [
            "gog",
            "sheets",
            "append",
            self.spreadsheet_id,
            a1_range,
            "--values-json",
            json.dumps(values_2d, ensure_ascii=False),
            "--input",
            "RAW",
            "--no-input",
        ]
        with file_lock(_sheets_lock_path()):
            _run_quiet(cmd, env=_env_account_cmd(self.account))

    def append_fixed(
        self,
        *,
        tab: str,
        start_row: int,
        start_col: str,
        end_col: str,
        values_2d: List[List[object]],
        sentinel_col: str = "A",
        sentinel_regex: str = r"^\d{4}-\d{2}-\d{2}$",
        scan_max_rows: int = 5000,
    ) -> str:
        """Append rows by explicitly computing the next row and using update.

        This avoids Sheets 'append' table heuristics that can shift columns when the sheet
        contains multiple sections.
        """
        if not values_2d:
            return ""

        # Read sentinel column to find the last review row (match by regex).
        scan_range = f"{tab}!{sentinel_col}{start_row}:{sentinel_col}{start_row + scan_max_rows - 1}"
        col_vals = self.get(scan_range)

        pat = re.compile(sentinel_regex)
        last = start_row - 1
        for i, row in enumerate(col_vals, start=start_row):
            v = row[0] if row else ""
            if isinstance(v, str) and pat.search(v.strip()):
                last = i

        next_row = last + 1
        end_row = next_row + len(values_2d) - 1
        write_range = f"{tab}!{start_col}{next_row}:{end_col}{end_row}"
        self.update(write_range, values_2d)
        return write_range
