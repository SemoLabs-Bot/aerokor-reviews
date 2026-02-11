from __future__ import annotations

import json
import os

from review_hub.sheets_admin import ensure_tabs_exist
from review_hub.sheets_client import GogSheetsClient

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def load_sink_config() -> dict:
    path = os.path.join(WORKSPACE_ROOT, "config/review-hub/google-sheets.sink.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    sink = load_sink_config()
    account = sink["account"]
    sheet_id = sink["sheetId"]

    # Ensure tab exists
    creds_path = os.path.join(os.path.expanduser("~"), "Library", "Application Support", "gogcli", "credentials.json")
    ensure_tabs_exist(
        spreadsheet_id=sheet_id,
        account_email=account,
        titles=["looker_reviews"],
        credentials_path=creds_path,
    )

    client = GogSheetsClient(account=account, spreadsheet_id=sheet_id)

    # One-cell ARRAYFORMULA that mirrors raw rows and adds a few normalized fields for Looker Studio.
    # Columns A:O are identical to raw (main_review). Additional columns:
    # P: review_date_norm (best-effort), Q: rating_num, R: body_len
    # IMPORTANT: Sheet locale is ko_KR, so function argument separators should be ';'.
    # We'll write headers as plain values in row 1, and put the ARRAYFORMULA in A2.
    headers = [[
        "collected_date",
        "collected_at",
        "brand",
        "platform",
        "product_name",
        "product_url",
        "review_id",
        "review_date",
        "rating",
        "author",
        "title",
        "body",
        "body_hash",
        "dedup_key",
        "source_url",
        "review_date_norm",
        "rating_num",
        "body_len",
    ]]

    # Use HSTACK to avoid locale-specific array-literal separators.
    # NOTE: We used to stop when collected_date (A) is blank.
    # That caused missing rows if some ingests left A empty.
    # Instead, treat a row as "present" when body (L) is present.
    # Also, if collected_date is blank, derive it from collected_at (B).
    formula = (
        "=ARRAYFORMULA("
        "IF(main_review!L3:L=\"\";;"
        "HSTACK("
        # A: collected_date (fill from collected_at if missing)
        "IF(main_review!A3:A<>\"\";main_review!A3:A;IFERROR(TEXT(DATEVALUE(LEFT(main_review!B3:B;10));\"yyyy-mm-dd\");\"\"));"
        # B:O
        "main_review!B3:O;"
        # P: review_date_norm (best-effort)
        "IFERROR(TEXT(DATEVALUE(SUBSTITUTE(LEFT(main_review!H3:H;10);\".\";\"-\"));\"yyyy-mm-dd\");\"\");"
        # Q: rating_num
        "IFERROR(VALUE(main_review!I3:I);\"\");"
        # R: body_len
        "LEN(main_review!L3:L)"
        ")"
        ")"
        ")"
    )

    # Headers can be written as RAW.
    client.update("looker_reviews!A1:R1", headers)

    # Formula must be USER_ENTERED (not RAW), otherwise it becomes plain text.
    import subprocess
    subprocess.check_call([
        "gog",
        "sheets",
        "update",
        sheet_id,
        "looker_reviews!A2",
        "--values-json",
        json.dumps([[formula]], ensure_ascii=False),
        "--input",
        "USER_ENTERED",
        "--no-input",
        "--account",
        account,
    ])


if __name__ == "__main__":
    main()
