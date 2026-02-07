from __future__ import annotations

import json
import os
import subprocess
import tempfile
from typing import Any, Dict, List, Optional

# NOTE: We avoid Python TLS issues (LibreSSL) by using curl for Google API calls.
# curl on macOS uses system TLS and is more reliable here.



def _env_account_cmd(account: Optional[str] = None):
    env = os.environ.copy()
    if account:
        env["GOG_ACCOUNT"] = account
    return env


def get_refresh_token_via_gog(*, account_email: str) -> str:
    """Export refresh token via `gog auth tokens export` to a temp file.

    NOTE: Contains secrets; we delete the temp file immediately after reading.
    """
    fd, path = tempfile.mkstemp(prefix="gog_refresh_", suffix=".json")
    os.close(fd)
    try:
        cmd = [
            "gog",
            "auth",
            "tokens",
            "export",
            account_email,
            "--out",
            path,
            "--overwrite",
            "--no-input",
        ]
        subprocess.check_call(cmd, env=_env_account_cmd(account_email))
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        rt = data.get("refresh_token") or data.get("refreshToken")
        if not rt:
            raise RuntimeError("refresh_token not found in exported token file")
        return str(rt)
    finally:
        try:
            os.remove(path)
        except Exception:
            pass


def exchange_refresh_for_access_token(*, client_id: str, client_secret: str, refresh_token: str) -> str:
    out = subprocess.check_output(
        [
            "curl",
            "-s",
            "-X",
            "POST",
            "https://oauth2.googleapis.com/token",
            "-d",
            f"client_id={client_id}",
            "-d",
            f"client_secret={client_secret}",
            "-d",
            f"refresh_token={refresh_token}",
            "-d",
            "grant_type=refresh_token",
        ]
    ).decode("utf-8")
    j = json.loads(out)
    token = j.get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in response: {j}")
    return str(token)


def sheets_metadata_via_gog(*, spreadsheet_id: str, account_email: str) -> Dict[str, Any]:
    cmd = [
        "gog",
        "sheets",
        "metadata",
        spreadsheet_id,
        "--json",
        "--no-input",
        "--account",
        account_email,
    ]
    out = subprocess.check_output(cmd).decode("utf-8")
    return json.loads(out)


def list_sheet_titles(*, spreadsheet_id: str, account_email: str) -> List[str]:
    md = sheets_metadata_via_gog(spreadsheet_id=spreadsheet_id, account_email=account_email)
    titles: List[str] = []
    for s in (md.get("sheets") or []):
        props = (s or {}).get("properties") or {}
        t = props.get("title")
        if t:
            titles.append(str(t))
    return titles


def add_sheet_tab(
    *,
    spreadsheet_id: str,
    account_email: str,
    title: str,
    client_id: str,
    client_secret: str,
) -> None:
    refresh_token = get_refresh_token_via_gog(account_email=account_email)
    access_token = exchange_refresh_for_access_token(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
    )

    body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": title,
                    }
                }
            }
        ]
    }

    out = subprocess.check_output(
        [
            "curl",
            "-s",
            "-X",
            "POST",
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate",
            "-H",
            f"Authorization: Bearer {access_token}",
            "-H",
            "Content-Type: application/json",
            "--data-binary",
            json.dumps(body, ensure_ascii=False),
        ]
    ).decode("utf-8")

    # If already exists, Sheets API returns an error object; treat as ok.
    try:
        j = json.loads(out)
    except Exception:
        j = {"text": out}

    if isinstance(j, dict) and j.get("error"):
        msg = json.dumps(j, ensure_ascii=False)
        if "이미 있습니다" in msg or "already exists" in msg or "duplicate" in msg.lower():
            return
        raise RuntimeError(msg)


def ensure_tabs_exist(
    *,
    spreadsheet_id: str,
    account_email: str,
    titles: List[str],
    credentials_path: str,
) -> List[str]:
    """Ensure sheet tabs exist. Returns list of newly created titles."""
    with open(credentials_path, "r", encoding="utf-8") as f:
        creds = json.load(f)
    client_id = creds["client_id"]
    client_secret = creds["client_secret"]

    existing = set(list_sheet_titles(spreadsheet_id=spreadsheet_id, account_email=account_email))
    created: List[str] = []
    for t in titles:
        if t in existing:
            continue
        add_sheet_tab(
            spreadsheet_id=spreadsheet_id,
            account_email=account_email,
            title=t,
            client_id=client_id,
            client_secret=client_secret,
        )
        created.append(t)
    return created
