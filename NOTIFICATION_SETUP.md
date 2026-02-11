# Notification Setup (Current State)

## 1) Heartbeat-based assistant notifications

Source of truth: `HEARTBEAT.md`

- Runs are intended for KST weekdays 09:00~19:00 via gateway `activeHours` setting.
- If there is no issue, output must be exactly `HEARTBEAT_OK`.
- Checks currently include:
  1. Gmail triage (new unread in 60m + urgent leftovers in 24h)
  2. Calendar conflicts/overload (next 24h)
  3. Flight check-in window detection (default open=departure-24h)
  4. Voice->Jira candidate detection from `logs/jira-voice/runs/` (excluding `_dev`)

## 2) Email notification flows

### Meta ads daily flow

File: `scripts/meta_ads_daily_flow.py`

- Sends completion email after Google Sheets update.
- SMTP helper: `skills/imap-smtp-email/scripts/smtp.js send`
- Default recipients/sender are fixed in script:
  - To: `realtiger@wekeepgrowing.com`
  - From: `semolabsbot@gmail.com`
- In batch mode (`--start`/`--end`), sends one email only when append succeeded.

### Imweb orders daily flow

File: `scripts/imweb_orders_daily_flow.py`

- Completion email is optional (`--email`).
- Defaults:
  - To: `realtiger@wekeepgrowing.com`
  - From: `IMWEB_EMAIL_FROM` env or `GOG_ACCOUNT` env, fallback `semolabsbot@gmail.com`
- SMTP helper is same as meta flow.

## 3) Review dashboard in-app notifications

File: `scripts/review-hub/export_reviews_json.py`

- On each publish, builds/updates `updates.json` feed entries.
- Entry is generated from low-rating reviews (rating <=2) collected today.
- Message format example: `2점 이하 리뷰 N건 추가`.

## 4) Scheduled job docs

File: `scripts/review-hub/cron-ohou.md`

- Contains a recommended OpenClaw isolated cron job for Ohou collector.
- Example schedule in doc: daily `01:15` (Asia/Seoul).
