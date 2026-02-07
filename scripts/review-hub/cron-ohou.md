# OpenClaw cron suggestion: review-hub ohou (오늘의집) collector

This repo includes a browser-based collector:

```bash
python3 -m review_hub.collect_ohou_browser --all-brands \
  --browser-profile openclaw \
  --per-page 100 --order recent \
  --max-pages-per-goods 50 --max-reviews-per-goods 800
```

## Recommended OpenClaw cron job (agent message payload)

Create an **isolated** cron job that runs daily (example: 01:15 KST):

```bash
openclaw cron add \
  --name review-hub-ohou \
  --description "Collect 오늘의집 reviews (ohou) -> JSON -> ingest to Sheets" \
  --cron "15 1 * * *" \
  --tz Asia/Seoul \
  --session isolated \
  --thinking off \
  --timeout-seconds 1800 \
  --message "Run: python3 -m review_hub.collect_ohou_browser --all-brands --browser-profile openclaw"
```

Notes:
- Use `--browser-profile openclaw` to avoid needing the Chrome Extension relay attachment.
- Dedup is enforced via `state/review-hub/dedup-keys.txt`.
- Concurrency is guarded via `state/review-hub/ohou-collector.lock`.
