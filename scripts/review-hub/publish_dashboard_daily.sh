#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."  # workspace root

# NOTE: This script should stay LIGHT. Avoid extra Sheets reads (quota).
# It exports dashboard data files from looker_reviews and publishes if changed.

# 1) Export latest data artifacts from Google Sheets looker_reviews
python3 scripts/review-hub/export_reviews_json.py >/dev/null

# 2) Commit+push only if changed (any file under data/)
if git diff --quiet -- data; then
  COUNT=$(python3 - <<'PY'
import json
p='data/reviews_meta.json'
try:
  j=json.load(open(p,'r',encoding='utf-8'))
  print(j.get('count') or 0)
except Exception:
  # fallback
  j=json.load(open('data/reviews.json','r',encoding='utf-8'))
  print(j.get('count') or 0)
PY
)
  echo "result=no_change count=${COUNT}"
  exit 0
fi

ts=$(date -u +"%Y-%m-%d")

git add data

git commit -m "Dashboard data: refresh (${ts})" >/dev/null

git push >/dev/null

COUNT=$(python3 - <<'PY'
import json
p='data/reviews_meta.json'
try:
  j=json.load(open(p,'r',encoding='utf-8'))
  print(j.get('count') or 0)
except Exception:
  j=json.load(open('data/reviews.json','r',encoding='utf-8'))
  print(j.get('count') or 0)
PY
)

echo "result=pushed count=${COUNT}"
