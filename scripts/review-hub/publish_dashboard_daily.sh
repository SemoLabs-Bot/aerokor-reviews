#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."  # workspace root

# NOTE: This script should stay LIGHT (hourly). Avoid extra Sheets reads (quota).
# - It only exports looker_reviews -> data/reviews.json and publishes if changed.
# - De-dupe / view-setup are handled by separate maintenance runs.

# 1) Export latest reviews.json from Google Sheets looker_reviews
python3 scripts/review-hub/export_reviews_json.py >/dev/null

# 2) Commit+push only if changed
if git diff --quiet -- data/reviews.json; then
  COUNT=$(python3 - <<'PY'
import json
p='data/reviews.json'
j=json.load(open(p,'r',encoding='utf-8'))
print(j.get('count') or 0)
PY
)
  echo "result=no_change count=${COUNT}"
  exit 0
fi

ts=$(date -u +"%Y-%m-%d")

git add data/reviews.json

git commit -m "Dashboard data: refresh reviews.json (${ts})" >/dev/null

git push >/dev/null

COUNT=$(python3 - <<'PY'
import json
p='data/reviews.json'
j=json.load(open(p,'r',encoding='utf-8'))
print(j.get('count') or 0)
PY
)

echo "result=pushed count=${COUNT}"
