#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."  # workspace root

# 1) Export latest reviews.json from Google Sheets looker_reviews
python3 scripts/review-hub/export_reviews_json.py >/dev/null

# 2) Commit+push only if changed
if git diff --quiet -- data/reviews.json; then
  echo "no_change"
  exit 0
fi

ts=$(date -u +"%Y-%m-%d")

git add data/reviews.json
# allow empty? no, we checked diff

git commit -m "Dashboard data: refresh reviews.json (${ts})" >/dev/null

git push >/dev/null

echo "pushed"
