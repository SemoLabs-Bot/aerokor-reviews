#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."  # workspace root

# Daily end-to-end:
# 1) Discover product URLs
# 2) Collect recent reviews (imweb + browser-based collectors)
# 3) Refresh looker_reviews view formula
# 4) Export dashboard data files
# 5) Commit+push if data changed

export PYTHONPATH=.

# 1) Discovery + (imweb) review collection
python3 -m review_hub.run_pipeline --max-products "${REVIEW_HUB_MAX_PRODUCTS:-200}" >/dev/null

# 2) Browser-based collectors (ohou/coupang/naver/smartstore/wadiz)
# Keep this bounded so cron doesn't overlap forever.
python3 -m review_hub.run_browser_queue --max-minutes "${REVIEW_HUB_BROWSER_MAX_MINUTES:-55}" --sleep "${REVIEW_HUB_BROWSER_SLEEP_S:-2}" >/dev/null || true

# 3) Ensure looker_reviews formula stays correct
PYTHONPATH=. python3 review_hub/setup_looker_views.py >/dev/null

# 4) Export data artifacts
python3 scripts/review-hub/export_reviews_json.py >/dev/null

# 5) Publish if changed
if git diff --quiet -- data; then
  echo "result=no_change"
  exit 0
fi

ts=$(date -u +"%Y-%m-%d")

git add data

git commit -m "Dashboard data: refresh (${ts})" >/dev/null

git push >/dev/null

echo "result=pushed"
