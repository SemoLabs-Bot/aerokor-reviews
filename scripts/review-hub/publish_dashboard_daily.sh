#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."  # workspace root

SHEET_ID="1hIRtmAw1k8ajicstoi2eI5Vp0pc6xSJE8HOL7ZlyVTU"
ACCOUNT="semolabsbot@gmail.com"

count_reviews() {
  # Count non-empty dedup_key rows in looker_reviews
  python3 - <<'PY'
import json,subprocess
SHEET_ID = "1hIRtmAw1k8ajicstoi2eI5Vp0pc6xSJE8HOL7ZlyVTU"
ACCOUNT = "semolabsbot@gmail.com"
out=subprocess.check_output([
  'gog','sheets','get',SHEET_ID,'looker_reviews!N2:N100000','--json','--no-input','--account',ACCOUNT
], text=True)
vals=json.loads(out).get('values',[])
print(sum(1 for r in vals if r and str(r[0]).strip()))
PY
}

BEFORE_COUNT="$(count_reviews)"

# 1) Export latest reviews.json from Google Sheets looker_reviews
python3 scripts/review-hub/export_reviews_json.py >/dev/null

# 2) Commit+push only if changed
if git diff --quiet -- data/reviews.json; then
  echo "result=no_change count=${BEFORE_COUNT}"
  exit 0
fi

ts=$(date -u +"%Y-%m-%d")

git add data/reviews.json

git commit -m "Dashboard data: refresh reviews.json (${ts})" >/dev/null

git push >/dev/null

AFTER_COUNT="$(count_reviews)"

echo "result=pushed count=${AFTER_COUNT}"
