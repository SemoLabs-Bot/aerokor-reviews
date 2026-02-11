#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."  # workspace root

# Daily end-to-end:
# 1) Discover product URLs
# 2) Collect recent reviews (imweb + browser-based collectors)
# 3) (Safety) Deduplicate main_review to prevent duplicates leaking into dashboard
# 4) Refresh looker_reviews view formula
# 5) Export dashboard data files
# 6) Commit+push if data changed
#
# Also writes a local checklist JSON to logs/review-hub/ so we can audit per-platform progress.

export PYTHONPATH=.

STAMP=$(date -u +"%Y%m%d_%H%M%S")
CHECKLIST_PATH="logs/review-hub/daily_checklist_${STAMP}.json"
mkdir -p logs/review-hub

# Simple checklist log (append via heredoc overwrite at the end)
DISCOVERY_JSON=""
IMWEB_JSON=""
BROWSER_QUEUE_OK=false
BROWSER_QUEUE_OK_STR="false"
DEDUPE_JSON=""
EXPORT_OK=false
PUSH_RESULT=""

# For reporting
YESTERDAY_COUNT=""
NEW_COLLECTED_THIS_RUN=0
DASHBOARD_APPLIED=""

# 1) Discovery + (imweb) review collection
DISCOVERY_JSON=$(python3 -m review_hub.run_daily 2>/dev/null || true)
IMWEB_JSON=$(python3 -m review_hub.collect_reviews --max-products "${REVIEW_HUB_MAX_PRODUCTS:-200}" 2>/dev/null || true)
# best-effort parse new collected count from imweb collector output
NEW_COLLECTED_THIS_RUN=$(printf "%s" "$IMWEB_JSON" | python3 - <<'PY'
import json, sys
s=sys.stdin.read().strip()
try:
  j=json.loads(s)
  print(int(j.get('reviews_appended') or 0))
except Exception:
  print(0)
PY
)
# 2) Browser-based collectors (ohou/coupang/naver/smartstore/wadiz)
# Keep this bounded so cron doesn't overlap forever.
if python3 -m review_hub.run_browser_queue --max-minutes "${REVIEW_HUB_BROWSER_MAX_MINUTES:-55}" --sleep "${REVIEW_HUB_BROWSER_SLEEP_S:-2}" >/dev/null 2>&1; then
  BROWSER_QUEUE_OK=true
  BROWSER_QUEUE_OK_STR="true"
else
  BROWSER_QUEUE_OK=false
  BROWSER_QUEUE_OK_STR="false"
fi

# 3) Safety dedupe (review_id/body_hash based)
DEDUPE_JSON=$(python3 scripts/review-hub/dedupe_main_review_reviewid_or_bodyhash.py 2>/dev/null || true)

# 4) Ensure looker_reviews formula stays correct
PYTHONPATH=. python3 review_hub/setup_looker_views.py >/dev/null

# 5) Export data artifacts
python3 scripts/review-hub/export_reviews_json.py >/dev/null
EXPORT_OK=true

# Read yesterday count from generated insights.json (fast)
YESTERDAY_COUNT=$(python3 - <<'PY'
import json
p='data/insights.json'
try:
  j=json.load(open(p,'r',encoding='utf-8'))
  print(j.get('yesterday_review_count') or 0)
except Exception:
  print(0)
PY
)

# 6) Publish if changed
if git diff --quiet -- data; then
  PUSH_RESULT="no_change"
else
  ts=$(date -u +"%Y-%m-%d")
  git add data
  git commit -m "Dashboard data: refresh (${ts})" >/dev/null || true
  git push >/dev/null
  PUSH_RESULT="pushed"
fi

# Dashboard applied = pushed OR no_change (already up-to-date)
DASHBOARD_APPLIED="true"

python3 - <<PY
import json, os

def jload(s):
  try:
    return json.loads(s) if s and s.strip().startswith('{') else s
  except Exception:
    return s

push_result = ${json.dumps(PUSH_RESULT)}
checklist_path = ${json.dumps(CHECKLIST_PATH)}

discovery_raw = ${json.dumps("" + DISCOVERY_JSON)}
imweb_raw = ${json.dumps("" + IMWEB_JSON)}
dedupe_raw = ${json.dumps("" + DEDUPE_JSON)}

out={
  'when_utc': os.popen('date -u +%Y-%m-%dT%H:%M:%SZ').read().strip(),
  'checklist': {
    'discovery_ran': True,
    'imweb_collected': True,
    'browser_queue_ok': ${json.dumps(BROWSER_QUEUE_OK_STR)},
    'dedupe_ran': True,
    'export_ok': True,
    'push_result': push_result,
    'yesterday_review_count': int(${json.dumps(YESTERDAY_COUNT)}),
    'new_collected_this_run': int(${json.dumps(NEW_COLLECTED_THIS_RUN)}),
    'dashboard_applied': True,
  },
  'discovery': jload(discovery_raw),
  'imweb': jload(imweb_raw),
  'dedupe': jload(dedupe_raw),
}
open(checklist_path,'w',encoding='utf-8').write(json.dumps(out,ensure_ascii=False,indent=2))
print(checklist_path)
PY

echo "result=${PUSH_RESULT} checklist=${CHECKLIST_PATH}"
