#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."  # workspace root

# NOTE: This script should stay LIGHT-ish, but user wants:
# "when we deploy, also make sure newly collected reviews are included".
# We therefore:
# - take a short bounded collection pass (optional, env-gated)
# - refresh looker_reviews formula
# - export dashboard data artifacts from looker_reviews
# - commit+push if data changed

LOCK="state/review-hub/publish_dashboard.lock"
export LOCK
mkdir -p "$(dirname "$LOCK")"

# Non-blocking lock (avoid overlapping publishes)
python3 - <<'PY'
import os, sys
p=os.environ['LOCK']
os.makedirs(os.path.dirname(p), exist_ok=True)
f=open(p,'a+',encoding='utf-8')
try:
  import fcntl
  try:
    fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
  except BlockingIOError:
    print('skip:locked')
    raise SystemExit(0)
  # Keep fd open while parent continues: write pid marker
  f.seek(0)
  f.truncate()
  f.write(str(os.getpid()))
  f.flush()
  os.environ['PUBLISH_LOCK_FD']=str(f.fileno())
  # Do not close; parent process continues in same process so fd lives.
except Exception:
  try:
    f.close()
  except Exception:
    pass
  raise
PY

# --- 0) (Optional) run a bounded collection pass before exporting ---
# Default: enabled. Set REVIEW_HUB_PUBLISH_COLLECT=0 to disable.
if [ "${REVIEW_HUB_PUBLISH_COLLECT:-1}" != "0" ]; then
  # a) quick imweb pass (fast, API-based)
  IMWEB_WORKERS="${IMWEB_WORKERS:-6}" \
    python3 -m review_hub.collect_reviews --max-products "${REVIEW_HUB_MAX_PRODUCTS:-200}" \
    >/dev/null 2>&1 || true

  # b) bounded browser queue (ohou/coupang/naver/smartstore/wadiz)
  python3 -m review_hub.run_browser_queue \
    --max-minutes "${REVIEW_HUB_BROWSER_MAX_MINUTES:-20}" \
    --sleep "${REVIEW_HUB_BROWSER_SLEEP_S:-2}" \
    >/dev/null 2>&1 || true
fi

# 1) Ensure looker_reviews formula stays correct (must be USER_ENTERED)
PYTHONPATH=. python3 review_hub/setup_looker_views.py >/dev/null 2>&1 || true

# 2) Export latest data artifacts from Google Sheets looker_reviews
python3 scripts/review-hub/export_reviews_json.py >/dev/null

# 3) Commit+push only if changed (any file under data/)
if git diff --quiet -- data; then
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
  echo "result=no_change count=${COUNT}"
  exit 0
fi

ts=$(date -u +"%Y-%m-%d")

git add data

git commit -m "Dashboard data: refresh (${ts})" >/dev/null || true

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
