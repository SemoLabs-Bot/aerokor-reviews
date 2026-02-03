#!/usr/bin/env bash
set -euo pipefail

TRANSCRIPT=$'오늘 회의에서:\n- 결제 플로우 이탈 원인 분석\n- 이벤트 트래킹 누락 수정\n- 다음주까지 리텐션 리포트 초안\n'

echo "== init run =="
RUN_JSON=$(node scripts/jira-voice/init-run.mjs --source transcript --title "gen-dry" --transcriptText "$TRANSCRIPT")
echo "$RUN_JSON"
RUN_ID=$(echo "$RUN_JSON" | node -e 'const s=require("fs").readFileSync(0,"utf8"); console.log(JSON.parse(s).run_id)')

echo "== generate candidates (dryRun) =="
node scripts/jira-voice/generate-candidates.mjs --runId "$RUN_ID" --dryRun --maxCandidates 3

echo "== inspect run =="
RUN_ID="$RUN_ID" node - <<'NODE'
const fs=require('fs');
const runId=process.env.RUN_ID;
const p=`logs/jira-voice/runs/${runId}.json`;
const j=JSON.parse(fs.readFileSync(p,'utf8'));
console.log('status:', j.status);
console.log('candidates:', j.candidates.length);
console.log('first.summary:', j.candidates[0].summary);
NODE
