#!/usr/bin/env bash
set -euo pipefail

# Dry-run test for Voice→Jira pipeline (no network)

export ATLASSIAN_SITE="https://example.atlassian.net"
export ATLASSIAN_EMAIL="devnull@example.com"
export ATLASSIAN_API_TOKEN="dummy"
export JIRA_PROJECT_KEY="TEST"

TRANSCRIPT=$'오늘 회의에서 다음을 논의했습니다.\n- 결제 플로우 이탈 원인 분석\n- 이벤트 트래킹 누락 수정\n- 다음주까지 리텐션 리포트 초안\n'

echo "== init run =="
RUN_JSON=$(node scripts/jira-voice/init-run.mjs --source transcript --title "dryrun" --transcriptText "$TRANSCRIPT")
echo "$RUN_JSON"
RUN_ID=$(echo "$RUN_JSON" | node -e 'const s=require("fs").readFileSync(0,"utf8"); console.log(JSON.parse(s).run_id)')

cat > /tmp/jira-candidates.json <<'JSON'
[
  {
    "summary": "[Tracking] 결제 이벤트 누락 점검 및 수정",
    "description": "## Context\n- Source: Discord voice / audio\n\n## Action Items\n- [ ] 결제 완료 이벤트 누락 여부 확인\n- [ ] 누락 시 서버/클라이언트 이벤트 추가\n",
    "labels": ["voice", "meeting"],
    "issueType": "Task"
  },
  {
    "summary": "[Analytics] 결제 플로우 이탈 원인 가설 정리",
    "description": "## Action Items\n- [ ] 퍼널 단계 정의\n- [ ] 이탈 구간 상위 2개 가설 작성\n",
    "labels": ["voice"],
    "issueType": "Task"
  }
]
JSON

echo "== add candidates =="
node scripts/jira-voice/add-candidates.mjs --runId "$RUN_ID" --candidatesFile /tmp/jira-candidates.json

echo "== apply (dry run) =="
node skills/local/jira-from-transcript/scripts/apply.mjs --runId "$RUN_ID" --indices 1,2 --approve yes --dryRun
