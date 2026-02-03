---
name: jira-from-transcript
description: Create Jira issue candidates from a transcript and (only after explicit user approval) create Jira Cloud issues via local scripts (idempotent). Use when the user says /jira-from-transcript, wants "voice→Jira", provides a transcript text, or asks to turn meeting notes into Jira tasks.
---

# /jira-from-transcript

**Discord DM only.** Never run in servers/groups.

## Workflow (safe)

1) **Prepare a run (no Jira creation)**
- If the user provides transcript text, initialize a run file (writes transcript + run manifest):

```bash
node scripts/jira-voice/init-run.mjs --source transcript --transcriptText "..."
```

- This creates:
  - `logs/jira-voice/transcripts/<run_id>.txt`
  - `logs/jira-voice/runs/<run_id>.json` (status=pending_candidates)

2) **Summarize + propose up to 10 candidates**
- Provide a short summary (3–6 bullets)
- Propose candidates (max 10). Each candidate must include:
  - `summary` (<=120 chars)
  - `description` (masked)
  - `labels` (default: voice; add meeting when appropriate)
  - optional: `issueType`, `priority`, `assigneeAccountId`, `idempotencyKey`

- Store the candidates into the run file and set status to `pending_approval`.

3) **Approval gate (required)**
- Ask: `승인: Jira 생성 (run_id=..., 1,3,5)`
- Do not create anything without explicit approval.

4) **Create selected issues (single command, idempotent)**

```bash
node skills/local/jira-from-transcript/scripts/apply.mjs \
  --runId <run_id> \
  --indices 1,3,5 \
  --approve yes
```

- This calls `scripts/jira-create-issue.mjs` internally per candidate.
- If a candidate was already created, it returns the existing issue (deduped).

## Hard stops

Stop and ask to configure if missing:
- `ATLASSIAN_SITE`
- `ATLASSIAN_EMAIL`
- `ATLASSIAN_API_TOKEN`
- `JIRA_PROJECT_KEY`

## References
- Jira create issue: https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-post
- ADF: https://developer.atlassian.com/cloud/jira/platform/apis/document/structure/
