apply.mjs

- Applies Jira creation for a stored run file.
- Requires --approve yes.
- Reads candidates from logs/jira-voice/runs/<runId>.json

Expected run file shape:
{
  "run_id": "...",
  "createdAt": "...",
  "source": "transcript"|"audio",
  "transcript_sha256": "sha256:...",
  "transcript_preview": "(masked)",
  "candidates": [
    {
      "summary": "...",
      "description": "...",
      "labels": ["voice","meeting"],
      "issueType": "Task",
      "priority": "Medium",
      "assigneeAccountId": "...",
      "idempotencyKey": "sha256:..."
    }
  ],
  "status": "pending_approval"
}
