# logs/jira-voice

- `transcripts/`: raw transcript text (unmasked, treat as sensitive)
- `runs/`: run manifests with masked preview + candidates + results

Conventions:
- run file: `runs/<run_id>.json`
- transcript file: `transcripts/<run_id>.txt`

Run lifecycle:
- pending_candidates → pending_approval → completed (or partial_or_failed)
