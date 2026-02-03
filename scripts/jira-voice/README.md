# scripts/jira-voice

Local helpers for Voice→Jira.

- `init-run.mjs`: create run_id + store transcript + create run manifest
- `add-candidates.mjs`: inject candidates into a run manifest
- `test-dryrun.sh`: end-to-end dry run (no network) using `--dryRun`

Run files:
- `logs/jira-voice/runs/<run_id>.json`
- `logs/jira-voice/transcripts/<run_id>.txt`
