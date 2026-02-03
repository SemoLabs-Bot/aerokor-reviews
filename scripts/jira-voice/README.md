# scripts/jira-voice

Local helpers for Voice→Jira.

- `init-run.mjs`: create run_id + store transcript + create run manifest
- `generate-candidates.mjs`: generate + store candidates (dryRun heuristic OR OpenAI with explicit gate)
- `add-candidates.mjs`: inject candidates into a run manifest (manual)
- `test-dryrun.sh`: end-to-end dry run (no network) using `--dryRun` apply
- `test-generate-dry.sh`: init + generate candidates (dryRun)

Run files:
- `logs/jira-voice/runs/<run_id>.json`
- `logs/jira-voice/transcripts/<run_id>.txt`
