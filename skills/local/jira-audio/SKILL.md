---
name: jira-audio
description: Transcribe an audio file (Discord voice message or attachment) into text, initialize a Jira run, then (only after approval) create Jira Cloud issues using local scripts with idempotency. Use when the user says /jira-audio, sends an audio file, or asks to turn a voice note into Jira tasks.
---

# /jira-audio <path>

**Discord DM only.** Never run in servers/groups.

## Workflow

1) **Get audio path**
- Input is a local path. If the user sent an attachment, prefer the MEDIA path.

2) **Transcribe + initialize run (no Jira creation)**
- Requires env `OPENAI_API_KEY`

```bash
node skills/local/jira-audio/scripts/run.mjs --path "/path/to/audio" --lang ko
```

This outputs JSON with `run_id`, `run_file`, and `transcript`.

3) **Then run /jira-from-transcript flow**
- Summarize + propose candidates
- Store candidates into the run file and set status=pending_approval
- Ask approval
- Create only selected issues via:

```bash
node skills/local/jira-from-transcript/scripts/apply.mjs --runId <run_id> --indices 1,3 --approve yes
```

## Hard stops

- If `OPENAI_API_KEY` is missing: stop and ask to set it
- If Jira env missing: stop (see jira-from-transcript skill)

## References
- OpenAI Speech-to-Text: https://platform.openai.com/docs/guides/speech-to-text
