run.mjs

Transcribe audio and initialize a jira-voice run.

Usage:
  node skills/local/jira-audio/scripts/run.mjs --path <audio> --lang ko

Requires:
  OPENAI_API_KEY

Outputs:
  { run_id, run_file, transcript } JSON

Next:
  Use /jira-from-transcript (candidate generation) + apply.mjs for creation.
