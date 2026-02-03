#!/usr/bin/env node
/**
 * jira-audio runner
 * - Transcribes audio via transcribe-openai.mjs (requires OPENAI_API_KEY)
 * - Initializes a jira-voice run via scripts/jira-voice/init-run.mjs
 * - Outputs JSON with transcript + run_id
 *
 * Does NOT create Jira issues.
 */

import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

function die(msg, code = 2) {
  process.stderr.write(msg + '\n');
  process.exit(code);
}

function parseArgs(argv) {
  const out = { lang: 'ko' };
  const take = (i) => {
    if (i + 1 >= argv.length) die(`Missing value for ${argv[i]}`);
    return argv[i + 1];
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    switch (a) {
      case '--path': out.audioPath = take(i++); break;
      case '--lang': out.lang = take(i++); break;
      case '--title': out.title = take(i++); break;
      default: die(`Unknown arg: ${a}`);
    }
  }
  return out;
}

function runNode(scriptPath, args) {
  const res = spawnSync(process.execPath, [scriptPath, ...args], { encoding: 'utf8' });
  const stdout = (res.stdout ?? '').trim();
  const stderr = (res.stderr ?? '').trim();
  if (res.status !== 0) {
    return { ok: false, stdout, stderr, status: res.status };
  }
  let parsed;
  try { parsed = JSON.parse(stdout); } catch { parsed = { raw: stdout, stderr }; }
  return { ok: true, result: parsed };
}

async function main() {
  const { audioPath, lang, title } = parseArgs(process.argv.slice(2));
  if (!process.env.OPENAI_API_KEY) die('Missing OPENAI_API_KEY');
  if (!audioPath) die('Missing --path');

  const p = path.resolve(audioPath);
  if (!fs.existsSync(p)) die(`Audio file not found: ${p}`);

  const transcribeScript = path.resolve('skills/local/jira-audio/scripts/transcribe-openai.mjs');
  const tr = runNode(transcribeScript, ['--path', p, '--lang', lang, '--mask']);
  if (!tr.ok || !tr.result?.ok) {
    process.stdout.write(JSON.stringify({ ok: false, step: 'transcribe', error: tr }, null, 2) + '\n');
    process.exit(1);
  }

  const transcript = tr.result.transcript ?? '';
  if (!transcript.trim()) die('Transcription produced empty transcript');

  const initScript = path.resolve('scripts/jira-voice/init-run.mjs');
  const init = runNode(initScript, ['--source', 'audio', '--title', title ?? 'voice', '--transcriptText', transcript]);
  if (!init.ok || !init.result?.ok) {
    process.stdout.write(JSON.stringify({ ok: false, step: 'init-run', error: init }, null, 2) + '\n');
    process.exit(1);
  }

  process.stdout.write(JSON.stringify({
    ok: true,
    run_id: init.result.run_id,
    run_file: init.result.run_file,
    transcript_sha256: init.result.transcript_sha256,
    transcript: transcript,
  }, null, 2) + '\n');
}

main().catch((e) => {
  process.stderr.write(String(e?.stack ?? e) + '\n');
  process.exit(1);
});
