#!/usr/bin/env node
/**
 * Prepare a jira-voice run from an audio file:
 *   audio -> transcript (OpenAI STT) -> init run -> generate candidates
 *
 * Does NOT create Jira issues.
 */

import path from 'node:path';
import { spawnSync } from 'node:child_process';

function die(msg, code = 2) {
  process.stderr.write(msg + '\n');
  process.exit(code);
}

function parseArgs(argv) {
  const out = { lang: 'ko', maxCandidates: 8 };
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
      case '--maxCandidates': out.maxCandidates = Number(take(i++)); break;
      case '--dryRunCandidates': out.dryRunCandidates = true; break;
      case '--allowRemote': out.allowRemote = take(i++); break; // yes
      case '--model': out.model = take(i++); break;
      case '--dev': out.dev = true; break;
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
  const args = parseArgs(process.argv.slice(2));
  const { audioPath, lang, title, maxCandidates, dryRunCandidates, allowRemote, model } = args;
  if (!audioPath) die('Missing --path');

  const audioRunner = path.resolve('skills/local/jira-audio/scripts/run.mjs');
  const tr = runNode(audioRunner, ['--path', path.resolve(audioPath), '--lang', lang, ...(title ? ['--title', title] : []), ...(args.dev ? ['--dev'] : [])]);
  if (!tr.ok || !tr.result?.ok) {
    process.stdout.write(JSON.stringify({ ok: false, step: 'jira-audio', error: tr }, null, 2) + '\n');
    process.exit(1);
  }

  const runId = tr.result.run_id;
  const runFile = tr.result.run_file;

  const genScript = path.resolve('scripts/jira-voice/generate-candidates.mjs');
  const genArgs = ['--runFile', runFile, '--maxCandidates', String(maxCandidates)];
  if (model) genArgs.push('--model', model);

  if (dryRunCandidates) {
    genArgs.push('--dryRun');
  } else if (allowRemote === 'yes') {
    genArgs.push('--allowRemote', 'yes');
  } else {
    // default: dryRun (avoid remote without explicit consent)
    genArgs.push('--dryRun');
  }

  const gen = runNode(genScript, genArgs);
  if (!gen.ok || !gen.result?.ok) {
    process.stdout.write(JSON.stringify({ ok: false, step: 'generate-candidates', run_id: runId, run_file: runFile, error: gen }, null, 2) + '\n');
    process.exit(1);
  }

  process.stdout.write(JSON.stringify({
    ok: true,
    run_id: runId,
    run_file: runFile,
    status: 'pending_approval',
    candidates: gen.result.candidates,
  }, null, 2) + '\n');
}

main().catch((e) => {
  process.stderr.write(String(e?.stack ?? e) + '\n');
  process.exit(1);
});
