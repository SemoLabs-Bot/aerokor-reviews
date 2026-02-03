#!/usr/bin/env node
/**
 * Prepare a jira-voice run from transcript text:
 *   init run -> generate candidates
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
  const out = { source: 'transcript', maxCandidates: 8 };
  const take = (i) => {
    if (i + 1 >= argv.length) die(`Missing value for ${argv[i]}`);
    return argv[i + 1];
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    switch (a) {
      case '--transcriptText': out.transcriptText = take(i++); break;
      case '--transcriptFile': out.transcriptFile = take(i++); break;
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
  const { transcriptText, transcriptFile, title, maxCandidates, dryRunCandidates, allowRemote, model } = args;
  if (!transcriptText && !transcriptFile) die('Provide --transcriptText or --transcriptFile');

  const initScript = path.resolve('scripts/jira-voice/init-run.mjs');
  const initArgs = ['--source', 'transcript', ...(title ? ['--title', title] : [])];
  if (args.dev) initArgs.push('--dev');
  if (transcriptText) initArgs.push('--transcriptText', transcriptText);
  if (transcriptFile) initArgs.push('--transcriptFile', path.resolve(transcriptFile));

  const init = runNode(initScript, initArgs);
  if (!init.ok || !init.result?.ok) {
    process.stdout.write(JSON.stringify({ ok: false, step: 'init-run', error: init }, null, 2) + '\n');
    process.exit(1);
  }

  const runId = init.result.run_id;
  const runFile = init.result.run_file;

  const genScript = path.resolve('scripts/jira-voice/generate-candidates.mjs');
  const genArgs = ['--runFile', runFile, '--maxCandidates', String(maxCandidates)];
  if (model) genArgs.push('--model', model);

  if (dryRunCandidates) {
    genArgs.push('--dryRun');
  } else if (allowRemote === 'yes') {
    genArgs.push('--allowRemote', 'yes');
  } else {
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
