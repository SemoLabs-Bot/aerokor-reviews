#!/usr/bin/env node
/**
 * Add candidates to an existing run file.
 *
 * Usage:
 *   node scripts/jira-voice/add-candidates.mjs --runId <id> --candidatesFile ./candidates.json
 */

import fs from 'node:fs';
import path from 'node:path';

function die(msg, code = 2) {
  process.stderr.write(msg + '\n');
  process.exit(code);
}

function parseArgs(argv) {
  const out = {};
  const take = (i) => {
    if (i + 1 >= argv.length) die(`Missing value for ${argv[i]}`);
    return argv[i + 1];
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    switch (a) {
      case '--runId': out.runId = take(i++); break;
      case '--runFile': out.runFile = take(i++); break;
      case '--candidatesFile': out.candidatesFile = take(i++); break;
      case '--status': out.status = take(i++); break;
      default: die(`Unknown arg: ${a}`);
    }
  }
  return out;
}

function runPathFromId(runId) {
  return path.resolve('logs/jira-voice/runs', `${runId}.json`);
}

async function main() {
  const { runId, runFile, candidatesFile, status } = parseArgs(process.argv.slice(2));
  if (!candidatesFile) die('Missing --candidatesFile');
  const runPath = runFile ? path.resolve(runFile) : (runId ? runPathFromId(runId) : null);
  if (!runPath) die('Provide --runId or --runFile');
  if (!fs.existsSync(runPath)) die(`Run file not found: ${runPath}`);

  const candidatesPath = path.resolve(candidatesFile);
  if (!fs.existsSync(candidatesPath)) die(`Candidates file not found: ${candidatesPath}`);

  const run = JSON.parse(fs.readFileSync(runPath, 'utf8'));
  const candidates = JSON.parse(fs.readFileSync(candidatesPath, 'utf8'));
  if (!Array.isArray(candidates)) die('Candidates file must be a JSON array.');

  run.candidates = candidates;
  run.status = status ?? 'pending_approval';
  run.updatedAt = new Date().toISOString();

  fs.writeFileSync(runPath, JSON.stringify(run, null, 2) + '\n', 'utf8');

  process.stdout.write(JSON.stringify({ ok: true, run_file: runPath, run_id: run.run_id, candidates: candidates.length, status: run.status }, null, 2) + '\n');
}

main().catch((e) => {
  process.stderr.write(String(e?.stack ?? e) + '\n');
  process.exit(1);
});
