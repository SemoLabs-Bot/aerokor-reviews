#!/usr/bin/env node
/**
 * Apply Jira issue creation for a stored run file.
 *
 * One explicit gate:
 *   --approve yes
 *
 * Usage:
 *   node skills/local/jira-from-transcript/scripts/apply.mjs \
 *     --runId 20260203-180000-ABCD \
 *     --indices 1,3,5 \
 *     --approve yes
 */

import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';

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
      case '--indices': out.indices = take(i++); break;
      case '--approve': out.approve = take(i++); break;
      case '--dryRun': out.dryRun = true; break;
      default: die(`Unknown arg: ${a}`);
    }
  }
  return out;
}

function sha256Hex(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

function saveJson(p, obj) {
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + '\n', 'utf8');
}

function normalizeIndices(s, max) {
  const parts = (s ?? '').split(',').map(x => x.trim()).filter(Boolean);
  if (!parts.length) die('No indices provided. Use --indices 1,3,5');
  const nums = parts.map(x => Number(x));
  if (nums.some(n => !Number.isInteger(n))) die('Indices must be integers (1-based).');
  if (nums.some(n => n < 1 || n > max)) die(`Index out of range. Valid: 1..${max}`);
  // unique preserve order
  const seen = new Set();
  return nums.filter(n => (seen.has(n) ? false : (seen.add(n), true)));
}

function requiredEnv(name) {
  const v = process.env[name];
  if (!v) die(`Missing env ${name}. Refuse to create Jira issues.`);
  return v;
}

function runPathFromId(runId) {
  return path.resolve('logs/jira-voice/runs', `${runId}.json`);
}

function callCreateIssue({ site, email, token, projectKey, issueType, summary, descriptionText, labels, priority, assigneeAccountId, idempotencyKey, dryRun }) {
  const tmpDir = path.join(os.tmpdir(), 'openclaw-jira');
  ensureDir(tmpDir);
  const descPath = path.join(tmpDir, `desc-${sha256Hex(idempotencyKey).slice(0, 10)}.txt`);
  fs.writeFileSync(descPath, descriptionText ?? '', 'utf8');

  const args = [
    path.resolve('scripts/jira-create-issue.mjs'),
    '--site', site,
    '--email', email,
    '--token', token,
    '--project', projectKey,
    '--issueType', issueType,
    '--summary', summary,
    '--descriptionFile', descPath,
    '--idempotencyKey', idempotencyKey,
  ];

  if (labels?.length) args.push('--labels', labels.join(','));
  if (priority) args.push('--priority', priority);
  if (assigneeAccountId) args.push('--assigneeAccountId', assigneeAccountId);
  if (dryRun) args.push('--dryRun');

  const res = spawnSync(process.execPath, args, { encoding: 'utf8' });
  const stdout = (res.stdout ?? '').trim();
  const stderr = (res.stderr ?? '').trim();

  let parsed;
  try { parsed = stdout ? JSON.parse(stdout) : null; } catch { parsed = { ok: false, raw: stdout, stderr }; }

  if (res.status !== 0) {
    return {
      ok: false,
      exitCode: res.status,
      stdout,
      stderr,
      result: parsed,
    };
  }

  return { ok: true, result: parsed };
}

async function main() {
  const { runId, runFile, indices, approve, dryRun } = parseArgs(process.argv.slice(2));
  if (approve !== 'yes') {
    die('Refusing to create Jira issues without explicit gate: --approve yes');
  }

  const filePath = runFile ? path.resolve(runFile) : (runId ? runPathFromId(runId) : null);
  if (!filePath) die('Provide --runId <id> or --runFile <path>');
  if (!fs.existsSync(filePath)) die(`Run file not found: ${filePath}`);

  const run = loadJson(filePath);
  const candidates = Array.isArray(run.candidates) ? run.candidates : [];
  if (!candidates.length) die('No candidates in run file.');

  const idx = normalizeIndices(indices, candidates.length);

  // Required Jira env
  const site = requiredEnv('ATLASSIAN_SITE');
  const email = requiredEnv('ATLASSIAN_EMAIL');
  const token = requiredEnv('ATLASSIAN_API_TOKEN');
  const projectKey = requiredEnv('JIRA_PROJECT_KEY');

  const results = [];

  for (const n of idx) {
    const c = candidates[n - 1];
    if (!c?.summary || !c?.description) {
      results.push({ index: n, ok: false, error: 'candidate missing summary/description' });
      continue;
    }

    const issueType = c.issueType ?? 'Task';
    const labels = Array.isArray(c.labels) ? c.labels : ['voice'];
    const priority = c.priority ?? undefined;
    const assigneeAccountId = c.assigneeAccountId ?? undefined;

    const idempotencyKey = c.idempotencyKey
      ?? `sha256:${sha256Hex(`${projectKey}|${issueType}|${c.summary}|${run.transcript_sha256 ?? ''}|${n}`)}`;

    const r = callCreateIssue({
      site,
      email,
      token,
      projectKey,
      issueType,
      summary: c.summary,
      descriptionText: c.description,
      labels,
      priority,
      assigneeAccountId,
      idempotencyKey,
      dryRun: !!dryRun,
    });

    results.push({ index: n, ...r });
  }

  // Update run file
  run.appliedAt = new Date().toISOString();
  run.apply = { indices: idx, dryRun: !!dryRun };
  run.results = results.map(r => ({
    index: r.index,
    ok: r.ok,
    result: r.ok ? r.result : r.result,
    stderr: r.ok ? undefined : r.stderr,
    stdout: r.ok ? undefined : r.stdout,
  }));
  run.status = dryRun ? 'dry_run_applied' : (results.every(r => r.ok && r.result?.ok) ? 'completed' : 'partial_or_failed');

  saveJson(filePath, run);

  const out = {
    ok: run.status === 'completed' || run.status === 'dry_run_applied',
    run_id: run.run_id ?? runId,
    run_file: filePath,
    status: run.status,
    applied: results.map(r => ({
      index: r.index,
      ok: r.ok,
      issue_key: r.ok ? r.result?.issue_key : undefined,
      issue_url: r.ok ? r.result?.issue_url : undefined,
      deduped: r.ok ? r.result?.deduped : undefined,
      error: r.ok ? undefined : (r.result?.error ?? r.stderr ?? r.stdout),
    }))
  };

  process.stdout.write(JSON.stringify(out, null, 2) + '\n');
}

main().catch((e) => {
  process.stderr.write(String(e?.stack ?? e) + '\n');
  process.exit(1);
});
