#!/usr/bin/env node
/**
 * Jira Cloud issue creator with local idempotency.
 * - No external deps (Node >= 18 has fetch; Node 24 OK)
 * - Writes logs/jira/idempotency.jsonl
 *
 * Usage:
 *   node scripts/jira-create-issue.mjs \
 *     --site https://YOUR.atlassian.net \
 *     --email you@company.com \
 *     --token $JIRA_API_TOKEN \
 *     --project KEY \
 *     --issueType Task \
 *     --summary "Do X" \
 *     --descriptionFile /path/to/desc.txt \
 *     --labels voice,meeting \
 *     --priority Medium \
 *     --assigneeAccountId 123:abc \
 *     --idempotencyKey sha256:... \
 *     --dryRun
 */

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

function die(msg, code = 2) {
  process.stderr.write(`${msg}\n`);
  process.exit(code);
}

function nowIso() {
  return new Date().toISOString();
}

function uuid() {
  // Node 19+: crypto.randomUUID
  return crypto.randomUUID();
}

function sha256Hex(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

function b64(s) {
  return Buffer.from(s, 'utf8').toString('base64');
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function parseArgs(argv) {
  const out = {
    labels: [],
    dryRun: false,
  };

  const take = (i) => {
    if (i + 1 >= argv.length) die(`Missing value for ${argv[i]}`);
    return argv[i + 1];
  };

  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;

    switch (a) {
      case '--site': out.site = take(i++); break;
      case '--email': out.email = take(i++); break;
      case '--token': out.token = take(i++); break;
      case '--project': out.projectKey = take(i++); break;
      case '--issueType': out.issueType = take(i++); break;
      case '--summary': out.summary = take(i++); break;
      case '--description': out.description = take(i++); break;
      case '--descriptionFile': out.descriptionFile = take(i++); break;
      case '--labels': {
        const v = take(i++);
        out.labels = v.split(',').map(s => s.trim()).filter(Boolean);
        break;
      }
      case '--priority': out.priority = take(i++); break;
      case '--assigneeAccountId': out.assigneeAccountId = take(i++); break;
      case '--idempotencyKey': out.idempotencyKey = take(i++); break;
      case '--dryRun': out.dryRun = true; break;
      default:
        die(`Unknown arg: ${a}`);
    }
  }
  return out;
}

function normalizeSite(site) {
  if (!site) return site;
  const s = site.replace(/\/$/, '');
  if (!/^https:\/\//.test(s)) die('Jira --site must start with https://');
  return s;
}

function normalizeSummary(s) {
  return (s ?? '').trim().replace(/\s+/g, ' ');
}

function readDescription(args) {
  if (args.descriptionFile) {
    const p = args.descriptionFile;
    if (!fs.existsSync(p)) die(`descriptionFile not found: ${p}`);
    return fs.readFileSync(p, 'utf8');
  }
  return args.description ?? '';
}

// Minimal ADF from plain text (paragraphs separated by blank lines)
function textToAdf(text) {
  const paragraphs = (text ?? '')
    .replace(/\r\n/g, '\n')
    .split(/\n\s*\n/g)
    .map(p => p.trim())
    .filter(Boolean);

  const content = paragraphs.map(p => ({
    type: 'paragraph',
    content: [{ type: 'text', text: p }]
  }));

  return {
    type: 'doc',
    version: 1,
    content: content.length ? content : [{ type: 'paragraph', content: [{ type: 'text', text: '' }] }]
  };
}

function loadIdempotencyLog(logPath) {
  if (!fs.existsSync(logPath)) return [];
  const lines = fs.readFileSync(logPath, 'utf8').split('\n').filter(Boolean);
  const rows = [];
  for (const line of lines) {
    try { rows.push(JSON.parse(line)); } catch { /* ignore */ }
  }
  return rows;
}

function findExisting(rows, idempotencyKey) {
  return rows.find(r => r && r.idempotency_key === idempotencyKey && r.issue_key);
}

async function main() {
  const runId = uuid();
  const args = parseArgs(process.argv.slice(2));

  // env fallback (but keep explicit args as highest priority)
  args.site = normalizeSite(args.site ?? process.env.ATLASSIAN_SITE);
  args.email = args.email ?? process.env.ATLASSIAN_EMAIL;
  args.token = args.token ?? process.env.ATLASSIAN_API_TOKEN;
  args.projectKey = args.projectKey ?? process.env.JIRA_PROJECT_KEY;
  args.issueType = args.issueType ?? process.env.JIRA_ISSUE_TYPE ?? 'Task';

  args.summary = normalizeSummary(args.summary);
  if (!args.site) die('Missing Jira site: --site or ATLASSIAN_SITE');
  if (!args.email) die('Missing Jira email: --email or ATLASSIAN_EMAIL');
  if (!args.token) die('Missing Jira API token: --token or ATLASSIAN_API_TOKEN');
  if (!args.projectKey) die('Missing Jira project key: --project or JIRA_PROJECT_KEY');
  if (!args.summary) die('Missing summary: --summary');

  const descriptionText = readDescription(args);
  const idempotencyKey = args.idempotencyKey ?? `sha256:${sha256Hex(`${args.projectKey}|${args.issueType}|${args.summary}|${descriptionText}`)}`;

  const logDir = path.resolve('logs/jira');
  ensureDir(logDir);
  const logPath = path.join(logDir, 'idempotency.jsonl');
  const rows = loadIdempotencyLog(logPath);

  const existing = findExisting(rows, idempotencyKey);
  if (existing) {
    const out = {
      ok: true,
      run_id: runId,
      deduped: true,
      idempotency_key: idempotencyKey,
      issue_key: existing.issue_key,
      issue_url: existing.issue_url,
      created_at: existing.created_at,
      note: 'Idempotency hit; returning existing issue.'
    };
    process.stdout.write(JSON.stringify(out, null, 2));
    return;
  }

  const payload = {
    fields: {
      project: { key: args.projectKey },
      summary: args.summary,
      issuetype: { name: args.issueType },
      description: textToAdf(descriptionText),
    }
  };

  if (args.labels?.length) payload.fields.labels = args.labels;
  if (args.priority) payload.fields.priority = { name: args.priority };
  if (args.assigneeAccountId) payload.fields.assignee = { accountId: args.assigneeAccountId };

  const reqHash = sha256Hex(JSON.stringify({
    site: args.site,
    email: args.email,
    projectKey: args.projectKey,
    issueType: args.issueType,
    summary: args.summary,
    descriptionText,
    labels: args.labels ?? [],
    priority: args.priority ?? null,
    assigneeAccountId: args.assigneeAccountId ?? null,
    idempotencyKey,
  }));

  if (args.dryRun) {
    const out = {
      ok: true,
      run_id: runId,
      dry_run: true,
      idempotency_key: idempotencyKey,
      request_hash: `sha256:${reqHash}`,
      request: {
        url: `${args.site}/rest/api/3/issue`,
        method: 'POST',
        fields: Object.keys(payload.fields),
      },
      note: 'Dry run only; no network call performed.'
    };
    process.stdout.write(JSON.stringify(out, null, 2));
    return;
  }

  const url = `${args.site}/rest/api/3/issue`;
  const auth = `Basic ${b64(`${args.email}:${args.token}`)}`;

  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': auth,
      'Accept': 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload)
  });

  const bodyText = await resp.text();
  let body;
  try { body = JSON.parse(bodyText); } catch { body = { raw: bodyText }; }

  if (!resp.ok) {
    const out = {
      ok: false,
      run_id: runId,
      idempotency_key: idempotencyKey,
      request_hash: `sha256:${reqHash}`,
      status: resp.status,
      error: body,
      note: 'Jira create issue failed.'
    };
    process.stdout.write(JSON.stringify(out, null, 2));
    process.exit(1);
  }

  const issueKey = body.key;
  const issueUrl = issueKey ? `${args.site}/browse/${issueKey}` : null;

  const record = {
    created_at: nowIso(),
    run_id: runId,
    idempotency_key: idempotencyKey,
    request_hash: `sha256:${reqHash}`,
    issue_key: issueKey,
    issue_url: issueUrl,
    site: args.site,
    project_key: args.projectKey,
    issue_type: args.issueType,
  };
  fs.appendFileSync(logPath, JSON.stringify(record) + '\n', 'utf8');

  const out = {
    ok: true,
    run_id: runId,
    deduped: false,
    idempotency_key: idempotencyKey,
    issue_key: issueKey,
    issue_url: issueUrl,
  };
  process.stdout.write(JSON.stringify(out, null, 2));
}

main().catch((e) => {
  process.stderr.write(String(e?.stack ?? e) + '\n');
  process.exit(1);
});
