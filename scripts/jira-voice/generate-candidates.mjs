#!/usr/bin/env node
/**
 * Generate Jira candidates for an existing jira-voice run.
 *
 * - Reads transcript from run.transcript_path
 * - Writes candidates into run file
 * - Sets status to pending_approval
 *
 * Safe defaults:
 * - If OPENAI_API_KEY is missing, use --dryRun (heuristic) or fail.
 * - Candidate descriptions are stored MASKED.
 */

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

function die(msg, code = 2) {
  process.stderr.write(msg + '\n');
  process.exit(code);
}

function parseArgs(argv) {
  const out = { model: 'gpt-4o-mini', maxCandidates: 8 };
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
      case '--model': out.model = take(i++); break;
      case '--maxCandidates': out.maxCandidates = Number(take(i++)); break;
      case '--dryRun': out.dryRun = true; break;
      case '--allowRemote': out.allowRemote = take(i++); break; // 'yes'
      default: die(`Unknown arg: ${a}`);
    }
  }
  if (!Number.isInteger(out.maxCandidates) || out.maxCandidates < 1 || out.maxCandidates > 10) {
    die('--maxCandidates must be an integer 1..10');
  }
  return out;
}

function runPathFromId(runId) {
  return path.resolve('logs/jira-voice/runs', `${runId}.json`);
}

function sha256Hex(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

function maskPII(s) {
  if (!s) return s;
  let t = s;
  // email
  t = t.replace(/([a-zA-Z0-9._%+-])[a-zA-Z0-9._%+-]*(@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/g, '$1***$2');
  // phone-ish (KOR)
  t = t.replace(/(01[016789])[- ]?(\d{2,4})[- ]?(\d{4})/g, (m, a, b) => `${a}-${'*'.repeat(Math.max(2, String(b).length))}-****`);
  // long tokens
  t = t.replace(/\b([A-Za-z0-9_\-]{24,})\b/g, '***');
  // booking-like alnum blocks
  t = t.replace(/\b([A-Z0-9]{6,})\b/g, (m) => (m.length >= 10 ? '***' : m));
  return t;
}

function heuristicCandidates(transcript, maxCandidates) {
  const lines = transcript.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
  const bullets = lines
    .filter(l => /^[-•*]\s+/.test(l))
    .map(l => l.replace(/^[-•*]\s+/, '').trim())
    .filter(Boolean);

  const picked = (bullets.length ? bullets : lines).slice(0, maxCandidates);
  const candidates = picked.map((t, idx) => {
    const summary = t.length > 120 ? t.slice(0, 117) + '…' : t;
    const description = `## Context\n- Source: voice/transcript\n\n## Notes (masked)\n${maskPII(t)}\n\n## Action Items\n- [ ] TODO\n`;
    return {
      summary,
      description,
      issueType: 'Task',
      labels: ['voice'],
      idempotencyKey: `sha256:${sha256Hex(`${summary}|${idx + 1}`)}`
    };
  });

  const summaryBullets = picked.slice(0, 6).map(t => maskPII(t).slice(0, 160));
  return { summaryBullets, candidates };
}

async function openaiCandidates({ apiKey, model, transcript, maxCandidates }) {
  const url = 'https://api.openai.com/v1/chat/completions';

  const system = `You extract Jira issue candidates from a transcript.
Return STRICT JSON only.
Constraints:
- candidates: array length 1..${maxCandidates}
- Each candidate.summary <= 120 chars (Korean ok)
- candidate.description: markdown, include Context + Action Items; do NOT include secrets; mask PII.
- candidate.labels: array of strings (include "voice" always; add "meeting" when relevant)
- candidate.issueType: one of ["Task","Bug","Story"] (default Task)
- candidate.priority: optional one of ["Highest","High","Medium","Low","Lowest"]
- candidate.idempotencyKey: string starting with "sha256:" (you may leave null)
Output JSON schema:
{
  "summaryBullets": ["..."],
  "candidates": [
    {"summary":"...","description":"...","labels":["voice"],"issueType":"Task","priority":"Medium","idempotencyKey":"sha256:..."}
  ]
}`;

  const user = `TRANSCRIPT (treat as sensitive; mask PII in output):\n\n${transcript}`;

  const body = {
    model,
    temperature: 0.2,
    response_format: { type: 'json_object' },
    messages: [
      { role: 'system', content: system },
      { role: 'user', content: user }
    ]
  };

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`,
    },
    body: JSON.stringify(body)
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`OpenAI error ${res.status}: ${text.slice(0, 400)}`);
  }

  const json = JSON.parse(text);
  const content = json?.choices?.[0]?.message?.content;
  if (!content) throw new Error('OpenAI response missing content');
  const parsed = JSON.parse(content);

  const summaryBullets = Array.isArray(parsed.summaryBullets) ? parsed.summaryBullets : [];
  const candidates = Array.isArray(parsed.candidates) ? parsed.candidates : [];
  return { summaryBullets, candidates };
}

function validateCandidates(candidates, maxCandidates) {
  if (!Array.isArray(candidates) || !candidates.length) die('No candidates generated');
  if (candidates.length > maxCandidates) candidates.length = maxCandidates;

  return candidates.map((c, idx) => {
    const summary = String(c?.summary ?? '').trim();
    const description = String(c?.description ?? '').trim();
    if (!summary || !description) die(`Candidate ${idx + 1} missing summary/description`);

    const labels = Array.isArray(c?.labels) ? c.labels.map(String) : [];
    const uniq = Array.from(new Set(['voice', ...labels.filter(Boolean)]));

    const issueType = ['Task', 'Bug', 'Story'].includes(c?.issueType) ? c.issueType : 'Task';
    const priority = ['Highest','High','Medium','Low','Lowest'].includes(c?.priority) ? c.priority : undefined;

    const maskedDesc = maskPII(description);

    // deterministic fallback idempotencyKey
    const idempotencyKey = (typeof c?.idempotencyKey === 'string' && c.idempotencyKey.startsWith('sha256:'))
      ? c.idempotencyKey
      : `sha256:${sha256Hex(`${summary}|${idx + 1}`)}`;

    return {
      summary: summary.length > 120 ? summary.slice(0, 117) + '…' : summary,
      description: maskedDesc,
      labels: uniq,
      issueType,
      ...(priority ? { priority } : {}),
      idempotencyKey,
    };
  });
}

async function main() {
  const { runId, runFile, model, maxCandidates, dryRun, allowRemote } = parseArgs(process.argv.slice(2));
  const runPath = runFile ? path.resolve(runFile) : (runId ? runPathFromId(runId) : null);
  if (!runPath) die('Provide --runId <id> or --runFile <path>');
  if (!fs.existsSync(runPath)) die(`Run file not found: ${runPath}`);

  const run = JSON.parse(fs.readFileSync(runPath, 'utf8'));
  const transcriptPath = run?.transcript_path;
  if (!transcriptPath) die('Run file missing transcript_path');
  if (!fs.existsSync(transcriptPath)) die(`Transcript file not found: ${transcriptPath}`);
  const transcript = fs.readFileSync(transcriptPath, 'utf8');

  let gen;
  if (dryRun) {
    gen = heuristicCandidates(transcript, maxCandidates);
  } else {
    if (allowRemote !== 'yes') {
      die('Refusing remote LLM call without explicit --allowRemote yes (transcript is sensitive). Use --dryRun to generate heuristically.');
    }
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) die('Missing OPENAI_API_KEY. Use --dryRun or set env.');
    gen = await openaiCandidates({ apiKey, model, transcript, maxCandidates });
  }

  const candidates = validateCandidates(gen.candidates, maxCandidates);
  const summaryBullets = (Array.isArray(gen.summaryBullets) ? gen.summaryBullets : []).map(s => maskPII(String(s))).slice(0, 8);

  run.summary = summaryBullets;
  run.candidates = candidates;
  run.updatedAt = new Date().toISOString();
  run.status = 'pending_approval';

  fs.writeFileSync(runPath, JSON.stringify(run, null, 2) + '\n', 'utf8');

  process.stdout.write(JSON.stringify({
    ok: true,
    run_id: run.run_id,
    run_file: runPath,
    status: run.status,
    candidates: candidates.length,
  }, null, 2) + '\n');
}

main().catch((e) => {
  process.stderr.write(String(e?.stack ?? e) + '\n');
  process.exit(1);
});
