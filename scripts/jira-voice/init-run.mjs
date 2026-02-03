#!/usr/bin/env node
/**
 * Initialize a Jira-voice run from transcript text or a transcript file.
 * Writes:
 *   logs/jira-voice/transcripts/<run_id>.txt
 *   logs/jira-voice/runs/<run_id>.json
 *
 * Does NOT call Jira. Does NOT create issues.
 */

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

function die(msg, code = 2) {
  process.stderr.write(msg + '\n');
  process.exit(code);
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function sha256Hex(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

function nowIso() {
  return new Date().toISOString();
}

function runId() {
  // human-friendly but unique enough
  const t = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  const y = t.getFullYear();
  const m = pad(t.getMonth() + 1);
  const d = pad(t.getDate());
  const hh = pad(t.getHours());
  const mm = pad(t.getMinutes());
  const ss = pad(t.getSeconds());
  const rand = crypto.randomBytes(2).toString('hex');
  return `${y}${m}${d}-${hh}${mm}${ss}-${rand}`;
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
  return t;
}

function parseArgs(argv) {
  const out = { source: 'transcript', dev: false };
  const take = (i) => {
    if (i + 1 >= argv.length) die(`Missing value for ${argv[i]}`);
    return argv[i + 1];
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    switch (a) {
      case '--transcriptFile': out.transcriptFile = take(i++); break;
      case '--transcriptText': out.transcriptText = take(i++); break;
      case '--source': out.source = take(i++); break;
      case '--title': out.title = take(i++); break;
      case '--dev': out.dev = true; break;
      default: die(`Unknown arg: ${a}`);
    }
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  let text = '';
  if (args.transcriptFile) {
    const p = path.resolve(args.transcriptFile);
    if (!fs.existsSync(p)) die(`transcriptFile not found: ${p}`);
    text = fs.readFileSync(p, 'utf8');
  } else if (args.transcriptText) {
    text = args.transcriptText;
  } else {
    // allow stdin
    try {
      text = fs.readFileSync(0, 'utf8');
    } catch {
      text = '';
    }
  }

  text = String(text ?? '').trim();
  if (!text) die('Empty transcript. Provide --transcriptFile, --transcriptText, or stdin.');

  const id = runId();
  const transcriptBuf = Buffer.from(text, 'utf8');
  const transcriptHash = `sha256:${sha256Hex(transcriptBuf)}`;

  const transcriptsDir = path.resolve('logs/jira-voice/transcripts', args.dev ? '_dev' : '');
  const runsDir = path.resolve('logs/jira-voice/runs', args.dev ? '_dev' : '');
  ensureDir(transcriptsDir);
  ensureDir(runsDir);

  const transcriptPath = path.join(transcriptsDir, `${id}.txt`);
  const runPath = path.join(runsDir, `${id}.json`);

  fs.writeFileSync(transcriptPath, text + '\n', 'utf8');

  const preview = maskPII(text).slice(0, 800);

  const run = {
    run_id: id,
    dev: !!args.dev,
    createdAt: nowIso(),
    source: args.source,
    title: args.title ?? null,
    transcript_sha256: transcriptHash,
    transcript_path: transcriptPath,
    transcript_preview: preview,
    candidates: [],
    status: 'pending_candidates'
  };

  fs.writeFileSync(runPath, JSON.stringify(run, null, 2) + '\n', 'utf8');

  process.stdout.write(JSON.stringify({
    ok: true,
    run_id: id,
    run_file: runPath,
    transcript_file: transcriptPath,
    transcript_sha256: transcriptHash,
  }, null, 2) + '\n');
}

main().catch((e) => {
  process.stderr.write(String(e?.stack ?? e) + '\n');
  process.exit(1);
});
