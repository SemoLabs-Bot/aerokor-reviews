#!/usr/bin/env node
/**
 * Minimal OpenAI STT client (no deps).
 * Requires: OPENAI_API_KEY
 *
 * Output: JSON { ok, run_id, transcript, transcript_sha256, language }
 */

import fs from 'node:fs';
import crypto from 'node:crypto';

function die(msg, code = 2) {
  process.stderr.write(msg + '\n');
  process.exit(code);
}

function uuid() {
  return crypto.randomUUID();
}

function sha256Hex(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

function maskPII(s) {
  if (!s) return s;
  let t = s;
  // email
  t = t.replace(/([a-zA-Z0-9._%+-])[a-zA-Z0-9._%+-]*(@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/g, '$1***$2');
  // phone-ish (KOR)
  t = t.replace(/(01[016789])[- ]?(\d{2,4})[- ]?(\d{4})/g, (m, a, b, c) => `${a}-${'*'.repeat(Math.max(2, b.length))}-${'****'}`);
  // PNR-like (5-8 alnum)
  t = t.replace(/\b([A-Z0-9]{2})([A-Z0-9]{3,4})([A-Z0-9]{1,2})\b/g, '$1***$3');
  return t;
}

function parseArgs(argv) {
  const out = { model: 'gpt-4o-mini-transcribe', lang: 'ko' };
  const take = (i) => {
    if (i + 1 >= argv.length) die(`Missing value for ${argv[i]}`);
    return argv[i + 1];
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    switch (a) {
      case '--path': out.path = take(i++); break;
      case '--model': out.model = take(i++); break;
      case '--lang': out.lang = take(i++); break;
      case '--mask': out.mask = true; break;
      default: die(`Unknown arg: ${a}`);
    }
  }
  return out;
}

async function main() {
  const runId = uuid();
  const { path, model, lang, mask } = parseArgs(process.argv.slice(2));
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) die('Missing OPENAI_API_KEY');
  if (!path) die('Missing --path');
  if (!fs.existsSync(path)) die(`File not found: ${path}`);

  const audioBuf = fs.readFileSync(path);
  const audioHash = sha256Hex(audioBuf);

  // multipart form
  const form = new FormData();
  form.set('model', model);
  form.set('file', new Blob([audioBuf]), 'audio');
  form.set('language', lang);

  const resp = await fetch('https://api.openai.com/v1/audio/transcriptions', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiKey}`,
    },
    body: form
  });

  const txt = await resp.text();
  let body;
  try { body = JSON.parse(txt); } catch { body = { raw: txt }; }

  if (!resp.ok) {
    process.stdout.write(JSON.stringify({ ok: false, run_id: runId, status: resp.status, error: body }, null, 2));
    process.exit(1);
  }

  const transcript = body.text ?? '';
  const outTranscript = mask ? maskPII(transcript) : transcript;

  process.stdout.write(JSON.stringify({
    ok: true,
    run_id: runId,
    language: lang,
    transcript_sha256: `sha256:${sha256Hex(Buffer.from(transcript, 'utf8'))}`,
    audio_sha256: `sha256:${audioHash}`,
    transcript: outTranscript,
  }, null, 2));
}

main().catch((e) => {
  process.stderr.write(String(e?.stack ?? e) + '\n');
  process.exit(1);
});
