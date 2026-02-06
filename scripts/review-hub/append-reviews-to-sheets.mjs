#!/usr/bin/env node
/**
 * Append review rows to Google Sheets via `gog sheets append`.
 * - Ensures header row exists
 * - Computes body_hash + dedup_key (sha256)
 * - Dedupes using a local state file (and optionally sheet scan later)
 *
 * Usage:
 *   node scripts/review-hub/append-reviews-to-sheets.mjs --input reviews.json
 *   node scripts/review-hub/append-reviews-to-sheets.mjs --input reviews.json --dry-run
 *
 * Input format: JSON array of objects with at least:
 *   platform, product_url, author, review_date, body
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { execFileSync } from 'child_process';

const args = new Map();
for (let i = 2; i < process.argv.length; i++) {
  const a = process.argv[i];
  if (a.startsWith('--')) {
    const k = a.slice(2);
    const v = (i + 1 < process.argv.length && !process.argv[i + 1].startsWith('--')) ? process.argv[++i] : 'true';
    args.set(k, v);
  }
}

const inputPath = args.get('input');
const dryRun = args.get('dry-run') === 'true' || args.get('dryRun') === 'true';

const configPath = args.get('config') || 'config/review-hub/google-sheets.sink.json';
const cfg = JSON.parse(fs.readFileSync(configPath, 'utf8'));

const sheetId = args.get('sheet-id') || args.get('sheetId') || cfg.sheetId;
const tab = args.get('tab') || cfg.tab;
const tz = cfg.timezone || 'Asia/Seoul';

if (!inputPath) {
  console.error('Missing --input <path-to-json>');
  process.exit(2);
}

function sha256(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

function normalizeBody(body) {
  if (body == null) return '';
  // Normalize for dedup: unify newlines, trim, collapse internal whitespace.
  return String(body)
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .replace(/[\t\f\v ]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function kstDateFromISO(iso) {
  const d = iso ? new Date(iso) : new Date();
  // Format YYYY-MM-DD in KST (tz)
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: tz,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(d);
  const y = parts.find(p => p.type === 'year')?.value;
  const m = parts.find(p => p.type === 'month')?.value;
  const dd = parts.find(p => p.type === 'day')?.value;
  return `${y}-${m}-${dd}`;
}

function gog(cmdArgs, { json = false } = {}) {
  const baseArgs = [...cmdArgs, '--no-input'];
  const out = execFileSync('gog', baseArgs, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
  return json ? JSON.parse(out) : out;
}

function readJsonArray(p) {
  const raw = fs.readFileSync(p, 'utf8');
  const data = JSON.parse(raw);
  if (!Array.isArray(data)) throw new Error('Input JSON must be an array');
  return data;
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function loadDedupSet(statePath) {
  try {
    const raw = fs.readFileSync(statePath, 'utf8');
    const set = new Set();
    for (const line of raw.split(/\n/)) {
      const k = line.trim();
      if (k) set.add(k);
    }
    return set;
  } catch {
    return new Set();
  }
}

function appendDedupKeys(statePath, keys) {
  if (!keys.length) return;
  fs.appendFileSync(statePath, keys.map(k => `${k}\n`).join(''), 'utf8');
}

function getHeaderRowRange(columns) {
  // A1:O1 for 15 columns
  const start = 'A1';
  const endCol = String.fromCharCode('A'.charCodeAt(0) + columns.length - 1);
  return `${tab}!${start}:${endCol}1`;
}

function getAppendRange(columns) {
  const endCol = String.fromCharCode('A'.charCodeAt(0) + columns.length - 1);
  return `${tab}!A:${endCol}`;
}

function ensureHeader(columns) {
  const range = getHeaderRowRange(columns);
  let existing;
  try {
    existing = gog(['sheets', 'get', sheetId, range, '--json'], { json: true });
  } catch (e) {
    // If sheet is empty or API returns nothing, treat as missing.
    existing = null;
  }

  const got = existing?.values?.[0] || [];
  const expected = columns;
  const matches = got.length === expected.length && got.every((v, i) => String(v) === String(expected[i]));

  if (matches) return { updated: false };

  if (dryRun) {
    console.log('[dry-run] would set header:', expected);
    return { updated: true };
  }

  gog([
    'sheets',
    'update',
    sheetId,
    range,
    '--values-json',
    JSON.stringify([expected]),
    '--input',
    'USER_ENTERED'
  ]);

  return { updated: true };
}

const reviews = readJsonArray(inputPath);

const statePath = path.resolve(cfg.dedup?.localStatePath || 'state/review-hub/dedup-keys.txt');
ensureDir(path.dirname(statePath));
const dedupSet = loadDedupSet(statePath);

const columns = cfg.columns;

function toRow(r) {
  const collectedAt = r.collected_at || r.collectedAt || new Date().toISOString();
  const collectedDate = r.collected_date || r.collectedDate || kstDateFromISO(collectedAt);

  const platform = r.platform || '';
  const productUrl = r.product_url || r.productUrl || '';
  const author = r.author || '';
  const reviewDate = r.review_date || r.reviewDate || '';
  const body = r.body || '';

  const bodyHash = r.body_hash || r.bodyHash || sha256(normalizeBody(body));
  const dedupKey = r.dedup_key || r.dedupKey || sha256([platform, productUrl, author, reviewDate, bodyHash].join('|'));

  const obj = {
    collected_date: collectedDate,
    collected_at: collectedAt,
    brand: r.brand || '',
    platform,
    product_name: r.product_name || r.productName || '',
    product_url: productUrl,
    review_id: r.review_id || r.reviewId || '',
    review_date: reviewDate,
    rating: r.rating ?? '',
    author,
    title: r.title || '',
    body,
    body_hash: bodyHash,
    dedup_key: dedupKey,
    source_url: r.source_url || r.sourceUrl || r.url || ''
  };

  const row = columns.map(c => obj[c] ?? '');
  return { row, dedupKey };
}

const rows = [];
const newKeys = [];
for (const r of reviews) {
  const { row, dedupKey } = toRow(r);
  if (dedupSet.has(dedupKey)) continue;
  dedupSet.add(dedupKey);
  rows.push(row);
  newKeys.push(dedupKey);
}

console.log(`input reviews: ${reviews.length}`);
console.log(`new rows (after dedup): ${rows.length}`);

if (!rows.length) process.exit(0);

// Ensure header
ensureHeader(columns);

// Append in chunks
const range = getAppendRange(columns);
const chunkSize = Number(args.get('chunk') || 200);

for (let i = 0; i < rows.length; i += chunkSize) {
  const chunk = rows.slice(i, i + chunkSize);

  if (dryRun) {
    console.log(`[dry-run] would append ${chunk.length} rows to ${sheetId} ${range}`);
    continue;
  }

  gog([
    'sheets',
    'append',
    sheetId,
    range,
    '--values-json',
    JSON.stringify(chunk),
    '--insert',
    'INSERT_ROWS',
    '--input',
    'USER_ENTERED'
  ]);

  console.log(`appended ${chunk.length} rows`);
}

if (!dryRun) {
  appendDedupKeys(statePath, newKeys);
  console.log(`dedup state updated: ${path.relative(process.cwd(), statePath)} (+${newKeys.length})`);
}
