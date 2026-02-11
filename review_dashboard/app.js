/* global Tabulator */

const META_URL = "../data/reviews_meta.json";
const INDEX_FALLBACK_URL = "../data/reviews_index.json";
const LEGACY_URL = "../data/reviews.json";

let INDEX_DIR = "../data/reviews_index";
let INDEX_FILE_PREFIX = "chunk-";
let INDEX_CHUNKS = 0;

let BODY_DIR = "../data/reviews_body";
let BODY_FILE_PREFIX = "chunk-";

const BODY_CACHE = new Map(); // chunkId -> by_key map

// Progressive loading state
let LOADED_CHUNKS = 0;
let LOADING_REST = false;

let ALL = [];
let table;
let TABLE_BUILT = false;
let PENDING_DATA = null;

// Notifications
const UPDATES_URL = "../data/updates.json";
let UPDATES = [];
const LS_READ = "reviewHub.updates.readIds";
const LS_DELETED = "reviewHub.updates.deletedIds";

let reviewModalEl = null;
let reviewModalBodyEl = null;
let reviewModalMetaEl = null;
let reviewModalRequestToken = 0;

function closeReviewModal() {
  reviewModalRequestToken += 1;
  if (!reviewModalEl) return;
  reviewModalEl.classList.remove("open");
  reviewModalEl.setAttribute("aria-hidden", "true");
  document.body.style.overflow = "";
}

function openReviewModal(data, bodyText) {
  if (!reviewModalEl) return;
  const product = String(data.product_name || "(상품 미상)");
  const author = String(data.author || "작성자 미상");
  const rating = Number(data.rating_num);
  const ratingText = Number.isFinite(rating) && rating > 0 ? `${rating}점` : "평점 없음";
  const reviewDate = String(data.review_date_norm || data.review_date || "-");

  const titleEl = document.getElementById("reviewModalTitle");
  if (titleEl) titleEl.textContent = product;
  if (reviewModalMetaEl) reviewModalMetaEl.textContent = `작성자: ${author} · 평점: ${ratingText} · 리뷰일: ${reviewDate}`;
  if (reviewModalBodyEl) reviewModalBodyEl.textContent = bodyText || "(리뷰 본문이 비어있어요.)";

  reviewModalEl.classList.add("open");
  reviewModalEl.setAttribute("aria-hidden", "false");
  document.body.style.overflow = "hidden";
}

function initReviewModal() {
  reviewModalEl = document.getElementById("reviewModal");
  reviewModalBodyEl = document.getElementById("reviewModalBody");
  reviewModalMetaEl = document.getElementById("reviewModalMeta");
  if (!reviewModalEl) return;

  const closeBtn = document.getElementById("reviewModalClose");
  if (closeBtn) closeBtn.onclick = closeReviewModal;

  reviewModalEl.addEventListener("click", (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    if (target.dataset.close === "1") closeReviewModal();
  });

  document.addEventListener("keydown", (ev) => {
    if (ev.key === "Escape" && reviewModalEl.classList.contains("open")) closeReviewModal();
  });
}


function uniq(arr) {
  return Array.from(new Set(arr.filter(Boolean)));
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function byText(x) {
  return String(x ?? "").toLowerCase();
}

function parseDateOnly(s) {
  // Expect YYYY-MM-DD; return comparable number
  if (!s) return null;
  const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return null;
  return Number(m[1] + m[2] + m[3]);
}

function parseIso(s) {
  if (!s) return null;
  const t = Date.parse(s);
  return Number.isNaN(t) ? null : t;
}

function setOptions(selectEl, values, { withAll = true } = {}) {
  const cur = selectEl.value;
  selectEl.innerHTML = "";
  if (withAll) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "(전체)";
    selectEl.appendChild(opt);
  }
  for (const v of values) {
    const opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    selectEl.appendChild(opt);
  }
  if (Array.from(selectEl.options).some(o => o.value === cur)) selectEl.value = cur;
}

function computeStats(rows) {
  const n = rows.length;
  let sum = 0;
  let cnt = 0;
  for (const r of rows) {
    const x = Number(r.rating_num);
    if (!Number.isNaN(x) && x > 0) {
      sum += x;
      cnt += 1;
    }
  }
  const avg = cnt ? (sum / cnt) : null;

  let last = null;
  for (const r of rows) {
    const t = parseIso(r.collected_at);
    if (t && (!last || t > last)) last = t;
  }

  return { n, avg, last };
}

function applyFilters() {
  const q = byText(document.getElementById("q").value);
  const brand = document.getElementById("brand").value;
  const platform = document.getElementById("platform").value;
  const product = document.getElementById("product").value;
  const from = parseDateOnly(document.getElementById("from").value);
  const to = parseDateOnly(document.getElementById("to").value);
  const minRating = Number(document.getElementById("minRating").value || 0);
  const maxRating = Number(document.getElementById("maxRating").value || 0);
  const sort = document.getElementById("sort").value;

  const filtered = ALL.filter(r => {
    if (brand && r.brand !== brand) return false;
    if (platform && r.platform !== platform) return false;
    if (product && r.product_name !== product) return false;

    const d = parseDateOnly(r.review_date_norm || r.review_date || "");
    if (from && (!d || d < from)) return false;
    if (to && (!d || d > to)) return false;

    const rr = Number(r.rating_num);
    if (minRating) {
      if (Number.isNaN(rr) || rr < minRating) return false;
    }
    if (maxRating) {
      if (Number.isNaN(rr) || rr > maxRating) return false;
    }

    if (q) {
      const hay = [r.product_name, r.title, r.body, r.brand, r.platform].map(byText).join(" ");
      if (!hay.includes(q)) return false;
    }

    return true;
  });

  // sort
  const sorters = {
    review_date_desc: (a, b) => (parseDateOnly(b.review_date_norm) || 0) - (parseDateOnly(a.review_date_norm) || 0),
    review_date_asc: (a, b) => (parseDateOnly(a.review_date_norm) || 0) - (parseDateOnly(b.review_date_norm) || 0),
    collected_at_desc: (a, b) => (parseIso(b.collected_at) || 0) - (parseIso(a.collected_at) || 0),
  };
  filtered.sort(sorters[sort] || sorters.review_date_desc);

  // If Tabulator hasn't finished building yet, queue data to avoid "Table Not Initialized" warnings.
  if (!TABLE_BUILT) {
    PENDING_DATA = filtered;
  } else {
    table.setData(filtered);
    // Tabulator sometimes needs a forced redraw after CSS/layout changes.
    try { table.redraw(true); } catch (e) {}
  }

  const st = computeStats(filtered);
  document.getElementById("statCount").textContent = st.n.toLocaleString();
  document.getElementById("statAvgRating").textContent = st.avg ? st.avg.toFixed(2) : "-";

  const sub = [];
  if (brand) sub.push(`brand=${brand}`);
  if (platform) sub.push(`platform=${platform}`);
  if (product) sub.push(`product=${product}`);
  if (from || to) sub.push(`date=${from ? document.getElementById("from").value : ""}..${to ? document.getElementById("to").value : ""}`);
  if (minRating || maxRating) sub.push(`rating=${minRating || ""}..${maxRating || ""}`);
  if (q) sub.push(`q=${q}`);

  document.getElementById("tableSub").textContent = sub.length ? sub.join(" · ") : "전체";
}

function clearFilters() {
  document.getElementById("q").value = "";
  document.getElementById("brand").value = "";
  document.getElementById("platform").value = "";
  document.getElementById("product").value = "";
  document.getElementById("from").value = "";
  document.getElementById("to").value = "";
  document.getElementById("minRating").value = "";
  document.getElementById("maxRating").value = "";
  document.getElementById("sort").value = "review_date_desc";
  applyFilters();
}

function buildChips() {
  const chips = document.getElementById("chips");
  chips.innerHTML = "";

  // While background loading, chips based on partial data are okay.
  const topProducts = {};
  for (const r of ALL) {
    const k = r.product_name || "(unknown)";
    topProducts[k] = (topProducts[k] || 0) + 1;
  }
  const entries = Object.entries(topProducts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);

  for (const [name, cnt] of entries) {
    const el = document.createElement("button");
    el.type = "button";
    el.className = "chip";
    el.title = `${name} (${cnt})`;

    const label = document.createElement("span");
    label.className = "chip-label";
    label.textContent = name;

    const count = document.createElement("span");
    count.className = "chip-count";
    count.textContent = cnt.toLocaleString();

    el.appendChild(label);
    el.appendChild(count);

    el.onclick = () => {
      // If products list isn't populated yet, this still works because we set the value directly.
      document.getElementById("product").value = name;
      applyFilters();
    };
    chips.appendChild(el);
  }
}

function mergedBody(r) {
  const t = String(r.title ?? "").trim();
  const b = String(r.body ?? "").trim();
  if (t && b) return `${t}\n\n${b}`;
  return t || b;
}

async function loadBodyForRow(r) {
  // r should contain dedup_key + body_chunk
  const key = String(r.dedup_key || "");
  const chunkId = Number(r.body_chunk);
  if (!key || !Number.isFinite(chunkId)) return r;

  // If already present, nothing to do.
  if (r.body || r.title) return r;

  if (!BODY_CACHE.has(chunkId)) {
    const url = `${BODY_DIR}/${BODY_FILE_PREFIX}${String(chunkId).padStart(3, "0")}.json`;
    const res = await fetch(url, { cache: "force-cache" });
    const payload = await res.json();
    BODY_CACHE.set(chunkId, payload.by_key || {});
  }

  const byKey = BODY_CACHE.get(chunkId) || {};
  const b = byKey[key];
  if (b) {
    r.title = b.title || "";
    r.body = b.body || "";
  }
  return r;
}

async function fetchIndexChunk(chunkId) {
  const url = `${INDEX_DIR}/${INDEX_FILE_PREFIX}${String(chunkId).padStart(3, "0")}.json`;
  const res = await fetch(url, { cache: "force-cache" });
  const payload = await res.json();
  return payload.rows || [];
}

function setTableSubLoading() {
  const el = document.getElementById("tableSub");
  if (!el) return;
  const total = INDEX_CHUNKS;
  if (!total) return;
  const pct = Math.floor((LOADED_CHUNKS / total) * 100);
  el.textContent = `로드 중… ${LOADED_CHUNKS}/${total} chunks (${pct}%)`;
}

function initTable() {
  const handleRowClick = async (e, row) => {
    const target = e && e.target;
    if (target instanceof HTMLElement && target.closest("a")) return;

    const data = row.getData();
    const requestToken = ++reviewModalRequestToken;
    openReviewModal(data, "불러오는 중…");

    try {
      await loadBodyForRow(data);
      if (requestToken !== reviewModalRequestToken) return;
      openReviewModal(data, mergedBody(data));
      // Refresh snippet cell now that body is loaded.
      try { row.update(data); } catch (err) {}
    } catch (err) {
      if (requestToken !== reviewModalRequestToken) return;
      openReviewModal(data, "본문을 불러오지 못했어요.");
    }
  };

  table = new Tabulator("#table", {
    height: "calc(100vh - 210px)",
    layout: "fitColumns",
    renderVertical: "virtual",
    index: "dedup_key",
    responsiveLayout: "collapse",
    pagination: true,
    paginationSize: 50,
    movableColumns: true,
    initialSort: [{ column: "review_date_norm", dir: "desc" }],
    columns: [
      { title: "리뷰일", field: "review_date_norm", width: 110 },

      { title: "브랜드", field: "brand", width: 110 },
      { title: "플랫폼", field: "platform", width: 110 },
      { title: "상품", field: "product_name", minWidth: 220 },
      { title: "평점", field: "rating_num", width: 80, hozAlign: "right" },
      { title: "길이", field: "body_len", width: 80, hozAlign: "right" },
      { title: "작성자", field: "author", width: 110 },
      { title: "본문", field: "body", minWidth: 320, formatter: (cell) => {
          const r = cell.getRow().getData();
          // Index payload doesn't include body/title; show placeholder until loaded.
          if (!r.body && !r.title) {
            const len = Number(r.body_len);
            const hint = Number.isFinite(len) && len > 0 ? `(${len}자)` : "";
            return `<span class="body-snippet" style="color:rgba(71,85,105,.95)">클릭해서 본문 보기 ${hint}</span>`;
          }
          const v = mergedBody(r);
          const s = String(v ?? "").replace(/\s+/g, " ").trim();
          if (!s) return "";
          const cut = s.length > 110 ? (s.slice(0, 110) + "…") : s;
          return `<span class="body-snippet">${escapeHtml(cut)}</span>`;
        }
      },
      { title: "링크", field: "source_url", width: 74, hozAlign: "center", headerHozAlign: "center", formatter: (cell) => {
          const v = cell.getValue();
          if (!v) return "";
          return `<span class="link-inline"><a class="source-link-icon" href="${v}" target="_blank" rel="noopener noreferrer" title="원문 열기" aria-label="원문 열기">↗</a></span>`;
        }
      },
    ],
  });

  // Tabulator v6 requires row click handling to be registered via event listeners.
  table.on("rowClick", handleRowClick);

  table.on("tableBuilt", () => {
    TABLE_BUILT = true;
    // If data arrived before the table finished building, apply it now.
    if (PENDING_DATA) {
      table.setData(PENDING_DATA);
      try { table.redraw(true); } catch (e) {}
      PENDING_DATA = null;
    }
  });

  // expose for debugging (avoid collision with window.table named access)
  window.__tabulator = table;
}

function initSidebarToggle() {
  const btn = document.getElementById("toggleSidebar");
  if (!btn) return;

  const key = "reviewHub.sidebarCollapsed";
  const apply = (collapsed) => {
    document.body.classList.toggle("sidebar-collapsed", !!collapsed);
    btn.textContent = collapsed ? "☰ 좌측 메뉴 열기" : "⟨ 좌측 메뉴 닫기";
    try { if (table) table.redraw(true); } catch (e) {}
  };

  const stored = localStorage.getItem(key);
  apply(stored === "1");

  btn.onclick = () => {
    const next = !document.body.classList.contains("sidebar-collapsed");
    localStorage.setItem(key, next ? "1" : "0");
    apply(next);
  };
}

function lsGetSet(key){
  try{
    const v = JSON.parse(localStorage.getItem(key) || "[]");
    return new Set(Array.isArray(v) ? v : []);
  }catch(e){
    return new Set();
  }
}

function lsSetFromSet(key, set){
  localStorage.setItem(key, JSON.stringify(Array.from(set)));
}

function renderNotifDot(){
  const dot = document.getElementById('notifDot');
  if(!dot) return;
  const read = lsGetSet(LS_READ);
  const deleted = lsGetSet(LS_DELETED);
  const unread = (UPDATES || []).some(u => !deleted.has(String(u.id)) && !read.has(String(u.id)));
  dot.classList.toggle('on', unread);
}

function openNotifPanel(open){
  const panel = document.getElementById('notifPanel');
  if(!panel) return;
  panel.classList.toggle('open', !!open);
  panel.setAttribute('aria-hidden', open ? 'false' : 'true');
}

function renderNotifList(){
  const list = document.getElementById('notifList');
  if(!list) return;

  const read = lsGetSet(LS_READ);
  const deleted = lsGetSet(LS_DELETED);

  const items = (UPDATES || []).filter(u => !deleted.has(String(u.id)));
  if(!items.length){
    list.innerHTML = `<div style="color:rgba(71,85,105,.95); font-size:12px; padding:10px">알림이 없습니다.</div>`;
    renderNotifDot();
    return;
  }

  list.innerHTML = '';
  for(const u of items){
    const id = String(u.id);
    const el = document.createElement('div');
    el.className = 'notif-item' + (read.has(id) ? '' : ' unread');

    const left = document.createElement('div');
    const msg = document.createElement('div');
    msg.className = 'msg';
    msg.textContent = u.message || '업데이트';
    const meta = document.createElement('div');
    meta.className = 'meta';
    const ts = String(u.generated_at || '').replace('T',' ').slice(0,19);
    meta.textContent = ts;
    left.appendChild(msg);
    left.appendChild(meta);

    const right = document.createElement('div');
    right.className = 'x';
    const xb = document.createElement('button');
    xb.type = 'button';
    xb.textContent = '✕';
    xb.title = '삭제';
    xb.onclick = (ev) => {
      ev.stopPropagation();
      const del = lsGetSet(LS_DELETED);
      del.add(id);
      lsSetFromSet(LS_DELETED, del);
      renderNotifList();
    };
    right.appendChild(xb);

    el.appendChild(left);
    el.appendChild(right);

    el.onclick = () => {
      // mark read
      const r = lsGetSet(LS_READ);
      r.add(id);
      lsSetFromSet(LS_READ, r);
      renderNotifDot();
      renderNotifList();
      openNotifPanel(false);
      openLowRatingModal(u);
    };

    list.appendChild(el);
  }

  renderNotifDot();
}

function openLowRatingModal(update){
  const modal = document.getElementById('notifModal');
  if(!modal) return;
  const meta = document.getElementById('notifModalMeta');
  const list = document.getElementById('notifModalList');
  const title = document.getElementById('notifModalTitle');

  const n = Number(update.low_rating_count || 0);
  title.textContent = `2점 이하 리뷰 (${n}건)`;
  meta.textContent = `${String(update.generated_at||'').replace('T',' ').slice(0,19)} · 기준: ≤${update.low_rating_threshold||2}점`;

  const rows = update.low_reviews || [];
  if(!rows.length){
    list.innerHTML = `<div style="color:rgba(71,85,105,.95); font-size:12px; padding:6px 2px">해당 업데이트에서 2점 이하 리뷰가 없습니다.</div>`;
  }else{
    const wrap = document.createElement('div');
    wrap.style.display='flex';
    wrap.style.flexDirection='column';
    wrap.style.gap='8px';

    for(const r of rows){
      const card = document.createElement('div');
      card.style.border='1px solid rgba(15,23,42,.12)';
      card.style.borderRadius='12px';
      card.style.padding='10px';
      card.style.background='rgba(248,250,252,.92)';
      card.style.cursor='pointer';

      const h = document.createElement('div');
      h.style.fontWeight='900';
      h.style.fontSize='13px';
      h.textContent = `[${r.brand||''}/${r.platform||''}] ${r.product_name||''} · ${r.rating_num}점`;

      const m = document.createElement('div');
      m.style.fontSize='11px';
      m.style.color='rgba(71,85,105,.95)';
      m.style.marginTop='4px';
      m.textContent = `${r.review_date_norm||''} · ${r.author||''}`;

      card.appendChild(h);
      card.appendChild(m);

      card.onclick = async () => {
        // Reuse the main review modal to show body (lazy loaded)
        try {
          await loadBodyForRow(r);
        } catch(e) {}
        openReviewModal({
          title: `${r.product_name||''} (${r.rating_num}점)`,
          meta: `brand=${r.brand||''} · platform=${r.platform||''} · author=${r.author||''} · date=${r.review_date_norm||''}`,
          body: mergedBody(r),
        });
      };

      wrap.appendChild(card);
    }
    list.innerHTML='';
    list.appendChild(wrap);
  }

  modal.classList.add('open');
  modal.setAttribute('aria-hidden','false');
}

function initNotifUI(){
  const btn = document.getElementById('notifBtn');
  const panel = document.getElementById('notifPanel');
  if(!btn || !panel) return;

  btn.onclick = () => {
    const open = !panel.classList.contains('open');
    openNotifPanel(open);
  };

  document.addEventListener('click', (ev) => {
    const t = ev.target;
    if(panel.contains(t) || btn.contains(t)) return;
    openNotifPanel(false);
  });

  const markAll = document.getElementById('notifMarkAllRead');
  if(markAll){
    markAll.onclick = () => {
      const r = lsGetSet(LS_READ);
      const del = lsGetSet(LS_DELETED);
      for(const u of (UPDATES||[])){
        const id = String(u.id);
        if(!del.has(id)) r.add(id);
      }
      lsSetFromSet(LS_READ, r);
      renderNotifList();
    };
  }

  const clearAll = document.getElementById('notifClearAll');
  if(clearAll){
    clearAll.onclick = () => {
      const del = lsGetSet(LS_DELETED);
      for(const u of (UPDATES||[])) del.add(String(u.id));
      lsSetFromSet(LS_DELETED, del);
      renderNotifList();
    };
  }

  const close = document.getElementById('notifModalClose');
  const modal = document.getElementById('notifModal');
  if(close && modal){
    close.onclick = () => {
      modal.classList.remove('open');
      modal.setAttribute('aria-hidden','true');
    };
    modal.addEventListener('click', (ev)=>{
      const t=ev.target;
      if(t && t.getAttribute && t.getAttribute('data-close')==='1'){
        modal.classList.remove('open');
        modal.setAttribute('aria-hidden','true');
      }
    });
  }
}

async function loadUpdates(){
  try{
    const res = await fetch(UPDATES_URL, { cache: 'no-store' });
    UPDATES = await res.json();
    if(!Array.isArray(UPDATES)) UPDATES = [];
  }catch(e){
    UPDATES = [];
  }
  renderNotifList();
}


async function main() {
  // Load meta first (small), then progressively load index chunks.
  let meta;
  try {
    const r = await fetch(META_URL, { cache: "no-store" });
    meta = await r.json();
  } catch (e) {
    // Fallback path (older deployments)
    const res2 = await fetch(LEGACY_URL, { cache: "no-store" });
    const legacy = await res2.json();
    ALL = legacy.rows || [];

    document.getElementById("sheetId").textContent = legacy.sheet_id || "";
    document.getElementById("statUpdated").textContent = legacy.generated_at ? legacy.generated_at.replace("T", " ").slice(0, 19) : "-";

    const brands = uniq(ALL.map(r => r.brand)).sort();
    const platforms = uniq(ALL.map(r => r.platform)).sort();
    const products = uniq(ALL.map(r => r.product_name)).sort();

    setOptions(document.getElementById("brand"), brands);
    setOptions(document.getElementById("platform"), platforms);
    setOptions(document.getElementById("product"), products);

    initReviewModal();
    initTable();
    initSidebarToggle();
    buildChips();
    applyFilters();
    document.getElementById("apply").onclick = applyFilters;
    document.getElementById("clear").onclick = clearFilters;
    document.getElementById("q").addEventListener("keydown", (ev) => {
      if (ev.key === "Enter") applyFilters();
    });
    return;
  }

  INDEX_DIR = `../data/${meta.index?.dir || "reviews_index"}`;
  INDEX_FILE_PREFIX = meta.index?.file_prefix || "chunk-";
  INDEX_CHUNKS = Number(meta.index?.chunks || 0);

  BODY_DIR = `../data/${meta.body?.dir || "reviews_body"}`;
  BODY_FILE_PREFIX = meta.body?.file_prefix || "chunk-";

  document.getElementById("sheetId").textContent = meta.sheet_id || "";
  document.getElementById("statUpdated").textContent = meta.generated_at ? meta.generated_at.replace("T", " ").slice(0, 19) : "-";

  // Set filter options from meta dims (fast; no need to scan ALL)
  setOptions(document.getElementById("brand"), (meta.dims?.brands || []));
  setOptions(document.getElementById("platform"), (meta.dims?.platforms || []));
  // Products list is intentionally not preloaded (can be huge). We'll build it on demand.
  setOptions(document.getElementById("product"), [], { withAll: true });

  initReviewModal();
  initTable();
  initSidebarToggle();

  // Load first chunk quickly for TTI
  const first = await fetchIndexChunk(0);
  ALL = first;
  LOADED_CHUNKS = 1;
  setTableSubLoading();

  buildChips();
  applyFilters();

  // Background-load remaining chunks (non-blocking)
  if (INDEX_CHUNKS > 1) {
    LOADING_REST = true;
    (async () => {
      for (let c = 1; c < INDEX_CHUNKS; c++) {
        try {
          const part = await fetchIndexChunk(c);
          if (part && part.length) {
            ALL.push(...part);
          }
          LOADED_CHUNKS = c + 1;
          setTableSubLoading();
        } catch (e) {
          // ignore chunk failure
        }
      }
      LOADING_REST = false;
      // Refresh products list once at the end.
      const products = uniq(ALL.map(r => r.product_name)).sort();
      setOptions(document.getElementById("product"), products);
      buildChips();
      // Re-apply filters to include everything.
      applyFilters();
    })();
  }

  document.getElementById("apply").onclick = applyFilters;
  document.getElementById("clear").onclick = clearFilters;

  // Enter to apply
  document.getElementById("q").addEventListener("keydown", (ev) => {
    if (ev.key === "Enter") applyFilters();
  });

  document.getElementById("apply").onclick = applyFilters;
  document.getElementById("clear").onclick = clearFilters;

  // Enter to apply
  document.getElementById("q").addEventListener("keydown", (ev) => {
    if (ev.key === "Enter") applyFilters();
  });
}

main().catch(err => {
  console.error(err);
  alert("데이터 로드 실패: " + String(err));
});
