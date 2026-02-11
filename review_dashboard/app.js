/* global Tabulator */

const DATA_URL = "../data/reviews.json";

let ALL = [];
let table;
let TABLE_BUILT = false;
let PENDING_DATA = null;

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

  const topProducts = {};
  for (const r of ALL) {
    const k = r.product_name || "(unknown)";
    topProducts[k] = (topProducts[k] || 0) + 1;
  }
  const entries = Object.entries(topProducts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);

  for (const [name, cnt] of entries) {
    const el = document.createElement("div");
    el.className = "chip";
    el.textContent = `${name} (${cnt})`;
    el.onclick = () => {
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

function initTable() {
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
          const v = mergedBody(r);
          const s = String(v ?? "").replace(/\s+/g, " ").trim();
          if (!s) return "";
          const cut = s.length > 110 ? (s.slice(0, 110) + "…") : s;
          return `<span class="body-snippet">${escapeHtml(cut)}</span>`;
        }
      },
      { title: "수집시각", field: "collected_at", width: 170 },
      { title: "링크", field: "source_url", width: 90, formatter: (cell) => {
          const v = cell.getValue();
          if (!v) return "";
          return `<a href="${v}" target="_blank" rel="noopener noreferrer">열기</a>`;
        }
      },
    ],
    rowClick: function (e, row) {
      const el = row.getElement();
      const already = el.querySelector(".review-body");
      if (already) {
        already.remove();
        return;
      }
      const data = row.getData();
      const body = document.createElement("div");
      body.className = "review-body";
      const meta = document.createElement("div");
      meta.className = "review-meta";
      meta.textContent = `platform=${data.platform || ""} · product_url=${data.product_url ? "(있음)" : ""} · review_id=${data.review_id || ""}`;
      const txt = document.createElement("div");
      txt.textContent = mergedBody(data);

      body.appendChild(meta);
      body.appendChild(txt);
      el.appendChild(body);
    },
  });

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
    // Copy for the button label:
    // - open (sidebar visible): show "close left menu" with a left chevron
    // - collapsed (sidebar hidden): show "open left menu" with a hamburger icon
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

async function main() {
  const res = await fetch(DATA_URL, { cache: "no-store" });
  const payload = await res.json();
  ALL = payload.rows || [];

  document.getElementById("sheetId").textContent = payload.sheet_id || "";
  document.getElementById("statUpdated").textContent = payload.generated_at ? payload.generated_at.replace("T", " ").slice(0, 19) : "-";

  const brands = uniq(ALL.map(r => r.brand)).sort();
  const platforms = uniq(ALL.map(r => r.platform)).sort();
  const products = uniq(ALL.map(r => r.product_name)).sort();

  setOptions(document.getElementById("brand"), brands);
  setOptions(document.getElementById("platform"), platforms);
  setOptions(document.getElementById("product"), products);

  initTable();
  initSidebarToggle();
  buildChips();
  // Initial render will happen after Tabulator emits tableBuilt.
  applyFilters();

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
