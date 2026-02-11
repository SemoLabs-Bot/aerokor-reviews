const INSIGHTS_URL = "../data/insights.json";
const LEGACY_URL = "../data/reviews.json";

function byText(x){ return String(x ?? ""); }

function ratingNum(r){
  const x = Number(r.rating_num);
  return Number.isFinite(x) ? x : null;
}

function computeStats(rows){
  const n = rows.length;
  let sum=0, cnt=0;
  let last=null;
  for(const r of rows){
    const x = ratingNum(r);
    if(x && x>0){ sum+=x; cnt++; }
    const t = Date.parse(r.collected_at);
    if(!Number.isNaN(t) && (!last || t>last)) last=t;
  }
  return { n, avg: cnt? sum/cnt : null, last };
}

function truncateLabel(s, max=18){
  const x = byText(s);
  if(x.length <= max) return x;
  return x.slice(0, Math.max(0, max-1)) + '…';
}

function ensureLabelTooltipEl(){
  let el = document.getElementById('labelTooltip');
  if(el) return el;

  el = document.createElement('div');
  el.id = 'labelTooltip';
  el.style.position = 'fixed';
  el.style.zIndex = '9999';
  el.style.maxWidth = '420px';
  el.style.padding = '8px 10px';
  el.style.borderRadius = '10px';
  el.style.border = '1px solid rgba(15,23,42,.14)';
  el.style.background = 'rgba(255,255,255,.98)';
  el.style.boxShadow = '0 10px 30px rgba(15,23,42,.12)';
  el.style.color = 'rgba(15,23,42,.92)';
  el.style.fontSize = '12px';
  el.style.fontWeight = '800';
  el.style.pointerEvents = 'none';
  el.style.display = 'none';

  document.body.appendChild(el);
  return el;
}

function showLabelTooltip({text, x, y}){
  const el = ensureLabelTooltipEl();
  el.textContent = text;
  el.style.left = `${Math.max(8, x)}px`;
  el.style.top = `${Math.max(8, y)}px`;
  el.style.display = 'block';
}

function hideLabelTooltip(){
  const el = document.getElementById('labelTooltip');
  if(el) el.style.display = 'none';
}

function attachYAxisLabelHoverTooltip(chart, fullLabels, {truncateTicks=false}={}){
  const canvas = chart.canvas;
  const yScale = chart.scales?.y;
  if(!yScale) return;

  // Because it's a canvas, we can't literally hover the text nodes.
  // Instead: when the mouse is on the left label area (before chartArea.left)
  // and close to a tick's y pixel, we show a tooltip.
  canvas.addEventListener('mouseleave', hideLabelTooltip);

  canvas.addEventListener('mousemove', (ev) => {
    const rect = canvas.getBoundingClientRect();
    const x = ev.clientX - rect.left;
    const y = ev.clientY - rect.top;

    // Only trigger when hovering the label area (left side of plot).
    if (x > chart.chartArea.left) {
      hideLabelTooltip();
      return;
    }

    // Find nearest tick by y distance.
    let bestI = -1;
    let bestDist = Infinity;
    for (let i = 0; i < fullLabels.length; i++) {
      const py = yScale.getPixelForTick(i);
      const d = Math.abs(py - y);
      if (d < bestDist) {
        bestDist = d;
        bestI = i;
      }
    }

    // Tolerance: half of a category step (or a safe fallback)
    const step = Math.abs(yScale.getPixelForTick(1) - yScale.getPixelForTick(0)) || 18;
    const tol = Math.max(10, step * 0.45);

    if (bestI >= 0 && bestDist <= tol) {
      const full = fullLabels[bestI] || '';
      // Show tooltip on the LEFT of the label area.
      // Position: a bit left of chartArea.left, aligned with the tick.
      const screenX = rect.left + Math.max(8, chart.chartArea.left - 14);
      const screenY = rect.top + yScale.getPixelForTick(bestI) - 16;
      showLabelTooltip({ text: full, x: screenX - 360, y: screenY });
    } else {
      hideLabelTooltip();
    }
  });
}

function chartBar(el, labels, data, {label, color, horizontal=false, truncateTicks=false, tooltipOnLabelHover=false}={}){
  const fullLabels = labels.map(byText);
  const shownLabels = truncateTicks ? fullLabels.map(x=>truncateLabel(x)) : fullLabels;

  const chart = new Chart(el, {
    type: 'bar',
    data: { labels: shownLabels, datasets: [{ label: label||'', data, backgroundColor: color||'rgba(37,99,235,.65)' }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: horizontal ? 'y' : 'x',
      plugins: {
        legend: { display: !!label },
        // If we want tooltip only on label hover, disable Chart.js tooltip.
        tooltip: tooltipOnLabelHover ? { enabled: false } : {
          callbacks: {
            // Show the FULL label on hover even when we truncated the tick text.
            title: (items) => {
              const i = items?.[0]?.dataIndex;
              if(i == null) return '';
              return fullLabels[i] || '';
            },
          },
        },
      },
      scales: horizontal
        ? {
            x: { beginAtZero: true },
            y: {
              ticks: {
                autoSkip: false,
                callback: (v, i) => truncateTicks ? truncateLabel(fullLabels[i]) : (fullLabels[i] || ''),
              },
            },
          }
        : {
            x: { ticks: { autoSkip: false, maxRotation: 60, minRotation: 0 } },
            y: { beginAtZero: true },
          },
    },
  });

  if (horizontal && tooltipOnLabelHover) {
    attachYAxisLabelHoverTooltip(chart, fullLabels, { truncateTicks });
  }

  return chart;
}

async function main(){
  // Prefer precomputed insights (fast). Fallback to legacy heavy data.
  let payload;
  try {
    const res = await fetch(INSIGHTS_URL, { cache: 'no-store' });
    payload = await res.json();
  } catch (e) {
    const res2 = await fetch(LEGACY_URL, { cache: 'no-store' });
    const legacy = await res2.json();
    // compute minimal aggregates from legacy rows (slow fallback)
    const rows = legacy.rows || [];
    const st = computeStats(rows);
    document.getElementById('statCount').textContent = st.n.toLocaleString();
    document.getElementById('statAvg').textContent = st.avg ? st.avg.toFixed(2) : '-';
    document.getElementById('statUpdated').textContent = legacy.generated_at ? legacy.generated_at.replace('T',' ').slice(0,19) : '-';
    return;
  }

  document.getElementById('statCount').textContent = Number(payload.count || 0).toLocaleString();
  // avg from brand_avg weighted (approx) isn't necessary; keep '-' for now.
  // We can compute exact avg in export if needed.
  document.getElementById('statAvg').textContent = '-';
  document.getElementById('statUpdated').textContent = payload.generated_at ? payload.generated_at.replace('T',' ').slice(0,19) : '-';

  // Top products (already aggregated)
  const top = payload.top_products || [];
  chartBar(
    document.getElementById('cTopProducts'),
    top.map(x=>x.name),
    top.map(x=>x.count),
    {
      label:'리뷰 수',
      horizontal:true,
      truncateTicks:true,
      tooltipOnLabelHover:true,
    }
  );

  // Brand avg
  const bavg = payload.brand_avg || [];
  chartBar(
    document.getElementById('cBrandAvg'),
    bavg.map(x=>`${x.name} (${x.count})`),
    bavg.map(x=>Number(Number(x.avg).toFixed(2))),
    {label:'평균 평점', color:'rgba(124,58,237,.65)'}
  );

  // Rating dist
  const dist = payload.rating_dist || [0,0,0,0,0];
  new Chart(document.getElementById('cRatingDist'), {
    type:'bar',
    data:{ labels:['1','2','3','4','5'], datasets:[{label:'건수', data:dist, backgroundColor:'rgba(2,132,199,.65)'}]},
    options:{ responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}, scales:{y:{beginAtZero:true}} }
  });
}

main().catch(err=>{ console.error(err); alert('Insights 로드 실패: '+String(err)); });
