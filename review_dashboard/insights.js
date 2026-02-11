const DATA_URL = "../data/reviews.json";

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

function chartBar(el, labels, data, {label, color, horizontal=false, truncateTicks=false}={}){
  const fullLabels = labels.map(byText);
  const shownLabels = truncateTicks ? fullLabels.map(x=>truncateLabel(x)) : fullLabels;

  return new Chart(el, {
    type: 'bar',
    data: { labels: shownLabels, datasets: [{ label: label||'', data, backgroundColor: color||'rgba(37,99,235,.65)' }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: horizontal ? 'y' : 'x',
      plugins: {
        legend: { display: !!label },
        tooltip: {
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
}

async function main(){
  const res = await fetch(DATA_URL, { cache: 'no-store' });
  const payload = await res.json();
  const rows = payload.rows || [];

  const st = computeStats(rows);
  document.getElementById('statCount').textContent = st.n.toLocaleString();
  document.getElementById('statAvg').textContent = st.avg ? st.avg.toFixed(2) : '-';
  document.getElementById('statUpdated').textContent = payload.generated_at ? payload.generated_at.replace('T',' ').slice(0,19) : '-';

  // Top products
  const prodCount = new Map();
  for(const r of rows){
    const k = r.product_name || '(unknown)';
    prodCount.set(k, (prodCount.get(k)||0)+1);
  }
  const topProds = Array.from(prodCount.entries()).sort((a,b)=>b[1]-a[1]).slice(0,15);
  // Use a horizontal bar chart to avoid label overlap; truncate long names with ellipsis.
  // Full names are shown in tooltip on hover.
  chartBar(
    document.getElementById('cTopProducts'),
    topProds.map(x=>x[0]),
    topProds.map(x=>x[1]),
    {label:'리뷰 수', horizontal:true, truncateTicks:true}
  );

  // Brand avg rating (min 20)
  const brandAgg = new Map();
  for(const r of rows){
    const b = r.brand || '(unknown)';
    const x = ratingNum(r);
    if(!x) continue;
    const cur = brandAgg.get(b) || {sum:0,cnt:0};
    cur.sum += x; cur.cnt += 1;
    brandAgg.set(b, cur);
  }
  const brandAvg = Array.from(brandAgg.entries())
    .filter(([_,v])=>v.cnt>=20)
    .map(([k,v])=>[k, v.sum/v.cnt, v.cnt])
    .sort((a,b)=>b[1]-a[1])
    .slice(0,15);
  chartBar(
    document.getElementById('cBrandAvg'),
    brandAvg.map(x=>`${x[0]} (${x[2]})`),
    brandAvg.map(x=>Number(x[1].toFixed(2))),
    {label:'평균 평점', color:'rgba(124,58,237,.65)'}
  );

  // Rating distribution
  const dist = [0,0,0,0,0];
  for(const r of rows){
    const x = ratingNum(r);
    if(!x) continue;
    const i = Math.min(5, Math.max(1, Math.round(x))) - 1;
    dist[i] += 1;
  }
  new Chart(document.getElementById('cRatingDist'), {
    type:'bar',
    data:{ labels:['1','2','3','4','5'], datasets:[{label:'건수', data:dist, backgroundColor:'rgba(2,132,199,.65)'}]},
    options:{ responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}, scales:{y:{beginAtZero:true}} }
  });
}

main().catch(err=>{ console.error(err); alert('Insights 로드 실패: '+String(err)); });
