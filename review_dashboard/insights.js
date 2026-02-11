const INSIGHTS_URL = "../data/insights.json";
const LEGACY_URL = "../data/reviews.json";

function byText(x){ return String(x ?? ""); }

function createGradient(ctx, area, from, to){
  const g = ctx.createLinearGradient(0, area.bottom, 0, area.top);
  g.addColorStop(0, from);
  g.addColorStop(1, to);
  return g;
}

function baseOptions({ horizontal=false, maxY=null }={}){
  return {
    responsive: true,
    maintainAspectRatio: false,
    indexAxis: horizontal ? 'y' : 'x',
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: 'rgba(15,23,42,.92)',
        titleColor: '#fff',
        bodyColor: '#e2e8f0',
        borderColor: 'rgba(148,163,184,.35)',
        borderWidth: 1,
        padding: 10,
        displayColors: false,
      },
    },
    scales: horizontal ? {
      x: {
        beginAtZero: true,
        max: maxY ?? undefined,
        grid: { color: 'rgba(148,163,184,.18)' },
        ticks: { color:'rgba(51,65,85,.92)', font:{weight:'700'} },
      },
      y: {
        grid: { display:false },
        ticks: { color:'rgba(30,41,59,.96)', font:{weight:'700', size:11} },
      },
    } : {
      x: {
        grid: { display:false },
        ticks: { color:'rgba(51,65,85,.92)', font:{weight:'700'} },
      },
      y: {
        beginAtZero: true,
        suggestedMax: maxY ?? undefined,
        grid: { color: 'rgba(148,163,184,.20)' },
        ticks: { color:'rgba(51,65,85,.92)', font:{weight:'700'} },
      },
    },
  };
}

function chartHorizontalBar(el, labels, data){
  const fullLabels = labels.map(byText);
  return new Chart(el, {
    type: 'bar',
    data: {
      labels: fullLabels,
      datasets: [{
        label:'리뷰 수',
        data,
        borderRadius: 8,
        borderSkipped: false,
        backgroundColor: (ctx) => {
          const { chart } = ctx;
          const area = chart.chartArea;
          if (!area) return 'rgba(37,99,235,.72)';
          return createGradient(chart.ctx, area, 'rgba(37,99,235,.55)', 'rgba(124,58,237,.86)');
        },
      }],
    },
    options: {
      ...baseOptions({ horizontal:true }),
      scales: {
        ...baseOptions({ horizontal:true }).scales,
        y: {
          grid: { display:false },
          ticks: {
            color:'rgba(30,41,59,.96)',
            font:{weight:'700', size:11},
            callback: (_, i) => {
              const x = fullLabels[i] || '';
              return x.length > 20 ? `${x.slice(0, 19)}…` : x;
            },
          },
        },
      },
      plugins: {
        ...baseOptions({ horizontal:true }).plugins,
        tooltip: {
          ...baseOptions({ horizontal:true }).plugins.tooltip,
          callbacks: {
            title: (items) => fullLabels[items?.[0]?.dataIndex] || '',
            label: (item) => `리뷰 수: ${Number(item.parsed.x || 0).toLocaleString()}건`,
          },
        },
      },
    },
  });
}

function chartBrandAvg(el, labels, data){
  return new Chart(el, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label:'평균 평점',
        data,
        fill: true,
        tension: .35,
        borderWidth: 3,
        borderColor: 'rgba(124,58,237,.95)',
        pointBackgroundColor: 'rgba(124,58,237,.95)',
        pointRadius: 4,
        pointHoverRadius: 6,
        backgroundColor: (ctx) => {
          const { chart } = ctx;
          const area = chart.chartArea;
          if (!area) return 'rgba(124,58,237,.20)';
          return createGradient(chart.ctx, area, 'rgba(124,58,237,.12)', 'rgba(124,58,237,.42)');
        },
      }],
    },
    options: {
      ...baseOptions({ maxY:5 }),
      plugins: {
        ...baseOptions({ maxY:5 }).plugins,
        legend: { display: true, labels:{ color:'rgba(51,65,85,.92)', boxWidth:14, font:{weight:'700'} } },
      },
      scales: {
        ...baseOptions({ maxY:5 }).scales,
        x: {
          ...baseOptions({ maxY:5 }).scales.x,
          ticks: {
            color:'rgba(51,65,85,.92)',
            font:{weight:'700'},
            callback: (_, i) => {
              const x = labels[i] || '';
              return x.length > 14 ? `${x.slice(0, 13)}…` : x;
            },
          },
        },
        y: {
          ...baseOptions({ maxY:5 }).scales.y,
          min: 0,
          max: 5,
          ticks: {
            color:'rgba(51,65,85,.92)',
            font:{weight:'700'},
            stepSize: 1,
          },
        },
      },
    },
  });
}

function chartRatingDonut(el, dist){
  const colors = [
    'rgba(239,68,68,.85)',
    'rgba(249,115,22,.85)',
    'rgba(234,179,8,.9)',
    'rgba(34,197,94,.85)',
    'rgba(37,99,235,.9)',
  ];
  return new Chart(el, {
    type: 'doughnut',
    data: {
      labels: ['1점','2점','3점','4점','5점'],
      datasets: [{
        data: dist,
        backgroundColor: colors,
        borderColor: 'rgba(255,255,255,.95)',
        borderWidth: 2,
        hoverOffset: 6,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '58%',
      plugins: {
        legend: {
          position: 'bottom',
          labels: {
            color:'rgba(51,65,85,.92)',
            font:{weight:'700'},
            boxWidth: 14,
            padding: 14,
          },
        },
        tooltip: {
          backgroundColor: 'rgba(15,23,42,.92)',
          callbacks: {
            label: (ctx) => `${ctx.label}: ${Number(ctx.parsed || 0).toLocaleString()}건`,
          },
        },
      },
    },
  });
}

async function main(){
  let payload;
  try {
    const res = await fetch(INSIGHTS_URL, { cache: 'no-store' });
    payload = await res.json();
  } catch (e) {
    const res2 = await fetch(LEGACY_URL, { cache: 'no-store' });
    const legacy = await res2.json();
    document.getElementById('statCount').textContent = Number(legacy?.rows?.length || 0).toLocaleString();
    document.getElementById('statAvg').textContent = '-';
    document.getElementById('statUpdated').textContent = legacy.generated_at ? legacy.generated_at.replace('T',' ').slice(0,19) : '-';
    return;
  }

  document.getElementById('statCount').textContent = Number(payload.count || 0).toLocaleString();
  document.getElementById('statAvg').textContent = '-';
  document.getElementById('statUpdated').textContent = payload.generated_at ? payload.generated_at.replace('T',' ').slice(0,19) : '-';

  const top = payload.top_products || [];
  chartHorizontalBar(
    document.getElementById('cTopProducts'),
    top.map(x=>x.name),
    top.map(x=>x.count)
  );

  const bavg = payload.brand_avg || [];
  chartBrandAvg(
    document.getElementById('cBrandAvg'),
    bavg.map(x=>`${x.name} (${x.count})`),
    bavg.map(x=>Number(Number(x.avg).toFixed(2)))
  );

  const dist = payload.rating_dist || [0,0,0,0,0];
  chartRatingDonut(document.getElementById('cRatingDist'), dist);
}

main().catch(err=>{ console.error(err); alert('Insights 로드 실패: '+String(err)); });
