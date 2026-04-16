const state = {
  payload: null,
  currentView: 'priority',
  currentWatchlist: 'daily',
  selectedInstrument: null,
  sortKey: '',
  sortDirection: 'desc',
};

const summaryGrid = document.getElementById('summaryGrid');
const dataTable = document.getElementById('dataTable');
const tableTitle = document.getElementById('tableTitle');
const resultCount = document.getElementById('resultCount');
const highlights = document.getElementById('highlights');
const watchlistContent = document.getElementById('watchlistContent');
const searchInput = document.getElementById('searchInput');
const viewSelect = document.getElementById('viewSelect');
const eventTypeSelect = document.getElementById('eventTypeSelect');
const biasSelect = document.getElementById('biasSelect');
const riskOnlySelect = document.getElementById('riskOnlySelect');
const refreshBtn = document.getElementById('refreshBtn');
const statusText = document.getElementById('statusText');
const detailDrawer = document.getElementById('detailDrawer');
const closeDetailBtn = document.getElementById('closeDetailBtn');
const distributionChart = document.getElementById('distributionChart');
const eventTypeChart = document.getElementById('eventTypeChart');
const contentSourceChart = document.getElementById('contentSourceChart');
const topCandidatesChart = document.getElementById('topCandidatesChart');

function badge(value) {
  const cls = value === 'positive' ? 'badge-positive' : value === 'negative' ? 'badge-negative' : 'badge-neutral';
  return `<span class="badge ${cls}">${escapeHtml(value || 'n/a')}</span>`;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function num(value, digits = 4) {
  const n = Number(value || 0);
  return Number.isFinite(n) ? n.toFixed(digits) : '0.0000';
}

function isNumericValue(value) {
  return value !== '' && value !== null && value !== undefined && !Number.isNaN(Number(value));
}

function readValue(row, path) {
  return path.split('.').reduce((acc, part) => acc && acc[part] !== undefined ? acc[part] : '', row);
}

function clickableInstrument(row) {
  return row.instrument || row?.priority?.instrument || '';
}

function defaultSortForView(view) {
  if (view === 'priority') return { key: 'priority_score', direction: 'desc' };
  if (view === 'risk') return { key: 'risk_attention_score', direction: 'desc' };
  if (view === 'events') return { key: 'card_score', direction: 'desc' };
  return { key: 'priority.priority_score', direction: 'desc' };
}

async function loadPayload() {
  statusText.textContent = '加载数据中…';
  const res = await fetch('../data/outputs/dashboard_data.json?ts=' + Date.now());
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  state.payload = await res.json();
  state.selectedInstrument = state.selectedInstrument || state.payload.highlights?.priority_top5?.[0]?.instrument || null;
  if (!state.sortKey) {
    const initialSort = defaultSortForView(state.currentView);
    state.sortKey = initialSort.key;
    state.sortDirection = initialSort.direction;
  }
  statusText.textContent = '数据已加载';
  renderSummary();
  renderCharts();
  renderEventTypeOptions();
  renderHighlights();
  renderWatchlist();
  renderTable();
  renderDetailDrawer();
}

function renderSummary() {
  const summary = state.payload.summary;
  const items = [
    ['priority候选', summary.priority_count],
    ['风险候选', summary.risk_count],
    ['事件卡片', summary.event_card_count],
    ['公告原文', summary.announcement_count],
    ['PDF正文成功', summary.pdf_excerpt_count],
    ['标题回退', summary.title_only_count],
  ];
  summaryGrid.innerHTML = items.map(([label, value]) => `
    <div class="summary-card">
      <h3>${label}</h3>
      <strong>${value}</strong>
    </div>
  `).join('');
}

function renderCharts() {
  renderDistributionChart();
  renderEventTypeChart();
  renderContentSourceChart();
  renderTopCandidatesChart();
}

function renderDistributionChart() {
  const priority = (state.payload.priority_candidates || []).slice(0, 12);
  const risk = state.payload.risk_candidates || [];
  const maxPriority = Math.max(...priority.map(x => Number(x.priority_score || 0)), 1);
  const maxRisk = Math.max(...risk.map(x => Number(x.risk_attention_score || 0)), 1);

  distributionChart.innerHTML = `
    <div class="legend"><span>Priority</span><span class="risk">Risk</span></div>
    <div class="chart-stack">
      ${priority.map(row => `
        <div class="dual-bar">
          <div class="bar-row"><span>${escapeHtml(row.instrument)}</span><div class="bar-track"><div class="bar-fill" style="width:${(Number(row.priority_score || 0) / maxPriority) * 100}%"></div></div><strong>${num(row.priority_score)}</strong></div>
          ${risk.find(x => x.instrument === row.instrument) ? (() => {
            const rr = risk.find(x => x.instrument === row.instrument);
            return `<div class="bar-row"><span class="muted">risk</span><div class="bar-track"><div class="bar-fill" style="background:linear-gradient(90deg,var(--red),#ff9b9b);width:${(Number(rr.risk_attention_score || 0) / maxRisk) * 100}%"></div></div><strong>${num(rr.risk_attention_score)}</strong></div>`;
          })() : ''}
        </div>
      `).join('')}
    </div>
  `;
}

function renderEventTypeChart() {
  const counts = {};
  (state.payload.event_cards || []).forEach(card => {
    const key = card.event_type || '其他';
    counts[key] = (counts[key] || 0) + 1;
  });
  const rows = Object.entries(counts).sort((a, b) => b[1] - a[1]);
  const max = Math.max(...rows.map(([, v]) => v), 1);
  eventTypeChart.innerHTML = `<div class="bar-list">${rows.map(([label, value]) => `
      <div class="bar-row"><span>${escapeHtml(label)}</span><div class="bar-track"><div class="bar-fill" style="width:${(value / max) * 100}%"></div></div><strong>${value}</strong></div>
    `).join('')}</div>`;
}

function renderContentSourceChart() {
  const summary = state.payload.summary || {};
  const total = Math.max(Number(summary.announcement_count || 0), 1);
  const pdf = Number(summary.pdf_excerpt_count || 0);
  const fallback = Number(summary.title_only_count || 0);
  contentSourceChart.innerHTML = `
    <div class="legend"><span class="pdf">PDF正文</span><span class="fallback">标题回退</span></div>
    <div class="bar-list">
      <div class="bar-row"><span>pdf_excerpt</span><div class="bar-track"><div class="bar-fill" style="background:linear-gradient(90deg,var(--green),#8af0bf);width:${(pdf / total) * 100}%"></div></div><strong>${pdf}</strong></div>
      <div class="bar-row"><span>title_only</span><div class="bar-track"><div class="bar-fill" style="background:linear-gradient(90deg,var(--yellow),#ffe59a);width:${(fallback / total) * 100}%"></div></div><strong>${fallback}</strong></div>
    </div>
    <p class="muted">命中率：${((pdf / total) * 100).toFixed(1)}%</p>
  `;
}

function renderTopCandidatesChart() {
  const rows = (state.payload.priority_candidates || []).slice(0, 10);
  const max = Math.max(...rows.map(x => Number(x.priority_score || 0)), 1);
  topCandidatesChart.innerHTML = `<div class="bar-list">${rows.map(row => `
      <div class="bar-row"><span>${escapeHtml(row.instrument)}</span><div class="bar-track"><div class="bar-fill" style="width:${(Number(row.priority_score || 0) / max) * 100}%"></div></div><strong>${num(row.priority_score)}</strong></div>
    `).join('')}</div>`;
}

function renderEventTypeOptions() {
  const types = [...new Set((state.payload.event_cards || []).map(x => x.event_type).filter(Boolean))].sort();
  eventTypeSelect.innerHTML = '<option value="">全部</option>' + types.map(x => `<option value="${x}">${x}</option>`).join('');
}

function renderHighlights() {
  const blocks = [
    ['多头前5', state.payload.highlights.priority_top5, row => `${row.priority_rank || '-'} | ${row.instrument} | priority ${num(row.priority_score)}`],
    ['风险前5', state.payload.highlights.risk_top5, row => `${row.risk_rank || '-'} | ${row.instrument} | risk ${num(row.risk_attention_score)}`],
    ['高分事件', state.payload.highlights.event_top10, row => `${row.instrument} | ${row.event_type} | card ${num(row.card_score)}`],
  ];
  highlights.innerHTML = blocks.map(([title, rows, formatter]) => `
    <div class="highlight-block">
      <h3>${title}</h3>
      ${(rows || []).map(row => `<div class="highlight-item">${escapeHtml(formatter(row))}<small>${escapeHtml(row.title || row.top_event_title || '')}</small></div>`).join('')}
    </div>
  `).join('');
}

function renderWatchlist() {
  const mapping = {
    daily: state.payload.daily_watchlist,
    weekly: state.payload.weekly_watchlist,
    risk: state.payload.risk_watchlist,
  };
  watchlistContent.textContent = mapping[state.currentWatchlist] || '';
  document.querySelectorAll('.watchlist-tab').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.watchlist === state.currentWatchlist);
  });
}

function getInstrumentDetail(instrument) {
  return (state.payload.instrument_details || []).find(item => item.instrument === instrument) || null;
}

function renderDetailDrawer() {
  const detail = getInstrumentDetail(state.selectedInstrument);
  if (!detail) {
    detailDrawer.className = 'detail-drawer empty';
    detailDrawer.innerHTML = '<p class="muted">点击左侧任意股票行，右侧会展示完整事件卡、风险信息和公告正文摘要。</p>';
    return;
  }
  detailDrawer.className = 'detail-drawer';
  const priority = detail.priority || {};
  const risk = detail.risk || {};
  const announcements = detail.announcements || [];
  const cards = detail.event_cards || [];

  detailDrawer.innerHTML = `
    <div class="detail-section">
      <h3>${escapeHtml(detail.instrument)}</h3>
      <div class="detail-grid">
        <div class="metric-chip">Priority<strong>${num(priority.priority_score)}</strong></div>
        <div class="metric-chip">Risk<strong>${risk.risk_attention_score ? num(risk.risk_attention_score) : '—'}</strong></div>
        <div class="metric-chip">事件卡<strong>${cards.length}</strong></div>
        <div class="metric-chip">公告条数<strong>${announcements.length}</strong></div>
      </div>
      <div class="card-meta">
        <span>top_event: ${escapeHtml(priority.top_event_type || '—')}</span>
        <span>top_risk: ${escapeHtml(risk.top_risk_event_type || '—')}</span>
      </div>
    </div>

    <div class="detail-section">
      <h3>事件卡</h3>
      ${cards.slice(0, 8).map(card => `
        <div class="card-block">
          <h4>${escapeHtml(card.title || '')}</h4>
          <div class="card-meta">
            <span>${escapeHtml(card.publish_date || '')}</span>
            <span>${escapeHtml(card.event_type || '')}</span>
            <span>card=${num(card.card_score)}</span>
            <span>risk=${num(card.risk_card_score)}</span>
            <span>${escapeHtml(card.importance || '')}</span>
            <span>${badge(card.bias || '')}</span>
          </div>
          <div class="announcement-text">${escapeHtml(card.summary || card.raw_content || '')}</div>
        </div>
      `).join('') || '<p class="muted">暂无事件卡。</p>'}
    </div>

    <div class="detail-section">
      <h3>公告正文摘要</h3>
      ${announcements.slice(0, 5).map(item => `
        <div class="announcement-block">
          <h4>${escapeHtml(item.title || '')}</h4>
          <div class="card-meta">
            <span>${escapeHtml(item.publish_date || '')}</span>
            <span>${escapeHtml(item.content_source || '')}</span>
            <span>len=${escapeHtml(item.content_length || '')}</span>
          </div>
          <div class="announcement-text">${escapeHtml((item.content || '').slice(0, 1200))}${(item.content || '').length > 1200 ? '…' : ''}</div>
        </div>
      `).join('') || '<p class="muted">暂无公告正文。</p>'}
    </div>
  `;
}

function rowHasRisk(row) {
  const instrument = clickableInstrument(row);
  const detail = instrument ? getInstrumentDetail(instrument) : null;
  return Boolean(
    row.has_risk === true ||
    row.risk_attention_score ||
    row.top_risk_title ||
    row.risk?.risk_attention_score ||
    detail?.risk?.risk_attention_score
  );
}

function applyFilters(rows, textExtractor) {
  const q = searchInput.value.trim().toLowerCase();
  const eventType = eventTypeSelect.value;
  const bias = biasSelect.value;
  const riskMode = riskOnlySelect.value;
  return rows.filter(row => {
    const hay = textExtractor(row).toLowerCase();
    const matchesQ = !q || hay.includes(q);
    const rowBias = row.bias || row.top_event_bias || row.top_risk_bias || '';
    const matchesBias = !bias || rowBias === bias;
    const matchesRisk = !riskMode || (riskMode === 'risk_only' ? rowHasRisk(row) : !rowHasRisk(row));
    const matchesType = !eventType || row.event_type === eventType || row.top_event_type === eventType || row.top_risk_event_type === eventType;
    return matchesQ && matchesType && matchesBias && matchesRisk;
  });
}

function sortRows(rows) {
  const key = state.sortKey;
  const direction = state.sortDirection === 'asc' ? 1 : -1;
  if (!key) return rows;
  return [...rows].sort((a, b) => {
    const av = readValue(a, key);
    const bv = readValue(b, key);
    if (isNumericValue(av) && isNumericValue(bv)) {
      return (Number(av) - Number(bv)) * direction;
    }
    return String(av).localeCompare(String(bv), 'zh-CN') * direction;
  });
}

function renderTable() {
  const payload = state.payload;
  let rows = [];
  let columns = [];
  let extractor = () => '';

  if (state.currentView === 'priority') {
    tableTitle.textContent = '多头优先池';
    rows = payload.priority_candidates || [];
    columns = [
      ['priority_rank', 'P'], ['instrument', '股票'], ['priority_score', 'Priority'], ['quant_score_norm', 'Quant'], ['event_score', 'Event'], ['top_event_type', '事件类型'], ['top_event_title', '顶部事件'], ['top_event_bias', 'Bias']
    ];
    extractor = row => `${row.instrument} ${row.top_event_title} ${row.top_event_summary}`;
  } else if (state.currentView === 'risk') {
    tableTitle.textContent = '风险观察池';
    rows = payload.risk_candidates || [];
    columns = [
      ['risk_rank', 'R'], ['instrument', '股票'], ['risk_attention_score', 'Risk'], ['risk_event_score', 'RiskEvent'], ['quant_score_norm', 'Quant'], ['top_risk_event_type', '风险类型'], ['top_risk_title', '顶部风险'], ['top_risk_bias', 'Bias']
    ];
    extractor = row => `${row.instrument} ${row.top_risk_title} ${row.top_risk_summary}`;
  } else if (state.currentView === 'events') {
    tableTitle.textContent = '事件卡片';
    rows = payload.event_cards || [];
    columns = [
      ['instrument', '股票'], ['event_type', '事件类型'], ['importance', '重要性'], ['bias', 'Bias'], ['card_score', 'Card'], ['risk_card_score', 'RiskCard'], ['title', '标题'], ['content_source', '正文来源']
    ];
    extractor = row => `${row.instrument} ${row.title} ${row.summary} ${row.raw_content}`;
  } else {
    tableTitle.textContent = '股票详情';
    rows = payload.instrument_details || [];
    columns = [
      ['instrument', '股票'], ['event_count', '事件数'], ['has_risk', '风险'], ['priority.priority_score', 'Priority'], ['risk.risk_attention_score', 'Risk'], ['priority.top_event_title', '顶部事件'], ['risk.top_risk_title', '顶部风险']
    ];
    extractor = row => `${row.instrument} ${row.priority?.top_event_title || ''} ${row.risk?.top_risk_title || ''}`;
  }

  rows = sortRows(applyFilters(rows, extractor));
  resultCount.textContent = `共 ${rows.length} 条`;

  const head = `<thead><tr>${columns.map(([key, label]) => {
    const active = state.sortKey === key;
    const indicator = active ? (state.sortDirection === 'asc' ? '▲' : '▼') : '';
    return `<th class="is-sortable" data-sort-key="${escapeHtml(key)}">${label}<span class="sort-indicator">${indicator}</span></th>`;
  }).join('')}</tr></thead>`;
  const body = `<tbody>${rows.map(row => {
    const instrument = clickableInstrument(row);
    const selected = instrument && instrument === state.selectedInstrument;
    return `<tr class="is-clickable ${selected ? 'selected-row' : ''}" data-instrument="${escapeHtml(instrument)}">${columns.map(([key]) => `<td>${formatCell(readValue(row, key), key)}</td>`).join('')}</tr>`;
  }).join('')}</tbody>`;
  dataTable.innerHTML = head + body;

  dataTable.querySelectorAll('th[data-sort-key]').forEach(th => {
    th.addEventListener('click', () => {
      const key = th.dataset.sortKey;
      if (state.sortKey === key) {
        state.sortDirection = state.sortDirection === 'asc' ? 'desc' : 'asc';
      } else {
        state.sortKey = key;
        state.sortDirection = key.toLowerCase().includes('rank') ? 'asc' : 'desc';
      }
      renderTable();
    });
  });

  dataTable.querySelectorAll('tbody tr[data-instrument]').forEach(tr => {
    tr.addEventListener('click', () => {
      state.selectedInstrument = tr.dataset.instrument;
      renderTable();
      renderDetailDrawer();
    });
  });
}

function formatCell(value, key) {
  if (key.toLowerCase().includes('bias')) return badge(value);
  if (typeof value === 'number') return value.toFixed(4);
  if (value === true) return '是';
  if (value === false) return '否';
  if (value === null || value === undefined) return '';
  const text = String(value);
  return escapeHtml(text.length > 160 ? text.slice(0, 160) + '…' : text);
}

document.querySelectorAll('.watchlist-tab').forEach(btn => {
  btn.addEventListener('click', () => {
    state.currentWatchlist = btn.dataset.watchlist;
    renderWatchlist();
  });
});

viewSelect.addEventListener('change', () => {
  state.currentView = viewSelect.value;
  const sort = defaultSortForView(state.currentView);
  state.sortKey = sort.key;
  state.sortDirection = sort.direction;
  renderTable();
});
searchInput.addEventListener('input', renderTable);
eventTypeSelect.addEventListener('change', renderTable);
biasSelect.addEventListener('change', renderTable);
riskOnlySelect.addEventListener('change', renderTable);
refreshBtn.addEventListener('click', loadPayload);
closeDetailBtn.addEventListener('click', () => {
  state.selectedInstrument = null;
  renderTable();
  renderDetailDrawer();
});

loadPayload().catch(err => {
  console.error(err);
  statusText.textContent = '加载失败';
  tableTitle.textContent = '加载失败';
  dataTable.innerHTML = `<tbody><tr><td>${escapeHtml(err.message)}</td></tr></tbody>`;
  detailDrawer.innerHTML = `<p class="muted">${escapeHtml(err.message)}</p>`;
});
