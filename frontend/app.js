const state = {
  payload: null,
  currentView: 'priority',
  currentWatchlist: 'daily',
  currentReport: 'validation',
  currentScreeningScanner: 'model_scanner',
  currentWangjiProfile: 'strict',
  customWangjiRuns: {},
  selectedInstrument: null,
  sortKey: '',
  sortDirection: 'desc',
};

const summaryGrid = document.getElementById('summaryGrid');
const screeningSummary = document.getElementById('screeningSummary');
const screeningTable = document.getElementById('screeningTable');
const screeningReport = document.getElementById('screeningReport');
const wangjiProfileTabs = document.getElementById('wangjiProfileTabs');
const wangjiControls = document.getElementById('wangjiControls');
const opsGrid = document.getElementById('opsGrid');
const recentArchives = document.getElementById('recentArchives');
const dataTable = document.getElementById('dataTable');
const tableTitle = document.getElementById('tableTitle');
const resultCount = document.getElementById('resultCount');
const highlights = document.getElementById('highlights');
const reportContent = document.getElementById('reportContent');
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

function metricValue(value, digits = 4) {
  if (value === null || value === undefined || value === '' || Number.isNaN(Number(value))) return 'n/a';
  return num(value, digits);
}

function isNumericValue(value) {
  return value !== '' && value !== null && value !== undefined && !Number.isNaN(Number(value));
}

function readValue(row, path) {
  return path.split('.').reduce((acc, part) => (acc && acc[part] !== undefined ? acc[part] : ''), row);
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
  renderScreening();
  renderOps();
  renderCharts();
  renderEventTypeOptions();
  renderHighlights();
  renderReport();
  renderWatchlist();
  renderTable();
  renderDetailDrawer();
}

function renderSummary() {
  const summary = state.payload.summary || {};
  const items = [
    ['priority候选', summary.priority_count],
    ['风险候选', summary.risk_count],
    ['事件卡片', summary.event_card_count],
    ['模型初筛', summary.model_scanner_count],
    ['王绩 strict', summary.wangji_strict_passed],
    ['王绩 relax', summary.wangji_relax_passed],
    ['时间线文件', summary.timeline_count],
    ['验证已比较天数', summary.validation_days_compared],
  ];
  summaryGrid.innerHTML = items.map(([label, value]) => `
    <div class="summary-card">
      <h3>${label}</h3>
      <strong>${escapeHtml(value)}</strong>
    </div>
  `).join('');
}

function defaultWangjiParams(profile) {
  const rules = state.payload?.screeners?.wangji_scanner?.[profile]?.summary?.rules || {};
  return {
    setup_days: rules.setup_days ?? (profile === 'strict' ? 10 : 7),
    close_range_max: rules.close_range_max ?? (profile === 'strict' ? 0.06 : 0.08),
    max_daily_abs_ret_max: rules.max_daily_abs_ret_max ?? (profile === 'strict' ? 0.04 : 0.05),
    breakout_ret_min: rules.breakout_ret_min ?? (profile === 'strict' ? 0.07 : 0.06),
    vol_ratio_min: rules.vol_ratio_min ?? (profile === 'strict' ? 2.0 : 1.8),
    pullback_daily_min: rules.pullback_daily_min ?? (profile === 'strict' ? -0.03 : -0.035),
    pullback_ret_min: rules.pullback_ret_min ?? (profile === 'strict' ? -0.06 : -0.08),
    pullback_avg_vol_ratio_max: rules.pullback_avg_vol_ratio_max ?? (profile === 'strict' ? 0.70 : 0.85),
  };
}

function currentWangjiResult() {
  const custom = state.customWangjiRuns[state.currentWangjiProfile];
  if (custom) return custom;
  const wangji = state.payload?.screeners?.wangji_scanner || {};
  return wangji[state.currentWangjiProfile] || { rows: [], report: '', summary: {} };
}

async function runWangjiScannerFromControls() {
  const params = {};
  wangjiControls.querySelectorAll('[data-param-key]').forEach(input => {
    params[input.dataset.paramKey] = input.value;
  });
  const statusNode = wangjiControls.querySelector('.wangji-controls-status');
  if (statusNode) statusNode.textContent = '生成中…';
  try {
    const res = await fetch('/api/wangji-scanner/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ profile: state.currentWangjiProfile, params }),
    });
    const data = await res.json();
    if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`);
    state.customWangjiRuns[state.currentWangjiProfile] = {
      rows: data.rows || [],
      report: data.report || '',
      summary: data.summary || {},
    };
    if (statusNode) statusNode.textContent = `已生成：${data.summary?.passed ?? 0} 只通过 / ${data.summary?.total ?? 0} 只评估`;
    renderScreening();
  } catch (err) {
    if (statusNode) statusNode.textContent = `生成失败：${err.message}`;
  }
}

function renderWangjiControls() {
  if (state.currentScreeningScanner !== 'wangji_scanner') {
    wangjiControls.innerHTML = '';
    return;
  }
  const params = defaultWangjiParams(state.currentWangjiProfile);
  wangjiControls.innerHTML = `
    <div class="wangji-controls-grid">
      <label>整理天数<input data-param-key="setup_days" type="number" min="3" step="1" value="${escapeHtml(params.setup_days)}" /></label>
      <label>整理振幅上限<input data-param-key="close_range_max" type="number" min="0.01" max="0.5" step="0.005" value="${escapeHtml(params.close_range_max)}" /></label>
      <label>整理单日波动上限<input data-param-key="max_daily_abs_ret_max" type="number" min="0.01" max="0.3" step="0.005" value="${escapeHtml(params.max_daily_abs_ret_max)}" /></label>
      <label>突破涨幅下限<input data-param-key="breakout_ret_min" type="number" min="0.01" max="0.3" step="0.005" value="${escapeHtml(params.breakout_ret_min)}" /></label>
      <label>放量倍数下限<input data-param-key="vol_ratio_min" type="number" min="0.5" max="10" step="0.1" value="${escapeHtml(params.vol_ratio_min)}" /></label>
      <label>单日回踩下限<input data-param-key="pullback_daily_min" type="number" min="-0.2" max="0" step="0.005" value="${escapeHtml(params.pullback_daily_min)}" /></label>
      <label>三日回撤下限<input data-param-key="pullback_ret_min" type="number" min="-0.3" max="0" step="0.005" value="${escapeHtml(params.pullback_ret_min)}" /></label>
      <label>回踩量比上限<input data-param-key="pullback_avg_vol_ratio_max" type="number" min="0.1" max="2" step="0.05" value="${escapeHtml(params.pullback_avg_vol_ratio_max)}" /></label>
    </div>
    <div class="wangji-controls-actions">
      <button id="runWangjiScannerBtn">按当前参数生成候选</button>
      <span class="wangji-controls-status">可调参数后点击生成</span>
    </div>
  `;
  const btn = document.getElementById('runWangjiScannerBtn');
  if (btn) btn.addEventListener('click', runWangjiScannerFromControls);
}

function screeningData() {
  const screeners = state.payload.screeners || {};
  if (state.currentScreeningScanner === 'wangji_scanner') {
    const current = currentWangjiResult();
    return {
      title: `wangji-scanner / ${state.currentWangjiProfile}`,
      rows: current.rows || [],
      report: current.report || '',
      summary: current.summary || {},
      columns: [
        ['scanner_rank', 'Rank'],
        ['instrument', '股票'],
        ['pattern_passed', '通过'],
        ['rules_passed_count', '规则命中'],
        ['breakout_ret', '突破涨幅'],
        ['vol_ratio_5', '放量倍数'],
        ['pullback_ret_3d', '3日回撤'],
        ['close_range_10', '10日振幅'],
      ],
      extractor: row => `${row.instrument} ${row.pattern_profile} ${row.breakout_ret}`,
    };
  }
  const model = screeners.model_scanner || { rows: [], report: '', summary: {} };
  return {
    title: 'model-scanner',
    rows: model.rows || [],
    report: model.report || '',
    summary: model.summary || {},
    columns: [
      ['rank', 'Rank'],
      ['instrument', '股票'],
      ['score', '模型分'],
      ['candidate_source', '来源'],
      ['datetime', '日期'],
    ],
    extractor: row => `${row.instrument} ${row.candidate_source} ${row.datetime}`,
  };
}

function renderScreening() {
  document.querySelectorAll('.screening-tab').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.scanner === state.currentScreeningScanner);
  });

  if (state.currentScreeningScanner === 'wangji_scanner') {
    wangjiProfileTabs.innerHTML = `
      <button class="screening-subtab ${state.currentWangjiProfile === 'strict' ? 'active' : ''}" data-profile="strict">strict</button>
      <button class="screening-subtab ${state.currentWangjiProfile === 'relax' ? 'active' : ''}" data-profile="relax">relax</button>
    `;
    wangjiProfileTabs.querySelectorAll('.screening-subtab').forEach(btn => {
      btn.addEventListener('click', () => {
        state.currentWangjiProfile = btn.dataset.profile;
        renderScreening();
      });
    });
  } else {
    wangjiProfileTabs.innerHTML = '';
  }

  renderWangjiControls();
  const current = screeningData();
  const summaryLines = [];
  if (state.currentScreeningScanner === 'wangji_scanner') {
    summaryLines.push(['总评估', current.summary.total || 0]);
    summaryLines.push(['通过数', current.summary.passed || 0]);
    summaryLines.push(['Top10', (current.summary.top_instruments || []).join(', ') || 'none']);
  } else {
    summaryLines.push(['总候选', current.summary.total || 0]);
    summaryLines.push(['来源', current.summary.candidate_source || 'n/a']);
    summaryLines.push(['日期', current.summary.candidate_date || 'n/a']);
  }
  screeningSummary.innerHTML = summaryLines.map(([label, value]) => `
    <div class="screening-metric">
      <h3>${label}</h3>
      <div>${escapeHtml(value)}</div>
    </div>
  `).join('');

  const head = `<thead><tr>${current.columns.map(([key, label]) => `<th>${escapeHtml(label)}</th>`).join('')}</tr></thead>`;
  const body = `<tbody>${current.rows.slice(0, 20).map(row => `<tr>${current.columns.map(([key]) => `<td>${formatCell(readValue(row, key), key)}</td>`).join('')}</tr>`).join('')}</tbody>`;
  screeningTable.innerHTML = head + body;
  screeningReport.textContent = current.report || '';
}

function renderOps() {
  const ops = state.payload.ops || {};
  const compare = ops.validation_compare || {};
  const backtest = ops.backtest || {};
  const diff = ops.archive_diff || {};
  const latestArchive = ops.latest_archive || {};
  const cards = [
    {
      title: '前向验证',
      lines: [
        `已比较天数：${escapeHtml(compare.days_compared ?? 0)}`,
        `priority赢5d天数：${escapeHtml(compare.priority_win_days_5d ?? 'n/a')}`,
        `avg excess delta 5d：${metricValue(compare.avg_excess_delta_5d)}`,
      ],
    },
    {
      title: '历史横截面评估',
      lines: [
        `rank IC mean：${metricValue(backtest.rank_ic_mean)}`,
        `rank IC IR：${metricValue(backtest.rank_ic_ir)}`,
        `top30 avg return：${metricValue(backtest.topk_avg_return)}`,
      ],
    },
    {
      title: '批次变化',
      lines: [
        `新进priority：${escapeHtml(diff.new_priority_count ?? 0)}`,
        `移出priority：${escapeHtml(diff.removed_priority_count ?? 0)}`,
        `新进risk：${escapeHtml(diff.new_risk_count ?? 0)}`,
      ],
    },
    {
      title: '最近运行',
      lines: [
        `latest batch：${escapeHtml(latestArchive.batch_name || 'n/a')}`,
        `archived at：${escapeHtml(latestArchive.archived_at || 'n/a')}`,
        `outputs：priority / risk / wangji / validation / timeline`,
      ],
    },
  ];
  opsGrid.innerHTML = cards.map(card => `
    <div class="ops-card">
      <h3>${card.title}</h3>
      ${card.lines.map(line => `<div class="ops-line">${escapeHtml(line)}</div>`).join('')}
    </div>
  `).join('');
  const archives = state.payload.recent_archives || [];
  recentArchives.innerHTML = archives.map(item => `
    <div class="archive-item">
      <strong>${escapeHtml(item.batch_name || '')}</strong>
      <span>${escapeHtml(item.archived_at || '')}</span>
    </div>
  `).join('') || '<p class="muted">暂无归档。</p>';
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

function renderReport() {
  const mapping = {
    validation: state.payload.strategy_validation_report,
    backtest: state.payload.backtest_report,
    archive_diff: state.payload.archive_diff_report,
  };
  reportContent.textContent = mapping[state.currentReport] || '';
  document.querySelectorAll('.report-tab').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.report === state.currentReport);
  });
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
    detailDrawer.innerHTML = '<p class="muted">点击左侧任意股票行，右侧会展示完整事件卡、风险信息、公告正文摘要和历史时间线。</p>';
    return;
  }
  detailDrawer.className = 'detail-drawer';
  const priority = detail.priority || {};
  const risk = detail.risk || {};
  const announcements = detail.announcements || [];
  const cards = detail.event_cards || [];
  const timeline = detail.timeline || [];
  detailDrawer.innerHTML = `
    <div class="detail-section">
      <h3>${escapeHtml(detail.instrument)}</h3>
      <div class="detail-grid">
        <div class="metric-chip">Priority<strong>${num(priority.priority_score)}</strong></div>
        <div class="metric-chip">Risk<strong>${risk.risk_attention_score ? num(risk.risk_attention_score) : '—'}</strong></div>
        <div class="metric-chip">事件卡<strong>${cards.length}</strong></div>
        <div class="metric-chip">时间线节点<strong>${timeline.length}</strong></div>
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
      <h3>时间线</h3>
      ${timeline.slice(0, 8).map(item => `
        <div class="timeline-item">
          <div class="timeline-title">${escapeHtml(item.run_batch || '')}</div>
          <div class="card-meta">
            <span>${escapeHtml(item.run_date || '')}</span>
            <span>priority_rank=${escapeHtml(item.priority_rank || '')}</span>
            <span>priority_score=${metricValue(item.priority_score)}</span>
            <span>risk_rank=${escapeHtml(item.risk_rank || '—')}</span>
          </div>
          <div class="announcement-text">${escapeHtml(item.top_event_title || '')}</div>
        </div>
      `).join('') || '<p class="muted">当前还没有这只票的归档时间线。</p>'}
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
  if (key === 'pattern_passed') return value ? '是' : '否';
  if (key.toLowerCase().includes('rank') || key === 'event_count' || key === 'rules_passed_count') {
    const n = Number(value);
    return Number.isFinite(n) ? String(Math.round(n)) : escapeHtml(value);
  }
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

document.querySelectorAll('.report-tab').forEach(btn => {
  btn.addEventListener('click', () => {
    state.currentReport = btn.dataset.report;
    renderReport();
  });
});

document.querySelectorAll('.screening-tab').forEach(btn => {
  btn.addEventListener('click', () => {
    state.currentScreeningScanner = btn.dataset.scanner;
    renderScreening();
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
  screeningTable.innerHTML = `<tbody><tr><td>${escapeHtml(err.message)}</td></tr></tbody>`;
  detailDrawer.innerHTML = `<p class="muted">${escapeHtml(err.message)}</p>`;
});
