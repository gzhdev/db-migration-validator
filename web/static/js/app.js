/* ============================================================
   Debug Viewer — client-side logic
   ============================================================ */

// ----------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------

function escapeHtml(str) {
    if (str === null || str === undefined) return '<span class="fv-null">NULL</span>';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

function typeBadge(type) {
    return `<span class="badge badge-type-${type}">${escapeHtml(type)}</span>`;
}

function pkLabel(pk) {
    if (!pk || typeof pk !== 'object') return escapeHtml(String(pk));
    return Object.entries(pk).map(([k, v]) => `${escapeHtml(k)}=${escapeHtml(v)}`).join(', ');
}

// ----------------------------------------------------------------
// Record card rendering
// ----------------------------------------------------------------

function renderDataCol(title, data, cssClass) {
    if (!data || typeof data !== 'object') {
        return `<div class="data-col ${cssClass}"><h4>${title}</h4><span class="fv-null">暂无数据</span></div>`;
    }
    const rows = Object.entries(data).map(([k, v]) =>
        `<div class="field-row"><span class="fn">${escapeHtml(k)}</span><span class="fv">${escapeHtml(v === null ? null : String(v))}</span></div>`
    ).join('');
    return `<div class="data-col ${cssClass}"><h4>${title}</h4>${rows || '<span class="fv-null">暂无数据</span>'}</div>`;
}

function renderDiffSection(comparison) {
    if (!comparison || typeof comparison !== 'object') return '';
    const mismatches = Object.entries(comparison).filter(([, r]) => r && r.match === false);
    if (!mismatches.length) return '';
    const items = mismatches.map(([field, r]) =>
        `<div class="diff-item">
           <span class="diff-field">${escapeHtml(field)}</span>
           <span class="diff-expected">期望: ${escapeHtml(r.expected === null ? 'NULL' : String(r.expected))}</span>
           <span class="diff-actual">实际: ${escapeHtml(r.actual === null ? 'NULL' : String(r.actual))}</span>
         </div>`
    ).join('');
    return `<div class="diff-section"><h5>字段差异</h5>${items}</div>`;
}

function renderVcSection(failures) {
    if (!failures || !failures.length) return '';
    const items = failures.map(f => {
        const reasons = (f.reasons || []).join(', ');
        return `<div class="vc-item"><strong>${escapeHtml(f.field)}</strong>: 值=${escapeHtml(f.value === null ? 'NULL' : String(f.value))} — ${escapeHtml(reasons)}</div>`;
    }).join('');
    return `<div class="vc-section"><h5>取值范围校验失败</h5>${items}</div>`;
}

function renderRecord(rec) {
    const pkStr = pkLabel(rec.primary_key);
    const rawCol = renderDataCol('原始数据', rec.raw_source, 'raw-source');
    const expCol = renderDataCol('期望值', rec.expected_values, 'expected');
    const tgtCol = renderDataCol('目标值', rec.target_values, 'target');
    const diff = renderDiffSection(rec.comparison_result);
    const vc = renderVcSection(rec.value_check_failures);

    return `
    <div class="record-card" data-type="${escapeHtml(rec.match_type)}">
      <div class="record-card-header" onclick="toggleCard(this)">
        <span class="pk-label">${pkStr}</span>
        ${typeBadge(rec.match_type)}
        <span class="toggle-arrow">&#9658;</span>
      </div>
      <div class="record-card-body" style="display:none">
        <div class="data-grid">
          ${rawCol}${expCol}${tgtCol}
        </div>
        ${diff}${vc}
      </div>
    </div>`;
}

function toggleCard(header) {
    const body = header.nextElementSibling;
    const arrow = header.querySelector('.toggle-arrow');
    const isOpen = body.style.display !== 'none';
    body.style.display = isOpen ? 'none' : 'block';
    arrow.classList.toggle('open', !isOpen);
}

// ----------------------------------------------------------------
// Pagination rendering
// ----------------------------------------------------------------

function renderPagination(current, total, onClick) {
    if (total <= 1) return '';

    function btn(label, page, active, disabled) {
        if (disabled) return `<span class="page-btn disabled">${label}</span>`;
        if (active)   return `<span class="page-btn active">${label}</span>`;
        return `<a class="page-btn" href="#" data-page="${page}">${label}</a>`;
    }

    let html = '<nav class="pagination">';
    html += btn('&laquo;', current - 1, false, current <= 1);

    for (let p = 1; p <= total; p++) {
        if (p === current) {
            html += btn(p, p, true, false);
        } else if (p <= 2 || p >= total - 1 || (p >= current - 2 && p <= current + 2)) {
            html += btn(p, p, false, false);
        } else if (p === 3 || p === total - 2) {
            html += '<span class="page-btn ellipsis">&hellip;</span>';
        }
    }

    html += btn('&raquo;', current + 1, false, current >= total);
    html += '</nav>';

    const container = document.getElementById('pagination-container');
    container.innerHTML = html;
    container.querySelectorAll('a.page-btn').forEach(a => {
        a.addEventListener('click', e => {
            e.preventDefault();
            onClick(parseInt(a.dataset.page));
        });
    });
}

// ----------------------------------------------------------------
// Records page main logic
// ----------------------------------------------------------------

(function () {
    if (typeof window.RUN_ID === 'undefined') return; // not on records page

    const runId = window.RUN_ID;
    let currentPage = 1;

    function getFilters() {
        const table = document.getElementById('filter-table').value;
        const activeBtn = document.querySelector('#filter-type .btn-filter.active');
        const matchType = activeBtn ? activeBtn.dataset.type : '';
        const pageSizeEl = document.getElementById('filter-page-size');
        const pageSize = pageSizeEl ? parseInt(pageSizeEl.value, 10) : 20;
        const pkSearch = (document.getElementById('filter-pk') || {}).value || '';
        return { table, matchType, pageSize, pkSearch: pkSearch.trim() };
    }

    function buildApiUrl(page) {
        const { table, matchType, pageSize, pkSearch } = getFilters();
        const params = new URLSearchParams({ page, page_size: pageSize });
        if (table)     params.set('table', table);
        if (matchType) params.set('match_type', matchType);
        if (pkSearch)  params.set('pk_search', pkSearch);
        return `/api/runs/${runId}/records?${params}`;
    }

    function buildPageUrl(page) {
        const { table, matchType, pageSize, pkSearch } = getFilters();
        const params = new URLSearchParams({ page, page_size: pageSize });
        if (table)     params.set('table', table);
        if (matchType) params.set('match_type', matchType);
        if (pkSearch)  params.set('pk_search', pkSearch);
        return `?${params}`;
    }

    function pushState(page) {
        const { pageSize, pkSearch } = getFilters();
        history.pushState({ page, pageSize, pkSearch }, '', buildPageUrl(page));
    }

    async function loadRecords(page) {
        currentPage = page;
        const container = document.getElementById('records-container');
        container.innerHTML = '<div class="loading">加载中…</div>';
        document.getElementById('pagination-container').innerHTML = '';

        try {
            const resp = await fetch(buildApiUrl(page));
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            const data = await resp.json();

            if (!data.records.length) {
                container.innerHTML = '<div class="loading">暂无符合条件的记录</div>';
                return;
            }

            const statusBar = `<div class="status-bar">共 ${data.total} 条记录，第 ${data.page}/${data.total_pages} 页</div>`;
            const cards = data.records.map(renderRecord).join('');
            container.innerHTML = statusBar + cards;

            renderPagination(data.page, data.total_pages, p => {
                pushState(p);
                loadRecords(p);
            });
        } catch (err) {
            container.innerHTML = `<div class="loading">加载失败: ${escapeHtml(err.message)}</div>`;
        }
    }

    // Sync filters → URL → reload
    function onFilterChange() {
        currentPage = 1;
        pushState(1);
        loadRecords(1);
    }

    document.getElementById('filter-table').addEventListener('change', onFilterChange);

    document.querySelectorAll('#filter-type .btn-filter').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('#filter-type .btn-filter').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            onFilterChange();
        });
    });

    const pageSizeEl = document.getElementById('filter-page-size');
    if (pageSizeEl) {
        pageSizeEl.addEventListener('change', onFilterChange);
    }

    // 主键搜索：防抖 400ms 后触发
    const pkInput = document.getElementById('filter-pk');
    const pkClearBtn = document.getElementById('btn-pk-clear');
    let pkDebounceTimer = null;
    if (pkInput) {
        pkInput.addEventListener('input', () => {
            clearTimeout(pkDebounceTimer);
            pkClearBtn.style.display = pkInput.value ? 'inline-block' : 'none';
            pkDebounceTimer = setTimeout(onFilterChange, 400);
        });
        pkInput.addEventListener('keydown', e => {
            if (e.key === 'Enter') { clearTimeout(pkDebounceTimer); onFilterChange(); }
        });
    }
    if (pkClearBtn) {
        pkClearBtn.style.display = 'none';
        pkClearBtn.addEventListener('click', () => {
            pkInput.value = '';
            pkClearBtn.style.display = 'none';
            onFilterChange();
        });
    }

    window.addEventListener('popstate', e => {
        const p = (e.state && e.state.page) || 1;
        const ps = (e.state && e.state.pageSize);
        if (ps && pageSizeEl) pageSizeEl.value = String(ps);
        const pk = (e.state && e.state.pkSearch) || '';
        if (pkInput) { pkInput.value = pk; if (pkClearBtn) pkClearBtn.style.display = pk ? 'inline-block' : 'none'; }
        loadRecords(p);
    });

    // Initial state from URL
    const initParams = new URLSearchParams(location.search);
    const initPage = parseInt(initParams.get('page') || '1', 10);
    const initPageSize = initParams.get('page_size');
    if (initPageSize && pageSizeEl) pageSizeEl.value = initPageSize;
    const initPk = initParams.get('pk_search') || '';
    if (initPk && pkInput) { pkInput.value = initPk; if (pkClearBtn) pkClearBtn.style.display = 'inline-block'; }
    loadRecords(initPage);
})();

// ----------------------------------------------------------------
// Delete run (index page)
// ----------------------------------------------------------------

function deleteRun(runId, btn) {
    if (!confirm(`确定删除 Run #${runId} 及其所有记录？`)) return;
    fetch(`/runs/${runId}`, { method: 'DELETE' })
        .then(r => r.json())
        .then(data => {
            if (data.ok) {
                const row = btn.closest('tr');
                if (row) row.remove();
            }
        })
        .catch(err => alert('删除失败: ' + err.message));
}
