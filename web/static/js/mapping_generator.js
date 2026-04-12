/* ============================================================
   Mapping Generator — client-side logic
   ============================================================ */

const MG = (function () {
    let tableCounter = 0;
    let currentFieldRow = null;   // for detail modal
    let lastGeneratedJSON = null;

    // ----------------------------------------------------------------
    // Section toggle
    // ----------------------------------------------------------------
    function toggleSection(id) {
        const el = document.getElementById(id);
        el.style.display = el.style.display === 'none' ? '' : 'none';
    }

    // ----------------------------------------------------------------
    // Oracle fields visibility
    // ----------------------------------------------------------------
    function initDbTypeListeners() {
        ['src', 'tgt'].forEach(prefix => {
            const sel = document.getElementById(`${prefix}-type`);
            sel.addEventListener('change', () => {
                const oracleFields = document.querySelector(`.mg-oracle-fields[data-db="${prefix}"]`);
                oracleFields.style.display = sel.value === 'oracle' ? '' : 'none';
            });
        });
    }

    // ----------------------------------------------------------------
    // Table management
    // ----------------------------------------------------------------
    function addTable(data) {
        const tpl = document.getElementById('tpl-table');
        const clone = tpl.content.cloneNode(true);
        const card = clone.querySelector('.mg-table-card');
        card.dataset.tableIdx = tableCounter++;

        document.getElementById('tables-container').appendChild(clone);

        if (data) {
            populateTable(card, data);
        } else {
            addField(card.querySelector('.mg-fields-header .btn-primary'));
        }
        return card;
    }

    function removeTable(btn) {
        const card = btn.closest('.mg-table-card');
        if (confirm('确定删除该表映射？')) card.remove();
    }

    function toggleTableBody(btn) {
        const body = btn.closest('.mg-table-card').querySelector('.mg-table-body');
        body.style.display = body.style.display === 'none' ? '' : 'none';
    }

    function updateTableTitle(input) {
        const card = input.closest('.mg-table-card');
        const mode = card.querySelector('.tbl-mode').value;
        let src, tgt;
        if (mode === 'single') {
            src = card.querySelector('.tbl-source-table').value || '?';
            tgt = card.querySelector('.tbl-target-table').value || '?';
        } else {
            src = 'JOIN';
            tgt = card.querySelector('.tbl-target-table-multi').value || '?';
        }
        card.querySelector('.mg-table-name').textContent = `${src} → ${tgt}`;
    }

    function onModeChange(sel) {
        const card = sel.closest('.mg-table-card');
        const isSingle = sel.value === 'single';
        card.querySelector('.mg-single-mode').style.display = isSingle ? '' : 'none';
        card.querySelector('.mg-multi-mode').style.display = isSingle ? 'none' : '';
        updateTableTitle(sel);
    }

    // ----------------------------------------------------------------
    // JOIN table management
    // ----------------------------------------------------------------
    function addJoinTable(btn, data) {
        const container = btn.closest('.mg-multi-mode').querySelector('.mg-join-tables');
        const tpl = document.getElementById('tpl-join-table');
        const clone = tpl.content.cloneNode(true);
        if (data) {
            const row = clone.querySelector('.mg-join-row');
            row.querySelector('.jt-table-name').value = data.table_name || '';
            row.querySelector('.jt-alias').value = data.alias || '';
            row.querySelector('.jt-schema').value = data.schema || '';
            row.querySelector('.jt-join-type').value = data.join_type || 'inner';
            row.querySelector('.jt-join-condition').value = data.join_condition || '';
        }
        container.appendChild(clone);
    }

    function removeJoinTable(btn) {
        btn.closest('.mg-join-row').remove();
    }

    // ----------------------------------------------------------------
    // Field management
    // ----------------------------------------------------------------
    function addField(btn, data) {
        const tbody = btn.closest('.mg-fields-section').querySelector('.mg-fields-body');
        const tpl = document.getElementById('tpl-field-row');
        const clone = tpl.content.cloneNode(true);
        const row = clone.querySelector('tr');

        if (data) {
            populateFieldRow(row, data);
        }
        tbody.appendChild(clone);
        return row;
    }

    function removeField(btn) {
        btn.closest('tr').remove();
    }

    function populateFieldRow(row, d) {
        row.querySelector('.fd-target').value = d.target_field || '';
        row.querySelector('.fd-source').value = d.source_field || '';
        row.querySelector('.fd-pk').checked = !!d.is_primary_key;
        row.querySelector('.fd-compare-rule').value = d.compare_rule || 'exact';
        row.querySelector('.fd-tolerance').value = d.tolerance != null ? d.tolerance : '';
        row.querySelector('.fd-nullable').checked = d.nullable !== false;

        if (d.transform) {
            row.querySelector('.fd-transform-type').value = d.transform.type || '';
            row.querySelector('.fd-transform-fields').value = (d.transform.fields || []).join('|');
            const params = Object.assign({}, d.transform);
            delete params.type;
            delete params.fields;
            if (Object.keys(params).length > 0) {
                row.querySelector('.fd-transform-params').value = JSON.stringify(params);
            }
        }
        // Store extra data
        row.dataset.description = d.description || '';
        row.dataset.minValue = d.min_value != null ? d.min_value : '';
        row.dataset.maxValue = d.max_value != null ? d.max_value : '';
        row.dataset.allowedValues = (d.allowed_values || []).join('|');
        row.dataset.pattern = d.pattern || '';
        row.dataset.valueCheckExpr = d.value_check_expr || '';
    }

    // ----------------------------------------------------------------
    // Field detail modal
    // ----------------------------------------------------------------
    function showFieldDetail(btn) {
        currentFieldRow = btn.closest('tr');
        const d = currentFieldRow.dataset;
        document.getElementById('fdm-description').value = d.description || '';
        document.getElementById('fdm-min-value').value = d.minValue || '';
        document.getElementById('fdm-max-value').value = d.maxValue || '';
        document.getElementById('fdm-allowed-values').value = d.allowedValues || '';
        document.getElementById('fdm-pattern').value = d.pattern || '';
        document.getElementById('fdm-value-check-expr').value = d.valueCheckExpr || '';
        document.getElementById('field-detail-modal').style.display = 'flex';
    }

    function closeFieldDetail() {
        document.getElementById('field-detail-modal').style.display = 'none';
        currentFieldRow = null;
    }

    function saveFieldDetail() {
        if (!currentFieldRow) return;
        currentFieldRow.dataset.description = document.getElementById('fdm-description').value;
        currentFieldRow.dataset.minValue = document.getElementById('fdm-min-value').value;
        currentFieldRow.dataset.maxValue = document.getElementById('fdm-max-value').value;
        currentFieldRow.dataset.allowedValues = document.getElementById('fdm-allowed-values').value;
        currentFieldRow.dataset.pattern = document.getElementById('fdm-pattern').value;
        currentFieldRow.dataset.valueCheckExpr = document.getElementById('fdm-value-check-expr').value;
        closeFieldDetail();
    }

    function onTransformChange(sel) {
        const row = sel.closest('tr');
        const sourceInput = row.querySelector('.fd-source');
        if (sel.value) {
            sourceInput.placeholder = '由转换生成';
        } else {
            sourceInput.placeholder = 'id 或 alias.field';
        }
    }

    // ----------------------------------------------------------------
    // Collect data → JSON
    // ----------------------------------------------------------------
    function collectDbConfig(prefix) {
        const type = document.getElementById(`${prefix}-type`).value;
        const cfg = { type };

        const host = document.getElementById(`${prefix}-host`).value.trim();
        const port = document.getElementById(`${prefix}-port`).value.trim();
        const database = document.getElementById(`${prefix}-database`).value.trim();
        const user = document.getElementById(`${prefix}-user`).value.trim();
        const passwordEnv = document.getElementById(`${prefix}-password-env`).value.trim();

        if (host) cfg.host = host;
        if (port) cfg.port = parseInt(port);
        if (database) cfg.database = database;
        if (user) cfg.user = user;
        if (passwordEnv) cfg.password_env = passwordEnv;

        if (type === 'oracle') {
            const sn = document.getElementById(`${prefix}-service-name`).value.trim();
            const sid = document.getElementById(`${prefix}-sid`).value.trim();
            const thick = document.getElementById(`${prefix}-thick-mode`).checked;
            const client = document.getElementById(`${prefix}-oracle-client`).value.trim();
            if (sn) cfg.service_name = sn;
            if (sid) cfg.sid = sid;
            if (thick) cfg.thick_mode = true;
            if (client) cfg.oracle_client = client;
        }
        return cfg;
    }

    function collectFieldMapping(row) {
        const fm = {};
        const target = row.querySelector('.fd-target').value.trim();
        if (!target) return null;
        fm.target_field = target;

        const transformType = row.querySelector('.fd-transform-type').value;
        const sourceField = row.querySelector('.fd-source').value.trim();

        if (transformType) {
            const transform = { type: transformType };
            const fieldsStr = row.querySelector('.fd-transform-fields').value.trim();
            if (fieldsStr) transform.fields = fieldsStr.split('|').map(f => f.trim()).filter(Boolean);
            const paramsStr = row.querySelector('.fd-transform-params').value.trim();
            if (paramsStr) {
                try {
                    Object.assign(transform, JSON.parse(paramsStr));
                } catch (e) {
                    console.warn('Invalid transform params JSON:', paramsStr);
                }
            }
            fm.transform = transform;
        } else if (sourceField) {
            fm.source_field = sourceField;
        }

        if (row.querySelector('.fd-pk').checked) fm.is_primary_key = true;

        const compareRule = row.querySelector('.fd-compare-rule').value;
        if (compareRule && compareRule !== 'exact') fm.compare_rule = compareRule;

        const tolerance = row.querySelector('.fd-tolerance').value;
        if (tolerance !== '') fm.tolerance = parseFloat(tolerance);

        if (!row.querySelector('.fd-nullable').checked) fm.nullable = false;

        // Extra fields from dataset
        const d = row.dataset;
        if (d.description) fm.description = d.description;
        if (d.minValue !== '' && d.minValue !== undefined) fm.min_value = parseFloat(d.minValue);
        if (d.maxValue !== '' && d.maxValue !== undefined) fm.max_value = parseFloat(d.maxValue);
        if (d.allowedValues) fm.allowed_values = d.allowedValues.split('|').filter(Boolean);
        if (d.pattern) fm.pattern = d.pattern;
        if (d.valueCheckExpr) fm.value_check_expr = d.valueCheckExpr;

        return fm;
    }

    function collectTableMapping(card) {
        const mode = card.querySelector('.tbl-mode').value;
        const mapping = {};

        if (mode === 'multi') {
            // Multi-table JOIN
            const joinRows = card.querySelectorAll('.mg-join-row');
            const sourceTables = [];
            joinRows.forEach(jr => {
                const st = {};
                const tn = jr.querySelector('.jt-table-name').value.trim();
                const alias = jr.querySelector('.jt-alias').value.trim();
                if (!tn || !alias) return;
                st.table_name = tn;
                st.alias = alias;
                const schema = jr.querySelector('.jt-schema').value.trim();
                if (schema) st.schema = schema;
                st.join_type = jr.querySelector('.jt-join-type').value;
                const jc = jr.querySelector('.jt-join-condition').value.trim();
                if (jc) st.join_condition = jc;
                sourceTables.push(st);
            });
            if (sourceTables.length) mapping.source_tables = sourceTables;

            mapping.target_table = card.querySelector('.tbl-target-table-multi').value.trim();
            const ts = card.querySelector('.tbl-target-schema-multi').value.trim();
            if (ts) mapping.target_schema = ts;
            const desc = card.querySelector('.tbl-description-multi').value.trim();
            if (desc) mapping.description = desc;

            // Table filters
            const filtersText = card.querySelector('.tbl-table-filters').value.trim();
            if (filtersText) {
                const tf = {};
                filtersText.split('\n').forEach(line => {
                    const sep = line.indexOf(':');
                    if (sep > 0) {
                        const alias = line.substring(0, sep).trim();
                        const cond = line.substring(sep + 1).trim();
                        if (alias && cond) tf[alias] = cond;
                    }
                });
                if (Object.keys(tf).length) mapping.table_filters = tf;
            }

            const ss = parseInt(card.querySelector('.tbl-sample-size-multi').value) || 0;
            if (ss) mapping.sample_size = ss;
            const bs = parseInt(card.querySelector('.tbl-batch-size-multi').value) || 1000;
            if (bs !== 1000) mapping.batch_size = bs;
        } else {
            // Single-table
            mapping.source_table = card.querySelector('.tbl-source-table').value.trim();
            mapping.target_table = card.querySelector('.tbl-target-table').value.trim();
            const ss_val = card.querySelector('.tbl-source-schema').value.trim();
            if (ss_val) mapping.source_schema = ss_val;
            const ts = card.querySelector('.tbl-target-schema').value.trim();
            if (ts) mapping.target_schema = ts;
            const desc = card.querySelector('.tbl-description').value.trim();
            if (desc) mapping.description = desc;

            const sf = card.querySelector('.tbl-source-filter').value.trim();
            const tf = card.querySelector('.tbl-target-filter').value.trim();
            if (sf || tf) {
                mapping.filters = {};
                if (sf) mapping.filters.source_filter = sf;
                if (tf) mapping.filters.target_filter = tf;
            }

            const ss = parseInt(card.querySelector('.tbl-sample-size').value) || 0;
            if (ss) mapping.sample_size = ss;
            const bs = parseInt(card.querySelector('.tbl-batch-size').value) || 1000;
            if (bs !== 1000) mapping.batch_size = bs;
        }

        // Field mappings
        const fieldRows = card.querySelectorAll('.mg-field-row');
        const fieldMappings = [];
        fieldRows.forEach(row => {
            const fm = collectFieldMapping(row);
            if (fm) fieldMappings.push(fm);
        });
        mapping.field_mappings = fieldMappings;

        return mapping;
    }

    function collectAll() {
        const config = { version: '1.0' };
        config.source_db = collectDbConfig('src');
        config.target_db = collectDbConfig('tgt');

        const tableCards = document.querySelectorAll('.mg-table-card');
        config.tables = [];
        tableCards.forEach(card => {
            config.tables.push(collectTableMapping(card));
        });

        // Global settings
        const gs = {};
        const workers = parseInt(document.getElementById('gs-workers').value) || 4;
        if (workers !== 4) gs.parallel_workers = workers;
        const timeout = parseInt(document.getElementById('gs-timeout').value) || 3600;
        if (timeout !== 3600) gs.timeout_seconds = timeout;
        const outputDir = document.getElementById('gs-output-dir').value.trim();
        if (outputDir && outputDir !== './validation_reports') gs.output_dir = outputDir;

        const formats = [];
        document.querySelectorAll('.gs-format:checked').forEach(cb => formats.push(cb.value));
        if (formats.length) gs.report_format = formats;

        if (Object.keys(gs).length) config.global_settings = gs;

        return config;
    }

    // ----------------------------------------------------------------
    // Generate / Preview / Download
    // ----------------------------------------------------------------
    function generateJSON() {
        lastGeneratedJSON = collectAll();
        previewJSON();
        return lastGeneratedJSON;
    }

    function previewJSON() {
        if (!lastGeneratedJSON) lastGeneratedJSON = collectAll();
        const section = document.getElementById('json-preview-section');
        const pre = document.getElementById('json-preview');
        pre.textContent = JSON.stringify(lastGeneratedJSON, null, 2);
        section.style.display = '';
        section.scrollIntoView({ behavior: 'smooth' });
    }

    function downloadJSON() {
        if (!lastGeneratedJSON) lastGeneratedJSON = collectAll();
        const blob = new Blob([JSON.stringify(lastGeneratedJSON, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'mapping_config.json';
        a.click();
        URL.revokeObjectURL(url);
    }

    function copyJSON() {
        const text = document.getElementById('json-preview').textContent;
        navigator.clipboard.writeText(text).then(() => {
            const btn = document.querySelector('#json-preview-section .btn-sm');
            const orig = btn.textContent;
            btn.textContent = '已复制';
            setTimeout(() => btn.textContent = orig, 1500);
        });
    }

    // ----------------------------------------------------------------
    // Load from existing JSON
    // ----------------------------------------------------------------
    function loadFromJSON() {
        const input = document.getElementById('json-file');
        input.onchange = () => {
            const file = input.files[0];
            if (!file) return;
            const reader = new FileReader();
            reader.onload = e => {
                try {
                    const config = JSON.parse(e.target.result);
                    populateFromConfig(config);
                } catch (err) {
                    alert('JSON 解析失败: ' + err.message);
                }
                input.value = '';
            };
            reader.readAsText(file);
        };
        input.click();
    }

    function populateFromConfig(config) {
        // Clear existing tables
        document.getElementById('tables-container').innerHTML = '';
        tableCounter = 0;

        // DB config
        if (config.source_db) populateDbConfig('src', config.source_db);
        if (config.target_db) populateDbConfig('tgt', config.target_db);

        // Tables
        (config.tables || []).forEach(tbl => addTable(tbl));

        // Global settings
        if (config.global_settings) {
            const gs = config.global_settings;
            if (gs.parallel_workers) document.getElementById('gs-workers').value = gs.parallel_workers;
            if (gs.timeout_seconds) document.getElementById('gs-timeout').value = gs.timeout_seconds;
            if (gs.output_dir) document.getElementById('gs-output-dir').value = gs.output_dir;
            if (gs.report_format) {
                document.querySelectorAll('.gs-format').forEach(cb => {
                    cb.checked = gs.report_format.includes(cb.value);
                });
            }
        }
    }

    function populateDbConfig(prefix, db) {
        document.getElementById(`${prefix}-type`).value = db.type || 'mysql';
        document.getElementById(`${prefix}-host`).value = db.host || '';
        document.getElementById(`${prefix}-port`).value = db.port || '';
        document.getElementById(`${prefix}-database`).value = db.database || '';
        document.getElementById(`${prefix}-user`).value = db.user || '';
        document.getElementById(`${prefix}-password-env`).value = db.password_env || '';

        // Trigger oracle fields visibility
        const oracleFields = document.querySelector(`.mg-oracle-fields[data-db="${prefix}"]`);
        if (db.type === 'oracle') {
            oracleFields.style.display = '';
            document.getElementById(`${prefix}-service-name`).value = db.service_name || '';
            document.getElementById(`${prefix}-sid`).value = db.sid || '';
            document.getElementById(`${prefix}-thick-mode`).checked = !!db.thick_mode;
            document.getElementById(`${prefix}-oracle-client`).value = db.oracle_client || '';
        } else {
            oracleFields.style.display = 'none';
        }
    }

    function populateTable(card, tbl) {
        const isMulti = !!tbl.source_tables;
        const modeSel = card.querySelector('.tbl-mode');
        modeSel.value = isMulti ? 'multi' : 'single';
        onModeChange(modeSel);

        if (isMulti) {
            card.querySelector('.tbl-target-table-multi').value = tbl.target_table || '';
            card.querySelector('.tbl-target-schema-multi').value = tbl.target_schema || '';
            card.querySelector('.tbl-description-multi').value = tbl.description || '';
            card.querySelector('.tbl-sample-size-multi').value = tbl.sample_size || 0;
            card.querySelector('.tbl-batch-size-multi').value = tbl.batch_size || 1000;

            // JOIN tables
            const addBtn = card.querySelector('.mg-multi-mode h4 .btn');
            (tbl.source_tables || []).forEach(st => addJoinTable(addBtn, st));

            // Table filters
            if (tbl.table_filters) {
                const lines = Object.entries(tbl.table_filters).map(([k, v]) => `${k}: ${v}`).join('\n');
                card.querySelector('.tbl-table-filters').value = lines;
            }
        } else {
            card.querySelector('.tbl-source-table').value = tbl.source_table || '';
            card.querySelector('.tbl-target-table').value = tbl.target_table || '';
            card.querySelector('.tbl-source-schema').value = tbl.source_schema || '';
            card.querySelector('.tbl-target-schema').value = tbl.target_schema || '';
            card.querySelector('.tbl-description').value = tbl.description || '';
            card.querySelector('.tbl-source-filter').value = (tbl.filters && tbl.filters.source_filter) || '';
            card.querySelector('.tbl-target-filter').value = (tbl.filters && tbl.filters.target_filter) || '';
            card.querySelector('.tbl-sample-size').value = tbl.sample_size || 0;
            card.querySelector('.tbl-batch-size').value = tbl.batch_size || 1000;
        }

        // Field mappings
        const addFieldBtn = card.querySelector('.mg-fields-header .btn-primary');
        (tbl.field_mappings || []).forEach(fm => addField(addFieldBtn, fm));

        // Update title
        updateTableTitle(card.querySelector(isMulti ? '.tbl-target-table-multi' : '.tbl-source-table'));
    }

    // ----------------------------------------------------------------
    // CSV import
    // ----------------------------------------------------------------
    function importCSV() {
        const fileInput = document.getElementById('csv-file');
        const errDiv = document.getElementById('csv-error');
        errDiv.style.display = 'none';

        const file = fileInput.files[0];
        if (!file) {
            errDiv.textContent = '请先选择 CSV 文件';
            errDiv.style.display = '';
            return;
        }

        const reader = new FileReader();
        reader.onload = e => {
            try {
                const rows = parseCSV(e.target.result);
                if (rows.length < 2) throw new Error('CSV 至少需要表头和一行数据');
                buildFromCSVRows(rows);
            } catch (err) {
                errDiv.textContent = 'CSV 解析错误: ' + err.message;
                errDiv.style.display = '';
            }
        };
        reader.readAsText(file);
    }

    function parseCSV(text) {
        const rows = [];
        let current = [];
        let field = '';
        let inQuotes = false;
        const chars = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n');

        for (let i = 0; i < chars.length; i++) {
            const ch = chars[i];
            if (inQuotes) {
                if (ch === '"') {
                    if (i + 1 < chars.length && chars[i + 1] === '"') {
                        field += '"';
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    field += ch;
                }
            } else {
                if (ch === '"') {
                    inQuotes = true;
                } else if (ch === ',') {
                    current.push(field);
                    field = '';
                } else if (ch === '\n') {
                    current.push(field);
                    field = '';
                    rows.push(current);
                    current = [];
                } else {
                    field += ch;
                }
            }
        }
        // Last field/row
        current.push(field);
        if (current.some(f => f.trim())) rows.push(current);
        return rows;
    }

    function buildFromCSVRows(rows) {
        const headers = rows[0].map(h => h.trim().toLowerCase());
        const col = name => headers.indexOf(name);

        // Group by source_table+target_table or join_tables+target_table
        const tableGroups = [];
        let currentGroup = null;

        for (let i = 1; i < rows.length; i++) {
            const r = rows[i];
            const get = name => {
                const idx = col(name);
                return idx >= 0 && idx < r.length ? r[idx].trim() : '';
            };

            const srcTable = get('source_table');
            const tgtTable = get('target_table');
            const joinTables = get('join_tables');

            // Start new group if source_table or target_table present
            if (srcTable || tgtTable || joinTables) {
                currentGroup = {
                    source_table: srcTable,
                    target_table: tgtTable,
                    join_tables: joinTables,
                    source_schema: get('source_schema'),
                    target_schema: get('target_schema'),
                    source_filter: get('source_filter'),
                    target_filter: get('target_filter'),
                    table_filters: get('table_filters'),
                    sample_size: get('sample_size'),
                    batch_size: get('batch_size'),
                    description: get('description') && !get('target_field') ? get('description') : '',
                    fields: []
                };
                tableGroups.push(currentGroup);
            }
            if (!currentGroup) continue;

            const targetField = get('target_field');
            if (!targetField) continue;

            const fieldData = {
                target_field: targetField,
                source_field: get('source_field'),
                is_primary_key: get('is_primary_key').toLowerCase() === 'true',
                transform_type: get('transform_type'),
                transform_fields: get('transform_fields'),
                transform_params: get('transform_params'),
                compare_rule: get('compare_rule') || 'exact',
                tolerance: get('tolerance'),
                nullable: get('nullable') !== 'false',
                description: get('description'),
                min_value: get('min_value'),
                max_value: get('max_value'),
                allowed_values: get('allowed_values'),
                pattern: get('pattern'),
                value_check_expr: get('value_check_expr')
            };
            currentGroup.fields.push(fieldData);
        }

        // Clear existing and build
        document.getElementById('tables-container').innerHTML = '';
        tableCounter = 0;

        tableGroups.forEach(g => {
            const tblData = {};

            if (g.join_tables) {
                try {
                    tblData.source_tables = JSON.parse(g.join_tables);
                } catch (e) {
                    console.warn('Invalid join_tables JSON:', g.join_tables);
                }
                if (g.table_filters) {
                    try { tblData.table_filters = JSON.parse(g.table_filters); } catch (e) {}
                }
            } else {
                tblData.source_table = g.source_table;
            }
            tblData.target_table = g.target_table;
            if (g.source_schema) tblData.source_schema = g.source_schema;
            if (g.target_schema) tblData.target_schema = g.target_schema;
            if (g.description) tblData.description = g.description;
            if (g.source_filter || g.target_filter) {
                tblData.filters = {};
                if (g.source_filter) tblData.filters.source_filter = g.source_filter;
                if (g.target_filter) tblData.filters.target_filter = g.target_filter;
            }
            if (g.sample_size) tblData.sample_size = parseInt(g.sample_size) || 0;
            if (g.batch_size) tblData.batch_size = parseInt(g.batch_size) || 1000;

            // Convert fields
            tblData.field_mappings = g.fields.map(f => {
                const fm = { target_field: f.target_field };

                if (f.transform_type) {
                    const transform = { type: f.transform_type };
                    if (f.transform_fields) {
                        transform.fields = f.transform_fields.split('|').map(s => s.trim()).filter(Boolean);
                    }
                    if (f.transform_params) {
                        try { Object.assign(transform, JSON.parse(f.transform_params)); } catch (e) {}
                    }
                    fm.transform = transform;
                } else if (f.source_field) {
                    fm.source_field = f.source_field;
                }

                if (f.is_primary_key) fm.is_primary_key = true;
                if (f.compare_rule && f.compare_rule !== 'exact') fm.compare_rule = f.compare_rule;
                if (f.tolerance) fm.tolerance = parseFloat(f.tolerance);
                if (f.nullable === false) fm.nullable = false;
                if (f.description) fm.description = f.description;
                if (f.min_value) fm.min_value = parseFloat(f.min_value);
                if (f.max_value) fm.max_value = parseFloat(f.max_value);
                if (f.allowed_values) fm.allowed_values = f.allowed_values.split('|').filter(Boolean);
                if (f.pattern) fm.pattern = f.pattern;
                if (f.value_check_expr) fm.value_check_expr = f.value_check_expr;

                return fm;
            });

            addTable(tblData);
        });
    }

    // ----------------------------------------------------------------
    // Init
    // ----------------------------------------------------------------
    function init() {
        initDbTypeListeners();
    }

    document.addEventListener('DOMContentLoaded', init);

    return {
        toggleSection,
        addTable,
        removeTable,
        toggleTableBody,
        updateTableTitle,
        onModeChange,
        addJoinTable,
        removeJoinTable,
        addField,
        removeField,
        showFieldDetail,
        closeFieldDetail,
        saveFieldDetail,
        onTransformChange,
        generateJSON,
        previewJSON,
        downloadJSON,
        copyJSON,
        loadFromJSON,
        importCSV
    };
})();
