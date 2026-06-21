(function () {
  'use strict';

  var shared = window.AdminManageShared;
  var fetchJSON = shared.fetchJSON;
  var setDialogOpenState = shared.setDialogOpenState;
  var setElementDisabled = shared.setElementDisabled;
  var setPageInteractionState = shared.setPageInteractionState;
  var trapFocusWithin = shared.trapFocusWithin;

  var STORAGE_KEY = 'adminCloneRunsManageState';
  var MAPPING_PAGE_SIZE = 25;

  var state = {
    items: [],
    selectedRunId: '',
    selectedDetail: null,
    offset: 0,
    limit: 20,
    total: 0,
    mappingItems: [],
    mappingOffset: 0,
    mappingLimit: MAPPING_PAGE_SIZE,
    mappingTotal: 0,
    mappingLoading: false,
    busy: false,
    deleteConfirm: ''
  };

  var authState = {
    authenticated: false,
    logoutTimer: null
  };

  document.addEventListener('DOMContentLoaded', async function () {
    var elements = getElements();
    if (!elements) return;
    initializeUI(elements);
    bindEvents(elements);
    await checkAuth(elements);
  });

  function getElements() {
    var elements = {
      page: document.getElementById('admin-clone-runs-page'),
      status: document.getElementById('admin-clone-runs-manage-status'),
      refreshBtn: document.getElementById('admin-clone-runs-manage-refresh-btn'),
      queryInput: document.getElementById('admin-clone-runs-query-input'),
      statusFilter: document.getElementById('admin-clone-runs-status-filter'),
      sortSelect: document.getElementById('admin-clone-runs-sort-select'),
      limitSelect: document.getElementById('admin-clone-runs-limit-select'),
      pageStatus: document.getElementById('admin-clone-runs-page-status'),
      prevBtn: document.getElementById('admin-clone-runs-prev-btn'),
      nextBtn: document.getElementById('admin-clone-runs-next-btn'),
      list: document.getElementById('admin-clone-runs-manage-list'),
      detailStatus: document.getElementById('admin-clone-runs-detail-status'),
      detailRefreshBtn: document.getElementById('admin-clone-runs-detail-refresh-btn'),
      deleteBtn: document.getElementById('admin-clone-runs-delete-btn'),
      detailSummary: document.getElementById('admin-clone-runs-detail-summary'),
      progressSummary: document.getElementById('admin-clone-runs-progress-summary'),
      failureList: document.getElementById('admin-clone-runs-failure-list'),
      mappingStatus: document.getElementById('admin-clone-runs-mapping-status'),
      mappingStatusFilter: document.getElementById('admin-clone-runs-mapping-status-filter'),
      mappingModeFilter: document.getElementById('admin-clone-runs-mapping-mode-filter'),
      mappingPrevBtn: document.getElementById('admin-clone-runs-mapping-prev-btn'),
      mappingNextBtn: document.getElementById('admin-clone-runs-mapping-next-btn'),
      mappingList: document.getElementById('admin-clone-runs-mapping-list'),
      deleteDialog: document.getElementById('admin-clone-run-delete-dialog'),
      deleteStatus: document.getElementById('admin-clone-run-delete-status'),
      deleteConfirmInput: document.getElementById('admin-clone-run-delete-confirm-input'),
      deleteConfirmHint: document.getElementById('admin-clone-run-delete-confirm-hint'),
      deleteCancelBtn: document.getElementById('admin-clone-run-delete-cancel-btn'),
      deleteConfirmBtn: document.getElementById('admin-clone-run-delete-confirm-btn'),
      loginDialog: document.getElementById('admin-login-dialog'),
      loginStatus: document.getElementById('admin-login-status'),
      passwordInput: document.getElementById('admin-password-input'),
      loginConfirmBtn: document.getElementById('admin-login-confirm-btn')
    };
    var requiredKeys = [
      'page',
      'status',
      'refreshBtn',
      'queryInput',
      'statusFilter',
      'sortSelect',
      'limitSelect',
      'pageStatus',
      'prevBtn',
      'nextBtn',
      'list',
      'detailStatus',
      'detailRefreshBtn',
      'deleteBtn',
      'detailSummary',
      'progressSummary',
      'failureList',
      'mappingStatus',
      'mappingStatusFilter',
      'mappingModeFilter',
      'mappingPrevBtn',
      'mappingNextBtn',
      'mappingList',
      'deleteDialog',
      'deleteStatus',
      'deleteConfirmInput',
      'deleteConfirmHint',
      'deleteCancelBtn',
      'deleteConfirmBtn',
      'loginDialog',
      'loginStatus',
      'passwordInput',
      'loginConfirmBtn'
    ];
    var missing = requiredKeys.filter(function (key) { return !elements[key]; });
    if (missing.length > 0) {
      console.warn('[admin_clone_runs] Missing required elements:', missing.join(', '));
      return null;
    }
    return elements;
  }

  function initializeUI(elements) {
    restorePersistentState(elements);
    renderRuns(elements);
    renderDetail(elements, null);
    setBusy(elements, false);
    closeDeleteDialog(elements, { skipFocusRestore: true });
  }

  function bindEvents(elements) {
    elements.loginConfirmBtn.addEventListener('click', function () {
      handleLogin(elements);
    });
    elements.passwordInput.addEventListener('keydown', function (event) {
      if (event.key === 'Enter') {
        event.preventDefault();
        handleLogin(elements);
      }
    });
    elements.refreshBtn.addEventListener('click', function () {
      loadRuns(elements, { resetOffset: false });
    });
    elements.queryInput.addEventListener('input', debounce(function () {
      state.offset = 0;
      persistState(elements);
      loadRuns(elements, { resetOffset: true });
    }, 250));
    elements.statusFilter.addEventListener('change', function () {
      state.offset = 0;
      persistState(elements);
      loadRuns(elements, { resetOffset: true });
    });
    elements.sortSelect.addEventListener('change', function () {
      state.offset = 0;
      persistState(elements);
      loadRuns(elements, { resetOffset: true });
    });
    elements.limitSelect.addEventListener('change', function () {
      state.limit = Number(elements.limitSelect.value || 20) || 20;
      state.offset = 0;
      persistState(elements);
      loadRuns(elements, { resetOffset: true });
    });
    elements.prevBtn.addEventListener('click', function () {
      state.offset = Math.max(0, state.offset - state.limit);
      persistState(elements);
      loadRuns(elements, { resetOffset: false });
    });
    elements.nextBtn.addEventListener('click', function () {
      if (state.offset + state.limit >= state.total) return;
      state.offset += state.limit;
      persistState(elements);
      loadRuns(elements, { resetOffset: false });
    });
    elements.detailRefreshBtn.addEventListener('click', function () {
      loadSelectedDetail(elements);
    });
    elements.mappingStatusFilter.addEventListener('change', function () {
      state.mappingOffset = 0;
      persistState(elements);
      loadSelectedMappings(elements, { resetOffset: true });
    });
    elements.mappingModeFilter.addEventListener('change', function () {
      state.mappingOffset = 0;
      persistState(elements);
      loadSelectedMappings(elements, { resetOffset: true });
    });
    elements.mappingPrevBtn.addEventListener('click', function () {
      state.mappingOffset = Math.max(0, state.mappingOffset - state.mappingLimit);
      persistState(elements);
      loadSelectedMappings(elements, { resetOffset: false });
    });
    elements.mappingNextBtn.addEventListener('click', function () {
      if (state.mappingOffset + state.mappingLimit >= state.mappingTotal) return;
      state.mappingOffset += state.mappingLimit;
      persistState(elements);
      loadSelectedMappings(elements, { resetOffset: false });
    });
    elements.deleteBtn.addEventListener('click', function () {
      openDeleteDialog(elements);
    });
    elements.deleteCancelBtn.addEventListener('click', function () {
      closeDeleteDialog(elements);
    });
    elements.deleteConfirmInput.addEventListener('input', function () {
      syncDeleteConfirmButton(elements);
    });
    elements.deleteConfirmInput.addEventListener('keydown', function (event) {
      if (event.key === 'Enter') {
        event.preventDefault();
        handleDeleteConfirm(elements);
      }
    });
    elements.deleteConfirmBtn.addEventListener('click', function () {
      handleDeleteConfirm(elements);
    });
    document.addEventListener('keydown', function (event) {
      if (!elements.loginDialog.hidden) {
        if (event.key === 'Tab') trapFocusWithin(elements.loginDialog, event);
        return;
      }
      if (!elements.deleteDialog.hidden && event.key === 'Tab') {
        trapFocusWithin(elements.deleteDialog, event);
      }
    });
  }

  async function checkAuth(elements) {
    try {
      var data = await fetchJSON('/api/admin/auth/check');
      if (data.authenticated) {
        authState.authenticated = true;
        closeLoginDialog(elements);
        setupAutoLogout(elements, data.remaining);
        await loadRuns(elements, { resetOffset: false });
        return;
      }
      openLoginDialog(elements);
    } catch (_error) {
      openLoginDialog(elements);
    }
  }

  async function handleLogin(elements) {
    var password = elements.passwordInput.value;
    if (!password) {
      elements.loginStatus.textContent = '请输入管理员密码。';
      elements.passwordInput.focus();
      return;
    }
    setElementDisabled(elements.loginConfirmBtn, true);
    elements.loginStatus.textContent = '';
    try {
      var data = await fetchJSON('/api/admin/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: password })
      });
      if (data.ok) {
        authState.authenticated = true;
        elements.passwordInput.value = '';
        closeLoginDialog(elements);
        setupAutoLogout(elements, data.expiry_duration);
        await loadRuns(elements, { resetOffset: false });
      }
    } catch (error) {
      elements.loginStatus.textContent = '认证失败：' + error.message;
      elements.passwordInput.focus();
    } finally {
      setElementDisabled(elements.loginConfirmBtn, false);
    }
  }

  function setupAutoLogout(elements, seconds) {
    if (authState.logoutTimer) window.clearTimeout(authState.logoutTimer);
    if (seconds <= 0) return;
    authState.logoutTimer = window.setTimeout(function () {
      authState.authenticated = false;
      elements.loginStatus.textContent = '会话已过期，请重新登录。';
      openLoginDialog(elements);
    }, seconds * 1000);
  }

  function openLoginDialog(elements) {
    setDialogOpenState(elements.loginDialog, true, {
      focusElement: elements.passwordInput
    });
    setPageInteractionState(elements.page, false);
  }

  function closeLoginDialog(elements) {
    elements.loginStatus.textContent = '';
    setDialogOpenState(elements.loginDialog, false, { skipFocusRestore: true });
    setPageInteractionState(elements.page, true);
  }

  async function loadRuns(elements, options) {
    var opts = options || {};
    if (opts.resetOffset) state.offset = 0;
    state.limit = Number(elements.limitSelect.value || 20) || 20;
    setBusy(elements, true);
    elements.status.textContent = '正在读取克隆记录...';
    try {
      var params = new URLSearchParams();
      params.set('limit', String(state.limit));
      params.set('offset', String(state.offset));
      params.set('sort', String(elements.sortSelect.value || 'updated_desc'));
      var status = String(elements.statusFilter.value || '').trim();
      var query = String(elements.queryInput.value || '').trim();
      if (status) params.set('status', status);
      if (query) params.set('q', query);
      var payload = await fetchJSON('/api/admin/clone/runs?' + params.toString());
      state.items = Array.isArray(payload.items) ? payload.items : [];
      state.total = Number(payload.total || 0) || 0;
      if (state.offset >= state.total && state.total > 0) {
        state.offset = Math.max(0, Math.floor((state.total - 1) / state.limit) * state.limit);
        persistState(elements);
        await loadRuns(elements, { resetOffset: false });
        return;
      }
      syncSelectedRunAfterList();
      renderRuns(elements);
      updatePageStatus(elements);
      elements.status.textContent = state.total > 0
        ? '共 ' + formatNumber(state.total) + ' 条克隆记录。'
        : '暂无匹配的克隆记录。';
      persistState(elements);
      await loadSelectedDetail(elements);
    } catch (error) {
      state.items = [];
      state.total = 0;
      state.selectedRunId = '';
      state.selectedDetail = null;
      clearSelectedRun(elements);
      renderRuns(elements);
      renderDetail(elements, null);
      updatePageStatus(elements);
      elements.status.textContent = '读取克隆记录失败：' + error.message;
    } finally {
      setBusy(elements, false);
    }
  }

  function syncSelectedRunAfterList() {
    if (state.selectedRunId) return;
    state.selectedRunId = state.items.length ? String(state.items[0].run_id || '') : '';
  }

  function findRun(runId) {
    var normalized = String(runId || '').trim();
    if (!normalized) return null;
    for (var i = 0; i < state.items.length; i += 1) {
      if (String(state.items[i].run_id || '') === normalized) return state.items[i];
    }
    return null;
  }

  async function loadSelectedDetail(elements) {
    var runId = String(state.selectedRunId || '').trim();
    if (!runId) {
      state.selectedDetail = null;
      state.deleteConfirm = '';
      state.mappingItems = [];
      state.mappingOffset = 0;
      state.mappingTotal = 0;
      renderDetail(elements, null);
      updateMappingStatus(elements);
      return;
    }
    elements.detailStatus.textContent = '正在读取记录详情...';
    setElementDisabled(elements.detailRefreshBtn, true);
    try {
      var payload = await fetchJSON(
        '/api/admin/clone/runs/' + encodeURIComponent(runId) + '/detail'
      );
      state.selectedDetail = payload || null;
      state.deleteConfirm = String((payload && payload.delete_confirm) || '');
      renderDetail(elements, state.selectedDetail);
      elements.detailStatus.textContent = '已读取记录详情。';
      await loadSelectedMappings(elements, { resetOffset: false });
    } catch (error) {
      state.selectedDetail = null;
      state.deleteConfirm = '';
      renderDetail(elements, null);
      renderMappingList(elements.mappingList, []);
      updateMappingStatus(elements, '读取映射前请先恢复有效记录详情。');
      elements.detailStatus.textContent = '读取详情失败：' + error.message;
      if (String(error.message || '').indexOf('不存在') !== -1) {
        clearSelectedRun(elements);
        syncSelectedRunAfterList();
        renderRuns(elements);
        if (state.selectedRunId) {
          await loadSelectedDetail(elements);
          return;
        }
      }
    } finally {
      setElementDisabled(elements.detailRefreshBtn, state.busy || !state.selectedRunId);
      syncDeleteButton(elements);
    }
  }

  async function loadSelectedMappings(elements, options) {
    var opts = options || {};
    var runId = String(state.selectedRunId || '').trim();
    if (opts.resetOffset) state.mappingOffset = 0;
    if (!runId) {
      state.mappingItems = [];
      state.mappingTotal = 0;
      renderMappingList(elements.mappingList, []);
      updateMappingStatus(elements);
      syncMappingControls(elements);
      return;
    }

    state.mappingLoading = true;
    syncMappingControls(elements);
    elements.mappingStatus.textContent = '正在读取消息映射...';
    try {
      var params = new URLSearchParams();
      params.set('limit', String(state.mappingLimit));
      params.set('offset', String(state.mappingOffset));
      var status = String(elements.mappingStatusFilter.value || '').trim();
      var mode = String(elements.mappingModeFilter.value || '').trim();
      if (status) params.set('status', status);
      if (mode) params.set('mode', mode);
      var payload = await fetchJSON(
        '/api/admin/clone/runs/' + encodeURIComponent(runId) + '/messages?' + params.toString()
      );
      state.mappingItems = Array.isArray(payload.items) ? payload.items : [];
      state.mappingTotal = Number(payload.total || 0) || 0;
      if (state.mappingOffset >= state.mappingTotal && state.mappingTotal > 0) {
        state.mappingOffset = Math.max(
          0,
          Math.floor((state.mappingTotal - 1) / state.mappingLimit) * state.mappingLimit
        );
        persistState(elements);
        await loadSelectedMappings(elements, { resetOffset: false });
        return;
      }
      renderMappingList(elements.mappingList, state.mappingItems);
      updateMappingStatus(elements);
      persistState(elements);
    } catch (error) {
      state.mappingItems = [];
      state.mappingTotal = 0;
      renderMappingList(elements.mappingList, []);
      elements.mappingStatus.textContent = '读取消息映射失败：' + error.message;
    } finally {
      state.mappingLoading = false;
      syncMappingControls(elements);
    }
  }

  function renderRuns(elements) {
    elements.list.textContent = '';
    if (!state.items.length) {
      var empty = document.createElement('p');
      empty.className = 'clone-run-empty';
      empty.textContent = '暂无克隆记录';
      elements.list.appendChild(empty);
      syncRunButtons(elements);
      return;
    }
    state.items.forEach(function (run) {
      elements.list.appendChild(createRunCard(elements, run));
    });
    syncRunButtons(elements);
  }

  function createRunCard(elements, run) {
    var card = document.createElement('article');
    var head = document.createElement('div');
    var title = document.createElement('h3');
    var status = document.createElement('span');
    var meta = document.createElement('div');
    var actions = document.createElement('div');
    var runId = String((run && run.run_id) || '');
    var normalizedStatus = String((run && run.status) || '').toLowerCase();
    var selected = runId && runId === state.selectedRunId;

    card.className = 'clone-run-card is-' + (normalizedStatus || 'unknown');
    if (selected) card.className += ' is-selected';
    head.className = 'clone-run-head';
    title.className = 'clone-run-title';
    status.className = 'clone-run-status is-' + (normalizedStatus || 'unknown');
    meta.className = 'clone-run-meta';
    actions.className = 'clone-run-actions';

    title.textContent = buildRunTitle(run);
    status.textContent = getRunStatusLabel(normalizedStatus);
    head.appendChild(title);
    head.appendChild(status);
    card.appendChild(head);

    appendRunPill(meta, '源消息数', formatNumber(run && run.source_message_count));
    appendRunPill(meta, '最后消息', formatDateTime(run && run.source_last_message_at));
    appendRunPill(meta, '目标 ID', run && run.target_chat_id ? String(run.target_chat_id) : '未创建');
    appendRunPill(meta, '更新时间', formatDateTime(run && run.updated_at));
    card.appendChild(meta);

    appendRunLink(actions, '打开源群', run && run.source_telegram_app_link);
    appendRunLink(actions, '打开目标', run && run.target_telegram_app_link);
    appendSelectButton(elements, actions, runId, selected);
    card.appendChild(actions);
    return card;
  }

  function appendRunPill(container, label, value) {
    var pill = document.createElement('div');
    var labelNode = document.createElement('span');
    var valueNode = document.createElement('strong');
    pill.className = 'clone-run-pill';
    labelNode.textContent = String(label || '');
    valueNode.textContent = String(value || '暂无');
    pill.appendChild(labelNode);
    pill.appendChild(valueNode);
    container.appendChild(pill);
  }

  function appendRunLink(container, label, href) {
    var normalizedHref = String(href || '').trim();
    if (!normalizedHref) return;
    var link = document.createElement('a');
    link.href = normalizedHref;
    link.textContent = String(label || '');
    link.rel = 'noopener noreferrer';
    container.appendChild(link);
  }

  function appendSelectButton(elements, container, runId, selected) {
    var button = document.createElement('button');
    button.type = 'button';
    button.className = selected ? 'btn clone-run-select is-selected' : 'btn clone-run-select';
    button.textContent = selected ? '已选择' : '查看详情';
    button.disabled = selected || state.busy || !runId;
    button.addEventListener('click', function () {
      selectRun(elements, runId);
      renderRuns(elements);
      loadSelectedDetail(elements);
    });
    container.appendChild(button);
  }

  function renderDetail(elements, payload) {
    var run = payload && payload.run ? payload.run : null;
    var migration = payload && payload.migration ? payload.migration : null;
    var preview = payload && payload.timeline_preview ? payload.timeline_preview : null;
    var summary = payload && payload.mapping_summary ? payload.mapping_summary : null;
    var failures = payload && Array.isArray(payload.failure_items) ? payload.failure_items : [];

    elements.detailSummary.textContent = '';
    elements.progressSummary.textContent = '';
    elements.failureList.textContent = '';

    if (!run) {
      state.deleteConfirm = '';
      elements.detailStatus.textContent = '请选择一条克隆记录。';
      appendSummaryPair(elements.detailSummary, '状态', '未选择');
      appendSummaryPair(elements.detailSummary, '目标', '未选择');
      appendMiniPair(elements.progressSummary, '时间线剩余', '0');
      renderFailureList(elements.failureList, []);
      renderMappingList(elements.mappingList, []);
      updateMappingStatus(elements);
      syncDeleteButton(elements);
      return;
    }

    appendSummaryPair(elements.detailSummary, '源群', run.source_title || run.source_chat_id);
    appendSummaryPair(elements.detailSummary, '源群 ID', run.source_chat_id);
    appendSummaryPair(elements.detailSummary, '源消息数', formatNumber(run.source_message_count));
    appendSummaryPair(elements.detailSummary, '源最后消息', formatDateTime(run.source_last_message_at));
    appendSummaryPair(elements.detailSummary, '目标', run.target_title || '未创建');
    appendSummaryPair(elements.detailSummary, '目标 ID', run.target_chat_id || '未创建');
    appendSummaryPair(elements.detailSummary, '状态', getRunStatusLabel(run.status));
    appendSummaryPair(elements.detailSummary, '更新时间', formatDateTime(run.updated_at));

    appendMiniPair(elements.progressSummary, '迁移状态', getMigrationStatusLabel(migration && migration.status));
    appendMiniPair(elements.progressSummary, '时间线总数', formatNumber(preview && preview.timeline_items_total));
    appendMiniPair(elements.progressSummary, '剩余时间线', formatNumber(preview && preview.timeline_remaining));
    appendMiniPair(elements.progressSummary, '文本已发', formatDoneTotal(migration && migration.text_sent, migration && migration.text_total));
    appendMiniPair(elements.progressSummary, '媒体已复制', formatDoneTotal(migration && migration.media_sent, migration && migration.media_total));
    appendMiniPair(elements.progressSummary, '映射总数', formatNumber(summary && summary.total));
    appendMiniPair(elements.progressSummary, '映射失败', formatNumber(summary && summary.error));

    renderFailureList(elements.failureList, failures);
    syncDeleteButton(elements);
  }

  function appendSummaryPair(container, label, value) {
    var wrap = document.createElement('div');
    var dt = document.createElement('dt');
    var dd = document.createElement('dd');
    dt.textContent = String(label || '');
    dd.textContent = String(value || '暂无');
    wrap.appendChild(dt);
    wrap.appendChild(dd);
    container.appendChild(wrap);
  }

  function appendMiniPair(container, label, value) {
    var wrap = document.createElement('div');
    var dt = document.createElement('dt');
    var dd = document.createElement('dd');
    dt.textContent = String(label || '');
    dd.textContent = String(value || '暂无');
    wrap.appendChild(dt);
    wrap.appendChild(dd);
    container.appendChild(wrap);
  }

  function renderFailureList(container, items) {
    container.textContent = '';
    if (!items.length) {
      var empty = document.createElement('li');
      empty.textContent = '暂无';
      container.appendChild(empty);
      return;
    }
    items.slice(0, 8).forEach(function (item) {
      var node = document.createElement('li');
      node.textContent = 'source_message_id='
        + String(item.source_message_id || '')
        + '，mode='
        + String(item.mode || '')
        + '，'
        + String(item.error_message || '未知错误');
      container.appendChild(node);
    });
  }

  function renderMappingList(container, items) {
    container.textContent = '';
    if (!items.length) {
      var empty = document.createElement('p');
      empty.className = 'clone-run-empty';
      empty.textContent = '暂无消息映射';
      container.appendChild(empty);
      return;
    }
    items.forEach(function (item) {
      var row = document.createElement('div');
      row.className = 'clone-mapping-row is-' + String(item.status || 'unknown');
      appendMappingCell(row, '源', item.source_message_id);
      appendMappingCell(row, '目标', item.target_message_id || '暂无');
      appendMappingCell(row, '模式', getMappingModeLabel(item.mode));
      appendMappingCell(row, '状态', getMappingStatusLabel(item.status));
      appendMappingCell(row, '更新时间', formatDateTime(item.updated_at));
      container.appendChild(row);
    });
  }

  function appendMappingCell(container, label, value) {
    var cell = document.createElement('div');
    var labelNode = document.createElement('span');
    var valueNode = document.createElement('strong');
    labelNode.textContent = String(label || '');
    valueNode.textContent = String(value || '暂无');
    cell.appendChild(labelNode);
    cell.appendChild(valueNode);
    container.appendChild(cell);
  }

  function openDeleteDialog(elements) {
    if (!state.selectedRunId || !state.deleteConfirm) return;
    elements.deleteStatus.textContent = '';
    elements.deleteConfirmInput.value = '';
    elements.deleteConfirmHint.textContent = '请输入确认码：' + state.deleteConfirm;
    syncDeleteConfirmButton(elements);
    setDialogOpenState(elements.deleteDialog, true, {
      focusElement: elements.deleteConfirmInput
    });
    setPageInteractionState(elements.page, false);
  }

  function closeDeleteDialog(elements, options) {
    setDialogOpenState(elements.deleteDialog, false, options || {});
    setPageInteractionState(elements.page, true);
  }

  async function handleDeleteConfirm(elements) {
    if (!state.selectedRunId || !state.deleteConfirm) return;
    var confirmText = String(elements.deleteConfirmInput.value || '').trim();
    if (confirmText !== state.deleteConfirm) {
      elements.deleteStatus.textContent = '确认码不匹配。';
      elements.deleteConfirmInput.focus();
      return;
    }
    setElementDisabled(elements.deleteConfirmBtn, true);
    elements.deleteStatus.textContent = '正在删除本地记录...';
    try {
      await fetchJSON(
        '/api/admin/clone/runs/' + encodeURIComponent(state.selectedRunId),
        {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ confirm: confirmText })
        }
      );
      closeDeleteDialog(elements);
      clearSelectedRun(elements);
      state.selectedDetail = null;
      state.mappingItems = [];
      state.mappingOffset = 0;
      state.mappingTotal = 0;
      await loadRuns(elements, { resetOffset: true });
      elements.status.textContent = '本地克隆记录已删除，Telegram 目标副本未删除。';
    } catch (error) {
      elements.deleteStatus.textContent = '删除失败：' + error.message;
    } finally {
      setElementDisabled(elements.deleteConfirmBtn, false);
      syncDeleteConfirmButton(elements);
    }
  }

  function syncRunButtons(elements) {
    setElementDisabled(elements.prevBtn, state.busy || state.offset <= 0);
    setElementDisabled(
      elements.nextBtn,
      state.busy || state.offset + state.limit >= state.total
    );
  }

  function syncDeleteButton(elements) {
    setElementDisabled(elements.deleteBtn, state.busy || !state.selectedRunId);
    setElementDisabled(elements.detailRefreshBtn, state.busy || !state.selectedRunId);
  }

  function syncDeleteConfirmButton(elements) {
    var ok = String(elements.deleteConfirmInput.value || '').trim() === state.deleteConfirm;
    setElementDisabled(elements.deleteConfirmBtn, !ok);
  }

  function syncMappingControls(elements) {
    var disabled = state.busy || state.mappingLoading || !state.selectedRunId;
    setElementDisabled(elements.mappingStatusFilter, disabled);
    setElementDisabled(elements.mappingModeFilter, disabled);
    setElementDisabled(elements.mappingPrevBtn, disabled || state.mappingOffset <= 0);
    setElementDisabled(
      elements.mappingNextBtn,
      disabled || state.mappingOffset + state.mappingLimit >= state.mappingTotal
    );
  }

  function updateMappingStatus(elements, overrideText) {
    if (overrideText) {
      elements.mappingStatus.textContent = overrideText;
      return;
    }
    if (!state.selectedRunId) {
      elements.mappingStatus.textContent = '请选择记录后查看消息映射。';
      return;
    }
    if (!state.mappingTotal) {
      elements.mappingStatus.textContent = '暂无匹配的消息映射。';
      return;
    }
    var start = state.mappingOffset + 1;
    var end = Math.min(state.mappingOffset + state.mappingLimit, state.mappingTotal);
    elements.mappingStatus.textContent = '显示 '
      + formatNumber(start)
      + '-'
      + formatNumber(end)
      + ' / '
      + formatNumber(state.mappingTotal)
      + ' 条消息映射。';
  }

  function selectRun(elements, runId) {
    state.selectedRunId = String(runId || '').trim();
    state.mappingOffset = 0;
    persistState(elements);
    syncUrlRunId(state.selectedRunId);
  }

  function clearSelectedRun(elements) {
    state.selectedRunId = '';
    persistState(elements);
    syncUrlRunId('');
  }

  function setBusy(elements, busy) {
    state.busy = !!busy;
    [
      elements.refreshBtn,
      elements.queryInput,
      elements.statusFilter,
      elements.sortSelect,
      elements.limitSelect
    ].forEach(function (element) {
      setElementDisabled(element, state.busy);
    });
    syncRunButtons(elements);
    syncDeleteButton(elements);
    syncMappingControls(elements);
  }

  function restorePersistentState(elements) {
    var stored = readStoredState();
    state.offset = normalizeNonnegativeInteger(
      stored.offset,
      0,
      Number.MAX_SAFE_INTEGER
    );
    state.limit = normalizeNonnegativeInteger(stored.limit, 20, 100);
    state.mappingOffset = normalizeNonnegativeInteger(
      stored.mappingOffset,
      0,
      Number.MAX_SAFE_INTEGER
    );
    state.mappingLimit = normalizeNonnegativeInteger(
      stored.mappingLimit,
      MAPPING_PAGE_SIZE,
      100
    ) || MAPPING_PAGE_SIZE;

    elements.queryInput.value = String(stored.query || '');
    elements.statusFilter.value = String(stored.status || '');
    elements.sortSelect.value = String(stored.sort || 'updated_desc');
    elements.limitSelect.value = String(state.limit || 20);
    elements.mappingStatusFilter.value = String(stored.mappingStatus || '');
    elements.mappingModeFilter.value = String(stored.mappingMode || '');

    var urlRunId = getRunIdFromLocation();
    var storedRunId = String(stored.selectedRunId || '').trim();
    state.selectedRunId = urlRunId || storedRunId;
    syncUrlRunId(state.selectedRunId);
    persistState(elements);
  }

  function persistState(elements) {
    if (!elements) return;
    try {
      window.localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
          query: String(elements.queryInput.value || ''),
          status: String(elements.statusFilter.value || ''),
          sort: String(elements.sortSelect.value || 'updated_desc'),
          limit: Number(elements.limitSelect.value || state.limit || 20) || 20,
          offset: state.offset,
          selectedRunId: String(state.selectedRunId || ''),
          mappingStatus: String(elements.mappingStatusFilter.value || ''),
          mappingMode: String(elements.mappingModeFilter.value || ''),
          mappingOffset: state.mappingOffset,
          mappingLimit: state.mappingLimit
        })
      );
    } catch (_error) {
      return;
    }
  }

  function readStoredState() {
    try {
      var raw = window.localStorage.getItem(STORAGE_KEY);
      if (!raw) return {};
      var parsed = JSON.parse(raw);
      return parsed && typeof parsed === 'object' ? parsed : {};
    } catch (_error) {
      return {};
    }
  }

  function getRunIdFromLocation() {
    try {
      var params = new URLSearchParams(window.location.search || '');
      return String(params.get('run_id') || '').trim();
    } catch (_error) {
      return '';
    }
  }

  function syncUrlRunId(runId) {
    try {
      var url = new URL(window.location.href);
      var normalized = String(runId || '').trim();
      if (normalized) {
        url.searchParams.set('run_id', normalized);
      } else {
        url.searchParams.delete('run_id');
      }
      window.history.replaceState({}, '', url.toString());
    } catch (_error) {
      return;
    }
  }

  function normalizeNonnegativeInteger(value, fallback, maxValue) {
    var text = String(value || '').trim();
    var number = text ? Number(text) : Number(fallback || 0);
    if (!Number.isFinite(number)) return Number(fallback || 0);
    number = Math.trunc(number);
    if (number < 0) return 0;
    var upperBound = Number(maxValue || number);
    if (!Number.isFinite(upperBound) || upperBound <= 0) return number;
    return Math.min(number, upperBound);
  }

  function updatePageStatus(elements) {
    if (!state.total) {
      elements.pageStatus.textContent = '暂无分页信息。';
      return;
    }
    var start = state.offset + 1;
    var end = Math.min(state.offset + state.limit, state.total);
    elements.pageStatus.textContent = '显示 '
      + formatNumber(start)
      + '-'
      + formatNumber(end)
      + ' / '
      + formatNumber(state.total);
  }

  function buildRunTitle(run) {
    var sourceTitle = String((run && run.source_title) || '未知源').trim();
    var targetTitle = String((run && run.target_title) || '').trim();
    return targetTitle ? sourceTitle + ' -> ' + targetTitle : sourceTitle;
  }

  function formatDoneTotal(done, total) {
    return formatNumber(done) + ' / ' + formatNumber(total);
  }

  function formatNumber(value) {
    var number = Number(value || 0);
    if (!Number.isFinite(number)) number = 0;
    return number.toLocaleString('zh-CN');
  }

  function formatDateTime(value) {
    var text = String(value || '').trim();
    if (!text) return '暂无';
    return text.replace('T', ' ').replace('+00:00', '');
  }

  function getRunStatusLabel(status) {
    var normalized = String(status || '').trim().toLowerCase();
    if (normalized === 'queued') return '排队中';
    if (normalized === 'running') return '执行中';
    if (normalized === 'done') return '已创建';
    if (normalized === 'error') return '失败';
    return normalized || '未知';
  }

  function getMigrationStatusLabel(status) {
    var normalized = String(status || '').trim().toLowerCase();
    if (normalized === 'queued') return '排队中';
    if (normalized === 'running') return '执行中';
    if (normalized === 'done') return '完成';
    if (normalized === 'error') return '失败';
    return normalized || '未执行';
  }

  function getMappingModeLabel(mode) {
    var normalized = String(mode || '').trim();
    if (normalized === 'text_replay') return '文本';
    if (normalized === 'media_copy') return '媒体';
    if (normalized === 'media_group_copy') return '媒体组';
    return normalized || '未知';
  }

  function getMappingStatusLabel(status) {
    var normalized = String(status || '').trim();
    if (normalized === 'done') return '完成';
    if (normalized === 'error') return '失败';
    return normalized || '未知';
  }

  function debounce(fn, delayMs) {
    var timerId = null;
    return function () {
      var args = arguments;
      if (timerId) window.clearTimeout(timerId);
      timerId = window.setTimeout(function () {
        fn.apply(null, args);
      }, delayMs);
    };
  }
})();
