(function () {
  'use strict';

  var shared = window.AdminManageShared;
  var DATABASE_READ_TIMEOUT_MS = 20000;
  var state = {
    items: [],
    offset: 0,
    limit: 20,
    total: 0,
    busy: false
  };

  document.addEventListener('DOMContentLoaded', async function () {
    var elements = getElements();
    if (!elements || !shared) return;
    initializeUI(elements);
    bindEvents(elements);
    await sessionController.checkAuth(elements);
  });

  function getElements() {
    var elements = {
      page: document.getElementById('admin-clone-runs-page'),
      status: document.getElementById('admin-clone-runs-manage-status'),
      refreshBtn: document.getElementById('admin-clone-runs-manage-refresh-btn'),
      queryInput: document.getElementById('admin-clone-runs-query-input'),
      statusFilter: document.getElementById('admin-clone-runs-status-filter'),
      sortSelect: document.getElementById('admin-clone-runs-sort-select'),
      pageStatus: document.getElementById('admin-clone-runs-page-status'),
      prevBtn: document.getElementById('admin-clone-runs-prev-btn'),
      nextBtn: document.getElementById('admin-clone-runs-next-btn'),
      list: document.getElementById('admin-clone-runs-manage-list'),
      loginDialog: document.getElementById('admin-login-dialog'),
      loginStatus: document.getElementById('admin-login-status'),
      passwordInput: document.getElementById('admin-password-input'),
      loginConfirmBtn: document.getElementById('admin-login-confirm-btn')
    };
    var missing = Object.keys(elements).filter(function (key) { return !elements[key]; });
    if (missing.length) {
      console.warn('[admin_clone_runs] Missing required elements:', missing.join(', '));
      return null;
    }
    return elements;
  }

  function initializeUI(elements) {
    elements.queryInput.value = '';
    elements.statusFilter.value = '';
    elements.sortSelect.value = 'updated_desc';
    renderRuns(elements);
    setBusy(elements, false);
  }

  function bindEvents(elements) {
    elements.loginConfirmBtn.addEventListener('click', function () {
      sessionController.handleLogin(elements);
    });
    elements.passwordInput.addEventListener('keydown', function (event) {
      if (event.key !== 'Enter') return;
      event.preventDefault();
      sessionController.handleLogin(elements);
    });
    elements.refreshBtn.addEventListener('click', function () {
      loadRuns(elements);
    });
    elements.queryInput.addEventListener('input', debounce(function () {
      loadRuns(elements, { resetOffset: true });
    }, 250));
    elements.statusFilter.addEventListener('change', function () {
      loadRuns(elements, { resetOffset: true });
    });
    elements.sortSelect.addEventListener('change', function () {
      loadRuns(elements, { resetOffset: true });
    });
    elements.prevBtn.addEventListener('click', function () {
      state.offset = Math.max(0, state.offset - state.limit);
      loadRuns(elements);
    });
    elements.nextBtn.addEventListener('click', function () {
      if (state.offset + state.limit >= state.total) return;
      state.offset += state.limit;
      loadRuns(elements);
    });
    document.addEventListener('keydown', function (event) {
      if (!elements.loginDialog.hidden && event.key === 'Tab') {
        shared.trapFocusWithin(elements.loginDialog, event);
      }
    });
  }

  async function loadRuns(elements, options) {
    var opts = options || {};
    if (opts.resetOffset) state.offset = 0;
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
        await loadRuns(elements);
        return;
      }
      renderRuns(elements);
      updatePageStatus(elements);
      elements.status.textContent = state.total
        ? '共 ' + shared.formatNumber(state.total) + ' 条克隆记录。打开一条记录继续操作。'
        : '暂无匹配的克隆记录。';
    } catch (error) {
      state.items = [];
      state.total = 0;
      renderRuns(elements);
      updatePageStatus(elements);
      elements.status.textContent = '读取克隆记录失败：' + error.message;
    } finally {
      setBusy(elements, false);
    }
  }

  function renderRuns(elements) {
    elements.list.textContent = '';
    if (!state.items.length) {
      var empty = document.createElement('p');
      empty.className = 'clone-run-empty';
      empty.textContent = hasActiveFilters(elements) ? '当前筛选条件下暂无克隆记录。' : '暂无克隆记录。';
      elements.list.appendChild(empty);
      syncPagingButtons(elements);
      return;
    }
    state.items.forEach(function (run) {
      elements.list.appendChild(createRunCard(run));
    });
    syncPagingButtons(elements);
  }

  function createRunCard(run) {
    var card = document.createElement('article');
    var head = document.createElement('div');
    var title = document.createElement('h3');
    var status = document.createElement('span');
    var meta = document.createElement('div');
    var actions = document.createElement('div');
    var normalizedStatus = String((run && run.status) || '').toLowerCase();

    card.className = 'clone-run-card is-' + (normalizedStatus || 'unknown');
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

    appendRunPill(meta, '源群（只读）', (run && run.source_title) || '未知源群');
    appendRunPill(meta, '目标副本', (run && (run.target_title || run.target_chat_id)) || '未创建');
    appendRunPill(meta, '创建状态', getRunStatusLabel(normalizedStatus));
    appendRunPill(meta, '更新时间', shared.formatDateTime(run && run.updated_at));
    card.appendChild(meta);

    appendRunLink(actions, '打开源群（只读）', run && run.source_telegram_app_link);
    appendRunLink(actions, '打开目标副本', run && run.target_telegram_app_link);
    appendRecordLink(actions, run);
    if (actions.childNodes.length) card.appendChild(actions);
    if (run && run.error_message) {
      var error = document.createElement('p');
      error.className = 'clone-run-error';
      error.textContent = '创建错误：' + String(run.error_message);
      card.appendChild(error);
    }
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

  function appendRecordLink(container, run) {
    var runId = String((run && run.run_id) || '').trim();
    if (!runId) return;
    var link = document.createElement('a');
    link.href = '/admin/clone/runs/detail?run_id=' + encodeURIComponent(runId);
    link.textContent = '打开记录';
    container.appendChild(link);
  }

  function setBusy(elements, busy) {
    state.busy = !!busy;
    [elements.refreshBtn, elements.queryInput, elements.statusFilter, elements.sortSelect].forEach(function (element) {
      shared.setElementDisabled(element, state.busy);
    });
    syncPagingButtons(elements);
  }

  function syncPagingButtons(elements) {
    shared.setElementDisabled(elements.prevBtn, state.busy || state.offset <= 0);
    shared.setElementDisabled(
      elements.nextBtn,
      state.busy || state.offset + state.limit >= state.total
    );
  }

  function updatePageStatus(elements) {
    if (!state.total) {
      elements.pageStatus.textContent = '暂无分页信息。';
      return;
    }
    elements.pageStatus.textContent = '显示 '
      + shared.formatNumber(state.offset + 1)
      + '-'
      + shared.formatNumber(Math.min(state.offset + state.limit, state.total))
      + ' / '
      + shared.formatNumber(state.total);
  }

  function hasActiveFilters(elements) {
    return !!String(elements.queryInput.value || '').trim()
      || !!String(elements.statusFilter.value || '').trim();
  }

  function buildRunTitle(run) {
    var sourceTitle = String((run && run.source_title) || '未知源').trim();
    var targetTitle = String((run && run.target_title) || '').trim();
    return targetTitle ? sourceTitle + ' -> ' + targetTitle : sourceTitle;
  }

  function getRunStatusLabel(status) {
    var normalized = String(status || '').trim().toLowerCase();
    if (normalized === 'queued') return '排队中';
    if (normalized === 'running') return '创建中';
    if (normalized === 'deleting') return '删除中';
    if (normalized === 'done') return '已创建';
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

  async function fetchJSON(url, options) {
    var requestOptions = Object.assign({}, options || {});
    if (!requestOptions.method || String(requestOptions.method).toUpperCase() === 'GET') {
      requestOptions.timeoutMs = requestOptions.timeoutMs || DATABASE_READ_TIMEOUT_MS;
    }
    return shared.fetchJSON(url, Object.assign(requestOptions, {
      onUnauthorized: sessionController.handleUnauthorizedResponse
    }));
  }

  var sessionController = shared.createAdminSessionController({
    afterAuth: async function (elements) {
      await loadRuns(elements);
    },
    getElements: getElements,
    getPageElement: function (elements) {
      return elements && elements.page ? elements.page : null;
    }
  });
})();
