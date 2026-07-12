(function () {
  'use strict';

  var shared = window.AdminManageShared;
  var DATABASE_READ_TIMEOUT_MS = 20000;
  var sharedFetchJSON = shared.fetchJSON;
  var formatDateTime = shared.formatDateTime;
  var setDialogOpenState = shared.setDialogOpenState;
  var setElementDisabled = shared.setElementDisabled;
  var setPageInteractionState = shared.setPageInteractionState;
  var trapFocusWithin = shared.trapFocusWithin;
  var normalizeNonnegativeInteger = shared.normalizeNonnegativeInteger;
  var formatNumber = shared.formatNumber;

  var state = {
    items: [],
    offset: 0,
    limit: 20,
    total: 0,
    busy: false,
    deleteJobId: '',
    pendingDeleteRun: null
  };

  document.addEventListener('DOMContentLoaded', async function () {
    var elements = getElements();
    if (!elements) return;
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
      'pageStatus',
      'prevBtn',
      'nextBtn',
      'list',
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
    initializeFilters(elements);
    renderRuns(elements);
    setBusy(elements, false);
  }

  function bindEvents(elements) {
    elements.loginConfirmBtn.addEventListener('click', function () {
      sessionController.handleLogin(elements);
    });
    elements.passwordInput.addEventListener('keydown', function (event) {
      if (event.key === 'Enter') {
        event.preventDefault();
        sessionController.handleLogin(elements);
      }
    });
    elements.refreshBtn.addEventListener('click', function () {
      loadRuns(elements, { resetOffset: false });
    });
    elements.queryInput.addEventListener('input', debounce(function () {
      state.offset = 0;
      loadRuns(elements, { resetOffset: true });
    }, 250));
    elements.statusFilter.addEventListener('change', function () {
      state.offset = 0;
      loadRuns(elements, { resetOffset: true });
    });
    elements.sortSelect.addEventListener('change', function () {
      state.offset = 0;
      loadRuns(elements, { resetOffset: true });
    });
    elements.prevBtn.addEventListener('click', function () {
      state.offset = Math.max(0, state.offset - state.limit);
      loadRuns(elements, { resetOffset: false });
    });
    elements.nextBtn.addEventListener('click', function () {
      if (state.offset + state.limit >= state.total) return;
      state.offset += state.limit;
      loadRuns(elements, { resetOffset: false });
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
      if (!elements.loginDialog.hidden && event.key === 'Tab') {
        trapFocusWithin(elements.loginDialog, event);
        return;
      }
      if (!elements.deleteDialog.hidden && event.key === 'Tab') {
        trapFocusWithin(elements.deleteDialog, event);
      }
    });
  }

  async function loadRuns(elements, options) {
    var opts = options || {};
    if (opts.resetOffset) state.offset = 0;
    state.limit = 20;
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
        await loadRuns(elements, { resetOffset: false });
        return;
      }
      renderRuns(elements);
      updatePageStatus(elements);
      elements.status.textContent = state.total > 0
        ? '共 ' + formatNumber(state.total) + ' 条克隆记录。'
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
      empty.textContent = hasActiveRunFilters(elements)
        ? '当前筛选条件下暂无克隆记录'
        : '暂无克隆记录';
      elements.list.appendChild(empty);
      syncRunButtons(elements);
      return;
    }
    state.items.forEach(function (run) {
      elements.list.appendChild(createRunCard(elements, run));
    });
    syncRunButtons(elements);
    syncRunDeleteButtons(elements);
  }

  function createRunCard(elements, run) {
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

    appendRunPill(meta, '源群（保留）', (run && run.source_title) || '未知源群');
    appendRunPill(meta, '克隆副本', run && run.target_title ? String(run.target_title) : '未创建');
    appendRunPill(meta, '创建状态', getRunStatusLabel(normalizedStatus));
    appendRunPill(meta, '更新时间', formatDateTime(run && run.updated_at));
    card.appendChild(meta);

    appendRunLink(actions, '打开源群（只读）', run && run.source_telegram_app_link);
    appendRunLink(actions, '打开克隆副本', run && run.target_telegram_app_link);
    appendRunResumeLink(actions, run);
    appendRunDetailLink(actions, run);
    appendRunMessageDeleteButton(elements, actions, run);
    appendRunDeleteButton(elements, actions, run);
    if (actions.childNodes.length > 0) {
      card.appendChild(actions);
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

  function appendRunResumeLink(container, run) {
    if (!canResumeMigration(run)) return;
    var link = document.createElement('a');
    link.href = buildRunMigrationHref(run);
    link.textContent = '继续克隆消息';
    container.appendChild(link);
  }

  function appendRunDetailLink(container, run) {
    var runId = String((run && run.run_id) || '').trim();
    if (!runId) return;
    var link = document.createElement('a');
    link.href = buildRunDetailHref(runId);
    link.textContent = '进入群详情';
    container.appendChild(link);
  }

  function appendRunMessageDeleteButton(elements, container, run) {
    if (!canDeleteRun(run) || !(run && run.target_chat_id)) return;
    var button = document.createElement('button');
    button.type = 'button';
    button.className = 'btn danger clone-run-message-delete';
    button.textContent = '删除局部克隆消息';
    button.setAttribute(
      'aria-label',
      '删除 ' + buildRunTitle(run) + ' 的局部克隆消息'
    );
    button.disabled = state.busy || !!state.deleteJobId;
    button.addEventListener('click', function () {
      window.location.assign(buildRunMessageDeleteHref(run));
    });
    container.appendChild(button);
  }

  function appendRunDeleteButton(elements, container, run) {
    if (!canDeleteRun(run)) return;
    var button = document.createElement('button');
    var targetExists = !!(run && run.target_chat_id);
    button.type = 'button';
    button.className = 'btn danger clone-run-delete';
    button.textContent = targetExists ? '删除克隆副本' : '清除失败记录';
    button.setAttribute(
      'aria-label',
      targetExists
        ? '删除 ' + buildRunTitle(run) + ' 的克隆副本和本地记录'
        : '清除 ' + buildRunTitle(run) + ' 的失败克隆记录'
    );
    button.disabled = state.busy || !!state.deleteJobId;
    button.addEventListener('click', function () {
      openDeleteDialog(elements, run);
    });
    container.appendChild(button);
  }

  function canDeleteRun(run) {
    var status = String((run && run.status) || '').trim().toLowerCase();
    return !!String((run && run.run_id) || '').trim()
      && status !== 'queued'
      && status !== 'running';
  }

  function deleteConfirmText(run) {
    return 'DELETE-CLONE-RUN:' + String((run && run.run_id) || '').trim();
  }

  function openDeleteDialog(elements, run) {
    if (!canDeleteRun(run) || state.busy || state.deleteJobId) return;
    state.pendingDeleteRun = run;
    elements.deleteStatus.textContent = '';
    elements.deleteConfirmInput.value = '';
    elements.deleteConfirmHint.textContent = run.target_chat_id
      ? '将删除 “' + buildRunTitle(run) + '” 的克隆副本和本地记录。源群不会受到影响。请输入确认码：' + deleteConfirmText(run)
      : '这条记录没有可删除的目标副本，将只清除失败记录及其本地链路。源群不会受到影响。请输入确认码：' + deleteConfirmText(run);
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
    var run = state.pendingDeleteRun;
    if (!run) return;
    var confirm = String(elements.deleteConfirmInput.value || '').trim();
    if (confirm !== deleteConfirmText(run)) {
      elements.deleteStatus.textContent = '确认码不匹配。';
      elements.deleteConfirmInput.focus();
      return;
    }

    setElementDisabled(elements.deleteConfirmBtn, true);
    elements.deleteStatus.textContent = '正在提交删除任务...';
    try {
      var payload = await fetchJSON(
        '/api/admin/clone/runs/' + encodeURIComponent(String(run.run_id)),
        {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ confirm: confirm })
        }
      );
      var jobId = String((payload && payload.job && payload.job.job_id) || '').trim();
      if (!jobId) throw new Error('删除任务响应缺少 job_id');
      state.pendingDeleteRun = null;
      closeDeleteDialog(elements);
      state.deleteJobId = jobId;
      setBusy(elements, true);
      elements.status.textContent = '正在删除克隆副本及本地记录...';
      pollDeleteJob(elements, jobId);
    } catch (error) {
      elements.deleteStatus.textContent = '删除任务创建失败：' + error.message;
      syncDeleteConfirmButton(elements);
    }
  }

  async function pollDeleteJob(elements, jobId) {
    if (state.deleteJobId !== jobId) return;
    try {
      var payload = await fetchJSON('/api/admin/jobs/' + encodeURIComponent(jobId));
      var job = payload && payload.job ? payload.job : null;
      var status = String((job && job.status) || '').trim().toLowerCase();
      if (status === 'done') {
        state.deleteJobId = '';
        setBusy(elements, false);
        await loadRuns(elements, { resetOffset: false });
        elements.status.textContent = '克隆副本及本地记录已删除。';
        return;
      }
      if (status === 'error') {
        state.deleteJobId = '';
        setBusy(elements, false);
        elements.status.textContent = '删除任务未完成，请刷新后查看记录。';
        return;
      }
      if (status !== 'queued' && status !== 'running') {
        throw new Error('删除任务状态异常');
      }
    } catch (error) {
      state.deleteJobId = '';
      setBusy(elements, false);
      elements.status.textContent = '删除任务状态读取失败：' + error.message;
      return;
    }
    window.setTimeout(function () {
      pollDeleteJob(elements, jobId);
    }, 1200);
  }

  function syncDeleteConfirmButton(elements) {
    var expected = deleteConfirmText(state.pendingDeleteRun);
    var supplied = String(elements.deleteConfirmInput.value || '').trim();
    setElementDisabled(elements.deleteConfirmBtn, !expected || supplied !== expected);
  }

  function syncRunButtons(elements) {
    setElementDisabled(elements.prevBtn, state.busy || state.offset <= 0);
    setElementDisabled(
      elements.nextBtn,
      state.busy || state.offset + state.limit >= state.total
    );
  }

  function syncRunDeleteButtons(elements) {
    var disabled = state.busy || !!state.deleteJobId;
    var buttons = elements.list.querySelectorAll(
      '.clone-run-delete, .clone-run-message-delete'
    );
    Array.prototype.forEach.call(buttons, function (button) {
      setElementDisabled(button, disabled);
    });
  }

  function setBusy(elements, busy) {
    state.busy = !!busy;
    [
      elements.refreshBtn,
      elements.queryInput,
      elements.statusFilter,
      elements.sortSelect
    ].forEach(function (element) {
      setElementDisabled(element, state.busy);
    });
    syncRunButtons(elements);
    syncRunDeleteButtons(elements);
  }

  function initializeFilters(elements) {
    state.offset = 0;
    state.limit = 20;
    elements.queryInput.value = '';
    elements.statusFilter.value = '';
    elements.sortSelect.value = 'updated_desc';
  }

  function hasActiveRunFilters(elements) {
    return !!String(elements.queryInput.value || '').trim()
      || !!String(elements.statusFilter.value || '').trim();
  }

  function canResumeMigration(run) {
    return String((run && run.status) || '').trim().toLowerCase() === 'done'
      && !!(run && run.target_chat_id)
      && !!String((run && run.run_id) || '').trim();
  }

  function buildRunMigrationHref(run) {
    return '/admin/clone/migrate?run_id=' + encodeURIComponent(String(run.run_id || ''));
  }

  function buildRunDetailHref(runId) {
    return '/admin/clone/runs/detail?run_id=' + encodeURIComponent(String(runId || ''));
  }

  function buildRunMessageDeleteHref(run) {
    return '/admin/clone/runs/messages/delete?run_id='
      + encodeURIComponent(String((run && run.run_id) || ''));
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

  function getRunStatusLabel(status) {
    var normalized = String(status || '').trim().toLowerCase();
    if (normalized === 'queued') return '排队中';
    if (normalized === 'running') return '执行中';
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
    return sharedFetchJSON(url, Object.assign(requestOptions, {
      onUnauthorized: sessionController.handleUnauthorizedResponse
    }));
  }

  var sessionController = shared.createAdminSessionController({
    afterAuth: async function (elements) {
      await loadRuns(elements, { resetOffset: false });
    },
    getElements: getElements,
    getPageElement: function (elements) {
      return elements && elements.page ? elements.page : null;
    }
  });
})();
