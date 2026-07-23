(function () {
  'use strict';

  var shared = window.AdminManageShared;
  var DATABASE_READ_TIMEOUT_MS = 20000;
  var MAX_DELETE_COUNT = 100000;
  var MAX_MESSAGE_ID = 2147483647;
  var MAX_DELETE_DELAY_MS = 10000;
  var state = {
    runId: readRunId(),
    run: null,
    remoteMessageCount: null,
    remoteMessageCountError: '',
    remoteMessageCountLoading: false,
    busy: false,
    operationKind: 'partial',
    pendingRangeSelection: null,
    job: {
      jobId: '',
      lastSeq: 0,
      lastProgressKey: '',
      pollToken: 0,
      retryCount: 0,
      timerId: null,
      isPolling: false
    }
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
      page: document.getElementById('admin-clone-message-delete-page'),
      backLink: document.getElementById('admin-clone-message-delete-back-link'),
      target: document.getElementById('admin-clone-message-delete-target'),
      status: document.getElementById('admin-clone-message-delete-status'),
      refreshMessageCountBtn: document.getElementById('admin-clone-message-count-refresh-btn'),
      form: document.getElementById('admin-clone-message-delete-form'),
      selection: document.getElementById('admin-clone-message-delete-selection'),
      selectionPreview: document.getElementById('admin-clone-message-delete-preview'),
      delay: document.getElementById('admin-clone-message-delete-delay'),
      formStatus: document.getElementById('admin-clone-message-delete-form-status'),
      submitBtn: document.getElementById('admin-clone-message-delete-submit'),
      logPanel: document.getElementById('admin-clone-message-delete-log-panel'),
      logContainer: document.getElementById('admin-clone-message-delete-log-container'),
      clearLogsBtn: document.getElementById('admin-clear-clone-message-delete-logs-btn'),
      rangeDialog: document.getElementById('admin-clone-message-range-dialog'),
      rangeDialogStatus: document.getElementById('admin-clone-message-range-dialog-status'),
      rangeConfirmInput: document.getElementById('admin-clone-message-range-confirm-input'),
      rangeConfirmHint: document.getElementById('admin-clone-message-range-confirm-hint'),
      rangeCancelBtn: document.getElementById('admin-clone-message-range-cancel-btn'),
      rangeConfirmBtn: document.getElementById('admin-clone-message-range-confirm-btn'),
      resetOpenBtn: document.getElementById('admin-clone-message-reset-open-btn'),
      resetStatus: document.getElementById('admin-clone-message-reset-status'),
      resetDialog: document.getElementById('admin-clone-message-reset-dialog'),
      resetDialogStatus: document.getElementById('admin-clone-message-reset-dialog-status'),
      resetConfirmInput: document.getElementById('admin-clone-message-reset-confirm-input'),
      resetConfirmHint: document.getElementById('admin-clone-message-reset-confirm-hint'),
      resetCancelBtn: document.getElementById('admin-clone-message-reset-cancel-btn'),
      resetConfirmBtn: document.getElementById('admin-clone-message-reset-confirm-btn'),
      loginDialog: document.getElementById('admin-login-dialog'),
      loginStatus: document.getElementById('admin-login-status'),
      passwordInput: document.getElementById('admin-password-input'),
      loginConfirmBtn: document.getElementById('admin-login-confirm-btn')
    };
    var required = Object.keys(elements).filter(function (key) { return !elements[key]; });
    if (required.length) {
      console.warn('[admin_clone_message_delete] Missing required elements:', required.join(', '));
      return null;
    }
    return elements;
  }

  function initializeUI(elements) {
    shared.ensurePlaceholder(elements.logContainer);
    shared.syncClearLogsButtonVisibility(elements);
    syncBackLink(elements);
    renderRun(elements);
    renderSelectionPreview(elements);
    setBusy(elements, false);
    closeRangeDialog(elements, { skipFocusRestore: true });
    closeResetDialog(elements, { skipFocusRestore: true });
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
    elements.selection.addEventListener('input', function () {
      renderSelectionPreview(elements);
      syncSubmitButton(elements);
    });
    elements.delay.addEventListener('input', function () {
      syncSubmitButton(elements);
    });
    elements.refreshMessageCountBtn.addEventListener('click', function () {
      loadTargetMessageCount(elements, { force: true });
    });
    elements.form.addEventListener('submit', function (event) {
      event.preventDefault();
      requestDeleteJob(elements);
    });
    elements.clearLogsBtn.addEventListener('click', function () {
      shared.clearLogs(elements);
    });
    elements.rangeCancelBtn.addEventListener('click', function () {
      closeRangeDialog(elements);
    });
    elements.rangeConfirmInput.addEventListener('input', function () {
      syncRangeConfirmButton(elements);
    });
    elements.rangeConfirmInput.addEventListener('keydown', function (event) {
      if (event.key === 'Escape') {
        event.preventDefault();
        closeRangeDialog(elements);
        return;
      }
      if (event.key !== 'Enter') return;
      event.preventDefault();
      if (!elements.rangeConfirmBtn.disabled) submitConfirmedRangeDelete(elements);
    });
    elements.rangeConfirmBtn.addEventListener('click', function () {
      submitConfirmedRangeDelete(elements);
    });
    elements.resetOpenBtn.addEventListener('click', function () {
      openResetDialog(elements);
    });
    elements.resetCancelBtn.addEventListener('click', function () {
      closeResetDialog(elements);
    });
    elements.resetConfirmInput.addEventListener('input', function () {
      syncResetConfirmButton(elements);
    });
    elements.resetConfirmInput.addEventListener('keydown', function (event) {
      if (event.key === 'Escape') {
        event.preventDefault();
        closeResetDialog(elements);
        return;
      }
      if (event.key !== 'Enter') return;
      event.preventDefault();
      if (!elements.resetConfirmBtn.disabled) submitResetJob(elements);
    });
    elements.resetConfirmBtn.addEventListener('click', function () {
      submitResetJob(elements);
    });
  }

  async function loadRun(elements) {
    if (!state.runId) {
      state.run = null;
      renderRun(elements);
      elements.status.textContent = '缺少克隆记录标识，无法选择目标副本。';
      return;
    }
    setBusy(elements, true);
    elements.status.textContent = '正在读取克隆记录...';
    try {
      var payload = await fetchJSON(
        '/api/admin/clone/runs/' + encodeURIComponent(state.runId)
      );
      state.run = payload && payload.run ? payload.run : null;
      if (!state.run) throw new Error('克隆记录响应缺少目标副本');
      renderRun(elements);
      elements.status.textContent = state.run.target_chat_id
        ? (messageResetRequired()
          ? '上次完整清空未完成，当前只能重试完整清空，不能局部删除或继续迁移。'
          : '已选择目标副本。局部删除由第二账号执行，完整清空由目标副本创建账号执行。')
        : '该记录尚未创建目标副本，不能执行消息删除或回退。';
      await resumeMatchingDeleteJob(elements);
      if (!state.job.isPolling) {
        await loadTargetMessageCount(elements, { force: true });
      }
    } catch (error) {
      state.run = null;
      renderRun(elements);
      elements.status.textContent = '读取克隆记录失败：' + error.message;
    } finally {
      if (!state.job.isPolling) setBusy(elements, false);
    }
  }

  function renderRun(elements) {
    elements.target.textContent = '';
    var run = state.run;
    if (!run) {
      appendSummaryItem(elements.target, '目标副本', '尚未加载');
      syncSubmitButton(elements);
      return;
    }
    appendSummaryItem(elements.target, '源群（保留）', run.source_title || run.source_chat_id || '未知源群');
    appendSummaryItem(elements.target, '克隆副本', run.target_title || run.target_chat_id || '未创建');
    appendSummaryItem(elements.target, '目标群 ID', run.target_chat_id || '未创建');
    appendSummaryItem(
      elements.target,
      '当前远端消息数',
      remoteMessageCountDisplay()
    );
    appendSummaryItem(
      elements.target,
      '执行账号',
      '局部删除使用第二账号；完整清空使用目标副本创建账号'
    );
    syncSubmitButton(elements);
  }

  function appendSummaryItem(container, label, value) {
    var item = document.createElement('div');
    var term = document.createElement('dt');
    var description = document.createElement('dd');
    term.textContent = String(label || '');
    description.textContent = String(value || '暂无');
    item.appendChild(term);
    item.appendChild(description);
    container.appendChild(item);
  }

  function remoteMessageCountDisplay() {
    if (state.remoteMessageCountLoading) return '正在读取...';
    if (state.remoteMessageCountError) return '读取失败：' + state.remoteMessageCountError;
    if (state.remoteMessageCount === null) return '待读取';
    return shared.formatNumber(state.remoteMessageCount) + ' 条';
  }

  async function loadTargetMessageCount(elements, options) {
    var opts = options || {};
    if (
      !state.run
      || !state.run.target_chat_id
      || state.remoteMessageCountLoading
      || (state.busy && !opts.force)
    ) {
      return;
    }
    state.remoteMessageCountLoading = true;
    state.remoteMessageCountError = '';
    renderRun(elements);
    syncMessageCountRefreshButton(elements);
    try {
      var payload = await fetchJSON(
        '/api/admin/clone/runs/'
          + encodeURIComponent(state.runId)
          + '/target-message-count'
      );
      var count = Number(payload && payload.message_count);
      if (!Number.isSafeInteger(count) || count < 0) {
        throw new Error('目标副本消息数量响应异常');
      }
      state.remoteMessageCount = count;
    } catch (error) {
      state.remoteMessageCount = null;
      state.remoteMessageCountError = error.message;
    } finally {
      state.remoteMessageCountLoading = false;
      renderRun(elements);
      syncMessageCountRefreshButton(elements);
    }
  }

  function readSelection() {
    var text = String(getElements().selection.value || '').trim();
    var match = /^([1-9]\d*)(?:-([1-9]\d*))?$/.exec(text);
    if (!match) {
      return { error: '请输入正整数，或两个正整数构成的区间。' };
    }
    var first = Number(match[1]);
    var last = match[2] ? Number(match[2]) : null;
    if (!Number.isSafeInteger(first) || (last !== null && !Number.isSafeInteger(last))) {
      return { error: '消息数量或消息 ID 超出支持范围。' };
    }
    if (last !== null && last > MAX_MESSAGE_ID) {
      return { error: '消息 ID 超出 Telegram 支持范围。' };
    }
    if (last !== null && first > last) {
      return { error: '区间起始消息 ID 不能大于结束消息 ID。' };
    }
    var count = last === null ? first : last - first + 1;
    if (count > MAX_DELETE_COUNT) {
      return { error: '单次最多删除 ' + shared.formatNumber(MAX_DELETE_COUNT) + ' 条消息，请分批处理。' };
    }
    return {
      mode: last === null ? 'latest' : 'range',
      count: count,
      first: first,
      last: last,
      text: text
    };
  }

  function readDelay() {
    var text = String(getElements().delay.value || '').trim();
    if (!/^\d+$/.test(text)) return { error: '批次间隔必须是 0 到 10000 的整数。' };
    var delay = Number(text);
    if (!Number.isSafeInteger(delay) || delay > MAX_DELETE_DELAY_MS) {
      return { error: '批次间隔必须是 0 到 10000 的整数。' };
    }
    return { value: delay };
  }

  function renderSelectionPreview(elements) {
    var selection = readSelection();
    if (selection.error) {
      elements.selectionPreview.textContent = selection.error;
      elements.selectionPreview.className = 'clone-message-delete-preview is-error';
      return;
    }
    var message = selection.mode === 'latest'
      ? '将按源消息 ID 从新到旧回滚最后 ' + shared.formatNumber(selection.count) + ' 条已克隆内容；公告等未映射消息不参与计数，删除后续克隆会从最早回退位置继续。'
      : '将清理目标消息 ID ' + selection.first + ' 到 ' + selection.last + '，共 ' + shared.formatNumber(selection.count) + ' 个 ID；克隆映射保持不变，后续迁移不会补回。';
    elements.selectionPreview.textContent = message;
    elements.selectionPreview.className = 'clone-message-delete-preview';
    elements.submitBtn.textContent = selection.mode === 'latest'
      ? '开始回滚局部克隆消息'
      : '永久删除目标消息 ID 区间';
  }

  function messageResetRequired() {
    return String((state.run && state.run.phase) || '').trim().toLowerCase()
      === 'message_reset_required';
  }

  function syncSubmitButton(elements) {
    var selection = readSelection();
    var delay = readDelay();
    var canSubmit = !!(state.run && state.run.target_chat_id)
      && !selection.error
      && !delay.error
      && !messageResetRequired()
      && !state.busy;
    shared.setElementDisabled(elements.submitBtn, !canSubmit);
  }

  function requestDeleteJob(elements) {
    if (state.busy || !state.run || !state.run.target_chat_id || messageResetRequired()) return;
    var selection = readSelection();
    var delay = readDelay();
    if (selection.error || delay.error) {
      elements.formStatus.textContent = selection.error || delay.error;
      syncSubmitButton(elements);
      return;
    }
    if (selection.mode === 'range') {
      openRangeDialog(elements, selection);
      return;
    }
    submitDeleteJob(elements, selection, '');
  }

  async function submitDeleteJob(elements, selection, rangeConfirm) {
    if (state.busy || !state.run || !state.run.target_chat_id || messageResetRequired()) return;
    var delay = readDelay();
    if (!selection || selection.error || delay.error) return;

    setBusy(elements, true);
    state.operationKind = 'partial';
    elements.formStatus.textContent = '正在创建删除任务...';
    try {
      var payload = await fetchJSON(
        '/api/admin/clone/runs/' + encodeURIComponent(state.runId) + '/delete-messages',
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            selection: selection.text,
            delete_delay_ms: delay.value,
            confirm: rangeConfirm || undefined
          })
        }
      );
      var jobId = shared.getCreatedJobId(payload);
      closeRangeDialog(elements, { skipFocusRestore: true });
      shared.clearLogs(elements);
      elements.logPanel.open = true;
      elements.formStatus.textContent = '删除任务已创建，正在连接第二账号...';
      jobPollController.start(state.job, jobId, { clearLogs: false });
    } catch (error) {
      var statusElement = selection.mode === 'range'
        ? elements.rangeDialogStatus
        : elements.formStatus;
      statusElement.textContent = '创建删除任务失败：' + error.message;
      setBusy(elements, false);
    }
  }

  async function resumeMatchingDeleteJob(elements) {
    if (!state.run || !state.run.target_chat_id || state.job.isPolling) return;
    try {
      var payload = await fetchJSON('/api/admin/jobs/active');
      var job = payload && payload.job ? payload.job : null;
      var jobType = String((job && job.job_type) || '');
      if (jobType !== 'clone_message_delete' && jobType !== 'clone_message_reset') return;
      if (Number(job.target_chat_id) !== Number(state.run.target_chat_id)) return;
      state.operationKind = jobType === 'clone_message_reset' ? 'reset' : 'partial';
      var statusElement = state.operationKind === 'reset' ? elements.resetStatus : elements.formStatus;
      statusElement.textContent = state.operationKind === 'reset'
        ? '检测到正在执行的完整清空任务，已恢复日志。'
        : '检测到正在执行的局部消息删除任务，已恢复日志。';
      elements.logPanel.open = true;
      jobPollController.start(state.job, String(job.job_id || ''), { resume: true });
    } catch (error) {
      elements.formStatus.textContent = '读取进行中的任务失败：' + error.message;
    }
  }

  function setBusy(elements, busy) {
    state.busy = !!busy;
    shared.setElementDisabled(elements.selection, state.busy);
    shared.setElementDisabled(elements.delay, state.busy);
    syncRangeConfirmButton(elements);
    shared.setElementDisabled(
      elements.resetOpenBtn,
      state.busy || !state.run || !state.run.target_chat_id
    );
    syncResetConfirmButton(elements);
    syncMessageCountRefreshButton(elements);
    syncSubmitButton(elements);
  }

  function syncMessageCountRefreshButton(elements) {
    var disabled = state.busy
      || !state.run
      || !state.run.target_chat_id
      || state.remoteMessageCountLoading;
    shared.setElementDisabled(elements.refreshMessageCountBtn, disabled);
  }

  function resetConfirmText() {
    return 'RESET-CLONE-MESSAGES:' + state.runId;
  }

  function rangeConfirmText(selection) {
    return 'DELETE-TARGET-MESSAGE-RANGE:'
      + state.runId
      + ':'
      + selection.first
      + '-'
      + selection.last;
  }

  function openRangeDialog(elements, selection) {
    state.pendingRangeSelection = selection;
    elements.rangeDialogStatus.textContent = '';
    elements.rangeConfirmInput.value = '';
    elements.rangeConfirmHint.textContent = '请输入永久删除确认码：' + rangeConfirmText(selection);
    elements.rangeDialog.hidden = false;
    syncRangeConfirmButton(elements);
    elements.rangeConfirmInput.focus();
  }

  function closeRangeDialog(elements, options) {
    var opts = options || {};
    elements.rangeDialog.hidden = true;
    elements.rangeDialogStatus.textContent = '';
    elements.rangeConfirmInput.value = '';
    state.pendingRangeSelection = null;
    syncRangeConfirmButton(elements);
    if (!opts.skipFocusRestore) elements.submitBtn.focus();
  }

  function syncRangeConfirmButton(elements) {
    var selection = state.pendingRangeSelection;
    var matches = !!selection
      && String(elements.rangeConfirmInput.value || '').trim() === rangeConfirmText(selection);
    shared.setElementDisabled(elements.rangeConfirmBtn, state.busy || !matches);
  }

  function submitConfirmedRangeDelete(elements) {
    var selection = state.pendingRangeSelection;
    if (!selection || elements.rangeConfirmBtn.disabled) return;
    submitDeleteJob(elements, selection, rangeConfirmText(selection));
  }

  function openResetDialog(elements) {
    if (state.busy || !state.run || !state.run.target_chat_id) return;
    elements.resetDialogStatus.textContent = '';
    elements.resetConfirmInput.value = '';
    elements.resetConfirmHint.textContent = '请输入确认码：' + resetConfirmText();
    elements.resetDialog.hidden = false;
    syncResetConfirmButton(elements);
    elements.resetConfirmInput.focus();
  }

  function closeResetDialog(elements, options) {
    var opts = options || {};
    elements.resetDialog.hidden = true;
    elements.resetDialogStatus.textContent = '';
    elements.resetConfirmInput.value = '';
    syncResetConfirmButton(elements);
    if (!opts.skipFocusRestore) elements.resetOpenBtn.focus();
  }

  function syncResetConfirmButton(elements) {
    var matches = String(elements.resetConfirmInput.value || '').trim() === resetConfirmText();
    shared.setElementDisabled(elements.resetConfirmBtn, state.busy || !matches);
  }

  async function submitResetJob(elements) {
    if (
      state.busy
      || !state.run
      || !state.run.target_chat_id
      || String(elements.resetConfirmInput.value || '').trim() !== resetConfirmText()
    ) {
      return;
    }
    var delay = readDelay();
    if (delay.error) {
      elements.resetDialogStatus.textContent = delay.error;
      return;
    }

    setBusy(elements, true);
    state.operationKind = 'reset';
    elements.resetDialogStatus.textContent = '正在创建完整清空任务...';
    try {
      var payload = await fetchJSON(
        '/api/admin/clone/runs/' + encodeURIComponent(state.runId) + '/reset-messages',
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            confirm: resetConfirmText(),
            delete_delay_ms: delay.value
          })
        }
      );
      var jobId = shared.getCreatedJobId(payload);
      closeResetDialog(elements, { skipFocusRestore: true });
      shared.clearLogs(elements);
      elements.logPanel.open = true;
      elements.resetStatus.textContent = '完整清空任务已创建，正在连接目标副本创建账号...';
      jobPollController.start(state.job, jobId, { clearLogs: false });
    } catch (error) {
      elements.resetDialogStatus.textContent = '创建完整清空任务失败：' + error.message;
      setBusy(elements, false);
    }
  }

  function readRunId() {
    var params = new URLSearchParams(window.location.search || '');
    return String(params.get('run_id') || '').trim();
  }

  function syncBackLink(elements) {
    if (!elements || !elements.backLink) return;
    elements.backLink.href = state.runId
      ? '/admin/clone/runs/detail?run_id=' + encodeURIComponent(state.runId)
      : '/admin/clone/runs/manage';
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

  var jobPollController = shared.createAdminJobPollController({
    fetchJSON: fetchJSON,
    appendLog: function (message) {
      var elements = getElements();
      if (elements) shared.appendLog(elements, message);
    },
    getElements: getElements,
    setBusy: setBusy,
    intervalMs: 1200,
    setInitialState: function (_jobState, options) {
      var elements = getElements();
      if (elements && options.clearLogs) shared.clearLogs(elements);
    },
    onSnapshot: function (snapshot) {
      var elements = getElements();
      if (!elements) return;
      var progress = snapshot && snapshot.progress ? snapshot.progress : {};
      var current = Number(progress.current || 0);
      var total = progress.total === null || progress.total === undefined
        ? ''
        : '/' + shared.formatNumber(progress.total);
      var statusElement = state.operationKind === 'reset' ? elements.resetStatus : elements.formStatus;
      statusElement.textContent = snapshot.stop_requested
        ? '已收到停止请求，正在完成当前批次...'
        : (state.operationKind === 'reset' ? '正在完整清空：' : '正在删除：')
          + shared.formatNumber(current) + total + ' 条消息。';
    },
    getDoneMessage: function (_jobState, snapshot) {
      var stage = String((((snapshot || {}).progress || {}).stage) || '');
      return stage === 'stopped'
        ? (state.operationKind === 'reset' ? '完整清空已停止。' : '局部消息删除已停止。')
        : (state.operationKind === 'reset' ? '完整清空任务已完成。' : '局部消息删除任务已完成。');
    },
    getErrorMessage: function () {
      return state.operationKind === 'reset'
        ? '完整清空失败，请查看上方日志。'
        : '局部消息删除失败，请查看上方日志。';
    },
    onDone: async function (snapshot) {
      var elements = getElements();
      if (!elements) return;
      var stage = String((((snapshot || {}).progress || {}).stage) || '');
      var statusElement = state.operationKind === 'reset' ? elements.resetStatus : elements.formStatus;
      statusElement.textContent = stage === 'stopped'
        ? '删除已停止，未提交的消息不会被处理。'
        : (state.operationKind === 'reset'
          ? '目标副本消息与全部迁移状态已清空。'
          : '局部消息删除已完成；整数回滚可通过续克隆补齐，目标消息 ID 区间不会补回。');
      if (state.operationKind === 'reset') {
        await loadRun(elements);
      } else {
        await loadTargetMessageCount(elements, { force: true });
      }
    },
    onError: async function () {
      var elements = getElements();
      if (!elements) return;
      var statusElement = state.operationKind === 'reset' ? elements.resetStatus : elements.formStatus;
      statusElement.textContent = state.operationKind === 'reset'
        ? '完整清空失败，远端可能已部分删除；请重新执行完整清空后再继续迁移。'
        : '局部消息删除失败，远端可能已部分删除；请查看执行日志后再继续迁移。';
      if (state.operationKind === 'reset') {
        await loadRun(elements);
      } else {
        await loadTargetMessageCount(elements, { force: true });
      }
    }
  });

  var sessionController = shared.createAdminSessionController({
    afterAuth: async function (elements) {
      await loadRun(elements);
    },
    getElements: getElements,
    getPageElement: function (elements) {
      return elements && elements.page ? elements.page : null;
    }
  });
})();
