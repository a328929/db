(function () {
  'use strict';

  var shared = window.AdminManageShared;
  var appendLog = shared.appendLog;
  var buildDialogTargetPreview = shared.buildDialogTargetPreview;
  var buildNamedConfirmText = shared.buildNamedConfirmText;
  var buildScopeRequestPayload = shared.buildScopeRequestPayload;
  var clearLogs = shared.clearLogs;
  var ensurePlaceholder = shared.ensurePlaceholder;
  var sharedFetchJSON = shared.fetchJSON;
  var getConfirmationTarget = shared.getConfirmationTarget;
  var getCreatedJobId = shared.getCreatedJobId;
  var getSelectedOptionLabel = shared.getSelectedOptionLabel;
  var getCurrentTargetInfo = shared.getCurrentTargetInfo;
  var getVisibleDialog = shared.getVisibleDialog;
  var getRequiredIntegerChatId = shared.getRequiredIntegerChatId;
  var isAllScopeValue = shared.isAllScopeValue;
  var isChatScopeValue = shared.isChatScopeValue;
  var isNoneScopeValue = shared.isNoneScopeValue;
  var normalizeChats = shared.normalizeChats;
  var pickFirstNumber = shared.pickFirstNumber;
  var pickFirstText = shared.pickFirstText;
  var sharedPostJSON = shared.postJSON;
  var renderChatOptions = shared.renderChatOptions;
  var setDialogOpenState = shared.setDialogOpenState;
  var setElementDisabled = shared.setElementDisabled;
  var setElementHidden = shared.setElementHidden;
  var setPageInteractionState = shared.setPageInteractionState;
  var setStatsLineText = shared.setStatsLineText;
  var syncClearLogsButtonVisibility = shared.syncClearLogsButtonVisibility;
  var trapFocusWithin = shared.trapFocusWithin;

  var JOB_POLL_INTERVAL_MS = 3000;
  var JOB_POLL_RETRY_MAX_COUNT = 20;
  var JOB_POLL_RETRY_BASE_MS = 3000;
  var JOB_POLL_MAX_COUNT = 28800;
  var JOB_POLL_MAX_DURATION_MS = 86400000;

  var jobPollState = {
    jobId: '',
    lastSeq: 0,
    timerId: null,
    isPolling: false,
    startedAt: 0,
    pollCount: 0,
    pollToken: 0,
    retryCount: 0,
    lastProgressKey: '',
    stopRequested: false
  };

  var authState = {
    authenticated: false,
    logoutTimer: null
  };

  document.addEventListener('DOMContentLoaded', async function () {
    var elements = getElements();
    if (!elements) {
      return;
    }

    initializeUI(elements);
    bindEvents(elements);

    // 首次进入执行验证检查
    await checkAuth(elements);

    window.AdminManageUI = {
      appendLog: function (message) {
        appendLog(elements, message);
      },
      clearLogs: function () {
        clearLogs(elements);
      }
    };
  });

  function getElements() {
    var elements = {
      scopeSelect: document.getElementById('admin-scope-select'),
      startUpdateBtn: document.getElementById('admin-start-update-btn'),
      stopJobBtn: document.getElementById('admin-stop-job-btn'),
      deleteDataBtn: document.getElementById('admin-delete-data-btn'),
      deleteEmptyChatsBtn: document.getElementById('admin-delete-empty-chats-btn'),
      logContainer: document.getElementById('admin-log-container'),
      clearLogsBtn: document.getElementById('admin-clear-logs-btn'),
      cleanupEmptyBtn: document.getElementById('admin-cleanup-empty-btn'),
      openCleanupDialogBtn: document.getElementById('admin-open-cleanup-dialog-btn'),
      cleanupDialog: document.getElementById('admin-cleanup-dialog'),
      cleanupInput: document.getElementById('admin-cleanup-input'),
      cleanupCancelBtn: document.getElementById('admin-cleanup-cancel-btn'),
      cleanupConfirmBtn: document.getElementById('admin-cleanup-confirm-btn'),
      openAddDialogBtn: document.getElementById('admin-open-add-dialog-btn'),
      dialog: document.getElementById('admin-add-target-dialog'),
      dialogInput: document.getElementById('admin-target-input'),
      dialogCancelBtn: document.getElementById('admin-dialog-cancel-btn'),
      dialogConfirmBtn: document.getElementById('admin-dialog-confirm-btn'),
      statScope: document.getElementById('admin-stat-scope'),
      statMessages: document.getElementById('admin-stat-messages'),
      loginDialog: document.getElementById('admin-login-dialog'),
      loginStatus: document.getElementById('admin-login-status'),
      passwordInput: document.getElementById('admin-password-input'),
      loginConfirmBtn: document.getElementById('admin-login-confirm-btn')
    };

    var requiredKeys = [
      'scopeSelect',
      'startUpdateBtn',
      'stopJobBtn',
      'deleteDataBtn',
      'deleteEmptyChatsBtn',
      'logContainer',
      'clearLogsBtn',
      'cleanupEmptyBtn',
      'openCleanupDialogBtn',
      'cleanupDialog',
      'cleanupInput',
      'cleanupCancelBtn',
      'cleanupConfirmBtn',
      'openAddDialogBtn',
      'dialog',
      'dialogInput',
      'dialogCancelBtn',
      'dialogConfirmBtn',
      'statScope',
      'statMessages',
      'loginDialog',
      'loginStatus',
      'passwordInput',
      'loginConfirmBtn'
    ];

    var missing = requiredKeys.filter(function (key) {
      return !elements[key];
    });

    if (missing.length > 0) {
      console.warn('[admin_manage] Missing required elements:', missing.join(', '));
      return null;
    }

    return elements;
  }

  function initializeUI(elements) {
    updateControlVisibility(elements);
    setAdminControlsBusy(elements, false);
    ensurePlaceholder(elements.logContainer);
    syncClearLogsButtonVisibility(elements);
    closeAddDialog(elements, { skipFocusRestore: true });
    closeCleanupDialog(elements, { skipFocusRestore: true });
  }

  function bindEvents(elements) {
    // 认证相关事件
    elements.loginConfirmBtn.addEventListener('click', function () {
      handleLogin(elements);
    });

    elements.passwordInput.addEventListener('keydown', function (event) {
      if (event.key === 'Enter') {
        event.preventDefault();
        handleLogin(elements);
      }
    });

    elements.scopeSelect.addEventListener('change', function () {
      updateControlVisibility(elements);
      loadStatsByCurrentSelection(elements);
    });

    elements.clearLogsBtn.addEventListener('click', function () {
      clearLogs(elements);
    });

    elements.startUpdateBtn.addEventListener('click', function () {
      handleStartUpdateClick(elements);
    });

    elements.stopJobBtn.addEventListener('click', function () {
      handleStopJobClick(elements);
    });

    elements.deleteDataBtn.addEventListener('click', function () {
      handleDeleteDataClick(elements);
    });

    elements.deleteEmptyChatsBtn.addEventListener('click', function () {
      handleDeleteEmptyChatsClick(elements);
    });

    elements.cleanupEmptyBtn.addEventListener('click', function () {
      handleCleanupEmptyClick(elements);
    });

    elements.openCleanupDialogBtn.addEventListener('click', function () {
      openCleanupDialog(elements);
    });

    elements.cleanupCancelBtn.addEventListener('click', function () {
      closeCleanupDialog(elements);
    });

    elements.cleanupConfirmBtn.addEventListener('click', function () {
      handleCleanupDialogConfirm(elements);
    });

    elements.cleanupInput.addEventListener('keydown', function (event) {
      if (event.key !== 'Enter' || elements.cleanupDialog.hidden) {
        return;
      }
      event.preventDefault();
      handleCleanupDialogConfirm(elements);
    });

    elements.openAddDialogBtn.addEventListener('click', function () {
      openAddDialog(elements);
    });

    elements.dialogCancelBtn.addEventListener('click', function () {
      closeAddDialog(elements);
    });

    elements.dialogConfirmBtn.addEventListener('click', function () {
      handleDialogConfirm(elements);
    });

    elements.dialogInput.addEventListener('keydown', function (event) {
      if (event.key !== 'Enter' || elements.dialog.hidden) {
        return;
      }
      event.preventDefault();
      handleDialogConfirm(elements);
    });

    document.addEventListener('keydown', function (event) {
      if (!elements || !elements.dialog) {
        return;
      }

      if (event.key === 'Tab' && isAnyDialogOpen(elements)) {
        trapFocusWithin(getActiveDialog(elements), event);
        return;
      }

      if (event.key === 'Escape' && isAnyDialogOpen(elements)) {
        closeActiveDialog(elements);
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
        loadInitialReadOnlyData(elements);
      } else {
        openLoginDialog(elements);
      }
    } catch (e) {
      openLoginDialog(elements);
    }
  }

  async function handleLogin(elements) {
    var password = elements.passwordInput.value;
    if (!password) {
      setLoginStatus(elements, '请输入管理员密码。');
      elements.passwordInput.focus();
      return;
    }

    setLoginStatus(elements, '');
    setElementDisabled(elements.loginConfirmBtn, true);
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
        loadInitialReadOnlyData(elements);
        appendLog(elements, '认证成功，欢迎进入管理系统');
      }
    } catch (e) {
      setLoginStatus(elements, '认证失败：' + e.message);
      elements.passwordInput.focus();
    } finally {
      setElementDisabled(elements.loginConfirmBtn, false);
    }
  }

  function setupAutoLogout(elements, seconds) {
    if (authState.logoutTimer) clearTimeout(authState.logoutTimer);
    if (seconds <= 0) return;

    authState.logoutTimer = setTimeout(function () {
      authState.authenticated = false;
      setLoginStatus(elements, '会话已过期，请重新登录。');
      openLoginDialog(elements);
    }, seconds * 1000);
  }

  function setLoginStatus(elements, message) {
    if (!elements || !elements.loginStatus) {
      return;
    }
    elements.loginStatus.textContent = String(message || '');
  }

  function openLoginDialog(elements) {
    setDialogOpenState(elements.loginDialog, true, {
      focusElement: elements.passwordInput
    });
    setPageInteractionState(document.getElementById('admin-manage-page'), false);
  }

  function closeLoginDialog(elements) {
    setLoginStatus(elements, '');
    setDialogOpenState(elements.loginDialog, false, {
      skipFocusRestore: true
    });
    setPageInteractionState(document.getElementById('admin-manage-page'), true);
  }

  function getActiveDialog(elements) {
    return getVisibleDialog([
      elements && elements.loginDialog,
      elements && elements.cleanupDialog,
      elements && elements.dialog
    ]);
  }

  function isAnyDialogOpen(elements) {
    return !!getActiveDialog(elements);
  }

  async function loadInitialReadOnlyData(elements) {
    await syncReadOnlyData(elements, {
      chatsErrorPrefix: '读取群组列表失败',
      statsErrorPrefix: '读取统计信息失败'
    });
    await resumeActiveJobPolling(elements);
  }

  async function fetchChatsIntoSelect(elements) {
    var data = await fetchJSON('/api/admin/chats');
    var chats = normalizeChats(data);
    renderChatOptions(elements.scopeSelect, chats);
  }

  async function loadChatsIntoSelect(elements, errorPrefix) {
    try {
      await fetchChatsIntoSelect(elements);
    } catch (error) {
      appendLog(elements, String(errorPrefix || '读取群组列表失败') + '：' + error.message);
      return false;
    }
    return true;
  }

  async function fetchStatsBySelection(selectedChatId) {
    var statsPath = '/api/admin/stats';

    if (isChatScopeValue(selectedChatId)) {
      statsPath += '?chat_id=' + encodeURIComponent(selectedChatId);
    }

    return fetchJSON(statsPath);
  }

  async function loadStatsByCurrentSelection(elements, errorPrefix) {
    var selectedChatId = elements.scopeSelect.value;
    try {
      var data = await fetchStatsBySelection(selectedChatId);
      applyStatsToHeader(elements, data, selectedChatId);
    } catch (error) {
      appendLog(elements, String(errorPrefix || '读取统计信息失败') + '：' + error.message);
      return false;
    }
    return true;
  }

  async function syncReadOnlyData(elements, options) {
    if (!elements || !elements.scopeSelect) {
      return false;
    }

    var opts = options || {};
    var preserveSelection = !!opts.preserveSelection;
    var previousSelection = preserveSelection
      ? String(elements.scopeSelect.value || 'all')
      : '';

    var chatsLoaded = await loadChatsIntoSelect(
      elements,
      opts.chatsErrorPrefix || '读取群组列表失败'
    );
    if (!chatsLoaded) {
      return false;
    }

    if (preserveSelection) {
      var selectElement = elements.scopeSelect;
      var hasPreviousOption = Array.prototype.some.call(selectElement.options, function (option) {
        return option && String(option.value) === previousSelection;
      });
      selectElement.value = hasPreviousOption ? previousSelection : 'all';
      updateControlVisibility(elements);
    }

    return loadStatsByCurrentSelection(
      elements,
      opts.statsErrorPrefix || '读取统计信息失败'
    );
  }

  async function refreshReadOnlyDataAfterJob(elements) {
    return syncReadOnlyData(elements, {
      preserveSelection: true,
      chatsErrorPrefix: '刷新群组列表失败',
      statsErrorPrefix: '刷新统计失败'
    });
  }

  async function resumeActiveJobPolling(elements) {
    if (jobPollState.isPolling) {
      return;
    }

    try {
      var payload = await fetchJSON('/api/admin/jobs/active');
      var job = payload && payload.job && typeof payload.job === 'object' && !Array.isArray(payload.job)
        ? payload.job
        : null;
      var jobId = job && job.job_id ? String(job.job_id) : '';
      var status = job && typeof job.status === 'string' ? job.status.trim().toLowerCase() : '';
      if (!jobId || (status !== 'queued' && status !== 'running')) {
        return;
      }
      appendLog(elements, '检测到正在执行的任务，继续监控：' + jobId);
      startJobPolling(elements, jobId);
    } catch (error) {
      appendLog(elements, '检查正在执行的任务失败：' + error.message);
    }
  }

  function getTargetScopeLabel(target) {
    return shared.getTargetScopeLabel(target);
  }

  function confirmAction(elements, confirmText, cancelMessage) {
    if (window.confirm(confirmText)) {
      return true;
    }
    appendLog(elements, cancelMessage);
    return false;
  }

  async function postJSON(url, payload) {
    return sharedPostJSON(url, payload, {
      onUnauthorized: handleUnauthorizedResponse
    });
  }

  async function createJobAndStartPolling(elements, options) {
    var payload = await postJSON(options.url, options.requestPayload);
    var jobId = getCreatedJobId(payload);
    appendLog(elements, String(options.successMessage || '').replace('{jobId}', jobId));

    if (typeof options.onSuccess === 'function') {
      options.onSuccess(jobId);
    }
    startJobPolling(elements, jobId);
    return jobId;
  }

  function applyStatsToHeader(elements, payload, selectedChatId) {
    var data = payload && payload.data ? payload.data : payload;

    if (isChatScopeValue(selectedChatId)) {
      var targetName = pickFirstText(
        data && data.chat_name,
        data && data.chat_title,
        getSelectedOptionLabel(elements.scopeSelect, selectedChatId),
        '未知目标'
      );
      elements.statScope.textContent = targetName || '未知目标';
      setStatsLineText(elements.statScope, '当前目标：', '');

      elements.statMessages.textContent = pickFirstNumber(
        data && data.message_count,
        data && data.msg_count,
        '--'
      );
      setStatsLineText(elements.statMessages, '消息数量 ', '');
      return;
    }

    elements.statScope.textContent = pickFirstNumber(
      data && data.chat_count,
      data && data.scope_count,
      data && data.total_chats,
      data && data.count,
      '--'
    );
    setStatsLineText(elements.statScope, '当前共有 ', ' 个频道/群组');

    elements.statMessages.textContent = pickFirstNumber(
      data && data.message_count,
      data && data.total_messages,
      data && data.msg_count,
      '--'
    );
    setStatsLineText(elements.statMessages, '消息数量 ', '');
  }

  async function handleCleanupDialogConfirm(elements) {
    var keyword = (elements.cleanupInput.value || '').trim();
    if (!keyword) {
      appendLog(elements, '请输入需要清理的字段');
      elements.cleanupInput.focus();
      return;
    }

    var target = getCurrentTargetInfo(elements);
    if (target.isNone) {
      appendLog(elements, '请选择“全部”或某一个群组后再执行垃圾清理');
      return;
    }

    var scopeLabel = getTargetScopeLabel(target);
    var confirmText = '确认执行垃圾清理？关键字：' + keyword + '；范围：' + scopeLabel + '。';
    if (!confirmAction(elements, confirmText, '已取消垃圾清理操作')) {
      return;
    }

    var requestPayload = {
      keyword: keyword
    };

    try {
      requestPayload = Object.assign(
        requestPayload,
        buildScopeRequestPayload(target, 'chat_id 参数非法')
      );
      requestPayload.confirm = 'CLEANUP:' + requestPayload.scope + ':' + getConfirmationTarget(requestPayload) + ':' + keyword;
      await createJobAndStartPolling(elements, {
        url: '/api/admin/jobs/cleanup',
        requestPayload: requestPayload,
        successMessage: '垃圾清理任务已创建：{jobId}，范围：' + scopeLabel,
        onSuccess: function () {
          elements.cleanupInput.value = '';
          closeCleanupDialog(elements);
        }
      });
    } catch (error) {
      appendLog(elements, '创建垃圾清理任务失败：' + error.message);
    }
  }


  async function handleCleanupEmptyClick(elements) {
    var target = getCurrentTargetInfo(elements);
    if (target.isNone) {
      appendLog(elements, '请选择“全部”或某一个群组后再执行不可搜索数据清理');
      return;
    }

    var scopeLabel = getTargetScopeLabel(target);
    var confirmText = '确认执行不可搜索数据清理？范围：' + scopeLabel + '。';
    if (!confirmAction(elements, confirmText, '已取消不可搜索数据清理操作')) {
      return;
    }

    try {
      var requestPayload = buildScopeRequestPayload(target, 'chat_id 参数非法');
      requestPayload.confirm = 'CLEANUP_EMPTY:' + requestPayload.scope + ':' + getConfirmationTarget(requestPayload);
      await createJobAndStartPolling(elements, {
        url: '/api/admin/jobs/cleanup-empty',
        requestPayload: requestPayload,
        successMessage: '不可搜索数据清理任务已创建：{jobId}，范围：' + scopeLabel
      });
    } catch (error) {
      appendLog(elements, '创建不可搜索数据清理任务失败：' + error.message);
    }
  }

  async function handleStartUpdateClick(elements) {
    var target = getCurrentTargetInfo(elements);
    if (!target.isChat && !target.isAll) {
      appendLog(elements, '请选择具体群组/频道后再执行更新');
      return;
    }

    var confirmText = target.isAll
      ? '确认执行增量更新全部群聊？'
      : buildNamedConfirmText('确认执行增量更新？', '确认执行增量更新：', target.label);

    if (!confirmAction(elements, confirmText, '已取消更新操作')) {
      return;
    }

    var requestPayload = {
      chat_id: 'all'
    };

    if (!target.isAll) {
      requestPayload.chat_id = getRequiredIntegerChatId(
        target,
        'chat_id 参数非法'
      );
    }

    try {
      await createJobAndStartPolling(elements, {
        url: '/api/admin/jobs/update',
        requestPayload: requestPayload,
        successMessage: '增量更新任务已创建：{jobId}'
      });
    } catch (error) {
      appendLog(elements, '创建增量更新任务失败：' + error.message);
    }
  }

  async function handleStopJobClick(elements) {
    var jobId = String(jobPollState.jobId || '').trim();
    if (!jobId) {
      appendLog(elements, '当前没有正在轮询的任务');
      return;
    }

    if (!window.confirm('确认请求停止当前任务？已开始抓取的群组会继续完成，之后不再启动新的群组。')) {
      appendLog(elements, '已取消停止请求');
      return;
    }

    setElementDisabled(elements.stopJobBtn, true);
    try {
      var payload = await postJSON('/api/admin/jobs/' + encodeURIComponent(jobId) + '/stop', {});
      if (!payload || payload.ok === false) {
        throw new Error((payload && payload.error) || '停止请求失败');
      }
      jobPollState.stopRequested = true;
      appendLog(elements, '已请求停止：等待当前并发中的群组完成');
      setStopButtonState(elements);
    } catch (error) {
      appendLog(elements, '请求停止失败：' + error.message);
      setStopButtonState(elements);
    }
  }

  async function handleDeleteDataClick(elements) {
    var target = getCurrentTargetInfo(elements);
    if (!target.isChat) {
      appendLog(elements, '请选择具体群组/频道后再执行删除');
      return;
    }

    var confirmText = buildNamedConfirmText(
      '确认删除该群/频道数据？',
      '确认删除该群/频道数据：',
      target.label
    );

    if (!confirmAction(elements, confirmText, '已取消删除操作')) {
      return;
    }

    try {
      var chatId = getRequiredIntegerChatId(target, '当前目标 ID 非法');
      await createJobAndStartPolling(elements, {
        url: '/api/admin/jobs/delete',
        requestPayload: {
          chat_id: chatId,
          confirm: 'DELETE:' + chatId
        },
        successMessage: '删除任务已创建：{jobId}'
      });
    } catch (error) {
      appendLog(elements, '创建删除任务失败：' + error.message);
    }
  }

  async function handleDeleteEmptyChatsClick(elements) {
    var target = getCurrentTargetInfo(elements);
    if (!target.isAll) {
      appendLog(elements, '请选择“全部”后再删除零消息群组');
      return;
    }

    var confirmText = '确认删除所有消息数量为 0 的群组/频道？\n系统会再次检查真实消息表，避免误删仍有消息的群组。';
    if (!confirmAction(elements, confirmText, '已取消删除零消息群组操作')) {
      return;
    }

    try {
      await createJobAndStartPolling(elements, {
        url: '/api/admin/jobs/delete-empty-chats',
        requestPayload: {
          confirm: 'DELETE_EMPTY_CHATS'
        },
        successMessage: '零消息群组删除任务已创建：{jobId}'
      });
    } catch (error) {
      appendLog(elements, '创建零消息群组删除任务失败：' + error.message);
    }
  }


  function startJobPolling(elements, jobId) {
    var normalizedJobId = String(jobId || '').trim();
    if (!normalizedJobId) {
      return;
    }

    stopJobPolling(undefined, elements);

    jobPollState.pollToken += 1;
    jobPollState.jobId = normalizedJobId;
    jobPollState.lastSeq = 0;
    jobPollState.startedAt = Date.now();
    jobPollState.pollCount = 0;
    jobPollState.retryCount = 0;
    jobPollState.lastProgressKey = '';
    jobPollState.stopRequested = false;
    jobPollState.isPolling = true;
    setAdminControlsBusy(elements, true);

    pollJobProgress(elements);
  }

  function stopJobPolling(expectedToken, elements) {
    if (typeof expectedToken === 'number' && jobPollState.pollToken !== expectedToken) {
      return;
    }
    if (jobPollState.timerId) {
      window.clearTimeout(jobPollState.timerId);
    }
    jobPollState.timerId = null;
    jobPollState.isPolling = false;
    jobPollState.stopRequested = false;
    setAdminControlsBusy(elements, false);
  }

  function isPollContextActive(jobId, pollToken) {
    return jobPollState.isPolling
      && jobPollState.jobId === jobId
      && jobPollState.pollToken === pollToken;
  }

  function scheduleJobPolling(elements, jobId, pollToken) {
    scheduleJobPollingWithDelay(elements, jobId, pollToken, JOB_POLL_INTERVAL_MS);
  }

  function scheduleJobPollingWithDelay(elements, jobId, pollToken, delayMs) {
    if (!isPollContextActive(jobId, pollToken)) {
      return;
    }
    jobPollState.timerId = window.setTimeout(function () {
      pollJobProgress(elements);
    }, Math.max(250, Number(delayMs) || JOB_POLL_INTERVAL_MS));
  }

  function buildSnapshotProgressMessage(snapshot) {
    return shared.buildSnapshotProgressMessage(snapshot);
  }

  async function pollJobProgress(elements) {
    if (!jobPollState.isPolling || !jobPollState.jobId) {
      return;
    }

    var jobId = jobPollState.jobId;
    var pollToken = jobPollState.pollToken;
    if (!isPollContextActive(jobId, pollToken)) {
      return;
    }

    jobPollState.pollCount += 1;
    var elapsed = Date.now() - jobPollState.startedAt;
    if (jobPollState.pollCount > JOB_POLL_MAX_COUNT || elapsed > JOB_POLL_MAX_DURATION_MS) {
      if (!isPollContextActive(jobId, pollToken)) {
        return;
      }
      appendLog(elements, '任务日志轮询已停止：达到轮询上限');
      stopJobPolling(pollToken, elements);
      return;
    }

    try {
      if (!isPollContextActive(jobId, pollToken)) {
        return;
      }
      var logsPayload = await fetchJSON(
        '/api/admin/jobs/'
          + encodeURIComponent(jobId)
          + '/logs?after_seq='
          + encodeURIComponent(String(jobPollState.lastSeq || 0))
      );
      if (!isPollContextActive(jobId, pollToken)) {
        return;
      }

      var logs = logsPayload && Array.isArray(logsPayload.logs) ? logsPayload.logs : [];
      for (var i = 0; i < logs.length; i += 1) {
        if (!isPollContextActive(jobId, pollToken)) {
          return;
        }
        var line = logs[i];
        if (!line || typeof line.message !== 'string') {
          continue;
        }
        appendLog(elements, line.message);
        if (typeof line.seq === 'number' && Number.isFinite(line.seq)) {
          jobPollState.lastSeq = Math.max(jobPollState.lastSeq, line.seq);
        }
      }

      var snapshotPayload = await fetchJSON('/api/admin/jobs/' + encodeURIComponent(jobId));
      if (!isPollContextActive(jobId, pollToken)) {
        return;
      }

      var isSnapshotObject = snapshotPayload && typeof snapshotPayload === 'object' && !Array.isArray(snapshotPayload);
      var snapshotOk = isSnapshotObject && snapshotPayload.ok !== false;
      var snapshot = snapshotOk && snapshotPayload.job && typeof snapshotPayload.job === 'object' && !Array.isArray(snapshotPayload.job)
        ? snapshotPayload.job
        : null;
      var status = snapshot && typeof snapshot.status === 'string' ? snapshot.status.trim() : '';

      if (!snapshot || !status) {
        if (!isPollContextActive(jobId, pollToken)) {
          return;
        }
        appendLog(elements, '任务状态响应异常，已停止轮询');
        stopJobPolling(pollToken, elements);
        return;
      }

      if (status !== 'queued' && status !== 'running' && status !== 'done' && status !== 'error') {
        if (!isPollContextActive(jobId, pollToken)) {
          return;
        }
        appendLog(elements, '任务状态异常：' + status + '，已停止轮询');
        stopJobPolling(pollToken, elements);
        return;
      }

      jobPollState.retryCount = 0;
      var progressState = buildSnapshotProgressMessage(snapshot);
      jobPollState.stopRequested = !!snapshot.stop_requested;
      setStopButtonState(elements);
      if (progressState.key && progressState.key !== jobPollState.lastProgressKey) {
        jobPollState.lastProgressKey = progressState.key;
        if (progressState.message) {
          appendLog(elements, progressState.message);
        }
      }

      if (status === 'done') {
        if (!isPollContextActive(jobId, pollToken)) {
          return;
        }
        appendLog(elements, '任务执行完成');
        stopJobPolling(pollToken, elements);
        try {
          await refreshReadOnlyDataAfterJob(elements);
        } catch (refreshError) {
          appendLog(elements, '刷新统计失败：' + refreshError.message);
        }
        return;
      }
      if (status === 'error') {
        if (!isPollContextActive(jobId, pollToken)) {
          return;
        }
        appendLog(elements, '任务执行失败，请检查日志');
        stopJobPolling(pollToken, elements);
        return;
      }
    } catch (error) {
      if (!isPollContextActive(jobId, pollToken)) {
        return;
      }
      jobPollState.retryCount += 1;
      if (jobPollState.retryCount > JOB_POLL_RETRY_MAX_COUNT) {
        appendLog(elements, '任务日志轮询失败次数过多，已停止轮询：' + error.message);
        stopJobPolling(pollToken, elements);
        return;
      }
      appendLog(
        elements,
        '任务日志轮询失败，稍后自动重试（'
          + jobPollState.retryCount
          + '/'
          + JOB_POLL_RETRY_MAX_COUNT
          + '）：'
          + error.message
      );
      scheduleJobPollingWithDelay(
        elements,
        jobId,
        pollToken,
        JOB_POLL_RETRY_BASE_MS * Math.min(jobPollState.retryCount, 5)
      );
      return;
    }

    scheduleJobPolling(elements, jobId, pollToken);
  }

  async function fetchJSON(url, options) {
    return sharedFetchJSON(url, Object.assign({}, options || {}, {
      onUnauthorized: handleUnauthorizedResponse
    }));
  }

  function handleUnauthorizedResponse() {
    authState.authenticated = false;
    var els = getElements();
    if (els) {
      setLoginStatus(els, '会话已过期，请重新登录。');
      openLoginDialog(els);
    }
  }

  function updateControlVisibility(elements) {
    var scopeValue = String(elements.scopeSelect.value || 'none');
    var hasSelectedTarget = !isNoneScopeValue(scopeValue);
    var hasChatTarget = isChatScopeValue(scopeValue);
    var hasAllTarget = isAllScopeValue(scopeValue);

    elements.deleteDataBtn.hidden = !hasChatTarget;
    elements.deleteEmptyChatsBtn.hidden = !hasAllTarget;
    elements.startUpdateBtn.hidden = !hasChatTarget && !hasAllTarget;
    elements.startUpdateBtn.textContent = hasAllTarget ? '增量更新全部群聊' : '增量更新当前群聊';
    elements.stopJobBtn.hidden = !jobPollState.isPolling;
    elements.cleanupEmptyBtn.hidden = !hasSelectedTarget;
    elements.openCleanupDialogBtn.hidden = !hasSelectedTarget;

    setAdminControlsBusy(elements, jobPollState.isPolling);
  }

  function setStopButtonState(elements) {
    if (!elements || !elements.stopJobBtn) {
      return;
    }
    elements.stopJobBtn.hidden = !jobPollState.isPolling;
    elements.stopJobBtn.textContent = jobPollState.stopRequested ? '停止请求已发送' : '停止任务';
    setElementDisabled(elements.stopJobBtn, !jobPollState.isPolling || jobPollState.stopRequested);
  }

  function setAdminControlsBusy(elements, isBusy) {
    if (!elements) {
      return;
    }

    var disabled = !!isBusy;
    setElementDisabled(elements.scopeSelect, disabled);
    setElementDisabled(elements.startUpdateBtn, disabled);
    setStopButtonState(elements);
    setElementDisabled(elements.deleteDataBtn, disabled);
    setElementDisabled(elements.deleteEmptyChatsBtn, disabled);
    setElementDisabled(elements.cleanupEmptyBtn, disabled);
    setElementDisabled(elements.openCleanupDialogBtn, disabled);
    setElementDisabled(elements.cleanupConfirmBtn, disabled);
    setElementDisabled(elements.openAddDialogBtn, disabled);
    setElementDisabled(elements.dialogConfirmBtn, disabled);

    if (elements.logContainer && typeof elements.logContainer.setAttribute === 'function') {
      elements.logContainer.setAttribute('aria-busy', disabled ? 'true' : 'false');
    }
  }

  function openAddDialog(elements) {
    setDialogOpenState(elements.dialog, true, {
      focusElement: elements.dialogInput
    });
  }

  function closeAddDialog(elements, options) {
    setDialogOpenState(elements.dialog, false, {
      restoreFocusElement: elements.openAddDialogBtn,
      skipFocusRestore: options && options.skipFocusRestore
    });
  }

  function openCleanupDialog(elements) {
    setDialogOpenState(elements.cleanupDialog, true, {
      focusElement: elements.cleanupInput
    });
  }

  function closeCleanupDialog(elements, options) {
    setDialogOpenState(elements.cleanupDialog, false, {
      restoreFocusElement: elements.openCleanupDialogBtn,
      skipFocusRestore: options && options.skipFocusRestore
    });
  }

  function closeActiveDialog(elements) {
    if (elements && elements.loginDialog && !elements.loginDialog.hidden) {
      // 登录对话框不允许通过 Esc 关闭
      return;
    }
    if (elements && elements.cleanupDialog && !elements.cleanupDialog.hidden) {
      closeCleanupDialog(elements);
      return;
    }
    if (elements && elements.dialog && !elements.dialog.hidden) {
      closeAddDialog(elements);
    }
  }

  async function handleDialogConfirm(elements) {
    var value = elements && elements.dialogInput ? elements.dialogInput.value.trim() : '';

    if (!value) {
      appendLog(elements, '请输入群组名称或链接');
      return;
    }

    var preview = buildDialogTargetPreview(value);
    if (!confirmAction(elements, '确认新增/更新抓取目标：' + preview + '？', '已取消新增抓取目标')) {
      return;
    }

    try {
      await createJobAndStartPolling(elements, {
        url: '/api/admin/jobs/harvest',
        requestPayload: {
          target: value
        },
        successMessage: '抓取任务已创建：{jobId}',
        onSuccess: function () {
          elements.dialogInput.value = '';
          closeAddDialog(elements);
        }
      });
    } catch (error) {
      appendLog(elements, '创建抓取任务失败：' + error.message);
    }
  }

})();
