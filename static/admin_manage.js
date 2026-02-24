(function () {
  'use strict';

  var JOB_POLL_INTERVAL_MS = 3000;
  var JOB_POLL_MAX_COUNT = 28800;
  var JOB_POLL_MAX_DURATION_MS = 86400000;

  var jobPollState = {
    jobId: '',
    lastSeq: 0,
    timerId: null,
    isPolling: false,
    startedAt: 0,
    pollCount: 0,
    pollToken: 0
  };

  document.addEventListener('DOMContentLoaded', function () {
    var elements = getElements();
    if (!elements) {
      return;
    }

    initializeUI(elements);
    bindEvents(elements);
    loadInitialReadOnlyData(elements);

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
      incrementalCheckbox: document.getElementById('admin-incremental-checkbox'),
      incrementalLabel: document.querySelector('label[for="admin-incremental-checkbox"]'),
      startUpdateBtn: document.getElementById('admin-start-update-btn'),
      deleteDataBtn: document.getElementById('admin-delete-data-btn'),
      logContainer: document.getElementById('admin-log-container'),
      clearLogsBtn: document.getElementById('admin-clear-logs-btn'),
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
      statMessages: document.getElementById('admin-stat-messages')
    };

    var requiredKeys = [
      'scopeSelect',
      'incrementalCheckbox',
      'incrementalLabel',
      'startUpdateBtn',
      'deleteDataBtn',
      'logContainer',
      'clearLogsBtn',
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
      'statMessages'
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
    elements.clearLogsBtn.hidden = true;
    closeAddDialog(elements, { skipFocusRestore: true });
    closeCleanupDialog(elements, { skipFocusRestore: true });
  }

  function bindEvents(elements) {
    elements.scopeSelect.addEventListener('change', function () {
      updateControlVisibility(elements);
      loadStatsByCurrentSelection(elements);
    });

    elements.incrementalCheckbox.addEventListener('change', function () {
      updateControlVisibility(elements);
    });

    elements.clearLogsBtn.addEventListener('click', function () {
      clearLogs(elements);
    });

    elements.startUpdateBtn.addEventListener('click', function () {
      handleStartUpdateClick(elements);
    });

    elements.deleteDataBtn.addEventListener('click', function () {
      handleDeleteDataClick(elements);
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
        trapDialogFocus(elements, event);
        return;
      }

      if (event.key === 'Escape' && isAnyDialogOpen(elements)) {
        closeActiveDialog(elements);
      }
    });
  }

  function getActiveDialog(elements) {
    if (elements && elements.cleanupDialog && !elements.cleanupDialog.hidden) {
      return elements.cleanupDialog;
    }
    if (elements && elements.dialog && !elements.dialog.hidden) {
      return elements.dialog;
    }
    return null;
  }

  function isAnyDialogOpen(elements) {
    return !!getActiveDialog(elements);
  }

  function getDialogFocusableElements(elements) {
    var dialog = getActiveDialog(elements);
    if (!dialog) {
      return [];
    }

    var selector = [
      'a[href]',
      'button',
      'input',
      'select',
      'textarea',
      '[tabindex]'
    ].join(', ');

    var nodes = Array.prototype.slice.call(dialog.querySelectorAll(selector));
    return nodes.filter(function (node) {
      if (!node || node.disabled) {
        return false;
      }
      if (node.hidden) {
        return false;
      }
      if (node.getAttribute('aria-hidden') === 'true') {
        return false;
      }
      if (node.getAttribute('tabindex') === '-1') {
        return false;
      }
      return node.getClientRects().length > 0;
    });
  }

  function trapDialogFocus(elements, event) {
    if (!event || !elements || !isAnyDialogOpen(elements)) {
      return;
    }

    var focusable = getDialogFocusableElements(elements);
    if (!focusable.length) {
      event.preventDefault();
      return;
    }

    var activeElement = document.activeElement;
    var first = focusable[0];
    var last = focusable[focusable.length - 1];
    var isShift = !!event.shiftKey;

    if (focusable.indexOf(activeElement) === -1) {
      event.preventDefault();
      (isShift ? last : first).focus();
      return;
    }

    if (!isShift && activeElement === last) {
      event.preventDefault();
      first.focus();
      return;
    }

    if (isShift && activeElement === first) {
      event.preventDefault();
      last.focus();
    }
  }

  async function loadInitialReadOnlyData(elements) {
    await loadChatsIntoSelect(elements);
    await loadStatsByCurrentSelection(elements);
  }

  async function loadChatsIntoSelect(elements) {
    try {
      var data = await fetchJSON('/api/admin/chats');
      var chats = normalizeChats(data);
      renderChatOptions(elements.scopeSelect, chats);
    } catch (error) {
      appendLog(elements, '读取群组列表失败：' + error.message);
    }
  }

  function normalizeChats(payload) {
    // 主字段契约：chat_title/message_count；兼容字段仅兜底，后续可移除。
    function normalizeChatItem(chat) {
      if (!chat || typeof chat !== 'object') {
        return null;
      }

      var chatId = chat.chat_id;
      if (chatId === undefined || chatId === null) {
        return null;
      }

      var chatTitle = '';
      if (typeof chat.chat_title === 'string' && chat.chat_title.trim()) {
        chatTitle = chat.chat_title.trim();
      } else if (typeof chat.chat_name === 'string' && chat.chat_name.trim()) {
        chatTitle = chat.chat_name.trim();
      } else if (typeof chat.title === 'string' && chat.title.trim()) {
        chatTitle = chat.title.trim();
      } else {
        chatTitle = String(chatId);
      }

      var messageCount = chat.message_count;
      if (messageCount === undefined || messageCount === null) {
        messageCount = chat.msg_count;
      }

      return {
        chat_id: chatId,
        chat_title: chatTitle,
        message_count: messageCount
      };
    }

    function normalizeChatList(items) {
      return items.map(normalizeChatItem).filter(function (item) {
        return !!item;
      });
    }

    if (Array.isArray(payload)) {
      return normalizeChatList(payload);
    }
    if (payload && Array.isArray(payload.items)) {
      return normalizeChatList(payload.items);
    }
    if (payload && Array.isArray(payload.chats)) {
      return normalizeChatList(payload.chats);
    }
    return [];
  }

  function renderChatOptions(selectElement, chats) {
    selectElement.innerHTML = '';

    var noneOption = document.createElement('option');
    noneOption.value = 'none';
    noneOption.textContent = '无';
    selectElement.appendChild(noneOption);

    var allOption = document.createElement('option');
    allOption.value = 'all';
    allOption.textContent = '全部';
    allOption.selected = true;
    selectElement.appendChild(allOption);

    chats.forEach(function (chat) {
      if (!chat || chat.chat_id === undefined || chat.chat_id === null) {
        return;
      }
      var option = document.createElement('option');
      var chatId = String(chat.chat_id);
      option.value = chatId;
      option.textContent = buildChatOptionText(chat, chatId);
      selectElement.appendChild(option);
    });
  }

  function buildChatOptionText(chat, fallbackId) {
    var title = (typeof chat.chat_title === 'string' && chat.chat_title.trim())
      ? chat.chat_title.trim()
      : fallbackId;
    var count = chat.message_count;

    return (count === undefined || count === null) ? title : title + '（' + String(count) + '）';
  }

  async function loadStatsByCurrentSelection(elements) {
    var selectedChatId = elements.scopeSelect.value;
    var statsPath = '/api/admin/stats';

    if (isChatScopeValue(selectedChatId)) {
      statsPath += '?chat_id=' + encodeURIComponent(selectedChatId);
    }

    try {
      var data = await fetchJSON(statsPath);
      applyStatsToHeader(elements, data, selectedChatId);
    } catch (error) {
      appendLog(elements, '读取统计信息失败：' + error.message);
    }
  }

  async function refreshReadOnlyDataAfterJob(elements) {
    if (!elements || !elements.scopeSelect) {
      return;
    }

    var previousSelection = String(elements.scopeSelect.value || 'all');

    try {
      await loadChatsIntoSelect(elements);
    } catch (error) {
      appendLog(elements, '刷新群组列表失败：' + error.message);
      return;
    }

    var selectElement = elements.scopeSelect;
    var hasPreviousOption = Array.prototype.some.call(selectElement.options, function (option) {
      return option && String(option.value) === previousSelection;
    });
    selectElement.value = hasPreviousOption ? previousSelection : 'all';

    updateControlVisibility(elements);

    try {
      await loadStatsByCurrentSelection(elements);
    } catch (error) {
      appendLog(elements, '刷新统计失败：' + error.message);
    }
  }

  function setStatsLineText(valueElement, prefixText, suffixText) {
    if (!valueElement || !valueElement.parentNode) {
      return;
    }

    var parent = valueElement.parentNode;
    var prefixNode = valueElement.previousSibling;
    if (!prefixNode || prefixNode.nodeType !== Node.TEXT_NODE) {
      if (prefixNode && prefixNode.nodeType === Node.ELEMENT_NODE) {
        prefixNode.textContent = '';
      }
      prefixNode = document.createTextNode('');
      parent.insertBefore(prefixNode, valueElement);
    }

    var suffixNode = valueElement.nextSibling;
    if (!suffixNode || suffixNode.nodeType !== Node.TEXT_NODE) {
      if (suffixNode && suffixNode.nodeType === Node.ELEMENT_NODE) {
        suffixNode.textContent = '';
      }
      suffixNode = document.createTextNode('');
      parent.insertBefore(suffixNode, valueElement.nextSibling);
    }

    prefixNode.nodeValue = String(prefixText || '');
    suffixNode.nodeValue = String(suffixText || '');
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

  function getSelectedOptionLabel(selectElement, value) {
    var option = Array.prototype.find.call(selectElement.options, function (opt) {
      return opt.value === value;
    });
    return option ? option.textContent : '';
  }

  function isAllScopeValue(value) {
    return String(value || '').trim() === 'all';
  }

  function isNoneScopeValue(value) {
    return String(value || '').trim() === 'none';
  }

  function isChatScopeValue(value) {
    var normalized = String(value || '').trim();
    return !!normalized && normalized !== 'none' && normalized !== 'all';
  }

  function getCurrentTargetInfo(elements) {
    var selectElement = elements && elements.scopeSelect;
    var scopeValue = selectElement ? String(selectElement.value || '') : '';
    var label = selectElement ? getSelectedOptionLabel(selectElement, scopeValue) : '';
    var trimmedLabel = typeof label === 'string' ? label.trim() : '';

    return {
      scopeValue: scopeValue,
      chatId: isChatScopeValue(scopeValue) ? scopeValue : '',
      label: trimmedLabel,
      isNone: isNoneScopeValue(scopeValue),
      isAll: isAllScopeValue(scopeValue),
      isChat: isChatScopeValue(scopeValue)
    };
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

    var scopeLabel = target.isAll ? '全部数据' : (target.label || target.chatId);
    var confirmText = '确认执行垃圾清理？关键字：' + keyword + '；范围：' + scopeLabel + '。';
    if (!window.confirm(confirmText)) {
      appendLog(elements, '已取消垃圾清理操作');
      return;
    }

    var requestPayload = {
      keyword: keyword,
      scope: target.isAll ? 'all' : 'chat'
    };

    if (!target.isAll) {
      var chatIdNumber = Number(target.chatId);
      if (!Number.isFinite(chatIdNumber) || !Number.isInteger(chatIdNumber)) {
        appendLog(elements, '创建垃圾清理任务失败：chat_id 参数非法');
        return;
      }
      requestPayload.chat_id = chatIdNumber;
    }

    try {
      var payload = await fetchJSON('/api/admin/jobs/cleanup', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestPayload)
      });
      var job = payload && payload.job ? payload.job : null;
      var jobId = job && job.job_id ? String(job.job_id) : '';
      if (!jobId) {
        throw new Error('任务创建成功但缺少 job_id');
      }

      appendLog(elements, '垃圾清理任务已创建：' + jobId + '，范围：' + scopeLabel);
      elements.cleanupInput.value = '';
      closeCleanupDialog(elements);
      startJobPolling(elements, jobId);
    } catch (error) {
      appendLog(elements, '创建垃圾清理任务失败：' + error.message);
    }
  }

  async function handleStartUpdateClick(elements) {
    var target = getCurrentTargetInfo(elements);
    if (!target.isChat && !target.isAll) {
      appendLog(elements, '请选择具体群组/频道后再执行更新');
      return;
    }

    if (target.isChat && (!elements.incrementalCheckbox || !elements.incrementalCheckbox.checked)) {
      appendLog(elements, '未启用增量更新，无法执行更新');
      return;
    }

    var confirmText = '确认执行增量更新？';
    if (target.isAll) {
      confirmText = '确认执行增量更新全部群聊？';
    } else if (target.label) {
      confirmText = '确认执行增量更新：' + target.label + '？';
    }

    if (!window.confirm(confirmText)) {
      appendLog(elements, '已取消更新操作');
      return;
    }

    var requestPayload = {
      chat_id: 'all',
      incremental: true
    };

    if (!target.isAll) {
      var chatIdNumber = Number(target.chatId);
      if (!Number.isFinite(chatIdNumber) || !Number.isInteger(chatIdNumber)) {
        appendLog(elements, '创建增量更新任务失败：chat_id 参数非法');
        return;
      }
      requestPayload.chat_id = chatIdNumber;
    }

    try {
      var payload = await fetchJSON('/api/admin/jobs/update', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestPayload)
      });
      var job = payload && payload.job ? payload.job : null;
      var jobId = job && job.job_id ? String(job.job_id) : '';
      if (!jobId) {
        throw new Error('任务创建成功但缺少 job_id');
      }

      appendLog(elements, '增量更新任务已创建：' + jobId);
      startJobPolling(elements, jobId);
    } catch (error) {
      appendLog(elements, '创建增量更新任务失败：' + error.message);
    }
  }

  async function handleDeleteDataClick(elements) {
    var target = getCurrentTargetInfo(elements);
    if (!target.isChat) {
      appendLog(elements, '请选择具体群组/频道后再执行删除');
      return;
    }

    var confirmText = '确认删除该群/频道数据？';
    if (target.label) {
      confirmText = '确认删除该群/频道数据：' + target.label + '？';
    }

    if (!window.confirm(confirmText)) {
      appendLog(elements, '已取消删除操作');
      return;
    }

    var chatIdNumber = Number(target.chatId);
    if (!Number.isFinite(chatIdNumber) || !Number.isInteger(chatIdNumber)) {
      appendLog(elements, '当前目标 ID 非法');
      return;
    }

    try {
      var payload = await fetchJSON('/api/admin/jobs/delete', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          chat_id: chatIdNumber
        })
      });
      var job = payload && payload.job ? payload.job : null;
      var jobId = job && job.job_id ? String(job.job_id) : '';
      if (!jobId) {
        throw new Error('任务创建成功但缺少 job_id');
      }

      appendLog(elements, '删除任务已创建：' + jobId);
      startJobPolling(elements, jobId);
    } catch (error) {
      appendLog(elements, '创建删除任务失败：' + error.message);
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
    setAdminControlsBusy(elements, false);
  }

  function isPollContextActive(jobId, pollToken) {
    return jobPollState.isPolling
      && jobPollState.jobId === jobId
      && jobPollState.pollToken === pollToken;
  }

  function scheduleJobPolling(elements, jobId, pollToken) {
    if (!isPollContextActive(jobId, pollToken)) {
      return;
    }
    jobPollState.timerId = window.setTimeout(function () {
      pollJobProgress(elements);
    }, JOB_POLL_INTERVAL_MS);
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
      var logsPayload = await fetchJSON('/api/admin/jobs/' + encodeURIComponent(jobId) + '/logs?after_seq=' + encodeURIComponent(String(jobPollState.lastSeq || 0)));
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
      appendLog(elements, '任务日志轮询失败：' + error.message);
      stopJobPolling(pollToken, elements);
      return;
    }

    scheduleJobPolling(elements, jobId, pollToken);
  }

  async function fetchJSON(url, options) {
    var requestOptions = options || {};
    var requestHeaders = requestOptions.headers || {};

    var response;
    try {
      response = await fetch(url, {
        method: requestOptions.method || 'GET',
        headers: requestHeaders,
        body: requestOptions.body
      });
    } catch (networkError) {
      throw new Error('网络请求失败');
    }

    if (!response.ok) {
      var errorMessage = 'HTTP ' + response.status;
      try {
        var errorPayload = await response.json();
        if (errorPayload && typeof errorPayload.error === 'string' && errorPayload.error.trim()) {
          errorMessage += ' ' + errorPayload.error.trim();
        }
      } catch (ignoreErrorPayload) {
        // ignore parse failure, keep HTTP status message.
      }
      throw new Error(errorMessage);
    }

    try {
      return await response.json();
    } catch (parseError) {
      throw new Error('响应 JSON 解析失败');
    }
  }

  function pickFirstText() {
    for (var i = 0; i < arguments.length; i += 1) {
      var value = arguments[i];
      if (typeof value === 'string' && value.trim()) {
        return value.trim();
      }
      if (typeof value === 'number') {
        return String(value);
      }
    }
    return '';
  }

  function pickFirstNumber() {
    for (var i = 0; i < arguments.length; i += 1) {
      var value = arguments[i];
      if (typeof value === 'number' && Number.isFinite(value)) {
        return String(value);
      }
      if (typeof value === 'string' && value.trim() !== '' && !Number.isNaN(Number(value))) {
        return String(Number(value));
      }
    }
    return '0';
  }

  function updateControlVisibility(elements) {
    var scopeValue = String(elements.scopeSelect.value || 'none');
    var hasSelectedTarget = !isNoneScopeValue(scopeValue);
    var hasChatTarget = isChatScopeValue(scopeValue);
    var hasAllTarget = isAllScopeValue(scopeValue);

    setElementHidden(elements.incrementalCheckbox, !hasChatTarget);
    setElementHidden(elements.incrementalLabel, !hasChatTarget);

    elements.deleteDataBtn.hidden = !hasChatTarget;
    elements.startUpdateBtn.hidden = (!hasChatTarget && !hasAllTarget) || (hasChatTarget && !elements.incrementalCheckbox.checked);
    elements.startUpdateBtn.textContent = hasAllTarget ? '增量更新全部群聊' : '增量更新当前群聊';
    elements.openCleanupDialogBtn.hidden = !hasSelectedTarget;

    setAdminControlsBusy(elements, jobPollState.isPolling);
  }

  function setElementDisabled(element, disabled) {
    if (!element || typeof element.disabled === 'undefined') {
      return;
    }
    element.disabled = !!disabled;
  }

  function setAdminControlsBusy(elements, isBusy) {
    if (!elements) {
      return;
    }

    var disabled = !!isBusy;
    setElementDisabled(elements.scopeSelect, disabled);
    setElementDisabled(elements.incrementalCheckbox, disabled);
    setElementDisabled(elements.startUpdateBtn, disabled);
    setElementDisabled(elements.deleteDataBtn, disabled);
    setElementDisabled(elements.openCleanupDialogBtn, disabled);
    setElementDisabled(elements.cleanupConfirmBtn, disabled);
    setElementDisabled(elements.openAddDialogBtn, disabled);
    setElementDisabled(elements.dialogConfirmBtn, disabled);

    if (elements.logContainer && typeof elements.logContainer.setAttribute === 'function') {
      elements.logContainer.setAttribute('aria-busy', disabled ? 'true' : 'false');
    }
  }

  function appendLog(elements, message) {
    if (!message) {
      return;
    }

    removePlaceholder(elements.logContainer);

    var line = document.createElement('p');
    line.textContent = String(message);
    elements.logContainer.appendChild(line);

    elements.logContainer.scrollTop = elements.logContainer.scrollHeight;
    elements.clearLogsBtn.hidden = false;
  }

  function clearLogs(elements) {
    elements.logContainer.textContent = '';
    ensurePlaceholder(elements.logContainer);
    elements.clearLogsBtn.hidden = true;
  }

  function ensurePlaceholder(container) {
    if (container.querySelector('[data-placeholder="true"]')) {
      return;
    }

    var placeholder = document.createElement('p');
    placeholder.textContent = '暂无日志';
    placeholder.setAttribute('data-placeholder', 'true');
    container.appendChild(placeholder);
  }

  function removePlaceholder(container) {
    var placeholder = container.querySelector('[data-placeholder="true"]');
    if (placeholder) {
      placeholder.remove();
    } else if (container.textContent.trim() === '暂无日志') {
      container.textContent = '';
    }
  }

  function openAddDialog(elements) {
    elements.dialog.hidden = false;
    elements.dialogInput.focus();
  }

  function closeAddDialog(elements, options) {
    var opts = options || {};
    elements.dialog.hidden = true;

    if (!opts.skipFocusRestore) {
      elements.openAddDialogBtn.focus();
    }
  }

  function openCleanupDialog(elements) {
    elements.cleanupDialog.hidden = false;
    elements.cleanupInput.focus();
  }

  function closeCleanupDialog(elements, options) {
    var opts = options || {};
    elements.cleanupDialog.hidden = true;

    if (!opts.skipFocusRestore) {
      elements.openCleanupDialogBtn.focus();
    }
  }

  function closeActiveDialog(elements) {
    if (elements && elements.cleanupDialog && !elements.cleanupDialog.hidden) {
      closeCleanupDialog(elements);
      return;
    }
    if (elements && elements.dialog && !elements.dialog.hidden) {
      closeAddDialog(elements);
    }
  }

  function buildDialogTargetPreview(value) {
    var text = String(value || '').trim();
    if (text.length <= 40) {
      return text;
    }
    return text.slice(0, 40) + '…';
  }

  async function handleDialogConfirm(elements) {
    var value = elements && elements.dialogInput ? elements.dialogInput.value.trim() : '';

    if (!value) {
      appendLog(elements, '请输入群组名称或链接');
      return;
    }

    var preview = buildDialogTargetPreview(value);
    if (!window.confirm('确认新增/更新抓取目标：' + preview + '？')) {
      appendLog(elements, '已取消新增抓取目标');
      return;
    }

    try {
      var payload = await fetchJSON('/api/admin/jobs/harvest', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          target: value
        })
      });
      var job = payload && payload.job ? payload.job : null;
      var jobId = job && job.job_id ? String(job.job_id) : '';
      if (!jobId) {
        throw new Error('任务创建成功但缺少 job_id');
      }

      appendLog(elements, '抓取任务已创建：' + jobId);
      elements.dialogInput.value = '';
      closeAddDialog(elements);
      startJobPolling(elements, jobId);
    } catch (error) {
      appendLog(elements, '创建抓取任务失败：' + error.message);
    }
  }

  function setElementHidden(element, hidden) {
    if (!element) {
      return;
    }
    element.hidden = hidden;
  }
})();
