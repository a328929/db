(function () {
  'use strict';

  var shared = window.AdminManageShared;
  var appendLog = shared.appendLog;
  var clearLogs = shared.clearLogs;
  var ensurePlaceholder = shared.ensurePlaceholder;
  var getCreatedJobId = shared.getCreatedJobId;
  var setDialogOpenState = shared.setDialogOpenState;
  var setElementDisabled = shared.setElementDisabled;
  var setPageInteractionState = shared.setPageInteractionState;
  var syncClearLogsButtonVisibility = shared.syncClearLogsButtonVisibility;

  var JOB_POLL_INTERVAL_MS = 3000;
  var JOB_POLL_RETRY_MAX_COUNT = 20;
  var JOB_POLL_RETRY_BASE_MS = 3000;

  var jobPollState = {
    jobId: '',
    lastSeq: 0,
    timerId: null,
    isPolling: false,
    pollToken: 0,
    retryCount: 0,
    lastProgressKey: '',
    onDone: null,
    doneMessage: '',
    errorMessage: ''
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
      sortSelect: document.getElementById('admin-channel-sort-select'),
      refreshChannelsBtn: document.getElementById('admin-channel-refresh-btn'),
      channelListToggleBtn: document.getElementById('admin-channel-list-toggle-btn'),
      channelCount: document.getElementById('admin-channel-count'),
      channelList: document.getElementById('admin-channel-list'),
      scanMissingBtn: document.getElementById('admin-scan-missing-btn'),
      refreshMissingBtn: document.getElementById('admin-refresh-missing-btn'),
      missingListToggleBtn: document.getElementById('admin-missing-list-toggle-btn'),
      missingStatus: document.getElementById('admin-missing-status'),
      missingList: document.getElementById('admin-missing-list'),
      scanAbsentBtn: document.getElementById('admin-scan-absent-btn'),
      refreshAbsentBtn: document.getElementById('admin-refresh-absent-btn'),
      absentListToggleBtn: document.getElementById('admin-absent-list-toggle-btn'),
      absentStatus: document.getElementById('admin-absent-status'),
      absentList: document.getElementById('admin-absent-list'),
      logContainer: document.getElementById('admin-channel-log-container'),
      clearLogsBtn: document.getElementById('admin-clear-channel-logs-btn'),
      loginDialog: document.getElementById('admin-login-dialog'),
      passwordInput: document.getElementById('admin-password-input'),
      loginConfirmBtn: document.getElementById('admin-login-confirm-btn')
    };

    var requiredKeys = [
      'sortSelect',
      'refreshChannelsBtn',
      'channelListToggleBtn',
      'channelCount',
      'channelList',
      'scanMissingBtn',
      'refreshMissingBtn',
      'missingListToggleBtn',
      'missingStatus',
      'missingList',
      'scanAbsentBtn',
      'refreshAbsentBtn',
      'absentListToggleBtn',
      'absentStatus',
      'absentList',
      'logContainer',
      'clearLogsBtn',
      'loginDialog',
      'passwordInput',
      'loginConfirmBtn'
    ];
    var missing = requiredKeys.filter(function (key) { return !elements[key]; });
    if (missing.length > 0) {
      console.warn('[admin_channels] Missing required elements:', missing.join(', '));
      return null;
    }
    return elements;
  }

  function initializeUI(elements) {
    ensurePlaceholder(elements.logContainer);
    syncClearLogsButtonVisibility(elements);
    setBusy(elements, false);
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
    elements.sortSelect.addEventListener('change', function () {
      loadChannels(elements);
    });
    elements.refreshChannelsBtn.addEventListener('click', function () {
      loadChannels(elements);
    });
    elements.channelListToggleBtn.addEventListener('click', function () {
      toggleListArea(elements.channelListToggleBtn, elements.channelList);
    });
    elements.refreshMissingBtn.addEventListener('click', function () {
      loadMissingChannels(elements);
    });
    elements.missingListToggleBtn.addEventListener('click', function () {
      toggleListArea(elements.missingListToggleBtn, elements.missingList);
    });
    elements.scanMissingBtn.addEventListener('click', function () {
      handleScanMissingClick(elements);
    });
    elements.refreshAbsentBtn.addEventListener('click', function () {
      loadAbsentChannels(elements);
    });
    elements.absentListToggleBtn.addEventListener('click', function () {
      toggleListArea(elements.absentListToggleBtn, elements.absentList);
    });
    elements.scanAbsentBtn.addEventListener('click', function () {
      handleScanAbsentClick(elements);
    });
    elements.clearLogsBtn.addEventListener('click', function () {
      clearLogs(elements);
    });
  }

  async function checkAuth(elements) {
    try {
      var data = await fetchJSON('/api/admin/auth/check');
      if (data.authenticated) {
        authState.authenticated = true;
        closeLoginDialog(elements);
        setupAutoLogout(data.remaining);
        await loadInitialData(elements);
        return;
      }
      openLoginDialog(elements);
    } catch (_error) {
      openLoginDialog(elements);
    }
  }

  async function handleLogin(elements) {
    var password = elements.passwordInput.value;
    if (!password) return;
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
        setupAutoLogout(data.expiry_duration);
        appendLog(elements, '认证成功，已进入频道管理');
        await loadInitialData(elements);
      }
    } catch (error) {
      alert('认证失败：' + error.message);
    }
  }

  function setupAutoLogout(seconds) {
    if (authState.logoutTimer) window.clearTimeout(authState.logoutTimer);
    if (seconds <= 0) return;
    authState.logoutTimer = window.setTimeout(function () {
      alert('会话已过期，请重新登录');
      window.location.reload();
    }, seconds * 1000);
  }

  function openLoginDialog(elements) {
    setDialogOpenState(elements.loginDialog, true, {
      focusElement: elements.passwordInput
    });
    setPageInteractionState(document.getElementById('admin-channels-page'), false);
  }

  function closeLoginDialog(elements) {
    setDialogOpenState(elements.loginDialog, false, { skipFocusRestore: true });
    setPageInteractionState(document.getElementById('admin-channels-page'), true);
  }

  async function loadInitialData(elements) {
    await loadChannels(elements);
    await loadMissingChannels(elements);
    await loadAbsentChannels(elements);
  }

  function formatDateTime(value) {
    var text = String(value || '').trim();
    if (!text) return '暂无';
    return text.replace('T', ' ').replace(/\.\d+.*$/, '');
  }

  function formatNumber(value) {
    var n = Number(value || 0);
    if (!Number.isFinite(n)) return '0';
    return String(Math.trunc(n)).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  }

  function createTextElement(tagName, className, text) {
    var el = document.createElement(tagName);
    if (className) el.className = className;
    el.textContent = String(text || '');
    return el;
  }

  function setListCollapsed(toggleButton, listElement, collapsed) {
    listElement.hidden = !!collapsed;
    toggleButton.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    toggleButton.textContent = collapsed ? '展开列表' : '收起列表';
  }

  function toggleListArea(toggleButton, listElement) {
    setListCollapsed(toggleButton, listElement, !listElement.hidden);
  }

  function createInfoPill(label, value) {
    var pill = document.createElement('span');
    pill.className = 'channel-info-pill';
    pill.appendChild(createTextElement('span', 'channel-info-label', label));
    pill.appendChild(createTextElement('strong', '', value || '暂无'));
    return pill;
  }

  function createChannelRecordItem(options) {
    var item = document.createElement('article');
    item.className = 'channel-list-item';

    var head = document.createElement('div');
    head.className = 'channel-item-head';

    var titleWrap = document.createElement('div');
    titleWrap.className = 'channel-item-title-wrap';
    titleWrap.appendChild(createTextElement('h3', 'channel-item-title', options.title));
    if (options.subtitle) {
      titleWrap.appendChild(createTextElement('p', 'channel-item-subtitle', options.subtitle));
    }
    head.appendChild(titleWrap);

    var metrics = document.createElement('div');
    metrics.className = 'channel-item-metrics';
    (options.metrics || []).forEach(function (metric) {
      metrics.appendChild(createInfoPill(metric.label, metric.value));
    });
    head.appendChild(metrics);
    item.appendChild(head);

    var meta = document.createElement('div');
    meta.className = 'channel-item-meta';
    (options.meta || []).forEach(function (entry) {
      meta.appendChild(createInfoPill(entry.label, entry.value));
    });
    if (meta.children.length > 0) item.appendChild(meta);

    if (options.actions) item.appendChild(options.actions);
    if (options.note) item.appendChild(createTextElement('p', 'missing-channel-note', options.note));
    return item;
  }

  function createChannelActions(item, elements, options) {
    var actions = document.createElement('div');
    var actionOptions = options || {};
    actions.className = 'channel-actions';

    if (item.telegram_app_link) {
      var appLink = document.createElement('a');
      appLink.href = item.telegram_app_link;
      appLink.textContent = '打开客户端';
      appLink.setAttribute('aria-label', '使用 Telegram 客户端打开该群组或频道');
      actions.appendChild(appLink);
    }

    if (item.telegram_web_link) {
      var webLink = document.createElement('a');
      webLink.href = item.telegram_web_link;
      webLink.target = '_blank';
      webLink.rel = 'noopener noreferrer';
      webLink.textContent = '网页入口';
      actions.appendChild(webLink);
    }

    var copyBtn = document.createElement('button');
    copyBtn.type = 'button';
    copyBtn.textContent = '复制信息';
    copyBtn.addEventListener('click', function () {
      copyChannelInfo(item, elements);
    });
    actions.appendChild(copyBtn);

    if (actionOptions.allowDelete) {
      var deleteBtn = document.createElement('button');
      deleteBtn.type = 'button';
      deleteBtn.className = 'danger-action';
      deleteBtn.textContent = '删除数据';
      deleteBtn.setAttribute('aria-label', '从数据库删除该群组或频道的全部数据');
      deleteBtn.addEventListener('click', function () {
        handleDeleteAbsentChannel(elements, item);
      });
      actions.appendChild(deleteBtn);
    }
    return actions;
  }

  function renderChannels(elements, channels) {
    elements.channelList.textContent = '';
    if (!Array.isArray(channels) || channels.length === 0) {
      var empty = document.createElement('div');
      empty.className = 'empty-box';
      empty.textContent = '暂无已入库群组或频道。';
      elements.channelList.appendChild(empty);
      elements.channelCount.textContent = '共 0 个群组/频道。';
      return;
    }

    channels.forEach(function (channel) {
      var parts = [];
      if (channel.chat_username) parts.push('@' + channel.chat_username);
      if (channel.chat_type) parts.push(channel.chat_type);
      elements.channelList.appendChild(
        createChannelRecordItem({
          title: channel.chat_title || ('Chat ' + channel.chat_id),
          subtitle: parts.join(' | '),
          metrics: [
            { label: '消息数', value: formatNumber(channel.message_count) },
            { label: '最后消息', value: formatDateTime(channel.last_message_at) }
          ],
          meta: [
            { label: 'chat_id', value: String(channel.chat_id) },
            { label: '用户名', value: channel.chat_username ? '@' + channel.chat_username : '' },
            { label: '类型', value: channel.chat_type || '' },
          ],
          actions: createChannelActions(channel, elements),
          note: channel.has_public_link
            ? ''
            : '私有群组通常没有稳定网页入口；客户端链接不可用时可复制信息后在 Telegram 中定位。'
        })
      );
    });
    elements.channelCount.textContent = '共 ' + channels.length + ' 个群组/频道。';
  }

  async function loadChannels(elements) {
    elements.channelCount.textContent = '正在读取列表...';
    try {
      var data = await fetchJSON(
        '/api/admin/channels?sort=' + encodeURIComponent(elements.sortSelect.value)
      );
      if (!data.ok) throw new Error(data.error || '读取失败');
      renderChannels(elements, data.channels || []);
    } catch (error) {
      elements.channelCount.textContent = '读取列表失败：' + error.message;
    }
  }

  function renderMissingChannels(elements, items) {
    elements.missingList.textContent = '';
    if (!Array.isArray(items) || items.length === 0) {
      var empty = document.createElement('div');
      empty.className = 'empty-box';
      empty.textContent = '暂无未入库扫描结果。';
      elements.missingList.appendChild(empty);
      elements.missingStatus.textContent = '暂无未入库扫描结果，可点击“扫描当前账号”。';
      return;
    }

    items.forEach(function (item) {
      var metaParts = [];
      if (item.chat_username) metaParts.push('@' + item.chat_username);
      if (item.chat_type) metaParts.push(item.chat_type);

      elements.missingList.appendChild(
        createChannelRecordItem({
          title: item.chat_title || ('Chat ' + item.chat_id),
          subtitle: metaParts.join(' | '),
          metrics: [
            { label: 'chat_id', value: String(item.chat_id) },
            { label: '扫描', value: formatDateTime(item.scanned_at) }
          ],
          meta: [
            { label: 'chat_id', value: String(item.chat_id) },
            { label: '用户名', value: item.chat_username ? '@' + item.chat_username : '' },
            { label: '类型', value: item.chat_type || '' },
          ],
          actions: createChannelActions(item, elements),
          note: item.has_public_link
            ? ''
            : '私有群组通常没有稳定网页入口；客户端链接不可用时可复制信息后在 Telegram 中定位。'
        })
      );
    });
    elements.missingStatus.textContent = '发现 ' + items.length + ' 个已加入但未入库的群组/频道。';
  }

  function copyChannelInfo(item, elements) {
    var text = [
      item.chat_title || '',
      'chat_id: ' + item.chat_id,
      item.chat_username ? '@' + item.chat_username : ''
    ].filter(Boolean).join('\n');

    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(function () {
        appendLog(elements, '已复制：' + (item.chat_title || item.chat_id));
      }).catch(function () {
        appendLog(elements, text);
      });
      return;
    }
    appendLog(elements, text);
  }

  async function loadMissingChannels(elements) {
    try {
      var data = await fetchJSON('/api/admin/channels/missing');
      if (!data.ok) throw new Error(data.error || '读取失败');
      renderMissingChannels(elements, data.items || []);
    } catch (error) {
      elements.missingStatus.textContent = '读取扫描结果失败：' + error.message;
    }
  }

  function renderAbsentChannels(elements, items) {
    elements.absentList.textContent = '';
    if (!Array.isArray(items) || items.length === 0) {
      var empty = document.createElement('div');
      empty.className = 'empty-box';
      empty.textContent = '暂无账号外数据库扫描结果。';
      elements.absentList.appendChild(empty);
      elements.absentStatus.textContent = '暂无账号外数据库扫描结果，可点击“扫描账号外数据”。';
      return;
    }

    items.forEach(function (item) {
      var metaParts = [];
      if (item.chat_username) metaParts.push('@' + item.chat_username);
      if (item.chat_type) metaParts.push(item.chat_type);

      elements.absentList.appendChild(
        createChannelRecordItem({
          title: item.chat_title || ('Chat ' + item.chat_id),
          subtitle: metaParts.join(' | '),
          metrics: [
            { label: '消息数', value: formatNumber(item.message_count) },
            { label: '更新', value: formatDateTime(item.last_seen_at) },
            { label: '扫描', value: formatDateTime(item.scanned_at) }
          ],
          meta: [
            { label: 'chat_id', value: String(item.chat_id) },
            { label: '用户名', value: item.chat_username ? '@' + item.chat_username : '' },
            { label: '类型', value: item.chat_type || '' },
          ],
          actions: createChannelActions(item, elements, { allowDelete: true }),
          note: item.has_public_link
            ? ''
            : '私有群组通常没有稳定网页入口；删除前可复制信息核对目标。'
        })
      );
    });
    elements.absentStatus.textContent = '发现 ' + items.length + ' 个数据库中存在但账号未加入的群组/频道。';
  }

  async function loadAbsentChannels(elements) {
    try {
      var data = await fetchJSON('/api/admin/channels/absent');
      if (!data.ok) throw new Error(data.error || '读取失败');
      renderAbsentChannels(elements, data.items || []);
    } catch (error) {
      elements.absentStatus.textContent = '读取扫描结果失败：' + error.message;
    }
  }

  async function handleScanMissingClick(elements) {
    if (!window.confirm('确认扫描当前 Telegram 账号中已加入但未入库的群组或频道？')) {
      appendLog(elements, '已取消扫描');
      return;
    }
    try {
      var payload = await fetchJSON('/api/admin/channels/missing/scan', {
        method: 'POST'
      });
      var jobId = getCreatedJobId(payload);
      appendLog(elements, '扫描任务已创建：' + jobId);
      startJobPolling(elements, jobId, {
        doneMessage: '扫描任务执行完成',
        errorMessage: '扫描任务执行失败，请检查日志',
        onDone: function () {
          return loadMissingChannels(elements);
        }
      });
    } catch (error) {
      appendLog(elements, '创建扫描任务失败：' + error.message);
    }
  }

  async function handleScanAbsentClick(elements) {
    if (!window.confirm('确认扫描数据库中存在但当前账号未加入的群组或频道？')) {
      appendLog(elements, '已取消扫描');
      return;
    }
    try {
      var payload = await fetchJSON('/api/admin/channels/absent/scan', {
        method: 'POST'
      });
      var jobId = getCreatedJobId(payload);
      appendLog(elements, '扫描任务已创建：' + jobId);
      startJobPolling(elements, jobId, {
        doneMessage: '扫描任务执行完成',
        errorMessage: '扫描任务执行失败，请检查日志',
        onDone: function () {
          return loadAbsentChannels(elements);
        }
      });
    } catch (error) {
      appendLog(elements, '创建扫描任务失败：' + error.message);
    }
  }

  async function handleDeleteAbsentChannel(elements, item) {
    if (jobPollState.isPolling) {
      appendLog(elements, '已有任务进行中，请等待完成后再删除');
      return;
    }

    var chatId = Number(item && item.chat_id);
    if (!Number.isInteger(chatId) || chatId === 0) {
      appendLog(elements, '无法删除：chat_id 非法');
      return;
    }

    var title = item.chat_title || ('Chat ' + chatId);
    var messageCount = formatNumber(item.message_count);
    var confirmText = [
      '确认从数据库删除该群组/频道的全部数据？',
      title + ' (' + chatId + ')',
      '消息数：' + messageCount
    ].join('\n');
    if (!window.confirm(confirmText)) {
      appendLog(elements, '已取消删除操作');
      return;
    }

    try {
      var payload = await fetchJSON('/api/admin/jobs/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: chatId,
          confirm: 'DELETE:' + chatId
        })
      });
      var jobId = getCreatedJobId(payload);
      appendLog(elements, '删除任务已创建：' + jobId);
      startJobPolling(elements, jobId, {
        doneMessage: '删除任务执行完成',
        errorMessage: '删除任务执行失败，请检查日志',
        onDone: async function () {
          await loadChannels(elements);
          await loadAbsentChannels(elements);
        }
      });
    } catch (error) {
      appendLog(elements, '创建删除任务失败：' + error.message);
    }
  }

  function startJobPolling(elements, jobId, options) {
    var normalizedJobId = String(jobId || '').trim();
    if (!normalizedJobId) return;
    var pollOptions = options || {};
    stopJobPolling(undefined, elements);
    jobPollState.pollToken += 1;
    jobPollState.jobId = normalizedJobId;
    jobPollState.lastSeq = 0;
    jobPollState.retryCount = 0;
    jobPollState.lastProgressKey = '';
    jobPollState.onDone = typeof pollOptions.onDone === 'function' ? pollOptions.onDone : null;
    jobPollState.doneMessage = pollOptions.doneMessage || '任务执行完成';
    jobPollState.errorMessage = pollOptions.errorMessage || '任务执行失败，请检查日志';
    jobPollState.isPolling = true;
    setBusy(elements, true);
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
    jobPollState.onDone = null;
    jobPollState.doneMessage = '';
    jobPollState.errorMessage = '';
    setBusy(elements, false);
  }

  function isPollContextActive(jobId, pollToken) {
    return jobPollState.isPolling
      && jobPollState.jobId === jobId
      && jobPollState.pollToken === pollToken;
  }

  function scheduleJobPollingWithDelay(elements, jobId, pollToken, delayMs) {
    if (!isPollContextActive(jobId, pollToken)) return;
    jobPollState.timerId = window.setTimeout(function () {
      pollJobProgress(elements);
    }, Math.max(250, Number(delayMs) || JOB_POLL_INTERVAL_MS));
  }

  async function pollJobProgress(elements) {
    if (!jobPollState.isPolling || !jobPollState.jobId) return;

    var jobId = jobPollState.jobId;
    var pollToken = jobPollState.pollToken;
    try {
      var logsPayload = await fetchJSON(
        '/api/admin/jobs/'
          + encodeURIComponent(jobId)
          + '/logs?after_seq='
          + encodeURIComponent(String(jobPollState.lastSeq || 0))
      );
      if (!isPollContextActive(jobId, pollToken)) return;

      var logs = logsPayload && Array.isArray(logsPayload.logs) ? logsPayload.logs : [];
      logs.forEach(function (line) {
        if (!line || typeof line.message !== 'string') return;
        appendLog(elements, line.message);
        if (typeof line.seq === 'number' && Number.isFinite(line.seq)) {
          jobPollState.lastSeq = Math.max(jobPollState.lastSeq, line.seq);
        }
      });

      var snapshotPayload = await fetchJSON('/api/admin/jobs/' + encodeURIComponent(jobId));
      if (!isPollContextActive(jobId, pollToken)) return;

      var snapshot = snapshotPayload && snapshotPayload.job ? snapshotPayload.job : null;
      var status = snapshot && typeof snapshot.status === 'string' ? snapshot.status.trim() : '';
      if (!snapshot || !status) {
        appendLog(elements, '任务状态响应异常，已停止轮询');
        stopJobPolling(pollToken, elements);
        return;
      }

      jobPollState.retryCount = 0;
      var progressState = shared.buildSnapshotProgressMessage(snapshot);
      if (progressState.key && progressState.key !== jobPollState.lastProgressKey) {
        jobPollState.lastProgressKey = progressState.key;
        if (progressState.message) appendLog(elements, progressState.message);
      }

      if (status === 'done') {
        appendLog(elements, jobPollState.doneMessage || '任务执行完成');
        var onDone = jobPollState.onDone;
        stopJobPolling(pollToken, elements);
        if (typeof onDone === 'function') {
          await onDone();
        }
        return;
      }
      if (status === 'error') {
        appendLog(elements, jobPollState.errorMessage || '任务执行失败，请检查日志');
        stopJobPolling(pollToken, elements);
        return;
      }
    } catch (error) {
      if (!isPollContextActive(jobId, pollToken)) return;
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
    scheduleJobPollingWithDelay(elements, jobId, pollToken, JOB_POLL_INTERVAL_MS);
  }

  function setBusy(elements, isBusy) {
    var disabled = !!isBusy;
    setElementDisabled(elements.sortSelect, disabled);
    setElementDisabled(elements.refreshChannelsBtn, disabled);
    setElementDisabled(elements.channelListToggleBtn, disabled);
    setElementDisabled(elements.scanMissingBtn, disabled);
    setElementDisabled(elements.refreshMissingBtn, disabled);
    setElementDisabled(elements.missingListToggleBtn, disabled);
    setElementDisabled(elements.scanAbsentBtn, disabled);
    setElementDisabled(elements.refreshAbsentBtn, disabled);
    setElementDisabled(elements.absentListToggleBtn, disabled);
    if (elements.logContainer && typeof elements.logContainer.setAttribute === 'function') {
      elements.logContainer.setAttribute('aria-busy', disabled ? 'true' : 'false');
    }
  }

  async function fetchJSON(url, options) {
    var requestOptions = options || {};
    var response;
    try {
      response = await fetch(url, {
        method: requestOptions.method || 'GET',
        headers: requestOptions.headers || {},
        body: requestOptions.body
      });
    } catch (_networkError) {
      throw new Error('网络请求失败');
    }

    if (!response.ok) {
      if (response.status === 401) {
        authState.authenticated = false;
        var els = getElements();
        if (els) openLoginDialog(els);
        throw new Error('未授权，请先登录');
      }
      var errorMessage = '操作失败 ';
      if (response.status === 429) {
        errorMessage = '请求太快了，请稍等一会儿再试';
      } else if (response.status === 500 || response.status === 503) {
        errorMessage = '数据库忙或系统异常，请 15 秒后再试';
      } else {
        errorMessage += '(HTTP ' + response.status + ')';
        try {
          var errorPayload = await response.json();
          if (errorPayload && typeof errorPayload.error === 'string' && errorPayload.error.trim()) {
            errorMessage += ' ' + errorPayload.error.trim();
          }
        } catch (_ignoreErrorPayload) {}
      }
      throw new Error(errorMessage);
    }

    try {
      return await response.json();
    } catch (_parseError) {
      throw new Error('响应 JSON 解析失败');
    }
  }
})();
