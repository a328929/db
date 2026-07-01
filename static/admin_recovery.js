(function () {
  'use strict';

  var shared = window.AdminManageShared;
  var appendLog = shared.appendLog;
  var clearLogs = shared.clearLogs;
  var ensurePlaceholder = shared.ensurePlaceholder;
  var sharedFetchJSON = shared.fetchJSON;
  var sharedPostJSON = shared.postJSON;
  var getCreatedJobId = shared.getCreatedJobId;
  var setDialogOpenState = shared.setDialogOpenState;
  var setElementDisabled = shared.setElementDisabled;
  var setPageInteractionState = shared.setPageInteractionState;
  var syncClearLogsButtonVisibility = shared.syncClearLogsButtonVisibility;
  var trapFocusWithin = shared.trapFocusWithin;
  var formatDateTime = shared.formatDateTime;
  var formatNumber = shared.formatNumber;

  var JOB_POLL_INTERVAL_MS = 3000;
  var JOB_POLL_RETRY_MAX_COUNT = 20;
  var JOB_POLL_RETRY_BASE_MS = 3000;
  var LIST_INITIAL_RENDER_BATCH_SIZE = 40;
  var LIST_RENDER_BATCH_SIZE = 40;
  var LIST_RENDER_STATUS_INTERVAL_MS = 250;

  var recoveryState = {
    items: [],
    overview: {},
    filterValue: 'pending',
    busy: false,
    renderToken: 0
  };

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
    errorMessage: '',
    stopRequested: false
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
      status: document.getElementById('admin-recovery-status'),
      totalValue: document.getElementById('admin-recovery-total'),
      pendingValue: document.getElementById('admin-recovery-pending'),
      inDbValue: document.getElementById('admin-recovery-in-db'),
      lastScanValue: document.getElementById('admin-recovery-last-scan'),
      refreshBtn: document.getElementById('admin-recovery-refresh-btn'),
      scanBtn: document.getElementById('admin-recovery-scan-btn'),
      restoreAllBtn: document.getElementById('admin-recovery-restore-all-btn'),
      stopJobBtn: document.getElementById('admin-recovery-stop-job-btn'),
      filterSelect: document.getElementById('admin-recovery-filter-select'),
      listToggleBtn: document.getElementById('admin-recovery-list-toggle-btn'),
      listStatus: document.getElementById('admin-recovery-list-status'),
      list: document.getElementById('admin-recovery-list'),
      logContainer: document.getElementById('admin-recovery-log-container'),
      clearLogsBtn: document.getElementById('admin-clear-recovery-logs-btn'),
      loginDialog: document.getElementById('admin-login-dialog'),
      loginStatus: document.getElementById('admin-login-status'),
      passwordInput: document.getElementById('admin-password-input'),
      loginConfirmBtn: document.getElementById('admin-login-confirm-btn')
    };

    var requiredKeys = [
      'status',
      'totalValue',
      'pendingValue',
      'inDbValue',
      'lastScanValue',
      'refreshBtn',
      'scanBtn',
      'restoreAllBtn',
      'stopJobBtn',
      'filterSelect',
      'listToggleBtn',
      'listStatus',
      'list',
      'logContainer',
      'clearLogsBtn',
      'loginDialog',
      'loginStatus',
      'passwordInput',
      'loginConfirmBtn'
    ];
    var missing = requiredKeys.filter(function (key) { return !elements[key]; });
    if (missing.length > 0) {
      console.warn('[admin_recovery] Missing required elements:', missing.join(', '));
      return null;
    }
    return elements;
  }

  function setLoginStatus(elements, message) {
    shared.setLoginStatus(elements, message);
  }

  function initializeUI(elements) {
    ensurePlaceholder(elements.logContainer);
    syncClearLogsButtonVisibility(elements);
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
      loadRecoveryData(elements);
    });
    elements.scanBtn.addEventListener('click', function () {
      handleScanClick(elements);
    });
    elements.restoreAllBtn.addEventListener('click', function () {
      handleRestoreAllClick(elements);
    });
    elements.stopJobBtn.addEventListener('click', function () {
      handleStopJobClick(elements);
    });
    elements.filterSelect.addEventListener('change', function () {
      recoveryState.filterValue = elements.filterSelect.value || 'pending';
      renderCandidates(elements);
    });
    elements.listToggleBtn.addEventListener('click', function () {
      toggleListArea(elements.listToggleBtn, elements.list);
    });
    elements.clearLogsBtn.addEventListener('click', function () {
      clearLogs(elements);
    });

    document.addEventListener('keydown', function (event) {
      if (!elements || !elements.loginDialog || elements.loginDialog.hidden) {
        return;
      }
      if (event.key === 'Tab') {
        trapFocusWithin(elements.loginDialog, event);
      }
    });
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

  function setCandidateListRenderBusy(elements, isBusy) {
    if (!elements || !elements.list || typeof elements.list.setAttribute !== 'function') return;
    elements.list.setAttribute('aria-busy', isBusy ? 'true' : 'false');
  }

  function nextCandidateRenderToken(elements) {
    recoveryState.renderToken += 1;
    setCandidateListRenderBusy(elements, false);
    return recoveryState.renderToken;
  }

  function isCandidateRenderCurrent(token) {
    return recoveryState.renderToken === token;
  }

  function scheduleCandidateRender(callback) {
    if (typeof window.requestIdleCallback === 'function') {
      window.requestIdleCallback(callback, { timeout: 120 });
      return;
    }
    window.setTimeout(callback, 16);
  }

  function createInfoPill(label, value) {
    var pill = document.createElement('span');
    pill.className = 'recovery-info-pill';
    pill.appendChild(createTextElement('span', 'recovery-info-label', label));
    pill.appendChild(createTextElement('strong', '', value || '暂无'));
    return pill;
  }

  function isCandidatePending(item) {
    return Number(item && item.in_database) !== 1;
  }

  function hasCandidateAvailabilityIssue(item) {
    return Boolean(String((item && item.availability_reason) || '').trim());
  }

  function isCandidateReady(item) {
    return !isCandidatePending(item) && !hasCandidateAvailabilityIssue(item) && Number((item && item.message_count) || 0) <= 0;
  }

  function isCandidateSummaryOnly(item) {
    return !isCandidatePending(item) && hasCandidateAvailabilityIssue(item) && Number((item && item.message_count) || 0) <= 0;
  }

  function isCandidateImported(item) {
    return !isCandidatePending(item) && !isCandidateReady(item) && !isCandidateSummaryOnly(item);
  }

  function filterCandidates(items) {
    var filterValue = recoveryState.filterValue || 'pending';
    if (filterValue === 'all') return items;
    if (filterValue === 'ready') {
      return items.filter(isCandidateReady);
    }
    if (filterValue === 'in_database') {
      return items.filter(function (item) {
        return !isCandidatePending(item);
      });
    }
    return items.filter(isCandidatePending);
  }

  function renderOverview(elements) {
    var overview = recoveryState.overview || {};
    var total = Number(overview.total_count || 0);
    var pending = Number(overview.pending_count || 0);
    var inDatabase = Number(overview.in_database_count || 0);

    elements.totalValue.textContent = formatNumber(total);
    elements.pendingValue.textContent = formatNumber(pending);
    elements.inDbValue.textContent = formatNumber(inDatabase);
    elements.lastScanValue.textContent = formatDateTime(overview.last_scanned_at);

    if (total <= 0) {
      elements.status.textContent = '暂无恢复候选，可点击“扫描 Session”。';
    } else {
      elements.status.textContent = '共 ' + formatNumber(total) + ' 个候选，待恢复摘要 ' + formatNumber(pending) + ' 个。';
    }
    setElementDisabled(elements.restoreAllBtn, recoveryState.busy || pending <= 0);
  }

  function getCandidateStateLabel(item) {
    if (isCandidatePending(item)) return '待恢复摘要';
    if (isCandidateSummaryOnly(item)) return '当前不可访问';
    if (isCandidateReady(item)) return '准备恢复';
    return '已在库';
  }

  function getCandidateHarvestTarget(item) {
    var username = String((item && item.chat_username) || '').trim().replace(/^@+/, '');
    if (username) return '@' + username;
    var sourceEntityId = String((item && item.source_entity_id) || '').trim();
    if (sourceEntityId) return sourceEntityId;
    return String((item && item.chat_title) || (item && item.chat_id) || '').trim();
  }

  function getCandidateActionLabel(item) {
    return String(
      (item && item.chat_title)
      || (item && item.chat_username && ('@' + item.chat_username))
      || (item && item.chat_id && ('Chat ' + item.chat_id))
      || '该群组或频道'
    ).trim();
  }

  function createCandidateActions(elements, item) {
    var actions = document.createElement('div');
    var candidateLabel = getCandidateActionLabel(item);
    actions.className = 'recovery-actions';

    if (item.telegram_app_link) {
      var appLink = document.createElement('a');
      appLink.href = item.telegram_app_link;
      appLink.textContent = '打开客户端';
      appLink.setAttribute('aria-label', '使用 Telegram 客户端打开 ' + candidateLabel);
      actions.appendChild(appLink);
    }

    if (item.telegram_web_link) {
      var webLink = document.createElement('a');
      webLink.href = item.telegram_web_link;
      webLink.target = '_blank';
      webLink.rel = 'noopener noreferrer';
      webLink.textContent = '网页入口';
      webLink.setAttribute('aria-label', '在新标签页打开 ' + candidateLabel + ' 的 Telegram 网页入口');
      actions.appendChild(webLink);
    }

    var copyBtn = document.createElement('button');
    copyBtn.type = 'button';
    copyBtn.textContent = '复制信息';
    copyBtn.setAttribute('aria-label', '复制 ' + candidateLabel + ' 的恢复候选信息');
    copyBtn.addEventListener('click', function () {
      copyCandidateInfo(elements, item);
    });
    actions.appendChild(copyBtn);

    if (isCandidatePending(item)) {
      var restoreBtn = document.createElement('button');
      restoreBtn.type = 'button';
      restoreBtn.className = 'primary-action';
      restoreBtn.textContent = '恢复摘要';
      restoreBtn.disabled = recoveryState.busy;
      restoreBtn.setAttribute('data-recovery-job-action', 'true');
      restoreBtn.setAttribute('aria-label', '恢复 ' + candidateLabel + ' 的群组或频道摘要到数据库');
      restoreBtn.addEventListener('click', function () {
        handleRestoreOneClick(elements, item);
      });
      actions.appendChild(restoreBtn);
    } else if (isCandidateReady(item)) {
      var addBtn = document.createElement('button');
      addBtn.type = 'button';
      addBtn.className = 'primary-action';
      addBtn.textContent = '添加入库';
      addBtn.disabled = recoveryState.busy;
      addBtn.setAttribute('data-recovery-job-action', 'true');
      addBtn.setAttribute('aria-label', '复用添加群组链路抓取 ' + candidateLabel + ' 并写入数据库');
      addBtn.addEventListener('click', function () {
        handleAddCandidateToDatabaseClick(elements, item);
      });
      actions.appendChild(addBtn);
    } else if (isCandidateSummaryOnly(item)) {
      var unavailableBtn = document.createElement('button');
      unavailableBtn.type = 'button';
      unavailableBtn.textContent = '当前不可访问';
      unavailableBtn.disabled = true;
      unavailableBtn.setAttribute('aria-label', candidateLabel + ' 当前不可访问，暂不能继续抓取');
      actions.appendChild(unavailableBtn);
    } else if (isCandidateImported(item)) {
      var importedBtn = document.createElement('button');
      importedBtn.type = 'button';
      importedBtn.textContent = '已在库';
      importedBtn.disabled = true;
      importedBtn.setAttribute('aria-label', candidateLabel + ' 已在数据库中');
      actions.appendChild(importedBtn);
    }

    return actions;
  }

  function createCandidateItem(elements, item) {
    var article = document.createElement('article');
    article.className = 'recovery-list-item';
    if (!isCandidatePending(item)) {
      article.className += ' is-recovered';
    }

    var head = document.createElement('div');
    head.className = 'recovery-item-head';

    var titleWrap = document.createElement('div');
    titleWrap.className = 'recovery-item-title-wrap';

    var titleRow = document.createElement('div');
    titleRow.className = 'recovery-item-title-row';
    titleRow.appendChild(createTextElement('h3', 'recovery-item-title', item.chat_title || ('Chat ' + item.chat_id)));

    var statePill = createTextElement('span', 'recovery-state-pill', getCandidateStateLabel(item));
    if (isCandidatePending(item)) {
      statePill.className += ' is-pending';
    } else if (isCandidateReady(item)) {
      statePill.className += ' is-ready';
    }
    titleRow.appendChild(statePill);
    titleWrap.appendChild(titleRow);

    var subtitleParts = [];
    if (item.chat_username) subtitleParts.push('@' + item.chat_username);
    if (item.chat_type) subtitleParts.push(item.chat_type);
    if (item.source_session) subtitleParts.push(item.source_session);
    titleWrap.appendChild(createTextElement('p', 'recovery-item-subtitle', subtitleParts.join(' | ')));
    head.appendChild(titleWrap);

    var metrics = document.createElement('div');
    metrics.className = 'recovery-item-metrics';
    metrics.appendChild(createInfoPill('chat_id', String(item.chat_id)));
    metrics.appendChild(createInfoPill('消息数', formatNumber(item.message_count)));
    metrics.appendChild(createInfoPill('最后消息', formatDateTime(item.last_message_at)));
    metrics.appendChild(createInfoPill('缓存时间', formatDateTime(item.session_entity_date)));
    head.appendChild(metrics);
    article.appendChild(head);

    var meta = document.createElement('div');
    meta.className = 'recovery-item-meta';
    meta.appendChild(createInfoPill('用户名', item.chat_username ? '@' + item.chat_username : ''));
    meta.appendChild(createInfoPill('来源', item.source_session || ''));
    meta.appendChild(createInfoPill('实体 ID', item.source_entity_id ? String(item.source_entity_id) : ''));
    if (item.source_access_hash) {
      meta.appendChild(createInfoPill('Access Hash', String(item.source_access_hash)));
    }
    meta.appendChild(createInfoPill('扫描', formatDateTime(item.scanned_at)));
    meta.appendChild(createInfoPill('摘要恢复', formatDateTime(item.recovered_at || item.database_last_seen_at)));
    article.appendChild(meta);

    article.appendChild(createCandidateActions(elements, item));
    var noteParts = [];
    if (hasCandidateAvailabilityIssue(item)) {
      noteParts.push(String(item.availability_reason || '').trim());
    }
    if (!item.has_public_link) {
      noteParts.push('私有群组通常没有稳定网页入口；可复制信息后在 Telegram 中核对。');
    }
    if (noteParts.length > 0) {
      article.appendChild(
        createTextElement(
          'p',
          'recovery-note',
          noteParts.join(' | ')
        )
      );
    }
    return article;
  }

  function renderCandidates(elements) {
    var items = filterCandidates(recoveryState.items);
    var token = nextCandidateRenderToken(elements);
    var total = items.length;
    var index = 0;
    var lastStatusUpdateAt = 0;
    elements.list.textContent = '';

    if (!Array.isArray(recoveryState.items) || recoveryState.items.length === 0) {
      elements.list.appendChild(createTextElement('div', 'empty-box', '暂无恢复候选。'));
      elements.listStatus.textContent = '暂无恢复候选，可点击“扫描 Session”。';
      return;
    }

    if (items.length === 0) {
      elements.list.appendChild(createTextElement('div', 'empty-box', '当前筛选条件下没有候选。'));
      elements.listStatus.textContent = '当前筛选 0 个，共 ' + recoveryState.items.length + ' 个候选。';
      return;
    }

    setCandidateListRenderBusy(elements, true);

    function setProgressStatus(force) {
      var now = Date.now();
      if (!force && index < total && now - lastStatusUpdateAt < LIST_RENDER_STATUS_INTERVAL_MS) {
        return;
      }
      lastStatusUpdateAt = now;
      elements.listStatus.textContent = '正在显示 '
        + index
        + '/'
        + total
        + ' 个，共 '
        + recoveryState.items.length
        + ' 个候选。';
    }

    function appendBatch() {
      if (!isCandidateRenderCurrent(token)) return;

      var fragment = document.createDocumentFragment();
      var batchSize = index === 0 ? LIST_INITIAL_RENDER_BATCH_SIZE : LIST_RENDER_BATCH_SIZE;
      var end = Math.min(index + batchSize, total);
      while (index < end) {
        fragment.appendChild(createCandidateItem(elements, items[index]));
        index += 1;
      }
      elements.list.appendChild(fragment);

      if (index < total) {
        setProgressStatus(false);
        scheduleCandidateRender(appendBatch);
        return;
      }

      setCandidateListRenderBusy(elements, false);
      elements.listStatus.textContent = '当前显示 ' + total + ' 个，共 ' + recoveryState.items.length + ' 个候选。';
    }

    appendBatch();
  }

  function copyCandidateInfo(elements, item) {
    var text = [
      item.chat_title || '',
      'chat_id: ' + item.chat_id,
      item.chat_username ? '@' + item.chat_username : '',
      item.source_session ? 'source: ' + item.source_session : '',
      item.source_entity_id ? 'entity_id: ' + item.source_entity_id : '',
      item.source_access_hash ? 'access_hash: ' + item.source_access_hash : '',
      item.availability_reason ? 'availability: ' + item.availability_reason : ''
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

  async function loadRecoveryData(elements) {
    nextCandidateRenderToken(elements);
    elements.status.textContent = '正在读取恢复状态...';
    try {
      var data = await fetchJSON('/api/admin/recovery');
      if (!data.ok) throw new Error(data.error || '读取失败');
      recoveryState.items = Array.isArray(data.items) ? data.items : [];
      recoveryState.overview = data.overview || {};
      renderOverview(elements);
      renderCandidates(elements);
    } catch (error) {
      elements.status.textContent = '读取恢复状态失败：' + error.message;
      elements.listStatus.textContent = '读取恢复候选失败：' + error.message;
    }
  }

  async function resumeActiveJobPolling(elements) {
    if (jobPollState.isPolling) return;

    try {
      var payload = await fetchJSON('/api/admin/jobs/active');
      var job = payload && payload.job && typeof payload.job === 'object' && !Array.isArray(payload.job)
        ? payload.job
        : null;
      var jobId = job && job.job_id ? String(job.job_id) : '';
      var status = job && typeof job.status === 'string' ? job.status.trim().toLowerCase() : '';
      if (!jobId || (status !== 'queued' && status !== 'running')) return;
      appendLog(elements, '检测到正在执行的任务，继续监控：' + jobId);
      startJobPolling(elements, jobId, {
        doneMessage: '任务执行完成',
        errorMessage: '任务执行失败，请检查日志',
        onDone: function () {
          return loadRecoveryData(elements);
        }
      });
    } catch (error) {
      appendLog(elements, '检查正在执行的任务失败：' + error.message);
    }
  }

  async function handleScanClick(elements) {
    if (!window.confirm('确认扫描本地 Telegram Session 缓存中的群组或频道？')) {
      appendLog(elements, '已取消扫描');
      return;
    }
    try {
      var payload = await fetchJSON('/api/admin/recovery/scan', {
        method: 'POST'
      });
      var jobId = getCreatedJobId(payload);
      appendLog(elements, '恢复扫描任务已创建：' + jobId);
      startJobPolling(elements, jobId, {
        doneMessage: '恢复扫描任务执行完成',
        errorMessage: '恢复扫描任务执行失败，请检查日志',
        onDone: function () {
          return loadRecoveryData(elements);
        }
      });
    } catch (error) {
      appendLog(elements, '创建恢复扫描任务失败：' + error.message);
    }
  }

  async function handleRestoreAllClick(elements) {
    var pendingCount = Number((recoveryState.overview || {}).pending_count || 0);
    if (pendingCount <= 0) {
      appendLog(elements, '没有待恢复候选');
      return;
    }
    if (!window.confirm('确认恢复全部未入库候选到 chats 表？\n待恢复数量：' + pendingCount)) {
      appendLog(elements, '已取消恢复');
      return;
    }
    await createRestoreJob(elements, {
      scope: 'all',
      confirm: 'RECOVER:all'
    });
  }

  async function handleRestoreOneClick(elements, item) {
    if (!isCandidatePending(item)) {
      appendLog(elements, '该候选已经在数据库中');
      return;
    }

    var chatId = Number(item && item.chat_id);
    if (!Number.isInteger(chatId) || chatId === 0) {
      appendLog(elements, '无法恢复：chat_id 非法');
      return;
    }

    var title = item.chat_title || ('Chat ' + chatId);
    if (!window.confirm('确认恢复该群组/频道摘要？\n' + title + ' (' + chatId + ')')) {
      appendLog(elements, '已取消恢复');
      return;
    }

    await createRestoreJob(elements, {
      scope: 'selected',
      chat_ids: [chatId],
      confirm: 'RECOVER:selected:' + String(chatId)
    });
  }

  async function createRestoreJob(elements, payload) {
    try {
      var data = await fetchJSON('/api/admin/recovery/restore', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      var jobId = getCreatedJobId(data);
      appendLog(elements, '恢复任务已创建：' + jobId);
      startJobPolling(elements, jobId, {
        doneMessage: '恢复任务执行完成',
        errorMessage: '恢复任务执行失败，请检查日志',
        onDone: function () {
          return loadRecoveryData(elements);
        }
      });
    } catch (error) {
      appendLog(elements, '创建恢复任务失败：' + error.message);
    }
  }

  async function handleAddCandidateToDatabaseClick(elements, item) {
    if (!isCandidateReady(item)) {
      appendLog(elements, '该候选当前不在准备恢复状态');
      return;
    }

    var target = getCandidateHarvestTarget(item);
    if (!target) {
      appendLog(elements, '无法添加入库：缺少用户名或 chat_id');
      return;
    }

    var title = item.chat_title || ('Chat ' + item.chat_id);
    if (!window.confirm('确认添加入库并开始抓取该群组/频道？\n' + title + '\n目标：' + target)) {
      appendLog(elements, '已取消添加入库');
      return;
    }

    try {
      var data = await postJSON('/api/admin/recovery/add', {
        target: target,
        chat_id: item.chat_id,
        chat_title: item.chat_title || '',
        chat_username: item.chat_username || '',
        source_session: item.source_session || '',
        source_entity_id: item.source_entity_id || '',
        source_access_hash: item.source_access_hash || ''
      });
      var jobId = getCreatedJobId(data);
      appendLog(elements, '添加入库任务已创建：' + jobId);
      startJobPolling(elements, jobId, {
        doneMessage: '添加入库任务执行完成',
        errorMessage: '添加入库任务执行失败，请检查日志',
        onDone: function () {
          return loadRecoveryData(elements);
        }
      });
    } catch (error) {
      appendLog(elements, '创建添加入库任务失败：' + error.message);
    }
  }

  async function handleStopJobClick(elements) {
    var jobId = String(jobPollState.jobId || '').trim();
    if (!jobId) {
      appendLog(elements, '当前没有正在监控的任务');
      return;
    }
    if (!window.confirm('确认请求停止当前任务？已开始抓取的群组会继续完成，之后不再启动新的群组。')) {
      appendLog(elements, '已取消停止请求');
      return;
    }

    setElementDisabled(elements.stopJobBtn, true);
    try {
      var payload = await fetchJSON('/api/admin/jobs/' + encodeURIComponent(jobId) + '/stop', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
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

  function startJobPolling(elements, jobId, options) {
    jobPollController.start(jobPollState, jobId, options);
  }

  function stopJobPolling(expectedToken, _elements) {
    jobPollController.stop(jobPollState, expectedToken);
  }

  function setBusy(elements, isBusy) {
    var disabled = !!isBusy;
    recoveryState.busy = disabled;
    setElementDisabled(elements.scanBtn, disabled);
    setElementDisabled(elements.restoreAllBtn, disabled || Number((recoveryState.overview || {}).pending_count || 0) <= 0);
    setStopButtonState(elements);
    Array.prototype.forEach.call(elements.list.querySelectorAll('button'), function (button) {
      var isJobAction = button.getAttribute('data-recovery-job-action') === 'true';
      setElementDisabled(button, (disabled && isJobAction) || button.textContent === '已在库');
    });
    if (elements.logContainer && typeof elements.logContainer.setAttribute === 'function') {
      elements.logContainer.setAttribute('aria-busy', disabled ? 'true' : 'false');
    }
  }

  function setStopButtonState(elements) {
    if (!elements || !elements.stopJobBtn) return;
    elements.stopJobBtn.hidden = !jobPollState.isPolling;
    elements.stopJobBtn.textContent = jobPollState.stopRequested ? '停止请求已发送' : '停止任务';
    setElementDisabled(elements.stopJobBtn, !jobPollState.isPolling || jobPollState.stopRequested);
  }

  async function fetchJSON(url, options) {
    return sharedFetchJSON(url, Object.assign({}, options || {}, {
      onUnauthorized: sessionController.handleUnauthorizedResponse
    }));
  }

  async function postJSON(url, payload) {
    return sharedPostJSON(url, payload, {
      onUnauthorized: sessionController.handleUnauthorizedResponse
    });
  }

  var sessionController = shared.createAdminSessionController({
    afterAuth: async function (elements, context) {
      await loadRecoveryData(elements);
      await resumeActiveJobPolling(elements);
      if (context.reason === 'login') {
        appendLog(elements, '认证成功，已进入群组恢复监控');
      }
    },
    getElements: getElements,
    getPageElement: function () {
      return document.getElementById('admin-recovery-page');
    }
  });

  var jobPollController = shared.createAdminJobPollController({
    appendLog: function (message) {
      var elements = getElements();
      if (elements) {
        appendLog(elements, message);
      }
    },
    fetchJSON: fetchJSON,
    getDoneMessage: function (state) {
      return state.doneMessage || '任务执行完成';
    },
    getElements: getElements,
    getErrorMessage: function (state) {
      return state.errorMessage || '任务执行失败，请检查日志';
    },
    onDone: async function (_snapshot, state) {
      if (typeof state.onDone === 'function') {
        await state.onDone();
      }
    },
    onSnapshot: function (snapshot, state) {
      var elements = getElements();
      state.stopRequested = !!snapshot.stop_requested;
      if (elements) {
        setStopButtonState(elements);
      }
    },
    onStop: function (state) {
      state.onDone = null;
      state.doneMessage = '';
      state.errorMessage = '';
      state.stopRequested = false;
    },
    setBusy: function (elements, isBusy) {
      if (elements) {
        setBusy(elements, isBusy);
      }
    },
    setInitialState: function (state, options) {
      var pollOptions = options || {};
      state.stopRequested = false;
      state.onDone = typeof pollOptions.onDone === 'function' ? pollOptions.onDone : null;
      state.doneMessage = pollOptions.doneMessage || '任务执行完成';
      state.errorMessage = pollOptions.errorMessage || '任务执行失败，请检查日志';
    }
  });
})();
