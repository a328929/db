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

  var JOB_POLL_INTERVAL_MS = 3000;
  var JOB_POLL_RETRY_MAX_COUNT = 20;
  var JOB_POLL_RETRY_BASE_MS = 3000;
  var STORAGE_KEY = 'adminCloneConsoleState';

  var cloneState = {
    items: [],
    runs: [],
    sourceChatId: '',
    selectedRunId: '',
    plan: null,
    migration: null,
    timelineMigration: null,
    timelinePreview: null,
    report: null,
    busy: false
  };

  var jobPollState = {
    jobId: '',
    lastSeq: 0,
    timerId: null,
    isPolling: false,
    pollToken: 0,
    retryCount: 0,
    lastProgressKey: ''
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
      sourceStatus: document.getElementById('admin-clone-source-status'),
      sortSelect: document.getElementById('admin-clone-sort-select'),
      refreshBtn: document.getElementById('admin-clone-refresh-btn'),
      sourceSelect: document.getElementById('admin-clone-source-select'),
      preflightBtn: document.getElementById('admin-clone-preflight-btn'),
      sourceSummary: document.getElementById('admin-clone-source-summary'),
      preflightStatus: document.getElementById('admin-clone-preflight-status'),
      metricsList: document.getElementById('admin-clone-metrics'),
      capabilities: document.getElementById('admin-clone-capabilities'),
      warnings: document.getElementById('admin-clone-warnings'),
      recommendation: document.getElementById('admin-clone-recommendation'),
      targetTitleInput: document.getElementById('admin-clone-target-title-input'),
      targetKindSelect: document.getElementById('admin-clone-target-kind-select'),
      confirmInput: document.getElementById('admin-clone-confirm-input'),
      confirmHint: document.getElementById('admin-clone-confirm-hint'),
      startBtn: document.getElementById('admin-clone-start-btn'),
      runsStatus: document.getElementById('admin-clone-runs-status'),
      runsRefreshBtn: document.getElementById('admin-clone-runs-refresh-btn'),
      runsList: document.getElementById('admin-clone-runs-list'),
      planStatus: document.getElementById('admin-clone-plan-status'),
	      planRefreshBtn: document.getElementById('admin-clone-plan-refresh-btn'),
	      deepPreflightBtn: document.getElementById('admin-clone-deep-preflight-btn'),
	      timelineMigrationBtn: document.getElementById('admin-clone-timeline-migration-btn'),
      planRunLabel: document.getElementById('admin-clone-plan-run-label'),
      planSummary: document.getElementById('admin-clone-plan-summary'),
      planBlocking: document.getElementById('admin-clone-plan-blocking'),
      planWarnings: document.getElementById('admin-clone-plan-warnings'),
	      messageLimitInput: document.getElementById('admin-clone-message-limit-input'),
	      sendDelayInput: document.getElementById('admin-clone-send-delay-input'),
	      timelineStatus: document.getElementById('admin-clone-timeline-status'),
	      timelineSummary: document.getElementById('admin-clone-timeline-summary'),
      logContainer: document.getElementById('admin-clone-log-container'),
      clearLogsBtn: document.getElementById('admin-clear-clone-logs-btn'),
      loginDialog: document.getElementById('admin-login-dialog'),
      loginStatus: document.getElementById('admin-login-status'),
      passwordInput: document.getElementById('admin-password-input'),
      loginConfirmBtn: document.getElementById('admin-login-confirm-btn')
    };

    var requiredKeys = [
      'sourceStatus',
      'sortSelect',
      'refreshBtn',
      'sourceSelect',
      'preflightBtn',
      'sourceSummary',
      'preflightStatus',
      'metricsList',
      'capabilities',
      'warnings',
      'recommendation',
      'targetTitleInput',
      'targetKindSelect',
      'confirmInput',
      'confirmHint',
      'startBtn',
      'runsStatus',
      'runsRefreshBtn',
      'runsList',
      'planStatus',
	      'planRefreshBtn',
	      'deepPreflightBtn',
	      'timelineMigrationBtn',
      'planRunLabel',
      'planSummary',
      'planBlocking',
      'planWarnings',
	      'messageLimitInput',
	      'sendDelayInput',
	      'timelineStatus',
	      'timelineSummary',
      'logContainer',
      'clearLogsBtn',
      'loginDialog',
      'loginStatus',
      'passwordInput',
      'loginConfirmBtn'
    ];
    var missing = requiredKeys.filter(function (key) { return !elements[key]; });
    if (missing.length > 0) {
      console.warn('[admin_clone] Missing required elements:', missing.join(', '));
      return null;
    }
    return elements;
  }

  function setLoginStatus(elements, message) {
    shared.setLoginStatus(elements, message);
  }

  function initializeUI(elements) {
    restoreConsoleState(elements);
    ensurePlaceholder(elements.logContainer);
    syncClearLogsButtonVisibility(elements);
    renderSourceSummary(elements, null);
    renderReport(elements, null);
	    renderCloneRuns(elements, []);
	    renderMigrationPlan(elements, null);
	      renderTimelineMigration(elements, null);
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
    elements.sortSelect.addEventListener('change', function () {
      persistConsoleState(elements);
      loadSourceChats(elements);
    });
    elements.refreshBtn.addEventListener('click', function () {
      loadSourceChats(elements);
    });
    elements.sourceSelect.addEventListener('change', function () {
      handleSourceChange(elements, { resetRunSelection: true });
      persistConsoleState(elements);
      loadCloneRuns(elements);
    });
    elements.preflightBtn.addEventListener('click', function () {
      handlePreflightClick(elements);
    });
    elements.confirmInput.addEventListener('input', function () {
      updateStartButtonState(elements);
    });
    elements.targetTitleInput.addEventListener('input', function () {
      updateStartButtonState(elements);
    });
    elements.startBtn.addEventListener('click', function () {
      handleStartCloneClick(elements);
    });
    elements.runsRefreshBtn.addEventListener('click', function () {
      loadCloneRuns(elements);
    });
    elements.planRefreshBtn.addEventListener('click', function () {
      loadSelectedRunPlan(elements);
    });
	    elements.deepPreflightBtn.addEventListener('click', function () {
	      handleDeepPreflightClick(elements);
	    });
    elements.timelineMigrationBtn.addEventListener('click', function () {
      handleTimelineMigrationClick(elements);
    });
    elements.messageLimitInput.addEventListener('input', function () {
      persistConsoleState(elements);
    });
    elements.sendDelayInput.addEventListener('input', function () {
      persistConsoleState(elements);
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

  async function loadSourceChats(elements) {
    var previousValue = String(elements.sourceSelect.value || cloneState.sourceChatId || '');
    setBusy(elements, true);
    elements.sourceStatus.textContent = '正在读取数据库群组...';
    try {
      var payload = await fetchJSON(
        '/api/admin/clone/chats?sort='
          + encodeURIComponent(String(elements.sortSelect.value || ''))
      );
      cloneState.items = Array.isArray(payload.items) ? payload.items : [];
      renderSourceOptions(elements, previousValue);
      elements.sourceStatus.textContent = '共 ' + formatNumber(cloneState.items.length) + ' 个可选源群组。';
      handleSourceChange(elements);
      await loadCloneRuns(elements);
    } catch (error) {
      cloneState.items = [];
      renderSourceOptions(elements, '');
      elements.sourceStatus.textContent = '读取源群组失败：' + error.message;
      appendLog(elements, '读取源群组失败：' + error.message);
    } finally {
      setBusy(elements, false);
    }
  }

  function renderSourceOptions(elements, preferredValue) {
    elements.sourceSelect.textContent = '';

    var placeholder = document.createElement('option');
    placeholder.value = '';
    placeholder.textContent = cloneState.items.length
      ? '全部源群组 / 最近副本'
      : '暂无可选群组';
    elements.sourceSelect.appendChild(placeholder);

    cloneState.items.forEach(function (item) {
      var chatId = item && item.chat_id;
      if (chatId === undefined || chatId === null) return;
      var option = document.createElement('option');
      option.value = String(chatId);
      option.textContent = buildChatOptionText(item);
      elements.sourceSelect.appendChild(option);
    });

    if (preferredValue && findSourceChat(preferredValue)) {
      elements.sourceSelect.value = preferredValue;
      return;
    }
    elements.sourceSelect.value = '';
  }

  function handleSourceChange(elements, options) {
    var opts = options || {};
    var selected = getSelectedSourceChat(elements);
    cloneState.report = null;
    cloneState.sourceChatId = selected ? String(selected.chat_id || '') : '';
    if (opts.resetRunSelection) {
      cloneState.selectedRunId = '';
      syncUrlRunId('');
    }
    cloneState.plan = null;
    cloneState.migration = null;
    cloneState.timelineMigration = null;
    cloneState.timelinePreview = null;
    renderSourceSummary(elements, selected);
    renderReport(elements, null);
    renderMigrationPlan(elements, null);
    renderTimelineMigration(elements, null);
    if (selected) {
      elements.targetKindSelect.value = inferTargetKindFromSource(selected.chat_type);
      elements.preflightStatus.textContent = '已选择源群组，请执行预检。';
    } else {
      elements.preflightStatus.textContent = '请选择源群组后执行预检。';
    }
    setBusy(elements, false);
  }

  function getSelectedSourceChat(elements) {
    return findSourceChat(String(elements.sourceSelect.value || ''));
  }

  function findSourceChat(chatId) {
    var normalized = String(chatId || '').trim();
    if (!normalized) return null;
    for (var i = 0; i < cloneState.items.length; i += 1) {
      if (String(cloneState.items[i].chat_id) === normalized) {
        return cloneState.items[i];
      }
    }
    return null;
  }

  function buildChatOptionText(item) {
    var title = String((item && item.chat_title) || (item && item.chat_id) || '').trim();
    var count = formatNumber((item && item.message_count) || 0);
    return title + '（' + count + ' 条）';
  }

  function inferTargetKindFromSource(chatType) {
    var value = String(chatType || '').trim().toLowerCase();
    if (value.indexOf('group') !== -1 || value === 'chat') {
      return 'megagroup';
    }
    return 'channel';
  }

  function renderSourceSummary(elements, item) {
    elements.sourceSummary.textContent = '';
    if (!item) {
      appendSummaryPair(elements.sourceSummary, '当前源', '未选择');
      appendSummaryPair(elements.sourceSummary, '消息数', '0');
      appendSummaryPair(elements.sourceSummary, '媒体元信息', '0');
      appendSummaryPair(elements.sourceSummary, '最后消息', '暂无');
      return;
    }

    appendSummaryPair(elements.sourceSummary, '当前源', item.chat_title || item.chat_id);
    appendSummaryPair(elements.sourceSummary, '消息数', formatNumber(item.message_count));
    appendSummaryPair(elements.sourceSummary, '媒体元信息', formatNumber(item.media_rows));
    appendSummaryPair(elements.sourceSummary, '最后消息', formatDateTime(item.last_message_at || item.last_seen_at));
  }

  function appendSummaryPair(container, label, value) {
    var wrapper = document.createElement('div');
    var term = document.createElement('dt');
    var detail = document.createElement('dd');
    term.textContent = String(label || '');
    detail.textContent = String(value || '暂无');
    wrapper.appendChild(term);
    wrapper.appendChild(detail);
    container.appendChild(wrapper);
  }

  async function handlePreflightClick(elements) {
    var selected = getSelectedSourceChat(elements);
    if (!selected) {
      elements.preflightStatus.textContent = '请先选择源群组。';
      return;
    }

    setBusy(elements, true);
    cloneState.report = null;
    renderReport(elements, null);
    elements.preflightStatus.textContent = '正在执行本地数据库和配置预检...';
    try {
      var payload = await postJSON('/api/admin/clone/preflight', {
        chat_id: Number(selected.chat_id)
      });
      cloneState.report = payload && payload.report ? payload.report : null;
      if (!cloneState.report) {
        throw new Error('预检响应缺少 report');
      }
      renderReport(elements, cloneState.report);
      appendLog(elements, '预检完成：' + getReportSourceLabel(cloneState.report));
    } catch (error) {
      elements.preflightStatus.textContent = '预检失败：' + error.message;
      appendLog(elements, '预检失败：' + error.message);
    } finally {
      setBusy(elements, false);
    }
  }

  function renderReport(elements, report) {
    elements.metricsList.textContent = '';
    elements.capabilities.textContent = '';
    elements.warnings.textContent = '';
    elements.recommendation.textContent = '';
    elements.confirmInput.value = '';
    elements.confirmHint.textContent = '预检通过后会生成确认码。';
    elements.targetTitleInput.value = '';

    if (!report) {
      updateStartButtonState(elements);
      return;
    }

    var target = report.target || {};
    var source = report.source || {};
    var metrics = report.metrics || {};
    elements.targetTitleInput.value = String(target.default_title || '');
    elements.targetKindSelect.value = inferTargetKindFromSource(source.chat_type);
    elements.confirmInput.value = String(report.confirm || '');
    elements.confirmHint.textContent = '确认码已生成，第二次点击“确认克隆”将创建结构副本。';

    renderMetricCards(elements.metricsList, metrics);
    renderCapabilities(elements.capabilities, report.capabilities || []);
    renderWarnings(elements.warnings, report.warnings || []);
    renderRecommendation(elements.recommendation, report.recommendation || {});

    var account = report.account || {};
    elements.preflightStatus.textContent = account.secondary_session_distinct
      ? '预检完成：结构克隆可执行。'
      : '预检完成：第二账号未就绪，暂不能启动克隆。';
    updateStartButtonState(elements);
  }

  function renderMetricCards(container, metrics) {
    var items = [
      ['total_messages', '消息总数', 'number'],
      ['text_messages', '可重建文本', 'number'],
      ['media_messages', '媒体消息', 'number'],
      ['grouped_messages', '媒体组消息', 'number'],
      ['media_metadata_coverage_percent', '媒体元信息覆盖', 'percent'],
      ['media_group_count', '媒体组数量', 'number'],
      ['suspect_media_group_count', '疑似残缺媒体组', 'number'],
      ['suspect_media_group_ratio_percent', '残缺媒体组占比', 'percent']
    ];

    items.forEach(function (item) {
      var card = document.createElement('li');
      var label = document.createElement('span');
      var value = document.createElement('strong');
      card.className = 'clone-stat-card';
      label.className = 'clone-stat-label';
      value.className = 'clone-stat-value';
      label.textContent = item[1];
      value.textContent = item[2] === 'percent'
        ? formatPercent(metrics[item[0]])
        : formatNumber(metrics[item[0]]);
      card.appendChild(label);
      card.appendChild(value);
      container.appendChild(card);
    });
  }

  function renderCapabilities(container, capabilities) {
    capabilities.forEach(function (capability) {
      var item = document.createElement('article');
      var head = document.createElement('div');
      var title = document.createElement('span');
      var status = document.createElement('span');
      var detail = document.createElement('p');
      var statusValue = String((capability && capability.status) || '').trim();

      item.className = 'clone-capability';
      head.className = 'clone-capability-head';
      title.className = 'clone-capability-title';
      status.className = 'clone-capability-status is-' + statusValue.replace(/_/g, '-');
      detail.className = 'clone-capability-detail';

      title.textContent = String((capability && capability.label) || '');
      status.textContent = getCapabilityStatusLabel(statusValue);
      detail.textContent = String((capability && capability.detail) || '');

      head.appendChild(title);
      head.appendChild(status);
      item.appendChild(head);
      item.appendChild(detail);
      container.appendChild(item);
    });
  }

  function renderWarnings(container, warnings) {
    warnings.forEach(function (warning) {
      var item = document.createElement('li');
      item.textContent = String(warning || '');
      container.appendChild(item);
    });
  }

  function renderRecommendation(container, recommendation) {
    var summary = String((recommendation && recommendation.summary) || '').trim();
    if (!summary) return;
    var level = String((recommendation && recommendation.level) || '').trim();
    var mode = String((recommendation && recommendation.mode) || '').trim();
    var prefix = level ? '评估 ' + level : '评估';
    container.textContent = mode ? prefix + ' / ' + mode + '：' + summary : prefix + '：' + summary;
  }

  function getCapabilityStatusLabel(status) {
    if (status === 'ready') return '可执行';
    if (status === 'blocked') return '阻断';
    if (status === 'requires_source') return '依赖源群';
    if (status === 'deferred') return '后续阶段';
    if (status === 'empty') return '无数据';
    return status || '未知';
  }

  function getReportSourceLabel(report) {
    var source = report && report.source ? report.source : {};
    return String(source.chat_title || source.chat_id || '未知源群组');
  }

  async function handleStartCloneClick(elements) {
    if (!cloneState.report) {
      elements.preflightStatus.textContent = '请先执行预检。';
      return;
    }
    if (!isStartAllowed(elements)) {
      elements.preflightStatus.textContent = '确认码或第二账号状态不满足启动条件。';
      return;
    }

    var source = cloneState.report.source || {};
    var chatId = Number(source.chat_id);
    if (!Number.isFinite(chatId) || !Number.isInteger(chatId)) {
      elements.preflightStatus.textContent = '预检报告中的 chat_id 异常。';
      return;
    }

    setBusy(elements, true);
    try {
      var payload = await postJSON('/api/admin/clone/jobs', {
        chat_id: chatId,
        target_title: elements.targetTitleInput.value,
        target_kind: elements.targetKindSelect.value,
        confirm: elements.confirmInput.value
      });
      var jobId = getCreatedJobId(payload);
      appendLog(elements, '结构克隆任务已创建：' + jobId);
      await loadCloneRuns(elements);
      startJobPolling(elements, jobId);
    } catch (error) {
      appendLog(elements, '创建结构克隆任务失败：' + error.message);
      elements.preflightStatus.textContent = '创建任务失败：' + error.message;
    } finally {
      setBusy(elements, false);
    }
  }

  async function resumeActiveJobPolling(elements) {
    try {
      var payload = await fetchJSON('/api/admin/jobs/active');
      var job = payload && payload.job ? payload.job : null;
      if (!job) return;
      var jobType = String(job.job_type || '').trim();
      var status = String(job.status || '').trim();
      if (!isCloneJobType(jobType) || ['queued', 'running'].indexOf(status) === -1) {
        return;
      }
      appendLog(elements, '检测到未完成的克隆系统任务，恢复日志轮询：' + String(job.job_id));
      startJobPolling(elements, String(job.job_id));
    } catch (error) {
      appendLog(elements, '检查活跃克隆任务失败：' + error.message);
    }
  }

  async function loadCloneRuns(elements) {
    var selected = getSelectedSourceChat(elements);
    setElementDisabled(elements.runsRefreshBtn, true);
    elements.runsStatus.textContent = selected
      ? '正在读取当前源群组的克隆记录...'
      : '正在读取最近克隆记录...';
    try {
      var url = selected
        ? '/api/admin/clone/runs?source_chat_id='
          + encodeURIComponent(String(selected.chat_id))
          + '&limit=20'
        : '/api/admin/clone/runs?limit=20';
      var payload = await fetchJSON(url);
      cloneState.runs = Array.isArray(payload.items) ? payload.items : [];
      syncSelectedRunFromRuns();
      renderCloneRuns(elements, cloneState.runs);
      elements.runsStatus.textContent = selected
        ? cloneState.runs.length
          ? '当前源群组共有 ' + formatNumber(cloneState.runs.length) + ' 条克隆记录。'
          : '当前源群组暂无克隆记录。'
        : cloneState.runs.length
          ? '最近共有 ' + formatNumber(cloneState.runs.length) + ' 条克隆记录。'
          : '暂无克隆记录。';
      persistConsoleState(elements);
      await loadSelectedRunPlan(elements);
      await loadSelectedRunMigration(elements);
	    } catch (error) {
	      cloneState.runs = [];
	      cloneState.selectedRunId = '';
	      cloneState.plan = null;
	      cloneState.migration = null;
	      cloneState.timelineMigration = null;
      cloneState.timelinePreview = null;
      renderCloneRuns(elements, []);
      renderMigrationPlan(elements, null);
      renderTimelineMigration(elements, null);
      elements.runsStatus.textContent = '读取克隆记录失败：' + error.message;
      appendLog(elements, '读取克隆记录失败：' + error.message);
      persistConsoleState(elements);
    } finally {
      setElementDisabled(elements.runsRefreshBtn, cloneState.busy || jobPollState.isPolling);
    }
  }

  function renderCloneRuns(elements, runs) {
    elements.runsList.textContent = '';
    if (!Array.isArray(runs) || runs.length <= 0) {
      var empty = document.createElement('p');
      empty.className = 'clone-run-empty';
      empty.textContent = '暂无克隆运行记录';
      elements.runsList.appendChild(empty);
      return;
    }

    runs.forEach(function (run) {
      elements.runsList.appendChild(createCloneRunCard(elements, run));
    });
  }

  function createCloneRunCard(elements, run) {
    var normalizedStatus = String((run && run.status) || '').trim().toLowerCase();
    var card = document.createElement('article');
    var head = document.createElement('div');
    var title = document.createElement('h3');
    var status = document.createElement('span');
    var meta = document.createElement('div');
    var actions = document.createElement('div');

    card.className = 'clone-run-card is-' + (normalizedStatus || 'unknown');
    head.className = 'clone-run-head';
    title.className = 'clone-run-title';
    status.className = 'clone-run-status is-' + (normalizedStatus || 'unknown');
    meta.className = 'clone-run-meta';
    actions.className = 'clone-run-actions';

    title.textContent = buildCloneRunTitle(run);
    status.textContent = getRunStatusLabel(normalizedStatus);
    head.appendChild(title);
    head.appendChild(status);
    card.appendChild(head);

    appendRunPill(meta, '源群组', (run && run.source_title) || '未知源');
    appendRunPill(meta, '目标类型', getTargetKindLabel((run && run.target_kind) || ''));
    appendRunPill(meta, '目标 ID', run && run.target_chat_id ? String(run.target_chat_id) : '未创建');
    appendRunPill(meta, '阶段', getRunPhaseLabel((run && run.phase) || ''));
    appendRunPill(meta, '创建时间', formatDateTime((run && run.created_at) || ''));
    appendRunPill(meta, '完成时间', formatDateTime((run && run.completed_at) || ''));
    card.appendChild(meta);

    appendRunLink(actions, '打开源群', run && run.source_telegram_app_link);
    appendRunLink(actions, '打开目标', run && run.target_telegram_app_link);
    appendRunSelectButton(elements, actions, run);
    if (actions.childNodes.length > 0) {
      card.appendChild(actions);
    }

    if (run && run.error_message) {
      var error = document.createElement('p');
      error.className = 'clone-run-error';
      error.textContent = '错误：' + String(run.error_message);
      card.appendChild(error);
    }
    return card;
  }

  function buildCloneRunTitle(run) {
    var sourceTitle = String((run && run.source_title) || '未知源').trim();
    var targetTitle = String((run && run.target_title) || '').trim();
    return targetTitle ? sourceTitle + ' -> ' + targetTitle : sourceTitle;
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

  function appendRunSelectButton(elements, container, run) {
    if (!isSelectableCloneRun(run)) return;
    var button = document.createElement('button');
    var runId = String((run && run.run_id) || '').trim();
    var selected = runId && runId === cloneState.selectedRunId;
    button.type = 'button';
    button.className = selected ? 'btn clone-run-select is-selected' : 'btn clone-run-select';
    button.textContent = selected ? '已选择' : '选择副本';
    button.setAttribute('aria-label', '选择 ' + buildCloneRunTitle(run) + ' 作为深度预检目标');
    button.disabled = selected || cloneState.busy || jobPollState.isPolling;
    button.addEventListener('click', function () {
      handleSelectCloneRun(elements, runId);
    });
    container.appendChild(button);
  }

  function isSelectableCloneRun(run) {
    return String((run && run.status) || '').trim().toLowerCase() === 'done'
      && !!String((run && run.run_id) || '').trim()
      && !!(run && run.target_chat_id);
  }

  function syncSelectedRunFromRuns() {
    if (
      cloneState.selectedRunId
      && isSelectableCloneRun(findCloneRun(cloneState.selectedRunId))
    ) {
      return;
    }
    cloneState.selectedRunId = '';
    for (var i = 0; i < cloneState.runs.length; i += 1) {
      if (isSelectableCloneRun(cloneState.runs[i])) {
        cloneState.selectedRunId = String(cloneState.runs[i].run_id);
        return;
      }
    }
  }

  function findCloneRun(runId) {
    var normalized = String(runId || '').trim();
    if (!normalized) return null;
    for (var i = 0; i < cloneState.runs.length; i += 1) {
      if (String(cloneState.runs[i].run_id) === normalized) {
        return cloneState.runs[i];
      }
    }
    return null;
  }

  function getSelectedCloneRun() {
    return findCloneRun(cloneState.selectedRunId);
  }

  async function handleSelectCloneRun(elements, runId) {
    cloneState.selectedRunId = String(runId || '').trim();
    cloneState.plan = null;
    cloneState.migration = null;
    cloneState.timelineMigration = null;
    cloneState.timelinePreview = null;
    persistConsoleState(elements);
    syncUrlRunId(cloneState.selectedRunId);
    renderCloneRuns(elements, cloneState.runs);
    renderMigrationPlan(elements, null);
    renderTimelineMigration(elements, null);
    await loadSelectedRunPlan(elements);
    await loadSelectedRunMigration(elements);
  }

  async function loadSelectedRunPlan(elements) {
    var run = getSelectedCloneRun();
    if (!run) {
      cloneState.plan = null;
      renderMigrationPlan(elements, null);
      elements.planStatus.textContent = '请选择已创建成功的副本。';
      setBusy(elements, cloneState.busy);
      return;
    }

    setElementDisabled(elements.planRefreshBtn, true);
    setElementDisabled(elements.deepPreflightBtn, true);
    elements.planStatus.textContent = '正在读取迁移计划...';
    try {
      var payload = await fetchJSON(
        '/api/admin/clone/runs/'
          + encodeURIComponent(String(run.run_id))
          + '/plan'
      );
      cloneState.plan = payload && payload.plan ? payload.plan : null;
      renderMigrationPlan(elements, cloneState.plan);
      elements.planStatus.textContent = cloneState.plan
        ? '已读取最新迁移计划。'
        : '当前副本还没有在线深度预检计划。';
    } catch (error) {
      cloneState.plan = null;
      renderMigrationPlan(elements, null);
      elements.planStatus.textContent = '读取迁移计划失败：' + error.message;
      appendLog(elements, '读取迁移计划失败：' + error.message);
    } finally {
      setBusy(elements, cloneState.busy);
    }
  }

  async function loadSelectedRunMigration(elements) {
	    var run = getSelectedCloneRun();
	    if (!run) {
	      cloneState.migration = null;
	      cloneState.timelineMigration = null;
	      cloneState.timelinePreview = null;
	      renderTimelineMigration(elements, null);
      return;
    }

    try {
      var payload = await fetchJSON(
        '/api/admin/clone/runs/'
          + encodeURIComponent(String(run.run_id))
          + '/migration'
      );
	      cloneState.migration = payload && payload.migration ? payload.migration : null;
	      cloneState.timelineMigration = payload && payload.timeline_migration
	        ? payload.timeline_migration
	        : null;
	      cloneState.timelinePreview = payload && payload.timeline_preview
	        ? payload.timeline_preview
	        : null;
	      renderTimelineMigration(elements, cloneState.timelineMigration);
    } catch (error) {
	      cloneState.migration = null;
	      cloneState.timelineMigration = null;
	      cloneState.timelinePreview = null;
	      renderTimelineMigration(elements, null);
      appendLog(elements, '读取克隆迁移记录失败：' + error.message);
    } finally {
      setBusy(elements, cloneState.busy);
    }
  }

	  async function handleDeepPreflightClick(elements) {
    var run = getSelectedCloneRun();
    if (!run) {
      elements.planStatus.textContent = '请先选择已创建成功的副本。';
      return;
    }

    setBusy(elements, true);
    try {
      var payload = await postJSON(
        '/api/admin/clone/runs/'
          + encodeURIComponent(String(run.run_id))
          + '/deep-preflight',
        {}
      );
      var jobId = getCreatedJobId(payload);
      cloneState.plan = payload && payload.plan ? payload.plan : null;
      renderMigrationPlan(elements, cloneState.plan);
      appendLog(elements, '深度预检任务已创建：' + jobId);
      startJobPolling(elements, jobId);
    } catch (error) {
      appendLog(elements, '创建深度预检任务失败：' + error.message);
      elements.planStatus.textContent = '创建深度预检任务失败：' + error.message;
	    } finally {
	      setBusy(elements, false);
	    }
	  }

	  async function handleTimelineMigrationClick(elements) {
	    var run = getSelectedCloneRun();
	    if (!run) {
	      elements.timelineStatus.textContent = '请先选择已创建成功的副本。';
	      return;
	    }
	    if (!isTimelineMigrationAllowed()) {
	      elements.timelineStatus.textContent = '最新迁移计划未满足完整时间线迁移条件。';
	      return;
	    }

	    setBusy(elements, true);
	    try {
	      var payload = await postJSON(
	        '/api/admin/clone/runs/'
	          + encodeURIComponent(String(run.run_id))
	          + '/migrate-timeline',
	        buildTimelineMigrationOptions(elements)
	      );
	      var jobId = getCreatedJobId(payload);
	      cloneState.migration = payload && payload.migration ? payload.migration : null;
	      cloneState.timelineMigration = cloneState.migration;
	      if (payload && payload.timeline_preview) cloneState.timelinePreview = payload.timeline_preview;
	      renderTimelineMigration(elements, cloneState.timelineMigration);
	      appendLog(elements, '完整时间线迁移任务已创建：' + jobId);
	      startJobPolling(elements, jobId);
	    } catch (error) {
	      appendLog(elements, '创建完整时间线迁移任务失败：' + error.message);
	      elements.timelineStatus.textContent = '创建完整时间线迁移任务失败：' + error.message;
	    } finally {
	      setBusy(elements, false);
	    }
	  }

	  function renderMigrationPlan(elements, plan) {
    var run = getSelectedCloneRun();
    elements.planSummary.textContent = '';
    elements.planBlocking.textContent = '';
    elements.planWarnings.textContent = '';
    elements.planRunLabel.textContent = run
      ? '当前副本：' + buildCloneRunTitle(run)
      : '当前未选择副本。';

    if (!run) {
      appendSummaryPair(elements.planSummary, '源访问', '未选择');
      appendSummaryPair(elements.planSummary, '目标访问', '未选择');
      appendSummaryPair(elements.planSummary, '文本策略', '未选择');
      appendSummaryPair(elements.planSummary, '媒体策略', '未选择');
      renderPlanList(elements.planBlocking, []);
      renderPlanList(elements.planWarnings, []);
      return;
    }

    if (!plan) {
      appendSummaryPair(elements.planSummary, '源访问', '未预检');
      appendSummaryPair(elements.planSummary, '目标访问', '未预检');
      appendSummaryPair(elements.planSummary, '文本策略', '未生成');
      appendSummaryPair(elements.planSummary, '媒体策略', '未生成');
      renderPlanList(elements.planBlocking, []);
      renderPlanList(elements.planWarnings, []);
      return;
    }

    appendSummaryPair(elements.planSummary, '计划状态', getPlanStatusLabel(plan.status));
    appendSummaryPair(elements.planSummary, '源访问', getAccessStatusLabel(plan.source_access));
    appendSummaryPair(elements.planSummary, '目标访问', getAccessStatusLabel(plan.target_access));
    appendSummaryPair(elements.planSummary, '迁移账号', getMigrationAccountLabel(getPlanTargetWriteAccount(plan)));
    appendSummaryPair(elements.planSummary, '文本策略', getTextStrategyLabel(plan.text_strategy));
    appendSummaryPair(elements.planSummary, '媒体策略', getMediaStrategyLabel(plan.media_strategy));
    appendSummaryPair(elements.planSummary, '媒体组策略', getMediaGroupStrategyLabel(plan.media_group_strategy));
    appendSummaryPair(elements.planSummary, '头像策略', getAvatarStrategyLabel(plan.avatar_strategy));
    renderPlanList(elements.planBlocking, plan.blocking_issues || []);
	    renderPlanList(elements.planWarnings, plan.warnings || []);
	  }

	  function renderTimelineMigration(elements, migration) {
	    elements.timelineSummary.textContent = '';
	    var preview = cloneState.timelinePreview || null;
	    var timelineMigration = migration && String(migration.mode || '') === 'timeline_replay'
	      ? migration
	      : null;
	    if (!getSelectedCloneRun()) {
	      elements.timelineStatus.textContent = '请选择已创建成功的副本。';
	      appendSummaryPair(elements.timelineSummary, '状态', '未选择');
	      appendSummaryPair(elements.timelineSummary, '时间线总数', '0');
	      appendSummaryPair(elements.timelineSummary, '剩余时间线', '0');
	      return;
	    }
	    if (!timelineMigration) {
	      elements.timelineStatus.textContent = '尚未执行完整时间线迁移。';
	      appendSummaryPair(elements.timelineSummary, '状态', '未执行');
	      appendTimelinePreviewSummary(elements, preview);
	      return;
	    }
	    elements.timelineStatus.textContent = buildTimelineMigrationStatusText(timelineMigration);
	    appendSummaryPair(elements.timelineSummary, '状态', getMigrationStatusLabel(timelineMigration.status));
	    appendSummaryPair(elements.timelineSummary, '阶段', getMigrationPhaseLabel(timelineMigration.phase));
	    appendSummaryPair(elements.timelineSummary, '文本总数', formatNumber(timelineMigration.text_total));
	    appendSummaryPair(elements.timelineSummary, '文本已发送', formatNumber(timelineMigration.text_sent));
	    appendSummaryPair(elements.timelineSummary, '媒体总数', formatNumber(timelineMigration.media_total));
	    appendSummaryPair(elements.timelineSummary, '媒体已复制', formatNumber(timelineMigration.media_sent));
	    appendSummaryPair(elements.timelineSummary, '媒体组已复制', formatNumber(timelineMigration.media_group_sent));
	    appendSummaryPair(elements.timelineSummary, '执行账号', String(timelineMigration.target_write_account || ''));
	    appendSummaryPair(elements.timelineSummary, '本次上限', formatLimit(timelineMigration.requested_limit));
	    appendSummaryPair(elements.timelineSummary, '发送间隔', formatNumber(timelineMigration.send_delay_ms) + 'ms');
	    appendTimelinePreviewSummary(elements, preview);
	  }

	  function appendTimelinePreviewSummary(elements, preview) {
	    var data = preview && typeof preview === 'object' ? preview : {};
	    var readinessReasons = Array.isArray(data.readiness_reasons) ? data.readiness_reasons : [];
	    appendSummaryPair(
	      elements.timelineSummary,
	      '执行评估',
	      data.can_migrate_timeline ? '可执行' : readinessReasons[0] || '未完成评估'
	    );
	    appendSummaryPair(elements.timelineSummary, '时间线总数', formatNumber(data.timeline_items_total));
	    appendSummaryPair(elements.timelineSummary, '剩余时间线', formatNumber(data.timeline_remaining));
	    appendSummaryPair(elements.timelineSummary, '文本剩余', formatNumber(data.text_remaining));
	    appendSummaryPair(elements.timelineSummary, '媒体剩余', formatNumber(data.media_remaining));
	    appendSummaryPair(elements.timelineSummary, '媒体组总数', formatGroupPair(data.media_group_total, data.media_group_candidate_items));
	    appendSummaryPair(elements.timelineSummary, '数据库风险组', formatGroupPair(data.db_self_check_risk_group_total, data.db_self_check_risk_group_items));
	  }

	  function buildTimelineMigrationStatusText(migration) {
	    var status = getMigrationStatusLabel((migration && migration.status) || '');
	    var updatedAt = formatDateTime((migration && migration.updated_at) || '');
	    var error = String((migration && migration.error_message) || '').trim();
	    if (error) return status + '：' + error;
	    return '最新完整时间线迁移状态：' + status + '，更新时间 ' + updatedAt + '。';
	  }

  function renderPlanList(container, items) {
    container.textContent = '';
    if (!Array.isArray(items) || items.length <= 0) {
      var empty = document.createElement('li');
      empty.textContent = '暂无';
      container.appendChild(empty);
      return;
    }
    items.forEach(function (item) {
      var node = document.createElement('li');
      node.textContent = String(item || '');
      container.appendChild(node);
    });
  }

  function getRunStatusLabel(status) {
    if (status === 'queued') return '排队中';
    if (status === 'running') return '执行中';
    if (status === 'done') return '已创建';
    if (status === 'error') return '失败';
    return status || '未知';
  }

  function getRunPhaseLabel(phase) {
    var normalized = String(phase || '').trim();
    if (normalized === 'queued') return '等待创建';
    if (normalized === 'loading_source') return '读取源群';
    if (normalized === 'validating') return '校验账号';
    if (normalized === 'creating') return '创建结构';
    if (normalized === 'done') return '结构完成';
    if (normalized === 'error') return '失败';
    return normalized || '未知';
  }

  function getTargetKindLabel(kind) {
    var normalized = String(kind || '').trim();
    if (normalized === 'megagroup') return '超级群';
    if (normalized === 'channel') return '频道';
    return normalized || '未知';
  }

  function getPlanStatusLabel(status) {
    var normalized = String(status || '').trim();
    if (normalized === 'queued') return '排队中';
    if (normalized === 'running') return '预检中';
    if (normalized === 'done') return '已生成';
    if (normalized === 'error') return '失败';
    return normalized || '未知';
  }

  function getAccessStatusLabel(status) {
    var normalized = String(status || '').trim();
    if (normalized === 'ok') return '可访问';
    if (normalized === 'missing') return '不存在/未命中';
    if (normalized === 'forbidden') return '无权限';
    if (normalized === 'rate_limited') return '频控中';
    if (normalized === 'error') return '异常';
    if (normalized === 'unknown') return '未知';
    return normalized || '未知';
  }

  function getMigrationAccountLabel(account) {
    var normalized = String(account || '').trim();
    if (normalized === 'primary') return '主账号';
    if (normalized === 'secondary') return '第二账号';
    if (normalized === 'unavailable') return '不可用';
    return normalized || '未确定';
  }

  function getMigrationStatusLabel(status) {
    var normalized = String(status || '').trim();
    if (normalized === 'queued') return '排队中';
    if (normalized === 'running') return '迁移中';
    if (normalized === 'done') return '完成';
    if (normalized === 'error') return '失败';
    return normalized || '未知';
  }

  function getMigrationPhaseLabel(phase) {
    var normalized = String(phase || '').trim();
    if (normalized === 'queued') return '等待迁移';
	    if (normalized === 'validating') return '校验计划';
	    if (normalized === 'connecting') return '连接账号';
	    if (normalized === 'replaying_timeline') return '重放完整时间线';
	    if (normalized === 'sending_text') return '发送文本';
    if (normalized === 'done') return '完成';
    if (normalized === 'limited_done') return '达到本次上限';
    if (normalized === 'stopped') return '已停止';
    if (normalized === 'error') return '失败';
    return normalized || '未知';
  }

  function getTextStrategyLabel(strategy) {
    var normalized = String(strategy || '').trim();
    if (normalized === 'database_replay') return '数据库重放';
    if (normalized === 'blocked') return '阻断';
    return normalized || '未生成';
  }

  function getMediaStrategyLabel(strategy) {
    var normalized = String(strategy || '').trim();
    if (normalized === 'source_copy_without_attribution') return '隐藏来源复制转发';
    if (normalized === 'relay_copy_without_attribution') return '固定中转频道桥接';
    if (normalized === 'impossible_without_local_vault') return '缺本地媒体保险库';
    if (normalized === 'blocked') return '阻断';
    return normalized || '未生成';
  }

  function getMediaGroupStrategyLabel(strategy) {
    var normalized = String(strategy || '').trim();
    if (normalized === 'strict_skip_incomplete') return '残缺组严格跳过';
    if (normalized === 'relay_api_rebuild') return '中转桥接重建相册';
    if (normalized === 'blocked_by_source_access') return '源群不可访问';
    if (normalized === 'blocked') return '阻断';
    return normalized || '未生成';
  }

  function getAvatarStrategyLabel(strategy) {
    var normalized = String(strategy || '').trim();
    if (normalized === 'copy_if_accessible') return '可访问时复制';
    if (normalized === 'skip_not_implemented') return '暂未执行';
    if (normalized === 'skip') return '跳过';
    return normalized || '未生成';
  }

  function startJobPolling(elements, jobId) {
    var normalizedJobId = String(jobId || '').trim();
    if (!normalizedJobId) return;
    stopJobPolling(undefined, elements);
    jobPollState.pollToken += 1;
    jobPollState.jobId = normalizedJobId;
    jobPollState.lastSeq = 0;
    jobPollState.retryCount = 0;
    jobPollState.lastProgressKey = '';
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
        appendLog(elements, getJobDoneMessage(snapshot));
        stopJobPolling(pollToken, elements);
        await loadCloneRuns(elements);
        await loadSelectedRunPlan(elements);
        await loadSelectedRunMigration(elements);
        return;
      }
      if (status === 'error') {
        appendLog(elements, getJobErrorMessage(snapshot));
        stopJobPolling(pollToken, elements);
        await loadCloneRuns(elements);
        await loadSelectedRunPlan(elements);
        await loadSelectedRunMigration(elements);
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

	  function isCloneJobType(jobType) {
	    var normalized = String(jobType || '').trim();
	    return normalized === 'clone_structure'
	      || normalized === 'clone_deep_preflight'
	      || normalized === 'clone_timeline_migration';
  }

	  function getJobDoneMessage(snapshot) {
	    var jobType = String((snapshot && snapshot.job_type) || '').trim();
	    if (jobType === 'clone_deep_preflight') return '深度预检任务执行完成';
	    if (jobType === 'clone_timeline_migration') return '完整时间线迁移任务执行完成';
    return '结构克隆任务执行完成';
  }

  function getJobErrorMessage(snapshot) {
    var jobType = String((snapshot && snapshot.job_type) || '').trim();
    if (jobType === 'clone_deep_preflight') return '深度预检任务执行失败，请检查日志';
    if (jobType === 'clone_timeline_migration') return '完整时间线迁移任务执行失败，请检查日志';
    return '结构克隆任务执行失败，请检查日志';
  }

  function restoreConsoleState(elements) {
    var stored = readStoredConsoleState();
    cloneState.sourceChatId = String(stored.sourceChatId || '').trim();
    cloneState.selectedRunId = getRunIdFromLocation()
      || String(stored.selectedRunId || '').trim();
    elements.sortSelect.value = String(stored.sort || elements.sortSelect.value || '');
    elements.messageLimitInput.value = String(stored.messageLimit || '');
    if (stored.sendDelay !== undefined && stored.sendDelay !== null) {
      elements.sendDelayInput.value = String(stored.sendDelay);
    }
    syncUrlRunId(cloneState.selectedRunId);
  }

  function persistConsoleState(elements) {
    if (!elements) return;
    try {
      window.localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
          sourceChatId: String(elements.sourceSelect.value || cloneState.sourceChatId || ''),
          selectedRunId: String(cloneState.selectedRunId || ''),
          sort: String(elements.sortSelect.value || ''),
          messageLimit: String(elements.messageLimitInput.value || ''),
          sendDelay: String(elements.sendDelayInput.value || '')
        })
      );
    } catch (_error) {
      return;
    }
  }

  function readStoredConsoleState() {
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

  function setBusy(elements, isBusy) {
    cloneState.busy = !!isBusy;
    var disabled = cloneState.busy || jobPollState.isPolling;
    setElementDisabled(elements.sortSelect, disabled);
    setElementDisabled(elements.refreshBtn, disabled);
    setElementDisabled(elements.sourceSelect, disabled);
    setElementDisabled(elements.preflightBtn, disabled || !getSelectedSourceChat(elements));
	    setElementDisabled(elements.runsRefreshBtn, disabled);
	    setElementDisabled(elements.planRefreshBtn, disabled || !getSelectedCloneRun(elements));
	    setElementDisabled(elements.deepPreflightBtn, disabled || !getSelectedCloneRun(elements));
	    setElementDisabled(elements.timelineMigrationBtn, disabled || !isTimelineMigrationAllowed());
    setElementDisabled(elements.messageLimitInput, disabled || !getSelectedCloneRun(elements));
    setElementDisabled(elements.sendDelayInput, disabled || !getSelectedCloneRun(elements));
    setElementDisabled(elements.targetTitleInput, disabled || !cloneState.report);
    setElementDisabled(elements.targetKindSelect, disabled || !cloneState.report);
    setElementDisabled(elements.confirmInput, disabled || !cloneState.report);
    if (elements.logContainer && typeof elements.logContainer.setAttribute === 'function') {
      elements.logContainer.setAttribute('aria-busy', disabled ? 'true' : 'false');
    }
    updateStartButtonState(elements);
  }

  function updateStartButtonState(elements) {
    setElementDisabled(elements.startBtn, !isStartAllowed(elements));
  }

	  function isStartAllowed(elements) {
    if (cloneState.busy || jobPollState.isPolling) return false;
    if (!cloneState.report) return false;
    var account = cloneState.report.account || {};
    if (!account.secondary_session_distinct) return false;
    var expectedConfirm = String(cloneState.report.confirm || '').trim();
    var suppliedConfirm = String(elements.confirmInput.value || '').trim();
    if (!expectedConfirm || expectedConfirm !== suppliedConfirm) return false;
	    return String(elements.targetTitleInput.value || '').trim().length > 0;
	  }

	  function isTimelineMigrationAllowed() {
	    if (cloneState.busy || jobPollState.isPolling) return false;
	    if (!getSelectedCloneRun()) return false;
	    var plan = cloneState.plan || {};
	    if (String(plan.status || '').trim() !== 'done') return false;
	    if (String(plan.target_access || '').trim() !== 'ok') return false;
	    var blocking = Array.isArray(plan.blocking_issues) ? plan.blocking_issues : [];
	    if (blocking.length > 0) return false;
	    if (cloneState.timelinePreview) {
	      if (cloneState.timelinePreview.can_migrate_timeline === false) return false;
	      if (Number(cloneState.timelinePreview.timeline_remaining || 0) <= 0) return false;
	      return true;
	    }
	    var canText = String(plan.text_strategy || '').trim() === 'database_replay'
	      && !!getPlanTargetWriteAccount(plan);
      var canMedia = String(plan.source_access || '').trim() === 'ok'
	      && isAllowedMediaStrategy(plan.media_strategy)
	      && !!getPlanMediaMigrationAccount(plan);
	    return canText || canMedia;
	  }

  function getPlanTargetWriteAccount(plan) {
    var source = plan || {};
    var capabilities = source.capabilities && typeof source.capabilities === 'object'
      ? source.capabilities
      : {};
    var payload = source.plan && typeof source.plan === 'object' ? source.plan : {};
    var candidates = [
      capabilities.target_write_account,
      payload.target_write_account,
      source.migration_account,
      payload.migration_account
    ];
    for (var i = 0; i < candidates.length; i += 1) {
      var normalized = String(candidates[i] || '').trim().toLowerCase();
      if (normalized === 'primary' || normalized === 'secondary') return normalized;
    }
    return '';
  }

  function getPlanMediaMigrationAccount(plan) {
    var source = plan || {};
    var payload = source.plan && typeof source.plan === 'object' ? source.plan : {};
    var relay = source.media_relay && typeof source.media_relay === 'object'
      ? source.media_relay
      : payload.media_relay && typeof payload.media_relay === 'object'
        ? payload.media_relay
        : {};
    if (String(source.media_strategy || '').trim() === 'relay_copy_without_attribution'
      && relay.enabled
      && relay.source_account
      && relay.target_account) {
      return String(relay.source_account).trim() + '->relay->' + String(relay.target_account).trim();
    }
    var candidates = [
      source.migration_account,
      payload.migration_account
    ];
    for (var i = 0; i < candidates.length; i += 1) {
      var normalized = String(candidates[i] || '').trim().toLowerCase();
      if (normalized === 'primary' || normalized === 'secondary') return normalized;
    }
    return '';
  }

  function isAllowedMediaStrategy(strategy) {
    var normalized = String(strategy || '').trim();
    return normalized === 'source_copy_without_attribution'
      || normalized === 'relay_copy_without_attribution';
  }

  function buildTimelineMigrationOptions(elements) {
    return {
      message_limit: normalizeNonnegativeInteger(elements.messageLimitInput.value, 0, 100000),
      send_delay_ms: normalizeNonnegativeInteger(elements.sendDelayInput.value, 500, 60000)
    };
  }

  function normalizeNonnegativeInteger(value, fallback, maxValue) {
    var text = String(value || '').trim();
    var n = text ? Number(text) : Number(fallback || 0);
    if (!Number.isFinite(n)) return Number(fallback || 0);
    n = Math.trunc(n);
    if (n < 0) return 0;
    return Math.min(n, Number(maxValue || n));
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

  function formatLimit(value) {
    var n = Number(value || 0);
    if (!Number.isFinite(n) || n <= 0) return '全部';
    return formatNumber(n);
  }

  function formatGroupPair(groups, items) {
    return formatNumber(groups) + ' 组 / ' + formatNumber(items) + ' 条';
  }

  function formatPercent(value) {
    var n = Number(value || 0);
    if (!Number.isFinite(n)) return '0%';
    return String(Math.round(n * 10) / 10) + '%';
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
      await loadSourceChats(elements);
      await resumeActiveJobPolling(elements);
      if (context.reason === 'login') {
        appendLog(elements, '认证成功，已进入克隆系统');
      }
    },
    getElements: getElements,
    getPageElement: function () {
      return document.getElementById('admin-clone-page');
    }
  });
})();
