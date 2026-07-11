(function () {
  'use strict';

  var shared = window.AdminManageShared;
  var appendLog = shared.appendLog;
  var clearLogs = shared.clearLogs;
  var ensurePlaceholder = shared.ensurePlaceholder;
  var sharedFetchJSON = shared.fetchJSON;
  var sharedPostJSON = shared.postJSON;
  var getCreatedJobId = shared.getCreatedJobId;
  var setElementDisabled = shared.setElementDisabled;
  var syncClearLogsButtonVisibility = shared.syncClearLogsButtonVisibility;
  var trapFocusWithin = shared.trapFocusWithin;
  var normalizeNonnegativeInteger = shared.normalizeNonnegativeInteger;
  var formatDateTime = shared.formatDateTime;
  var formatNumber = shared.formatNumber;

  var JOB_POLL_INTERVAL_MS = 3000;
  var JOB_POLL_RETRY_MAX_COUNT = 20;
  var JOB_POLL_RETRY_BASE_MS = 3000;
  var CLONE_MODE_CREATE = 'create';
  var CLONE_MODE_MIGRATE = 'migrate';

  var cloneState = {
    items: [],
    runs: [],
    sourceChatId: '',
    selectedRunId: '',
    requestedRunId: '',
    requestedRunError: '',
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

  var requestState = {
    runsToken: 0,
    planToken: 0,
    migrationToken: 0
  };

  document.addEventListener('DOMContentLoaded', async function () {
    var elements = getElements();
    if (!elements) {
      return;
    }

    initializeUI(elements);
    bindEvents(elements);
    await sessionController.checkAuth(elements);
  });

  function getElements() {
    var elements = {
      page: document.getElementById('admin-clone-page'),
      sourceStage: document.getElementById('admin-clone-source-stage'),
      createStage: document.getElementById('admin-clone-create-stage'),
      selectStage: document.getElementById('admin-clone-select-stage'),
      planStage: document.getElementById('admin-clone-plan-stage'),
      migrationStage: document.getElementById('admin-clone-migration-stage'),
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
      logPanel: document.getElementById('admin-clone-log-panel'),
      logContainer: document.getElementById('admin-clone-log-container'),
      clearLogsBtn: document.getElementById('admin-clear-clone-logs-btn'),
      loginDialog: document.getElementById('admin-login-dialog'),
      loginStatus: document.getElementById('admin-login-status'),
      passwordInput: document.getElementById('admin-password-input'),
      loginConfirmBtn: document.getElementById('admin-login-confirm-btn')
    };

    if (!elements.page) {
      console.warn('[admin_clone] Missing required element: page');
      return null;
    }

    elements.cloneMode = getPageMode(elements);
    var commonRequiredKeys = [
      'page',
      'sourceStatus',
      'sortSelect',
      'refreshBtn',
      'sourceSelect',
      'runsStatus',
      'runsRefreshBtn',
      'runsList',
      'logContainer',
      'clearLogsBtn',
      'loginDialog',
      'loginStatus',
      'passwordInput',
      'loginConfirmBtn'
    ];
    var createRequiredKeys = [
      'preflightBtn',
      'sourceSummary',
      'sourceStage',
      'createStage',
      'preflightStatus',
      'metricsList',
      'capabilities',
      'warnings',
      'recommendation',
      'targetTitleInput',
      'targetKindSelect',
      'confirmInput',
      'confirmHint',
      'startBtn'
    ];
    var migrateRequiredKeys = [
      'planStatus',
      'selectStage',
      'planStage',
      'migrationStage',
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
      'timelineSummary'
    ];
    var requiredKeys = commonRequiredKeys.slice();

    if (isCreatePage(elements)) {
      requiredKeys = requiredKeys.concat(createRequiredKeys);
    } else if (isMigratePage(elements)) {
      requiredKeys = requiredKeys.concat(migrateRequiredKeys);
    }

    var missing = requiredKeys.filter(function (key) {
      return !elements[key];
    });
    if (missing.length > 0) {
      console.warn('[admin_clone] Missing required elements:', missing.join(', '));
      return null;
    }
    return elements;
  }

  function getPageMode(elements) {
    var page = elements && elements.page ? elements.page : null;
    var rawMode = page && typeof page.getAttribute === 'function'
      ? page.getAttribute('data-clone-mode')
      : '';
    var normalized = String(rawMode || '').trim().toLowerCase();
    return normalized === CLONE_MODE_MIGRATE ? CLONE_MODE_MIGRATE : CLONE_MODE_CREATE;
  }

  function isCreatePage(elements) {
    return getPageMode(elements) === CLONE_MODE_CREATE;
  }

  function isMigratePage(elements) {
    return getPageMode(elements) === CLONE_MODE_MIGRATE;
  }

  function setLoginStatus(elements, message) {
    shared.setLoginStatus(elements, message);
  }

  function setStageState(element, state) {
    if (!element || typeof element.setAttribute !== 'function') {
      return;
    }
    element.setAttribute('data-stage-state', String(state || 'pending'));
  }

  function syncWorkflowStages(elements) {
    if (isCreatePage(elements)) {
      var hasReport = !!cloneState.report;
      setStageState(elements.sourceStage, hasReport ? 'complete' : 'current');
      setStageState(
        elements.createStage,
        hasReport ? (cloneState.busy || jobPollState.isPolling ? 'active' : 'current') : 'pending'
      );
      return;
    }

    if (!isMigratePage(elements)) {
      return;
    }

    var run = getSelectedCloneRun();
    var plan = cloneState.plan;
    var migration = cloneState.timelineMigration;
    var planStatus = String((plan && plan.status) || '').trim().toLowerCase();
    var migrationStatus = String((migration && migration.status) || '').trim().toLowerCase();
    var hasBlockingIssues = Array.isArray(plan && plan.blocking_issues)
      && plan.blocking_issues.length > 0;
    var remaining = Number((cloneState.timelinePreview || {}).timeline_remaining || 0);

    setStageState(elements.selectStage, run ? 'complete' : 'current');
    if (!run) {
      setStageState(elements.planStage, 'pending');
      setStageState(elements.migrationStage, 'pending');
      return;
    }

    if (!plan) {
      setStageState(elements.planStage, 'current');
      setStageState(elements.migrationStage, 'pending');
      return;
    }
    if (planStatus === 'queued' || planStatus === 'running') {
      setStageState(elements.planStage, 'active');
      setStageState(elements.migrationStage, 'pending');
      return;
    }
    if (planStatus === 'error' || hasBlockingIssues) {
      setStageState(elements.planStage, 'attention');
      setStageState(elements.migrationStage, 'attention');
      return;
    }

    setStageState(elements.planStage, planStatus === 'done' ? 'complete' : 'current');
    if (migrationStatus === 'queued' || migrationStatus === 'running') {
      setStageState(elements.migrationStage, 'active');
      return;
    }
    if (migrationStatus === 'error') {
      setStageState(elements.migrationStage, 'attention');
      return;
    }
    setStageState(
      elements.migrationStage,
      cloneState.timelinePreview && remaining <= 0 ? 'complete' : 'current'
    );
  }

  function openLogPanel(elements) {
    if (elements && elements.logPanel) {
      elements.logPanel.open = true;
    }
  }

  function initializeUI(elements) {
    initializeConsoleState(elements);
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
      if (event.key !== 'Enter') {
        return;
      }
      event.preventDefault();
      sessionController.handleLogin(elements);
    });

    elements.sortSelect.addEventListener('change', function () {
      loadSourceChats(elements);
    });
    elements.refreshBtn.addEventListener('click', function () {
      loadSourceChats(elements);
    });
    elements.sourceSelect.addEventListener('change', function () {
      handleSourceChange(elements, { resetRunSelection: true });
      loadCloneRuns(elements);
    });
    elements.runsRefreshBtn.addEventListener('click', function () {
      loadCloneRuns(elements);
    });
    elements.clearLogsBtn.addEventListener('click', function () {
      clearLogs(elements);
    });

    if (isCreatePage(elements)) {
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
    }

    if (isMigratePage(elements)) {
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
      });
      elements.sendDelayInput.addEventListener('input', function () {
      });
    }

    document.addEventListener('keydown', function (event) {
      if (!elements.loginDialog || elements.loginDialog.hidden) {
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
    elements.sourceStatus.textContent = isCreatePage(elements)
      ? '正在读取可克隆源群组...'
      : '正在读取源群组筛选项...';
    try {
      var payload = await fetchJSON(
        '/api/admin/clone/chats?sort='
          + encodeURIComponent(String(elements.sortSelect.value || ''))
      );
      cloneState.items = Array.isArray(payload.items) ? payload.items : [];
      renderSourceOptions(elements, previousValue);
      elements.sourceStatus.textContent = buildSourceStatusText(elements, cloneState.items.length);
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

  function buildSourceStatusText(elements, count) {
    var total = formatNumber(count || 0);
    if (isCreatePage(elements)) {
      return '共 ' + total + ' 个可选源群组。';
    }
    return '共 ' + total + ' 个源群组，可用于筛选副本记录。';
  }

  function renderSourceOptions(elements, preferredValue) {
    elements.sourceSelect.textContent = '';

    var placeholder = document.createElement('option');
    placeholder.value = '';
    placeholder.textContent = cloneState.items.length
      ? getSourcePlaceholderText(elements)
      : '暂无可选群组';
    elements.sourceSelect.appendChild(placeholder);

    cloneState.items.forEach(function (item) {
      var chatId = item && item.chat_id;
      if (chatId === undefined || chatId === null) {
        return;
      }
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

  function getSourcePlaceholderText(elements) {
    if (isCreatePage(elements)) {
      return '请选择要克隆的源群组';
    }
    return '全部源群组 / 最近副本';
  }

  function handleSourceChange(elements, options) {
    var opts = options || {};
    var selected = getSelectedSourceChat(elements);
    cloneState.report = null;
    cloneState.sourceChatId = selected ? String(selected.chat_id || '') : '';
    if (opts.resetRunSelection || isCreatePage(elements)) {
      cloneState.selectedRunId = '';
      if (opts.resetRunSelection) {
        cloneState.requestedRunId = '';
        cloneState.requestedRunError = '';
      }
      syncUrlRunId(elements, '');
    }
    cloneState.plan = null;
    cloneState.migration = null;
    cloneState.timelineMigration = null;
    cloneState.timelinePreview = null;
    renderSourceSummary(elements, selected);
    renderReport(elements, null);
    renderMigrationPlan(elements, null);
    renderTimelineMigration(elements, null);

    if (isCreatePage(elements)) {
      if (selected) {
        elements.targetKindSelect.value = inferTargetKindFromSource(selected.chat_type);
        elements.preflightStatus.textContent = '已选择源群组，可以开始检查。';
      } else {
        elements.preflightStatus.textContent = '请选择源群组后开始检查。';
      }
    }

    if (isMigratePage(elements)) {
      elements.planStatus.textContent = '选择副本后生成迁移方案。';
      elements.timelineStatus.textContent = '等待迁移方案完成。';
    }

    setBusy(elements, cloneState.busy);
  }

  function getSelectedSourceChat(elements) {
    return findSourceChat(String(elements.sourceSelect.value || ''));
  }

  function findSourceChat(chatId) {
    var normalized = String(chatId || '').trim();
    if (!normalized) {
      return null;
    }
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
    if (!elements.sourceSummary) {
      return;
    }
    elements.sourceSummary.textContent = '';
    if (!item) {
      appendSummaryPair(
        elements.sourceSummary,
        '当前源',
        isCreatePage(elements) ? '未选择' : '全部源群组'
      );
      appendSummaryPair(elements.sourceSummary, '消息数', '0');
      appendSummaryPair(elements.sourceSummary, '媒体元信息', '0');
      appendSummaryPair(elements.sourceSummary, '最后消息', '暂无');
      return;
    }

    appendSummaryPair(elements.sourceSummary, '当前源', item.chat_title || item.chat_id);
    appendSummaryPair(elements.sourceSummary, '消息数', formatNumber(item.message_count));
    appendSummaryPair(elements.sourceSummary, '媒体元信息', formatNumber(item.media_rows));
    appendSummaryPair(
      elements.sourceSummary,
      '最后消息',
      formatDateTime(item.last_message_at || item.last_seen_at)
    );
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
    elements.preflightStatus.textContent = '正在执行检查：读取数据库摘要并验证账号配置...';
    try {
      var payload = await postJSON('/api/admin/clone/preflight', {
        chat_id: Number(selected.chat_id)
      });
      cloneState.report = payload && payload.report ? payload.report : null;
      if (!cloneState.report) {
        throw new Error('检查响应缺少 report');
      }
      renderReport(elements, cloneState.report);
      appendLog(elements, '检查完成：' + getReportSourceLabel(cloneState.report));
    } catch (error) {
      elements.preflightStatus.textContent = '检查失败：' + error.message;
      appendLog(elements, '检查失败：' + error.message);
      openLogPanel(elements);
    } finally {
      setBusy(elements, false);
    }
  }

  function renderReport(elements, report) {
    if (!elements.metricsList) {
      return;
    }

    elements.metricsList.textContent = '';
    elements.capabilities.textContent = '';
    elements.warnings.textContent = '';
    elements.recommendation.textContent = '';
    elements.confirmInput.value = '';
    elements.confirmHint.textContent = '完成检查后会自动填入操作确认码。';
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
    elements.confirmHint.textContent = '确认码已填入，核对标题后即可创建空副本。';

    renderMetricCards(elements.metricsList, metrics);
    renderCapabilities(elements.capabilities, report.capabilities || []);
    renderWarnings(elements.warnings, report.warnings || []);
    renderRecommendation(elements.recommendation, report.recommendation || {});

    var account = report.account || {};
    elements.preflightStatus.textContent = account.secondary_session_distinct
      ? '检查完成：当前配置允许创建空副本。'
      : '检查完成：第二账号未就绪，暂不能创建空副本。';
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
    if (!summary) {
      return;
    }
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
      elements.preflightStatus.textContent = '请先完成检查。';
      return;
    }
    if (!isStartAllowed(elements)) {
      elements.preflightStatus.textContent = '确认码不匹配，或第二账号暂时不能创建空副本。';
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
      appendLog(elements, '空副本创建任务已创建：' + jobId);
      openLogPanel(elements);
      elements.runsStatus.textContent = '克隆群创建任务已提交，正在刷新创建记录...';
      await loadCloneRuns(elements);
      startJobPolling(elements, jobId);
    } catch (error) {
      appendLog(elements, '创建空副本任务失败：' + error.message);
      elements.preflightStatus.textContent = '创建任务失败：' + error.message;
      openLogPanel(elements);
    } finally {
      setBusy(elements, false);
    }
  }

  async function resumeActiveJobPolling(elements) {
    try {
      var payload = await fetchJSON('/api/admin/jobs/active');
      var job = payload && payload.job ? payload.job : null;
      if (!job) {
        return;
      }
      var jobType = String(job.job_type || '').trim();
      var status = String(job.status || '').trim();
      if (!isCloneJobType(jobType) || ['queued', 'running'].indexOf(status) === -1) {
        return;
      }
      appendLog(elements, '检测到未完成的克隆任务，恢复日志轮询：' + String(job.job_id));
      openLogPanel(elements);
      startJobPolling(elements, String(job.job_id));
    } catch (error) {
      appendLog(elements, '检查活跃克隆任务失败：' + error.message);
    }
  }

  async function loadCloneRuns(elements) {
    var requestToken = requestState.runsToken + 1;
    requestState.runsToken = requestToken;
    var selected = getSelectedSourceChat(elements);
    setElementDisabled(elements.runsRefreshBtn, true);
    elements.runsStatus.textContent = buildRunsLoadingText(elements, selected);
    try {
      var url = selected
        ? '/api/admin/clone/runs?source_chat_id='
          + encodeURIComponent(String(selected.chat_id))
          + '&limit=20'
        : '/api/admin/clone/runs?limit=20';
      var payload = await fetchJSON(url);
      if (requestToken !== requestState.runsToken) {
        return;
      }
      cloneState.runs = Array.isArray(payload.items) ? payload.items : [];
      if (isMigratePage(elements)) {
        await resolveRequestedCloneRun(requestToken);
        if (requestToken !== requestState.runsToken) {
          return;
        }
        syncSelectedRunFromRuns();
      } else {
        cloneState.selectedRunId = '';
      }
      syncUrlRunId(elements, cloneState.selectedRunId);
      renderCloneRuns(elements, cloneState.runs);
      elements.runsStatus.textContent = buildRunsStatusText(elements, selected, cloneState.runs);
      if (isMigratePage(elements)) {
        await loadSelectedRunPlan(elements);
        if (requestToken !== requestState.runsToken) {
          return;
        }
        await loadSelectedRunMigration(elements);
      }
    } catch (error) {
      if (requestToken !== requestState.runsToken) {
        return;
      }
      cloneState.runs = [];
      cloneState.selectedRunId = '';
      cloneState.plan = null;
      cloneState.migration = null;
      cloneState.timelineMigration = null;
      cloneState.timelinePreview = null;
      renderCloneRuns(elements, []);
      renderMigrationPlan(elements, null);
      renderTimelineMigration(elements, null);
      elements.runsStatus.textContent = '读取克隆群记录失败：' + error.message;
      appendLog(elements, '读取克隆群记录失败：' + error.message);
      syncUrlRunId(elements, '');
    } finally {
      if (requestToken !== requestState.runsToken) {
        return;
      }
      setElementDisabled(elements.runsRefreshBtn, cloneState.busy || jobPollState.isPolling);
    }
  }

  function buildRunsLoadingText(elements, selectedSource) {
    if (isCreatePage(elements)) {
      return selectedSource
        ? '正在读取当前源群组的创建记录...'
        : '正在读取最近创建记录...';
    }
    return selectedSource
      ? '正在读取当前源群组的克隆群记录...'
      : '正在读取最近可继续克隆的记录...';
  }

  function buildRunsStatusText(elements, selectedSource, runs) {
    var count = Array.isArray(runs) ? runs.length : 0;
    var total = formatNumber(count);
    if (isCreatePage(elements)) {
      if (selectedSource) {
        return count
          ? '当前源群组有 ' + total + ' 条创建记录。'
          : '当前源群组还没有创建记录。';
      }
      return count
        ? '最近 ' + total + ' 条创建记录。'
        : '暂无创建记录。';
    }

    if (cloneState.requestedRunError) {
      return cloneState.requestedRunError + '，请从列表中手动选择其他克隆群。';
    }
    if (selectedSource) {
      return count
        ? '当前源群组有 ' + total + ' 条记录。'
        : '当前源群组暂无克隆群记录。';
    }
    return count
      ? '最近 ' + total + ' 条克隆群记录。'
      : '暂无克隆群记录。';
  }

  function renderCloneRuns(elements, runs) {
    elements.runsList.textContent = '';
    if (!Array.isArray(runs) || runs.length <= 0) {
      var empty = document.createElement('p');
      empty.className = 'clone-run-empty';
      empty.textContent = isCreatePage(elements)
        ? '暂无创建记录'
        : '当前筛选条件下暂无可继续克隆的群';
      elements.runsList.appendChild(empty);
      return;
    }

    runs.forEach(function (run) {
      elements.runsList.appendChild(createCloneRunCard(elements, run));
    });
  }

  function createCloneRunCard(elements, run) {
    var normalizedStatus = String((run && run.status) || '').trim().toLowerCase();
    var runId = String((run && run.run_id) || '').trim();
    var selected = isMigratePage(elements) && runId && runId === cloneState.selectedRunId;
    var card = document.createElement('article');
    var head = document.createElement('div');
    var title = document.createElement('h3');
    var status = document.createElement('span');
    var meta = document.createElement('div');
    var actions = document.createElement('div');

    card.className = 'clone-run-card is-' + (normalizedStatus || 'unknown');
    if (selected) {
      card.className += ' is-selected';
    }
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
    appendRunPill(meta, '目标副本', getCloneRunTargetLabel(run));
    appendRunPill(meta, '阶段', getRunPhaseLabel((run && run.phase) || ''));
    appendRunPill(meta, '最近更新时间', formatDateTime((run && run.updated_at) || ''));
    card.appendChild(meta);

    appendRunLink(actions, '打开源群', run && run.source_telegram_app_link);
    appendRunLink(actions, '打开目标', run && run.target_telegram_app_link);
    appendRunDetailPageLink(actions, run);
    if (isCreatePage(elements)) {
      appendCreatePageRunAction(actions, run);
    } else {
      appendMigratePageRunAction(elements, actions, run);
    }
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

  function getCloneRunTargetLabel(run) {
    var targetTitle = String((run && run.target_title) || '').trim();
    if (targetTitle) {
      return targetTitle;
    }
    if (run && run.target_chat_id) {
      return String(run.target_chat_id);
    }
    return '未创建';
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
    if (!normalizedHref) {
      return;
    }
    var link = document.createElement('a');
    link.href = normalizedHref;
    link.textContent = String(label || '');
    link.rel = 'noopener noreferrer';
    container.appendChild(link);
  }

  function appendCreatePageRunAction(container, run) {
    var runId = String((run && run.run_id) || '').trim();
    var normalizedStatus = String((run && run.status) || '').trim().toLowerCase();
    if (isSelectableCloneRun(run)) {
      appendRunPageLink(
        container,
        '继续克隆消息',
        '/admin/clone/migrate?run_id=' + encodeURIComponent(runId)
      );
      return;
    }
    if (normalizedStatus === 'queued' || normalizedStatus === 'running') {
      appendRunHint(container, '克隆群创建完成后，这里会出现“继续克隆消息”。');
      return;
    }
    if (normalizedStatus === 'error') {
      appendRunHint(container, '这条克隆群创建失败，可先进入群详情排查原因。');
    }
  }

  function appendMigratePageRunAction(elements, container, run) {
    var normalizedStatus = String((run && run.status) || '').trim().toLowerCase();
    if (isSelectableCloneRun(run)) {
      appendRunSelectButton(elements, container, run);
      return;
    }
    if (normalizedStatus === 'queued' || normalizedStatus === 'running') {
      appendRunHint(container, '克隆群还没创建完成，暂不能继续克隆消息。');
      return;
    }
    if (normalizedStatus === 'error') {
      appendRunHint(container, '这条克隆群创建失败，可先进入群详情排查原因。');
    }
  }

  function appendRunDetailPageLink(container, run) {
    var runId = String((run && run.run_id) || '').trim();
    if (!runId) {
      return;
    }
    appendRunPageLink(
      container,
      '进入群详情',
      '/admin/clone/runs/detail?run_id=' + encodeURIComponent(runId)
    );
  }

  function appendRunPageLink(container, label, href) {
    var link = document.createElement('a');
    link.href = String(href || '');
    link.textContent = String(label || '');
    container.appendChild(link);
  }

  function appendRunHint(container, message) {
    var hint = document.createElement('span');
    hint.className = 'clone-run-note';
    hint.textContent = String(message || '');
    container.appendChild(hint);
  }

  function appendRunSelectButton(elements, container, run) {
    if (!isSelectableCloneRun(run)) {
      return;
    }
    var button = document.createElement('button');
    var runId = String((run && run.run_id) || '').trim();
    var selected = runId && runId === cloneState.selectedRunId;
    button.type = 'button';
    button.className = selected ? 'btn clone-run-select is-selected' : 'btn clone-run-select';
    button.textContent = selected ? '当前克隆群' : '选这个群继续克隆';
    button.setAttribute('aria-label', '选择 ' + buildCloneRunTitle(run) + ' 作为后续继续克隆的目标群');
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
    if (cloneState.selectedRunId && isSelectableCloneRun(findCloneRun(cloneState.selectedRunId))) {
      return;
    }
    if (cloneState.requestedRunId) {
      cloneState.selectedRunId = '';
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
    if (!normalized) {
      return null;
    }
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

  async function resolveRequestedCloneRun(requestToken) {
    var requestedRunId = String(cloneState.requestedRunId || '').trim();
    if (!requestedRunId || findCloneRun(requestedRunId)) {
      cloneState.requestedRunError = '';
      return;
    }

    try {
      var payload = await fetchJSON(
        '/api/admin/clone/runs/'
          + encodeURIComponent(requestedRunId)
          + '/detail'
      );
      if (requestToken !== requestState.runsToken) {
        return;
      }

      var run = payload && payload.run ? payload.run : null;
      if (String((run && run.run_id) || '').trim() !== requestedRunId) {
        throw new Error('指定记录不存在');
      }

      cloneState.runs = [run].concat(
        cloneState.runs.filter(function (item) {
          return String((item && item.run_id) || '').trim() !== requestedRunId;
        })
      );
      cloneState.requestedRunError = '';
    } catch (error) {
      if (requestToken !== requestState.runsToken) {
        return;
      }
      cloneState.selectedRunId = '';
      cloneState.requestedRunError = '无法读取 URL 指定的克隆群记录：' + error.message;
    }
  }

  async function handleSelectCloneRun(elements, runId) {
    if (!isMigratePage(elements)) {
      return;
    }
    cloneState.requestedRunId = '';
    cloneState.requestedRunError = '';
    cloneState.selectedRunId = String(runId || '').trim();
    cloneState.plan = null;
    cloneState.migration = null;
    cloneState.timelineMigration = null;
    cloneState.timelinePreview = null;
    syncUrlRunId(elements, cloneState.selectedRunId);
    renderCloneRuns(elements, cloneState.runs);
    renderMigrationPlan(elements, null);
    renderTimelineMigration(elements, null);
    setBusy(elements, cloneState.busy);
    await loadSelectedRunPlan(elements);
    await loadSelectedRunMigration(elements);
  }

  async function loadSelectedRunPlan(elements) {
    if (!isMigratePage(elements)) {
      return;
    }
    var requestToken = requestState.planToken + 1;
    requestState.planToken = requestToken;
    var run = getSelectedCloneRun();
    if (!run) {
      cloneState.plan = null;
      renderMigrationPlan(elements, null);
      elements.planStatus.textContent = '请选择已创建成功的克隆群。';
      setBusy(elements, cloneState.busy);
      return;
    }

    setElementDisabled(elements.planRefreshBtn, true);
    setElementDisabled(elements.deepPreflightBtn, true);
    elements.planStatus.textContent = '正在读取克隆计划...';
    try {
      var runId = String(run.run_id || '').trim();
      var payload = await fetchJSON(
        '/api/admin/clone/runs/'
          + encodeURIComponent(runId)
          + '/plan'
      );
      if (requestToken !== requestState.planToken || runId !== cloneState.selectedRunId) {
        return;
      }
      cloneState.plan = payload && payload.plan ? payload.plan : null;
      renderMigrationPlan(elements, cloneState.plan);
      elements.planStatus.textContent = buildPlanStatusText(cloneState.plan);
    } catch (error) {
      if (requestToken !== requestState.planToken) {
        return;
      }
      cloneState.plan = null;
      renderMigrationPlan(elements, null);
      elements.planStatus.textContent = '读取克隆计划失败：' + error.message;
      appendLog(elements, '读取克隆计划失败：' + error.message);
    } finally {
      if (requestToken !== requestState.planToken) {
        return;
      }
      setBusy(elements, cloneState.busy);
    }
  }

  function buildPlanStatusText(plan) {
    if (!plan) {
      return '尚未生成迁移方案。';
    }
    var status = String(plan.status || '').trim().toLowerCase();
    var blockingIssues = Array.isArray(plan.blocking_issues) ? plan.blocking_issues : [];
    if (status === 'done' && blockingIssues.length > 0) {
      return '迁移方案有阻断项。';
    }
    if (status === 'done') {
      return '迁移方案已就绪。';
    }
    if (status === 'error') {
      return '迁移方案生成失败。';
    }
    if (status === 'running' || status === 'queued') {
      return '迁移方案正在生成。';
    }
    return '已读取最新克隆计划。';
  }

  async function loadSelectedRunMigration(elements) {
    if (!isMigratePage(elements)) {
      return;
    }
    var requestToken = requestState.migrationToken + 1;
    requestState.migrationToken = requestToken;
    var run = getSelectedCloneRun();
    if (!run) {
      cloneState.migration = null;
      cloneState.timelineMigration = null;
      cloneState.timelinePreview = null;
      renderTimelineMigration(elements, null);
      return;
    }

    try {
      var runId = String(run.run_id || '').trim();
      var payload = await fetchJSON(
        '/api/admin/clone/runs/'
          + encodeURIComponent(runId)
          + '/migration'
      );
      if (requestToken !== requestState.migrationToken || runId !== cloneState.selectedRunId) {
        return;
      }
      cloneState.migration = payload && payload.migration ? payload.migration : null;
      cloneState.timelineMigration = payload && payload.timeline_migration
        ? payload.timeline_migration
        : null;
      cloneState.timelinePreview = payload && payload.timeline_preview
        ? payload.timeline_preview
        : null;
      renderTimelineMigration(elements, cloneState.timelineMigration);
    } catch (error) {
      if (requestToken !== requestState.migrationToken) {
        return;
      }
      cloneState.migration = null;
      cloneState.timelineMigration = null;
      cloneState.timelinePreview = null;
      renderTimelineMigration(elements, null);
      appendLog(elements, '读取迁移记录失败：' + error.message);
    } finally {
      if (requestToken !== requestState.migrationToken) {
        return;
      }
      setBusy(elements, cloneState.busy);
    }
  }

  async function handleDeepPreflightClick(elements) {
    var run = getSelectedCloneRun();
    if (!run) {
      elements.planStatus.textContent = '请先选择已创建成功的克隆群。';
      return;
    }

    setBusy(elements, true);
    elements.planStatus.textContent = '正在创建克隆计划任务...';
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
      elements.planStatus.textContent = '克隆计划任务已提交，稍后会自动刷新。';
      appendLog(elements, '克隆计划任务已创建：' + jobId);
      openLogPanel(elements);
      startJobPolling(elements, jobId);
    } catch (error) {
      appendLog(elements, '创建克隆计划任务失败：' + error.message);
      elements.planStatus.textContent = '创建克隆计划任务失败：' + error.message;
      openLogPanel(elements);
    } finally {
      setBusy(elements, false);
    }
  }

  async function handleTimelineMigrationClick(elements) {
    var run = getSelectedCloneRun();
    if (!run) {
      elements.timelineStatus.textContent = '请先选择已创建成功的克隆群。';
      return;
    }
    if (!isTimelineMigrationAllowed()) {
      var preview = cloneState.timelinePreview || {};
      var readinessReasons = Array.isArray(preview.readiness_reasons) ? preview.readiness_reasons : [];
      elements.timelineStatus.textContent = readinessReasons[0] || '克隆计划还未满足继续克隆条件。';
      return;
    }

    setBusy(elements, true);
    elements.timelineStatus.textContent = '正在创建继续克隆任务...';
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
      if (payload && payload.timeline_preview) {
        cloneState.timelinePreview = payload.timeline_preview;
      }
      renderTimelineMigration(elements, cloneState.timelineMigration);
      appendLog(elements, '继续克隆任务已创建：' + jobId);
      openLogPanel(elements);
      startJobPolling(elements, jobId);
    } catch (error) {
      appendLog(elements, '创建继续克隆任务失败：' + error.message);
      elements.timelineStatus.textContent = '创建继续克隆任务失败：' + error.message;
      openLogPanel(elements);
    } finally {
      setBusy(elements, false);
    }
  }

  function renderMigrationPlan(elements, plan) {
    if (!elements.planSummary) {
      return;
    }

    var run = getSelectedCloneRun();
    elements.planSummary.textContent = '';
    elements.planBlocking.textContent = '';
    elements.planWarnings.textContent = '';
    elements.planRunLabel.textContent = run
      ? '当前选中克隆群：' + buildCloneRunTitle(run)
      : '当前未选择克隆群。';

    if (!run) {
      appendSummaryPair(elements.planSummary, '克隆群', '未选择');
      appendSummaryPair(elements.planSummary, '计划状态', '未生成');
      appendSummaryPair(elements.planSummary, '源访问', '未选择');
      appendSummaryPair(elements.planSummary, '目标访问', '未选择');
      renderPlanList(elements.planBlocking, []);
      renderPlanList(elements.planWarnings, []);
      return;
    }

    if (!plan) {
      appendSummaryPair(elements.planSummary, '克隆群', getCloneRunTargetLabel(run));
      appendSummaryPair(elements.planSummary, '计划状态', '未生成');
      appendSummaryPair(elements.planSummary, '源访问', '未预检');
      appendSummaryPair(elements.planSummary, '目标访问', '未预检');
      renderPlanList(elements.planBlocking, []);
      renderPlanList(elements.planWarnings, []);
      return;
    }

    appendSummaryPair(elements.planSummary, '克隆群', getCloneRunTargetLabel(run));
    appendSummaryPair(elements.planSummary, '计划状态', getPlanStatusLabel(plan.status));
    appendSummaryPair(elements.planSummary, '源访问', getAccessStatusLabel(plan.source_access));
    appendSummaryPair(elements.planSummary, '目标访问', getAccessStatusLabel(plan.target_access));
    appendSummaryPair(
      elements.planSummary,
      '文本发送账号',
      getMigrationAccountLabel(getPlanTargetWriteAccount(plan))
    );
    appendSummaryPair(elements.planSummary, '文本方式', getTextStrategyLabel(plan.text_strategy));
    appendSummaryPair(elements.planSummary, '媒体方式', getMediaStrategyLabel(plan.media_strategy));
    appendSummaryPair(elements.planSummary, '执行路径', describePlanExecutionPath(plan));
    renderPlanList(elements.planBlocking, plan.blocking_issues || []);
    renderPlanList(elements.planWarnings, plan.warnings || []);
  }

  function renderTimelineMigration(elements, migration) {
    if (!elements.timelineSummary) {
      return;
    }

    var run = getSelectedCloneRun();
    var preview = cloneState.timelinePreview || null;
    var timelineMigration = migration && String(migration.mode || '') === 'timeline_replay'
      ? migration
      : null;
    elements.timelineSummary.textContent = '';

    if (!run) {
      elements.timelineStatus.textContent = '选择副本后继续。';
      appendSummaryPair(elements.timelineSummary, '克隆群', '未选择');
      appendSummaryPair(elements.timelineSummary, '状态', '未选择');
      appendSummaryPair(elements.timelineSummary, '时间线总数', '0');
      appendSummaryPair(elements.timelineSummary, '剩余时间线', '0');
      return;
    }

    appendSummaryPair(elements.timelineSummary, '克隆群', getCloneRunTargetLabel(run));
    if (!timelineMigration) {
      elements.timelineStatus.textContent = preview && preview.can_migrate_timeline
        ? '可以开始克隆消息。'
        : '等待迁移方案完成。';
      appendSummaryPair(elements.timelineSummary, '状态', '未执行');
      appendTimelinePreviewSummary(elements, preview);
      return;
    }

    elements.timelineStatus.textContent = buildTimelineMigrationStatusText(timelineMigration);
    appendSummaryPair(elements.timelineSummary, '状态', getMigrationStatusLabel(timelineMigration.status));
    appendSummaryPair(elements.timelineSummary, '阶段', getMigrationPhaseLabel(timelineMigration.phase));
    appendSummaryPair(elements.timelineSummary, '文本已发送', formatNumber(timelineMigration.text_sent));
    appendSummaryPair(elements.timelineSummary, '媒体已复制', formatNumber(timelineMigration.media_sent));
    appendSummaryPair(
      elements.timelineSummary,
      '执行方式',
      describeTimelineExecutionLabel(timelineMigration.target_write_account)
    );
    appendSummaryPair(elements.timelineSummary, '本次上限', formatLimit(timelineMigration.requested_limit));
    appendSummaryPair(
      elements.timelineSummary,
      '发送间隔',
      formatNumber(timelineMigration.send_delay_ms) + 'ms'
    );
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
  }

  function buildTimelineMigrationStatusText(migration) {
    var status = getMigrationStatusLabel((migration && migration.status) || '');
    var phase = getMigrationPhaseLabel((migration && migration.phase) || '');
    var updatedAt = formatDateTime((migration && migration.updated_at) || '');
    var error = String((migration && migration.error_message) || '').trim();
    if (error) {
      return status + '：' + error;
    }
    return '最近一次继续克隆：' + status + ' / ' + phase + '，更新时间 ' + updatedAt + '。';
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
    if (status === 'running') return '创建中';
    if (status === 'done') return '副本已创建';
    if (status === 'error') return '失败';
    return status || '未知';
  }

  function getRunPhaseLabel(phase) {
    var normalized = String(phase || '').trim();
    if (normalized === 'queued') return '等待创建';
    if (normalized === 'loading_source') return '读取源群';
    if (normalized === 'validating') return '校验账号';
    if (normalized === 'creating') return '创建结构';
    if (normalized === 'done') return '结构创建完成';
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
    if (normalized === 'running') return '生成中';
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

  function describePlanExecutionPath(plan) {
    var source = plan || {};
    var capabilities = source.capabilities && typeof source.capabilities === 'object'
      ? source.capabilities
      : {};
    var payload = source.plan && typeof source.plan === 'object' ? source.plan : {};
    var relay = capabilities.media_relay && typeof capabilities.media_relay === 'object'
      ? capabilities.media_relay
      : (payload.media_relay && typeof payload.media_relay === 'object' ? payload.media_relay : {});
    var mediaStrategy = String(source.media_strategy || '').trim();
    var textAccount = getPlanTargetWriteAccount(plan);
    if (mediaStrategy === 'relay_copy_without_attribution') {
      return '文本由'
        + getMigrationAccountLabel(textAccount)
        + '发送；媒体经中转群桥接：'
        + getMigrationAccountLabel(relay.source_account)
        + ' -> 中转群 -> '
        + getMigrationAccountLabel(relay.target_account);
    }
    if (mediaStrategy === 'source_copy_without_attribution') {
      return '文本和媒体都由'
        + getMigrationAccountLabel(textAccount)
        + '直接发送到克隆群';
    }
    return '等待生成可执行路径';
  }

  function describeTimelineExecutionLabel(label) {
    var text = String(label || '').trim();
    if (!text) return '未确定';

    var textAccount = '';
    var mediaPath = '';
    text.split(';').forEach(function (part) {
      var normalized = String(part || '').trim();
      if (!normalized) return;
      if (normalized.indexOf('text:') === 0) {
        textAccount = normalized.slice(5).trim();
        return;
      }
      if (normalized.indexOf('media:') === 0) {
        mediaPath = normalized.slice(6).trim();
      }
    });

    var parts = [];
    if (textAccount) {
      parts.push('文本由' + getMigrationAccountLabel(textAccount) + '发送');
    }
    if (mediaPath.indexOf('->relay->') > 0) {
      var relayParts = mediaPath.split('->relay->');
      parts.push(
        '媒体经中转群桥接：'
          + getMigrationAccountLabel(relayParts[0])
          + ' -> 中转群 -> '
          + getMigrationAccountLabel(relayParts[1])
      );
    } else if (mediaPath) {
      parts.push('媒体由' + getMigrationAccountLabel(mediaPath) + '直接复制');
    }

    return parts.length ? parts.join('；') : text;
  }

  function getMigrationStatusLabel(status) {
    var normalized = String(status || '').trim();
    if (normalized === 'queued') return '排队中';
    if (normalized === 'running') return '迁移中';
    if (normalized === 'done') return '已完成';
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
    if (normalized === 'database_replay') return '按数据库顺序发送';
    if (normalized === 'blocked') return '阻断';
    return normalized || '未生成';
  }

  function getMediaStrategyLabel(strategy) {
    var normalized = String(strategy || '').trim();
    if (normalized === 'source_copy_without_attribution') return '源群直接复制';
    if (normalized === 'relay_copy_without_attribution') return '通过中转群桥接';
    if (normalized === 'impossible_without_local_vault') return '缺本地媒体保险库';
    if (normalized === 'blocked') return '阻断';
    return normalized || '未生成';
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
    if (!isPollContextActive(jobId, pollToken)) {
      return;
    }
    jobPollState.timerId = window.setTimeout(function () {
      pollJobProgress(elements);
    }, Math.max(250, Number(delayMs) || JOB_POLL_INTERVAL_MS));
  }

  async function pollJobProgress(elements) {
    if (!jobPollState.isPolling || !jobPollState.jobId) {
      return;
    }

    var jobId = jobPollState.jobId;
    var pollToken = jobPollState.pollToken;
    try {
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
      logs.forEach(function (line) {
        if (!line || typeof line.message !== 'string') {
          return;
        }
        appendLog(elements, line.message);
        if (typeof line.seq === 'number' && Number.isFinite(line.seq)) {
          jobPollState.lastSeq = Math.max(jobPollState.lastSeq, line.seq);
        }
      });

      var snapshotPayload = await fetchJSON('/api/admin/jobs/' + encodeURIComponent(jobId));
      if (!isPollContextActive(jobId, pollToken)) {
        return;
      }

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
        if (progressState.message) {
          appendLog(elements, progressState.message);
        }
      }

      if (status === 'done') {
        appendLog(elements, getJobDoneMessage(snapshot));
        stopJobPolling(pollToken, elements);
        await loadCloneRuns(elements);
        return;
      }
      if (status === 'error') {
        appendLog(elements, getJobErrorMessage(snapshot));
        stopJobPolling(pollToken, elements);
        await loadCloneRuns(elements);
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
    if (jobType === 'clone_deep_preflight') return '克隆计划生成完成';
    if (jobType === 'clone_timeline_migration') return '继续克隆执行完成';
    return '空副本创建完成';
  }

  function getJobErrorMessage(snapshot) {
    var jobType = String((snapshot && snapshot.job_type) || '').trim();
    if (jobType === 'clone_deep_preflight') return '克隆计划生成失败，请检查日志';
    if (jobType === 'clone_timeline_migration') return '继续克隆执行失败，请检查日志';
    return '空副本创建失败，请检查日志';
  }

  function initializeConsoleState(elements) {
    cloneState.sourceChatId = '';
    cloneState.requestedRunId = isMigratePage(elements) ? getRunIdFromLocation() : '';
    cloneState.requestedRunError = '';
    cloneState.selectedRunId = cloneState.requestedRunId;
    elements.sortSelect.value = isCreatePage(elements) ? 'message_count_desc' : 'updated_desc';

    if (isMigratePage(elements)) {
      elements.messageLimitInput.value = '';
      elements.sendDelayInput.value = '500';
    }

    syncUrlRunId(elements, cloneState.selectedRunId);
  }

  function getRunIdFromLocation() {
    try {
      var params = new URLSearchParams(window.location.search || '');
      return String(params.get('run_id') || '').trim();
    } catch (_error) {
      return '';
    }
  }

  function syncUrlRunId(elements, runId) {
    try {
      var url = new URL(window.location.href);
      var normalized = String(runId || '').trim();
      if (isMigratePage(elements) && !normalized) {
        normalized = String(cloneState.requestedRunId || '').trim();
      }
      if (isMigratePage(elements) && normalized) {
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
    var selectedSource = getSelectedSourceChat(elements);
    var selectedRun = getSelectedCloneRun();

    setElementDisabled(elements.sortSelect, disabled);
    setElementDisabled(elements.refreshBtn, disabled);
    setElementDisabled(elements.sourceSelect, disabled);
    setElementDisabled(elements.runsRefreshBtn, disabled);

    if (isCreatePage(elements)) {
      setElementDisabled(elements.preflightBtn, disabled || !selectedSource);
      setElementDisabled(elements.targetTitleInput, disabled || !cloneState.report);
      setElementDisabled(elements.targetKindSelect, disabled || !cloneState.report);
      setElementDisabled(elements.confirmInput, disabled || !cloneState.report);
      updateStartButtonState(elements);
    }

    if (isMigratePage(elements)) {
      setElementDisabled(elements.planRefreshBtn, disabled || !selectedRun);
      setElementDisabled(elements.deepPreflightBtn, disabled || !selectedRun);
      setElementDisabled(elements.timelineMigrationBtn, disabled || !isTimelineMigrationAllowed());
      setElementDisabled(elements.messageLimitInput, disabled || !selectedRun);
      setElementDisabled(elements.sendDelayInput, disabled || !selectedRun);
    }

    if (elements.logContainer && typeof elements.logContainer.setAttribute === 'function') {
      elements.logContainer.setAttribute('aria-busy', disabled ? 'true' : 'false');
    }
    syncWorkflowStages(elements);
  }

  function updateStartButtonState(elements) {
    if (!elements.startBtn) {
      return;
    }
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
    var preview = cloneState.timelinePreview;
    if (!preview || typeof preview !== 'object') return false;
    if (preview.can_migrate_timeline !== true) return false;
    return Number(preview.timeline_remaining || 0) > 0;
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

  function buildTimelineMigrationOptions(elements) {
    return {
      message_limit: normalizeNonnegativeInteger(elements.messageLimitInput.value, 0, 100000),
      send_delay_ms: normalizeNonnegativeInteger(elements.sendDelayInput.value, 500, 60000)
    };
  }

  function formatLimit(value) {
    var n = Number(value || 0);
    if (!Number.isFinite(n) || n <= 0) return '全部';
    return formatNumber(n);
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
        appendLog(
          elements,
          isCreatePage(elements)
            ? '认证成功，已进入“创建空副本”页'
            : '认证成功，已进入“继续克隆消息”页'
        );
      }
    },
    getElements: getElements,
    getPageElement: function () {
      return document.getElementById('admin-clone-page');
    }
  });
})();
