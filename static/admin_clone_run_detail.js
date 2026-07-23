(function () {
  'use strict';

  var shared = window.AdminManageShared;
  var sharedFetchJSON = shared.fetchJSON;
  var sharedPostJSON = shared.postJSON;
  var setDialogOpenState = shared.setDialogOpenState;
  var setElementDisabled = shared.setElementDisabled;
  var setPageInteractionState = shared.setPageInteractionState;
  var trapFocusWithin = shared.trapFocusWithin;
  var formatDateTime = shared.formatDateTime;
  var formatNumber = shared.formatNumber;
  var normalizeNonnegativeInteger = shared.normalizeNonnegativeInteger;

  var MAPPING_PAGE_SIZE = 25;
  var DATABASE_READ_TIMEOUT_MS = 20000;

  var state = {
    runId: '',
    detail: null,
    mappingItems: [],
    mappingOffset: 0,
    mappingLimit: MAPPING_PAGE_SIZE,
    mappingTotal: 0,
    mappingLoading: false,
    busy: false,
    deleteConfirm: '',
    deleteJobId: '',
    operationJob: {
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
      page: document.getElementById('admin-clone-run-detail-page'),
      detailStatus: document.getElementById('admin-clone-runs-detail-status'),
      detailRefreshBtn: document.getElementById('admin-clone-runs-detail-refresh-btn'),
      operationStatus: document.getElementById('admin-clone-runs-operation-status'),
      planRefreshBtn: document.getElementById('admin-clone-runs-plan-refresh-btn'),
      deepPreflightBtn: document.getElementById('admin-clone-runs-deep-preflight-btn'),
      planSummary: document.getElementById('admin-clone-runs-plan-summary'),
      planBlocking: document.getElementById('admin-clone-runs-plan-blocking'),
      planWarnings: document.getElementById('admin-clone-runs-plan-warnings'),
      timelineStatus: document.getElementById('admin-clone-runs-timeline-status'),
      timelineMigrationBtn: document.getElementById('admin-clone-runs-timeline-migration-btn'),
      messageLimitInput: document.getElementById('admin-clone-runs-message-limit-input'),
      sendDelayInput: document.getElementById('admin-clone-runs-send-delay-input'),
      timelineSummary: document.getElementById('admin-clone-runs-timeline-summary'),
      operationLogPanel: document.getElementById('admin-clone-runs-operation-log-panel'),
      operationLogContainer: document.getElementById('admin-clone-runs-operation-log-container'),
      clearOperationLogsBtn: document.getElementById('admin-clear-clone-runs-operation-logs-btn'),
      messageDeleteLink: document.getElementById('admin-clone-runs-message-delete-link'),
      deleteBtn: document.getElementById('admin-clone-runs-delete-btn'),
      deleteHelp: document.getElementById('admin-clone-runs-delete-help'),
      detailSummary: document.getElementById('admin-clone-runs-detail-summary'),
      nextStep: document.getElementById('admin-clone-runs-next-step'),
      openSourceLink: document.getElementById('admin-clone-runs-open-source-link'),
      openTargetLink: document.getElementById('admin-clone-runs-open-target-link'),
      resumeLink: document.getElementById('admin-clone-runs-resume-link'),
      progressSummary: document.getElementById('admin-clone-runs-progress-summary'),
      failureBlock: document.getElementById('admin-clone-runs-failure-block'),
      failureSummaryText: document.getElementById('admin-clone-runs-failure-summary-text'),
      failureList: document.getElementById('admin-clone-runs-failure-list'),
      mappingStatus: document.getElementById('admin-clone-runs-mapping-status'),
      mappingBlock: document.getElementById('admin-clone-runs-mapping-block'),
      mappingSummaryText: document.getElementById('admin-clone-runs-mapping-summary-text'),
      mappingStatusFilter: document.getElementById('admin-clone-runs-mapping-status-filter'),
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
      'detailStatus',
      'detailRefreshBtn',
      'operationStatus',
      'planRefreshBtn',
      'deepPreflightBtn',
      'planSummary',
      'planBlocking',
      'planWarnings',
      'timelineStatus',
      'timelineMigrationBtn',
      'messageLimitInput',
      'sendDelayInput',
      'timelineSummary',
      'operationLogPanel',
      'operationLogContainer',
      'clearOperationLogsBtn',
      'messageDeleteLink',
      'deleteBtn',
      'deleteHelp',
      'detailSummary',
      'nextStep',
      'openSourceLink',
      'openTargetLink',
      'resumeLink',
      'progressSummary',
      'failureBlock',
      'failureSummaryText',
      'failureList',
      'mappingStatus',
      'mappingBlock',
      'mappingSummaryText',
      'mappingStatusFilter',
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
      console.warn('[admin_clone_run_detail] Missing required elements:', missing.join(', '));
      return null;
    }
    return elements;
  }

  function initializeUI(elements) {
    initializePageState(elements);
    shared.ensurePlaceholder(operationLogElements(elements).logContainer);
    syncOperationLogClearButton(elements);
    renderDetail(elements, null);
    setBusy(elements, false);
    closeDeleteDialog(elements, { skipFocusRestore: true });
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
    elements.detailRefreshBtn.addEventListener('click', function () {
      loadDetail(elements);
    });
    elements.planRefreshBtn.addEventListener('click', function () {
      loadDetail(elements);
    });
    elements.deepPreflightBtn.addEventListener('click', function () {
      startDeepPreflight(elements);
    });
    elements.timelineMigrationBtn.addEventListener('click', function () {
      startTimelineMigration(elements);
    });
    elements.clearOperationLogsBtn.addEventListener('click', function () {
      clearOperationLogs(elements);
    });
    elements.mappingStatusFilter.addEventListener('change', function () {
      state.mappingOffset = 0;
      loadMappings(elements, { resetOffset: true });
    });
    elements.mappingPrevBtn.addEventListener('click', function () {
      state.mappingOffset = Math.max(0, state.mappingOffset - state.mappingLimit);
      loadMappings(elements, { resetOffset: false });
    });
    elements.mappingNextBtn.addEventListener('click', function () {
      if (state.mappingOffset + state.mappingLimit >= state.mappingTotal) return;
      state.mappingOffset += state.mappingLimit;
      loadMappings(elements, { resetOffset: false });
    });
    elements.deleteBtn.addEventListener('click', function () {
      openDeleteDialog(elements);
    });
    elements.deleteCancelBtn.addEventListener('click', function () {
      if (state.deleteJobId) return;
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

  async function loadDetail(elements) {
    if (!state.runId) {
      renderDetail(elements, null);
      return;
    }
    setBusy(elements, true);
    elements.detailStatus.textContent = '正在读取克隆记录...';
    try {
      var payload = await fetchJSON(
        '/api/admin/clone/runs/' + encodeURIComponent(state.runId) + '/detail'
      );
      state.detail = payload || null;
      state.deleteConfirm = String((payload && payload.delete_confirm) || '');
      renderDetail(elements, state.detail);
      await loadMappings(elements, { resetOffset: false });
      await resumeOperationJobIfNeeded(elements);
    } catch (error) {
      state.detail = null;
      state.deleteConfirm = '';
      renderDetail(elements, null);
      renderMappingList(elements.mappingList, []);
      updateMappingStatus(elements, '读取映射前请先恢复有效记录详情。');
      elements.detailStatus.textContent = '读取记录失败：' + error.message;
    } finally {
      setBusy(elements, false);
    }
  }

  async function loadMappings(elements, options) {
    var opts = options || {};
    if (opts.resetOffset) state.mappingOffset = 0;
    if (!state.runId) {
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
      if (status) params.set('status', status);
      var payload = await fetchJSON(
        '/api/admin/clone/runs/' + encodeURIComponent(state.runId) + '/messages?' + params.toString()
      );
      state.mappingItems = Array.isArray(payload.items) ? payload.items : [];
      state.mappingTotal = Number(payload.total || 0) || 0;
      if (state.mappingOffset >= state.mappingTotal && state.mappingTotal > 0) {
        state.mappingOffset = Math.max(
          0,
          Math.floor((state.mappingTotal - 1) / state.mappingLimit) * state.mappingLimit
        );
        await loadMappings(elements, { resetOffset: false });
        return;
      }
      renderMappingList(elements.mappingList, state.mappingItems);
      updateMappingStatus(elements);
    } catch (error) {
      state.mappingItems = [];
      state.mappingTotal = 0;
      renderMappingList(elements.mappingList, []);
      updateMappingStatus(elements, '读取消息映射失败：' + error.message);
    } finally {
      state.mappingLoading = false;
      syncMappingControls(elements);
    }
  }

  function renderDetail(elements, payload) {
    var run = payload && payload.run ? payload.run : null;
    var plan = payload && payload.plan ? payload.plan : null;
    var migration = payload && payload.migration ? payload.migration : null;
    var taskReport = payload && payload.task_report ? payload.task_report : null;
    var groupProgress = payload && payload.group_progress ? payload.group_progress : null;
    var summary = payload && payload.mapping_summary ? payload.mapping_summary : null;
    var failures = payload && Array.isArray(payload.failure_items) ? payload.failure_items : [];

    elements.detailSummary.textContent = '';
    elements.progressSummary.textContent = '';
    elements.failureList.textContent = '';
    renderOperationPanel(elements, payload);
    renderDetailActions(elements, null, payload);

    if (!run) {
      elements.detailStatus.textContent = state.runId
        ? '这条记录暂时不可用，请返回列表重新选择。'
        : '缺少 run_id，请从记录列表进入详情页。';
      appendSummaryPair(elements.detailSummary, '状态', '未读取');
      appendSummaryPair(elements.detailSummary, '目标', '未读取');
      appendSummaryPair(elements.detailSummary, '克隆计划', '未读取');
      appendMiniPair(elements.progressSummary, '时间线剩余', '0');
      elements.nextStep.textContent = state.runId
        ? '请返回已克隆群管理重新进入有效详情页。'
        : '请回到“已克隆群管理”选择一条记录进入详情页。';
      renderFailureList(elements.failureList, []);
      renderFailureSummary(elements, []);
      renderMappingList(elements.mappingList, []);
      if (elements.mappingBlock) {
        elements.mappingBlock.open = false;
      }
      updateDeleteHelp(elements, null);
      updateMappingStatus(elements);
      syncDeleteButton(elements);
      return;
    }

    appendSummaryPair(elements.detailSummary, '源群', run.source_title || run.source_chat_id);
    appendSummaryPair(elements.detailSummary, '目标', run.target_title || '未创建');
    appendSummaryPair(elements.detailSummary, '状态', getRunStatusLabel(run.status));
    appendSummaryPair(elements.detailSummary, '克隆计划', getPlanStatusLabel(plan && plan.status));
    appendSummaryPair(elements.detailSummary, '消息克隆', getMigrationStatusLabel(migration && migration.status));
    appendSummaryPair(
      elements.detailSummary,
      '群剩余消息',
      formatGroupProgressMetric(groupProgress, 'messages_remaining')
    );
    appendSummaryPair(elements.detailSummary, '更新时间', formatDateTime(run.updated_at));

    appendMiniSection(elements.progressSummary, '群总进度');
    appendMiniPair(
      elements.progressSummary,
      '已完成消息',
      formatGroupProgressDoneTotal(groupProgress)
    );
    appendMiniPair(
      elements.progressSummary,
      '剩余消息',
      formatGroupProgressMetric(groupProgress, 'messages_remaining')
    );
    appendMiniPair(
      elements.progressSummary,
      '群文本',
      formatGroupProgressDoneTotal(groupProgress, 'text')
    );
    appendMiniPair(
      elements.progressSummary,
      '群媒体',
      formatGroupProgressDoneTotal(groupProgress, 'media')
    );
    appendMiniPair(
      elements.progressSummary,
      '最近核验',
      formatGroupProgressMetric(groupProgress, 'verified_at', true)
    );

    appendMiniSection(elements.progressSummary, '最近任务报告');
    appendTaskReport(elements.progressSummary, taskReport, migration);
    appendMiniPair(
      elements.progressSummary,
      '执行方式',
      describeTimelineExecutionLabel(migration && migration.target_write_account)
    );
    appendMiniPair(elements.progressSummary, '映射失败', formatNumber(summary && summary.error));

    elements.detailStatus.textContent = buildDetailStatusText(payload);
    elements.nextStep.textContent = buildNextStepText(payload);
    renderDetailActions(elements, run, payload);
    renderFailureList(elements.failureList, failures);
    renderFailureSummary(elements, failures);
    updateDeleteHelp(elements, run);
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

  function appendMiniSection(container, title) {
    var heading = document.createElement('div');
    heading.className = 'clone-summary-section';
    heading.textContent = String(title || '摘要');
    container.appendChild(heading);
  }

  function nonnegativeProgressNumber(value) {
    var number = Number(value || 0);
    return Number.isFinite(number) && number > 0 ? Math.trunc(number) : 0;
  }

  function isGroupProgressVerified(progress) {
    return String((progress && progress.assessment_state) || '').trim() === 'verified';
  }

  function formatGroupProgressMetric(progress, key, isDate) {
    if (!isGroupProgressVerified(progress)) {
      return key === 'messages_remaining' ? '尚未完成首次核验' : '尚未核验';
    }
    return isDate
      ? formatDateTime(progress && progress[key])
      : formatNumber(progress && progress[key]);
  }

  function formatGroupProgressDoneTotal(progress, prefix) {
    if (!isGroupProgressVerified(progress)) {
      return '尚未完成首次核验';
    }
    var keyPrefix = String(prefix || 'messages');
    return formatDoneTotal(
      progress && progress[keyPrefix + '_done'],
      progress && progress[keyPrefix + '_total']
    );
  }

  function buildTaskReport(migration) {
    var source = migration && typeof migration === 'object' ? migration : {};
    function outcome(prefix) {
      var sent = nonnegativeProgressNumber(source[prefix + '_sent']);
      var skipped = nonnegativeProgressNumber(source[prefix + '_skipped']);
      var failed = nonnegativeProgressNumber(source[prefix + '_failed']);
      return {
        sent: sent,
        skipped: skipped,
        failed: failed,
        processed: sent + skipped + failed
      };
    }
    var text = outcome('text');
    var media = outcome('media');
    var mediaGroups = outcome('media_group');
    return {
      requested_limit: nonnegativeProgressNumber(source.requested_limit),
      text: text,
      media: media,
      media_groups: mediaGroups,
      processed: text.processed + media.processed
    };
  }

  function formatTaskOutcome(outcome, completedLabel) {
    var data = outcome && typeof outcome === 'object' ? outcome : {};
    return completedLabel
      + ' ' + formatNumber(data.sent)
      + '，跳过 ' + formatNumber(data.skipped)
      + '，失败 ' + formatNumber(data.failed);
  }

  function appendTaskReport(container, report, migration) {
    var data = report && typeof report === 'object' ? report : buildTaskReport(migration);
    appendMiniPair(container, '本次处理', formatNumber(data.processed) + ' 条');
    appendMiniPair(container, '本次文本', formatTaskOutcome(data.text, '已发'));
    appendMiniPair(container, '本次媒体', formatTaskOutcome(data.media, '已复制'));
    appendMiniPair(container, '本次相册组', formatTaskOutcome(data.media_groups, '已复制'));
    appendMiniPair(container, '本次上限', formatLimit(data.requested_limit));
  }

  function formatLimit(value) {
    var number = nonnegativeProgressNumber(value);
    return number > 0 ? formatNumber(number) + ' 条' : '全部';
  }

  function renderDetailActions(elements, run, payload) {
    var groupProgress = payload && payload.group_progress ? payload.group_progress : null;
    var resumeHref = run
      && canResumeMigration(run)
      && !isGroupProgressComplete(groupProgress)
      ? '#admin-clone-run-operation-panel'
      : '';
    var resumeLabel = buildResumeLinkLabel(payload);

    syncActionLink(elements.openSourceLink, run && run.source_telegram_app_link, '打开源群');
    syncActionLink(elements.openTargetLink, run && run.target_telegram_app_link, '打开目标副本');
    syncActionLink(elements.resumeLink, resumeHref, resumeLabel);
    syncActionLink(
      elements.messageDeleteLink,
      canDeleteLocalMessages(run) ? buildRunMessageDeleteHref(run) : '',
      '打开局部删除'
    );
  }

  function renderOperationPanel(elements, payload) {
    var run = payload && payload.run ? payload.run : null;
    var plan = payload && payload.plan ? payload.plan : null;
    var migration = payload && payload.migration ? payload.migration : null;
    var preview = payload && payload.timeline_preview ? payload.timeline_preview : null;
    var groupProgress = payload && payload.group_progress ? payload.group_progress : null;
    var taskReport = payload && payload.task_report ? payload.task_report : null;

    elements.planSummary.textContent = '';
    elements.planBlocking.textContent = '';
    elements.planWarnings.textContent = '';
    elements.timelineSummary.textContent = '';

    if (!run) {
      elements.operationStatus.textContent = state.runId
        ? '当前记录不可用，无法执行在线预检或迁移。'
        : '请从记录目录打开一条克隆记录。';
      elements.timelineStatus.textContent = '需要有效记录后才能继续迁移。';
      appendSummaryPair(elements.planSummary, '状态', '未读取');
      appendSummaryPair(elements.timelineSummary, '状态', '未读取');
      renderPlanList(elements.planBlocking, []);
      renderPlanList(elements.planWarnings, []);
      syncOperationControls(elements, null, null, null, null);
      return;
    }

    renderPlanSummary(elements.planSummary, run, plan);
    renderPlanList(elements.planBlocking, plan && plan.blocking_issues);
    renderPlanList(elements.planWarnings, plan && plan.warnings);
    elements.operationStatus.textContent = buildOperationStatusText(run, plan);
    renderTimelineSummary(elements.timelineSummary, run, migration, preview, groupProgress, taskReport);
    elements.timelineStatus.textContent = buildTimelineStatusText(
      run,
      plan,
      migration,
      preview,
      groupProgress
    );
    syncOperationControls(elements, run, plan, preview, groupProgress);
  }

  function renderPlanSummary(container, run, plan) {
    appendSummaryPair(container, '目标副本', run.target_title || run.target_chat_id || '未创建');
    if (!plan) {
      appendSummaryPair(container, '预检状态', '尚未执行');
      appendSummaryPair(container, '目标写入账号', '未确定');
      appendSummaryPair(container, '文本方式', '未确定');
      appendSummaryPair(container, '媒体方式', '未确定');
      return;
    }
    appendSummaryPair(container, '预检状态', getPlanStatusLabel(plan.status));
    appendSummaryPair(container, '源访问', getAccessStatusLabel(plan.source_access));
    appendSummaryPair(container, '目标访问', getAccessStatusLabel(plan.target_access));
    appendSummaryPair(container, '目标写入账号', getMigrationAccountLabel(getPlanTargetWriteAccount(plan)));
    appendSummaryPair(container, '文本方式', getTextStrategyLabel(plan.text_strategy));
    appendSummaryPair(container, '媒体方式', getMediaStrategyLabel(plan.media_strategy));
  }

  function renderTimelineSummary(container, run, migration, preview, groupProgress, taskReport) {
    appendSummaryPair(container, '目标副本', run.target_title || run.target_chat_id || '未创建');
    if (!migration || String(migration.mode || '') !== 'timeline_replay') {
      appendSummaryPair(container, '最近任务', '尚未执行');
      appendGroupProgressSummary(container, groupProgress);
      appendTimelinePreviewSummary(container, preview);
      return;
    }
    appendSummaryPair(container, '最近任务', getMigrationStatusLabel(migration.status));
    appendSummaryPair(container, '当前阶段', getMigrationPhaseLabel(migration.phase));
    appendTaskReportSummary(container, taskReport, migration);
    appendSummaryPair(container, '执行方式', describeTimelineExecutionLabel(migration.target_write_account));
    appendSummaryPair(container, '本次上限', formatLimit(migration.requested_limit));
    appendSummaryPair(container, '发送间隔', formatNumber(migration.send_delay_ms) + 'ms');
    appendGroupProgressSummary(container, groupProgress);
    appendTimelinePreviewSummary(container, preview);
  }

  function buildOperationStatusText(run, plan) {
    var status = String(run.status || '').trim().toLowerCase();
    if (isMessageResetRequired(run)) {
      return '上次完整清空未完成；必须先进入消息删除与回退页面重试完整清空。';
    }
    if (status === 'queued' || status === 'running') {
      return '目标副本仍在创建中；创建完成后可执行在线预检。';
    }
    if (status === 'deleting') {
      return '正在删除目标副本及本地克隆链路，删除完成前不能继续迁移。';
    }
    if (status === 'error') {
      if (String(run.deletion_job_id || '').trim()) {
        return '目标副本删除任务未完成；可查看任务日志并重新提交删除请求。';
      }
      return '目标副本创建失败；请先查看失败样本，必要时删除失败记录后重新创建。';
    }
    if (!run.target_chat_id) {
      return '当前记录没有可用的目标副本。';
    }
    if (!plan) return '尚未执行在线预检。预检会确认源访问、目标写入权限和媒体路径。';
    if (String(plan.status || '').trim().toLowerCase() === 'error') return '最近一次在线预检失败，可重新执行。';
    if (hasPlanBlockingIssues(plan)) return '在线预检已完成，但存在阻断项。请先处理后再继续迁移。';
    if (String(plan.status || '').trim().toLowerCase() === 'done') return '在线预检已通过，可按下方设置继续迁移。';
    return '在线预检正在执行；完成后会自动刷新当前记录。';
  }

  function buildTimelineStatusText(run, plan, migration, preview, groupProgress) {
    if (!canResumeMigration(run)) return '当前记录尚不具备继续迁移条件。';
    if (!plan) return '先执行在线预检，再继续迁移消息。';
    if (hasPlanBlockingIssues(plan)) return '在线预检存在阻断项，暂不能继续迁移。';
    if (String(plan.status || '').trim().toLowerCase() !== 'done') return '正在等待在线预检完成。';
    if (isGroupProgressComplete(groupProgress)) return '最近核验显示可迁移消息已全部完成。';
    if (isMigrationActive(migration)) return '迁移任务正在执行；日志会持续更新。';
    if (isMigrationErrored(migration)) return '最近一次迁移未完成。修复权限或媒体问题后可在此重试。';
    if (String((preview && preview.assessment_state) || '').trim() === 'deferred') {
      return '执行时会先在后台核验本地时间线，然后继续迁移。';
    }
    if (isPreviewRemaining(preview) || isGroupProgressRemaining(groupProgress)) return '存在待迁移消息，可以继续迁移。';
    return '等待迁移状态刷新。';
  }

  function syncOperationControls(elements, run, plan, preview, groupProgress) {
    var planRunning = String((plan && plan.status) || '').trim().toLowerCase();
    var canPreflight = canResumeMigration(run) && !state.busy && !state.operationJob.isPolling;
    var canMigrate = canStartTimelineMigration(run, plan, preview, groupProgress);
    setElementDisabled(elements.planRefreshBtn, state.busy || !state.runId || state.operationJob.isPolling);
    setElementDisabled(elements.deepPreflightBtn, !canPreflight);
    setElementDisabled(elements.timelineMigrationBtn, !canMigrate);
    setElementDisabled(elements.messageLimitInput, state.busy || !canResumeMigration(run) || state.operationJob.isPolling);
    setElementDisabled(elements.sendDelayInput, state.busy || !canResumeMigration(run) || state.operationJob.isPolling);
    if (planRunning === 'queued' || planRunning === 'running') {
      setElementDisabled(elements.deepPreflightBtn, true);
    }
  }

  function canStartTimelineMigration(run, plan, preview, groupProgress) {
    if (state.busy || state.operationJob.isPolling || !canResumeMigration(run)) return false;
    if (!plan || String(plan.status || '').trim().toLowerCase() !== 'done') return false;
    if (hasPlanBlockingIssues(plan) || isGroupProgressComplete(groupProgress)) return false;
    if (!preview || preview.can_migrate_timeline !== true) return false;
    return String(preview.assessment_state || '').trim() === 'deferred'
      || Number(preview.timeline_remaining || 0) > 0;
  }

  async function startDeepPreflight(elements) {
    var run = state.detail && state.detail.run ? state.detail.run : null;
    if (!canResumeMigration(run) || state.busy || state.operationJob.isPolling) return;
    setBusy(elements, true);
    elements.operationStatus.textContent = '正在创建在线预检任务...';
    try {
      var payload = await postJSON(
        '/api/admin/clone/runs/' + encodeURIComponent(state.runId) + '/deep-preflight',
        {}
      );
      var jobId = shared.getCreatedJobId(payload);
      state.detail.plan = payload && payload.plan ? payload.plan : null;
      renderDetail(elements, state.detail);
      appendOperationLog(elements, '在线预检任务已创建：' + jobId);
      startOperationJobPolling(elements, jobId);
    } catch (error) {
      elements.operationStatus.textContent = '创建在线预检任务失败：' + error.message;
      appendOperationLog(elements, '创建在线预检任务失败：' + error.message);
      setBusy(elements, false);
    }
  }

  async function startTimelineMigration(elements) {
    var run = state.detail && state.detail.run ? state.detail.run : null;
    var plan = state.detail && state.detail.plan ? state.detail.plan : null;
    var preview = state.detail && state.detail.timeline_preview ? state.detail.timeline_preview : null;
    var groupProgress = state.detail && state.detail.group_progress ? state.detail.group_progress : null;
    if (!canStartTimelineMigration(run, plan, preview, groupProgress)) {
      elements.timelineStatus.textContent = '当前执行条件尚未满足，请先刷新状态或执行在线预检。';
      return;
    }
    setBusy(elements, true);
    elements.timelineStatus.textContent = '正在创建继续迁移任务...';
    try {
      var payload = await postJSON(
        '/api/admin/clone/runs/' + encodeURIComponent(state.runId) + '/migrate-timeline',
        {
          message_limit: normalizeNonnegativeInteger(elements.messageLimitInput.value, 0, 100000),
          send_delay_ms: normalizeNonnegativeInteger(elements.sendDelayInput.value, 500, 60000)
        }
      );
      var jobId = shared.getCreatedJobId(payload);
      state.detail.migration = payload && payload.timeline_migration
        ? payload.timeline_migration
        : (payload && payload.migration ? payload.migration : null);
      state.detail.timeline_preview = payload && payload.timeline_preview
        ? payload.timeline_preview
        : state.detail.timeline_preview;
      state.detail.task_report = payload && payload.task_report
        ? payload.task_report
        : state.detail.task_report;
      state.detail.group_progress = payload && payload.group_progress
        ? payload.group_progress
        : state.detail.group_progress;
      renderDetail(elements, state.detail);
      appendOperationLog(elements, '继续迁移任务已创建：' + jobId);
      startOperationJobPolling(elements, jobId);
    } catch (error) {
      elements.timelineStatus.textContent = '创建继续迁移任务失败：' + error.message;
      appendOperationLog(elements, '创建继续迁移任务失败：' + error.message);
      setBusy(elements, false);
    }
  }

  function startOperationJobPolling(elements, jobId) {
    operationJobPollController.start(state.operationJob, jobId, { clearLogs: false });
  }

  async function resumeOperationJobIfNeeded(elements) {
    if (state.operationJob.isPolling || !state.detail) return;
    var run = state.detail.run || {};
    var plan = state.detail.plan || {};
    var migration = state.detail.migration || {};
    var candidates = [
      {
        record: {
          status: run.status,
          job_id: run.deletion_job_id
        },
        label: '目标副本删除'
      },
      { record: migration, label: '迁移' },
      { record: plan, label: '在线预检' },
      { record: run, label: '目标副本创建' }
    ];
    for (var index = 0; index < candidates.length; index += 1) {
      var candidate = candidates[index];
      var status = String((candidate.record && candidate.record.status) || '').trim().toLowerCase();
      var jobId = String((candidate.record && candidate.record.job_id) || '').trim();
      if ((status !== 'queued' && status !== 'running' && status !== 'deleting') || !jobId) continue;
      try {
        var payload = await fetchJSON('/api/admin/jobs/' + encodeURIComponent(jobId));
        var job = payload && payload.job ? payload.job : null;
        var jobStatus = String((job && job.status) || '').trim().toLowerCase();
        if (jobStatus !== 'queued' && jobStatus !== 'running') continue;
        appendOperationLog(elements, '检测到进行中的' + candidate.label + '任务，已恢复日志：' + jobId);
        startOperationJobPolling(elements, jobId);
        return;
      } catch (error) {
        appendOperationLog(elements, '读取进行中任务失败：' + error.message);
        return;
      }
    }
  }

  function appendOperationLog(elements, message) {
    elements.operationLogPanel.open = true;
    shared.appendLog(operationLogElements(elements), message);
  }

  function clearOperationLogs(elements) {
    if (!elements) return;
    shared.clearLogs(operationLogElements(elements));
  }

  function syncOperationLogClearButton(elements) {
    if (!elements) return;
    shared.syncClearLogsButtonVisibility(operationLogElements(elements));
  }

  function operationLogElements(elements) {
    return {
      logContainer: elements && elements.operationLogContainer,
      clearLogsBtn: elements && elements.clearOperationLogsBtn
    };
  }

  function renderPlanList(container, items) {
    container.textContent = '';
    var values = Array.isArray(items) ? items : [];
    if (!values.length) {
      var empty = document.createElement('li');
      empty.textContent = '暂无';
      container.appendChild(empty);
      return;
    }
    values.forEach(function (value) {
      var item = document.createElement('li');
      item.textContent = String(value || '');
      container.appendChild(item);
    });
  }

  function appendTaskReportSummary(container, report, migration) {
    var data = report && typeof report === 'object' ? report : buildTaskReport(migration);
    appendSummaryPair(container, '本次处理', formatNumber(data.processed) + ' 条');
    appendSummaryPair(container, '本次文本', formatTaskOutcome(data.text, '已发'));
    appendSummaryPair(container, '本次媒体', formatTaskOutcome(data.media, '已复制'));
    appendSummaryPair(container, '本次相册组', formatTaskOutcome(data.media_groups, '已复制'));
  }

  function appendGroupProgressSummary(container, progress) {
    appendSummarySection(container, '群总进度');
    if (!isGroupProgressVerified(progress)) {
      appendSummaryPair(container, '核验状态', '尚未完成首次时间线核验');
      return;
    }
    appendSummaryPair(container, '已完成消息', formatGroupProgressDoneTotal(progress));
    appendSummaryPair(container, '剩余消息', formatGroupProgressMetric(progress, 'messages_remaining'));
    appendSummaryPair(container, '群文本', formatGroupProgressDoneTotal(progress, 'text'));
    appendSummaryPair(container, '群媒体', formatGroupProgressDoneTotal(progress, 'media'));
    appendSummaryPair(container, '未解决失败', formatNumber(progress && progress.messages_error));
    appendSummaryPair(container, '最近核验', formatGroupProgressMetric(progress, 'verified_at', true));
  }

  function appendTimelinePreviewSummary(container, preview) {
    var data = preview && typeof preview === 'object' ? preview : {};
    var reasons = Array.isArray(data.readiness_reasons) ? data.readiness_reasons : [];
    var label = data.can_migrate_timeline
      ? (String(data.assessment_state || '').trim() === 'deferred' ? '开始后后台核验' : '可执行')
      : (reasons[0] || '未完成评估');
    appendSummarySection(container, '执行条件');
    appendSummaryPair(container, '执行评估', label);
  }

  function appendSummarySection(container, title) {
    var heading = document.createElement('div');
    heading.className = 'clone-summary-section';
    heading.textContent = String(title || '摘要');
    container.appendChild(heading);
  }

  function syncActionLink(link, href, label) {
    if (!link) return;
    var normalizedHref = String(href || '').trim();
    if (!normalizedHref) {
      link.hidden = true;
      link.removeAttribute('href');
      return;
    }
    link.hidden = false;
    link.href = normalizedHref;
    link.textContent = String(label || '');
  }

  function buildResumeLinkLabel(payload) {
    var run = payload && payload.run ? payload.run : null;
    var plan = payload && payload.plan ? payload.plan : null;
    var migration = payload && payload.migration ? payload.migration : null;
    var preview = payload && payload.timeline_preview ? payload.timeline_preview : null;
    var groupProgress = payload && payload.group_progress ? payload.group_progress : null;
    if (!run || !canResumeMigration(run)) {
      return '去继续克隆消息';
    }
    if (!plan) return '去生成克隆计划';
    if (hasPlanBlockingIssues(plan)) return '去处理阻断项';
    if (isMigrationErrored(migration)) return '去重试继续克隆';
    if (isGroupProgressComplete(groupProgress)) return '群内消息已完成';
    if (isGroupProgressRemaining(groupProgress)) return '继续克隆消息';
    if (isPreviewRemaining(preview)) return '继续克隆消息';
    return '去继续克隆消息';
  }

  function buildDetailStatusText(payload) {
    var run = payload && payload.run ? payload.run : null;
    if (!run) {
      return '缺少有效克隆记录。';
    }
    var statusLabel = getRunStatusLabel(run.status);
    var targetLabel = run.target_title || run.target_chat_id || '未创建目标';
    return '当前记录：' + statusLabel + ' / ' + targetLabel;
  }

  function buildNextStepText(payload) {
    var run = payload && payload.run ? payload.run : null;
    var plan = payload && payload.plan ? payload.plan : null;
    var migration = payload && payload.migration ? payload.migration : null;
    var preview = payload && payload.timeline_preview ? payload.timeline_preview : null;
    var groupProgress = payload && payload.group_progress ? payload.group_progress : null;

    if (!run) {
      return '请回到“已克隆群管理”选择一条记录进入详情页。';
    }

    var runStatus = String(run.status || '').trim().toLowerCase();
    if (runStatus === 'queued' || runStatus === 'running') {
      return '这条记录还在创建克隆群。先等待创建完成，再继续克隆消息。';
    }
    if (runStatus === 'deleting') {
      return '这条记录正在删除目标副本及本地克隆链路，请等待删除任务完成。';
    }
    if (runStatus === 'error') {
      if (String(run.deletion_job_id || '').trim()) {
        return '目标副本删除任务已中断或失败。查看任务日志后，可以重新提交删除请求。';
      }
      return '这条记录在创建阶段失败了。先看失败样本和错误信息；如果目标副本没建出来，通常需要回到“创建空副本”重新创建。';
    }
    if (!run.target_chat_id) {
      return '这条记录还没有可用的目标副本，暂时不能继续迁移。';
    }
    if (!plan) {
      return '克隆群已经创建，可以先生成克隆计划。';
    }
    if (hasPlanBlockingIssues(plan)) {
      return '克隆计划已经生成，但存在阻断项。先处理阻断项，再继续克隆消息。';
    }
    if (isMigrationErrored(migration)) {
      return '最近一次继续克隆失败了。建议回到“继续克隆消息”重试。';
    }
    if (isGroupProgressComplete(groupProgress)) {
      return '最近一次核验显示该群的可迁移消息已全部完成。';
    }
    if (isGroupProgressRemaining(groupProgress)) {
      return '最近一次核验显示仍有 '
        + formatNumber(groupProgress.messages_remaining)
        + ' 条消息待处理，可以继续克隆。';
    }
    if (String((preview && preview.assessment_state) || '').trim() === 'deferred') {
      return '迁移方案已读取。首次继续克隆时系统会在后台核验本地消息。';
    }
    if (isPreviewRemaining(preview)) {
      return '这条记录还有剩余消息未处理，可以继续克隆。文本会按数据库顺序发送；媒体会按当前计划直接复制或通过中转群桥接。';
    }
    return '从当前摘要看，这条记录已经没有明显待处理时间线。只有在需要核对映射或清理本地数据时才继续留在本页。';
  }

  function renderFailureSummary(elements, items) {
    var failures = Array.isArray(items) ? items : [];
    elements.failureSummaryText.textContent = failures.length
      ? '最近 ' + formatNumber(Math.min(failures.length, 8)) + ' 条失败样本'
      : '暂无失败样本';
    elements.failureBlock.open = failures.length > 0;
  }

  function updateDeleteHelp(elements, run) {
    if (!run) {
      elements.deleteHelp.textContent = '需要有效记录后才能删除克隆副本。';
      return;
    }
    if (run.target_chat_id) {
      elements.deleteHelp.textContent = '当前将删除 “'
        + buildRunTitle(run)
        + '” 的目标副本，并清除本地运行记录、迁移方案、消息映射和关联任务历史。源群不会被读取、修改或删除。若目标已解散或创建账号无法访问，仍会继续清除本地克隆链路。';
      return;
    }
    elements.deleteHelp.textContent = '这条记录没有已创建的目标副本。删除会清除失败记录及其本地链路；源群不会被读取、修改或删除。';
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
      node.textContent = '源消息 '
        + String(item.source_message_id || '')
        + ' / '
        + getMappingModeLabel(item.mode)
        + ' / '
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
      appendMappingCell(
        row,
        '说明',
        item.error_message || (item.target_message_id ? '已建立映射' : '待补充')
      );
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
    if (!state.runId || !state.deleteConfirm) return;
    var run = state.detail && state.detail.run ? state.detail.run : null;
    elements.deleteStatus.textContent = '';
    elements.deleteConfirmInput.value = '';
    elements.deleteConfirmHint.textContent = run
      ? (run.target_chat_id
        ? '将删除 “' + buildRunTitle(run) + '” 的目标副本和本地克隆链路。源群不会受影响。请输入确认码：' + state.deleteConfirm
        : '将清除 “' + buildRunTitle(run) + '” 的失败记录及本地克隆链路。源群不会受影响。请输入确认码：' + state.deleteConfirm)
      : '请输入确认码：' + state.deleteConfirm;
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
    if (!state.runId || !state.deleteConfirm) return;
    var confirmText = String(elements.deleteConfirmInput.value || '').trim();
    if (confirmText !== state.deleteConfirm) {
      elements.deleteStatus.textContent = '确认码不匹配。';
      elements.deleteConfirmInput.focus();
      return;
    }
    setElementDisabled(elements.deleteConfirmBtn, true);
    elements.deleteStatus.textContent = '正在提交删除任务...';
    var deletionStarted = false;
    try {
      var payload = await fetchJSON(
        '/api/admin/clone/runs/' + encodeURIComponent(state.runId),
        {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ confirm: confirmText })
        }
      );
      var jobId = String((payload && payload.job && payload.job.job_id) || '').trim();
      if (!jobId) {
        throw new Error('删除任务响应缺少 job_id');
      }
      deletionStarted = true;
      state.deleteJobId = jobId;
      setElementDisabled(elements.deleteCancelBtn, true);
      elements.deleteStatus.textContent = '正在删除目标副本并清除本地克隆链路...';
      pollDeleteJob(elements, jobId);
    } catch (error) {
      elements.deleteStatus.textContent = '删除任务创建失败：' + error.message;
    } finally {
      if (!deletionStarted) {
        setElementDisabled(elements.deleteConfirmBtn, false);
        syncDeleteConfirmButton(elements);
      }
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
        setElementDisabled(elements.deleteCancelBtn, false);
        closeDeleteDialog(elements, { skipFocusRestore: true });
        window.location.assign('/admin/clone/runs/manage');
        return;
      }
      if (status === 'error') {
        state.deleteJobId = '';
        setElementDisabled(elements.deleteCancelBtn, false);
        elements.deleteStatus.textContent = '删除任务未完成，请刷新详情后查看记录。';
        syncDeleteConfirmButton(elements);
        return;
      }
      if (status !== 'queued' && status !== 'running') {
        throw new Error('删除任务状态异常');
      }
    } catch (error) {
      state.deleteJobId = '';
      setElementDisabled(elements.deleteCancelBtn, false);
      elements.deleteStatus.textContent = '删除任务状态读取失败：' + error.message;
      syncDeleteConfirmButton(elements);
      return;
    }
    window.setTimeout(function () {
      pollDeleteJob(elements, jobId);
    }, 1200);
  }

  function syncDeleteButton(elements) {
    setElementDisabled(
      elements.deleteBtn,
      state.busy
        || !state.runId
        || !state.deleteConfirm
        || !!state.deleteJobId
        || state.operationJob.isPolling
    );
    setElementDisabled(
      elements.detailRefreshBtn,
      state.busy || !state.runId || state.operationJob.isPolling
    );
  }

  function syncDeleteConfirmButton(elements) {
    var ok = String(elements.deleteConfirmInput.value || '').trim() === state.deleteConfirm;
    setElementDisabled(elements.deleteConfirmBtn, !ok || !!state.deleteJobId);
  }

  function syncMappingControls(elements) {
    var disabled = state.busy || state.mappingLoading || !state.runId;
    setElementDisabled(elements.mappingStatusFilter, disabled);
    setElementDisabled(elements.mappingPrevBtn, disabled || state.mappingOffset <= 0);
    setElementDisabled(
      elements.mappingNextBtn,
      disabled || state.mappingOffset + state.mappingLimit >= state.mappingTotal
    );
  }

  function updateMappingStatus(elements, overrideText) {
    var summaryText = '';
    if (overrideText) {
      elements.mappingStatus.textContent = overrideText;
      elements.mappingSummaryText.textContent = overrideText;
      return;
    }
    if (!state.runId) {
      summaryText = '缺少记录参数，无法读取消息映射。';
      elements.mappingStatus.textContent = summaryText;
      elements.mappingSummaryText.textContent = summaryText;
      return;
    }
    if (!state.mappingTotal) {
      summaryText = '暂无匹配的消息映射。';
      elements.mappingStatus.textContent = summaryText;
      elements.mappingSummaryText.textContent = summaryText;
      return;
    }
    var start = state.mappingOffset + 1;
    var end = Math.min(state.mappingOffset + state.mappingLimit, state.mappingTotal);
    summaryText = '显示 '
      + formatNumber(start)
      + '-'
      + formatNumber(end)
      + ' / '
      + formatNumber(state.mappingTotal)
      + ' 条消息映射';
    elements.mappingStatus.textContent = summaryText + '。';
    elements.mappingSummaryText.textContent = summaryText;
  }

  function setBusy(elements, busy) {
    state.busy = !!busy;
    syncDeleteButton(elements);
    syncMappingControls(elements);
    renderOperationPanel(elements, state.detail);
  }

  function initializePageState(elements) {
    state.mappingOffset = 0;
    state.mappingLimit = MAPPING_PAGE_SIZE;
    elements.mappingStatusFilter.value = '';
    elements.messageLimitInput.value = '';
    elements.sendDelayInput.value = '500';
    state.runId = getRunIdFromLocation();
  }

  function canResumeMigration(run) {
    return String((run && run.status) || '').trim().toLowerCase() === 'done'
      && !isMessageResetRequired(run)
      && !!(run && run.target_chat_id)
      && !!String((run && run.run_id) || '').trim();
  }

  function isMessageResetRequired(run) {
    return String((run && run.phase) || '').trim().toLowerCase()
      === 'message_reset_required';
  }

  function hasPlanBlockingIssues(plan) {
    if (!plan) return false;
    var blocking = Array.isArray(plan.blocking_issues) ? plan.blocking_issues : [];
    return String(plan.status || '').trim().toLowerCase() === 'done' && blocking.length > 0;
  }

  function isMigrationErrored(migration) {
    return String((migration && migration.status) || '').trim().toLowerCase() === 'error';
  }

  function isMigrationActive(migration) {
    var status = String((migration && migration.status) || '').trim().toLowerCase();
    return status === 'queued' || status === 'running';
  }

  function canDeleteLocalMessages(run) {
    var hasTarget = !!(run && run.target_chat_id)
      && !!String((run && run.run_id) || '').trim();
    return hasTarget
      && (canResumeMigration(run) || isMessageResetRequired(run))
      && !state.operationJob.isPolling;
  }

  function buildRunMessageDeleteHref(run) {
    return '/admin/clone/runs/messages/delete?run_id='
      + encodeURIComponent(String((run && run.run_id) || ''));
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
    for (var index = 0; index < candidates.length; index += 1) {
      var account = String(candidates[index] || '').trim().toLowerCase();
      if (account === 'primary' || account === 'secondary') return account;
    }
    return '';
  }

  function getAccessStatusLabel(status) {
    var normalized = String(status || '').trim();
    if (normalized === 'ok') return '可访问';
    if (normalized === 'missing') return '不存在/未命中';
    if (normalized === 'blocked') return '不可用';
    if (normalized === 'restricted') return '受限';
    return normalized || '未预检';
  }

  function getTextStrategyLabel(strategy) {
    var normalized = String(strategy || '').trim();
    if (normalized === 'database_replay') return '按数据库顺序发送';
    if (normalized === 'blocked') return '阻断';
    return normalized || '未确定';
  }

  function getMediaStrategyLabel(strategy) {
    var normalized = String(strategy || '').trim();
    if (normalized === 'source_copy_without_attribution') return '源群直接复制';
    if (normalized === 'relay_copy_without_attribution') return '通过中转群桥接';
    if (normalized === 'impossible_without_local_vault') return '缺少本地媒体保险库';
    if (normalized === 'blocked') return '阻断';
    return normalized || '未确定';
  }

  function isPreviewRemaining(preview) {
    return Number((preview && preview.timeline_remaining) || 0) > 0;
  }

  function isGroupProgressComplete(progress) {
    return isGroupProgressVerified(progress)
      && Number((progress && progress.messages_remaining) || 0) <= 0;
  }

  function isGroupProgressRemaining(progress) {
    return isGroupProgressVerified(progress)
      && Number((progress && progress.messages_remaining) || 0) > 0;
  }

  function getRunIdFromLocation() {
    try {
      var params = new URLSearchParams(window.location.search || '');
      return String(params.get('run_id') || '').trim();
    } catch (_error) {
      return '';
    }
  }

  function buildRunTitle(run) {
    var sourceTitle = String((run && run.source_title) || '未知源').trim();
    var targetTitle = String((run && run.target_title) || '').trim();
    return targetTitle ? sourceTitle + ' -> ' + targetTitle : sourceTitle;
  }

  function formatDoneTotal(done, total) {
    return formatNumber(done) + ' / ' + formatNumber(total);
  }

  function getRunStatusLabel(status) {
    var normalized = String(status || '').trim().toLowerCase();
    if (normalized === 'queued') return '排队中';
    if (normalized === 'running') return '执行中';
    if (normalized === 'deleting') return '删除中';
    if (normalized === 'done') return '已创建';
    if (normalized === 'error') return '失败';
    return normalized || '未知';
  }

  function getPlanStatusLabel(status) {
    var normalized = String(status || '').trim().toLowerCase();
    if (normalized === 'queued') return '排队中';
    if (normalized === 'running') return '生成中';
    if (normalized === 'done') return '已生成';
    if (normalized === 'error') return '失败';
    return normalized || '未生成';
  }

  function getMigrationStatusLabel(status) {
    var normalized = String(status || '').trim().toLowerCase();
    if (normalized === 'queued') return '排队中';
    if (normalized === 'running') return '执行中';
    if (normalized === 'done') return '完成';
    if (normalized === 'error') return '失败';
    return normalized || '未执行';
  }

  function getMigrationAccountLabel(account) {
    var normalized = String(account || '').trim().toLowerCase();
    if (normalized === 'primary') return '主账号';
    if (normalized === 'secondary') return '第二账号';
    if (normalized === 'unavailable') return '不可用';
    return normalized || '未确定';
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

  function getMigrationPhaseLabel(phase) {
    var normalized = String(phase || '').trim().toLowerCase();
    if (normalized === 'queued') return '等待迁移';
    if (normalized === 'validating') return '校验计划';
    if (normalized === 'connecting') return '连接账号';
    if (normalized === 'replaying_timeline') return '重放完整时间线';
    if (normalized === 'sending_text') return '发送文本';
    if (normalized === 'done') return '完成';
    if (normalized === 'limited_done') return '达到本次上限';
    if (normalized === 'stopped') return '已停止';
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

  async function fetchJSON(url, options) {
    var requestOptions = Object.assign({}, options || {});
    if (!requestOptions.method || String(requestOptions.method).toUpperCase() === 'GET') {
      requestOptions.timeoutMs = requestOptions.timeoutMs || DATABASE_READ_TIMEOUT_MS;
    }
    return sharedFetchJSON(url, Object.assign(requestOptions, {
      onUnauthorized: sessionController.handleUnauthorizedResponse
    }));
  }

  async function postJSON(url, payload) {
    return sharedPostJSON(url, payload, {
      onUnauthorized: sessionController.handleUnauthorizedResponse
    });
  }

  var operationJobPollController = shared.createAdminJobPollController({
    fetchJSON: fetchJSON,
    appendLog: function (message) {
      var elements = getElements();
      if (elements) appendOperationLog(elements, message);
    },
    getElements: getElements,
    setBusy: setBusy,
    intervalMs: 1200,
    setInitialState: function (_jobState, options) {
      var elements = getElements();
      if (!elements || !options.clearLogs) return;
      clearOperationLogs(elements);
    },
    onSnapshot: function (snapshot) {
      var elements = getElements();
      if (!elements) return;
      var jobType = String((snapshot && snapshot.job_type) || '').trim();
      var progress = snapshot && snapshot.progress ? snapshot.progress : {};
      var current = formatNumber(progress.current || 0);
      var total = progress.total === null || progress.total === undefined
        ? ''
        : '/' + formatNumber(progress.total);
      if (jobType === 'clone_deep_preflight') {
        elements.operationStatus.textContent = '在线预检正在执行' + (total ? '：' + current + total : '...');
        return;
      }
      if (jobType === 'clone_structure') {
        elements.operationStatus.textContent = '目标副本正在创建' + (total ? '：' + current + total : '...');
        return;
      }
      if (jobType === 'clone_target_delete') {
        elements.operationStatus.textContent = '目标副本正在删除' + (total ? '：' + current + total : '...');
        return;
      }
      elements.timelineStatus.textContent = snapshot.stop_requested
        ? '已收到停止请求，正在完成当前批次...'
        : '正在迁移：' + current + total + ' 条消息。';
    },
    getDoneMessage: function (_jobState, snapshot) {
      var jobType = String((snapshot && snapshot.job_type) || '');
      if (jobType === 'clone_deep_preflight') return '在线预检已完成。';
      if (jobType === 'clone_target_delete') return '目标副本删除任务已完成。';
      return '继续迁移任务已完成。';
    },
    getErrorMessage: function (_jobState, snapshot) {
      var jobType = String((snapshot && snapshot.job_type) || '');
      if (jobType === 'clone_deep_preflight') return '在线预检失败，请查看日志后重新执行。';
      if (jobType === 'clone_target_delete') return '目标副本删除失败，请查看日志后重试。';
      return '继续迁移失败，请查看日志和失败样本后重试。';
    },
    onDone: async function (snapshot, _jobState) {
      var elements = getElements();
      if (String((snapshot && snapshot.job_type) || '') === 'clone_target_delete') {
        window.location.assign('/admin/clone/runs/manage');
        return;
      }
      if (elements) await loadDetail(elements);
    },
    onError: async function () {
      var elements = getElements();
      if (elements) await loadDetail(elements);
    }
  });

  var sessionController = shared.createAdminSessionController({
    afterAuth: async function (elements) {
      await loadDetail(elements);
    },
    getElements: getElements,
    getPageElement: function (elements) {
      return elements && elements.page ? elements.page : null;
    }
  });
})();
