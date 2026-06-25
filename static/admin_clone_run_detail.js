(function () {
  'use strict';

  var shared = window.AdminManageShared;
  var sharedFetchJSON = shared.fetchJSON;
  var setDialogOpenState = shared.setDialogOpenState;
  var setElementDisabled = shared.setElementDisabled;
  var setPageInteractionState = shared.setPageInteractionState;
  var trapFocusWithin = shared.trapFocusWithin;
  var formatNumber = shared.formatNumber;

  var MAPPING_PAGE_SIZE = 25;

  var state = {
    runId: '',
    detail: null,
    mappingItems: [],
    mappingOffset: 0,
    mappingLimit: MAPPING_PAGE_SIZE,
    mappingTotal: 0,
    mappingLoading: false,
    busy: false,
    deleteConfirm: ''
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
      migrateLink: document.getElementById('admin-clone-run-detail-migrate-link'),
      migrateNavLink: document.getElementById('admin-clone-run-detail-migrate-nav-link'),
      detailStatus: document.getElementById('admin-clone-runs-detail-status'),
      detailRefreshBtn: document.getElementById('admin-clone-runs-detail-refresh-btn'),
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
      'migrateLink',
      'migrateNavLink',
      'detailStatus',
      'detailRefreshBtn',
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
    elements.detailStatus.textContent = '正在读取已克隆群详情...';
    try {
      var payload = await fetchJSON(
        '/api/admin/clone/runs/' + encodeURIComponent(state.runId) + '/detail'
      );
      state.detail = payload || null;
      state.deleteConfirm = String((payload && payload.delete_confirm) || '');
      syncMigrateLink(elements);
      renderDetail(elements, state.detail);
      await loadMappings(elements, { resetOffset: false });
    } catch (error) {
      state.detail = null;
      state.deleteConfirm = '';
      renderDetail(elements, null);
      renderMappingList(elements.mappingList, []);
      updateMappingStatus(elements, '读取映射前请先恢复有效记录详情。');
      elements.detailStatus.textContent = '读取详情失败：' + error.message;
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
    var preview = payload && payload.timeline_preview ? payload.timeline_preview : null;
    var summary = payload && payload.mapping_summary ? payload.mapping_summary : null;
    var failures = payload && Array.isArray(payload.failure_items) ? payload.failure_items : [];

    elements.detailSummary.textContent = '';
    elements.progressSummary.textContent = '';
    elements.failureList.textContent = '';
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
    appendSummaryPair(elements.detailSummary, '剩余时间线', formatNumber(preview && preview.timeline_remaining));
    appendSummaryPair(elements.detailSummary, '更新时间', formatDateTime(run.updated_at));

    appendMiniPair(elements.progressSummary, '迁移状态', getMigrationStatusLabel(migration && migration.status));
    appendMiniPair(elements.progressSummary, '时间线总数', formatNumber(preview && preview.timeline_items_total));
    appendMiniPair(elements.progressSummary, '剩余时间线', formatNumber(preview && preview.timeline_remaining));
    appendMiniPair(elements.progressSummary, '文本已发', formatDoneTotal(migration && migration.text_sent, migration && migration.text_total));
    appendMiniPair(elements.progressSummary, '媒体已复制', formatDoneTotal(migration && migration.media_sent, migration && migration.media_total));
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

  function renderDetailActions(elements, run, payload) {
    var resumeHref = run && canResumeMigration(run) ? buildRunMigrationHref(run) : '';
    var resumeLabel = buildResumeLinkLabel(payload);

    syncActionLink(elements.openSourceLink, run && run.source_telegram_app_link, '打开源群');
    syncActionLink(elements.openTargetLink, run && run.target_telegram_app_link, '打开目标副本');
    syncActionLink(elements.resumeLink, resumeHref, resumeLabel);
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
    if (!run || !canResumeMigration(run)) {
      return '去继续克隆消息';
    }
    if (!plan) return '去生成克隆计划';
    if (hasPlanBlockingIssues(plan)) return '去处理阻断项';
    if (isMigrationErrored(migration)) return '去重试继续克隆';
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
    return '当前查看：' + statusLabel + ' / ' + targetLabel;
  }

  function buildNextStepText(payload) {
    var run = payload && payload.run ? payload.run : null;
    var plan = payload && payload.plan ? payload.plan : null;
    var migration = payload && payload.migration ? payload.migration : null;
    var preview = payload && payload.timeline_preview ? payload.timeline_preview : null;

    if (!run) {
      return '请回到“已克隆群管理”选择一条记录进入详情页。';
    }

    var runStatus = String(run.status || '').trim().toLowerCase();
    if (runStatus === 'queued' || runStatus === 'running') {
      return '这条记录还在创建克隆群。先等待创建完成，再继续克隆消息。';
    }
    if (runStatus === 'error') {
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
    if (isPreviewRemaining(preview)) {
      return '这条记录还有剩余消息未处理，可以直接继续克隆消息。';
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
      elements.deleteHelp.textContent = '不会删除 Telegram 上已经创建的克隆群；只会删除本地数据库中的运行记录、计划、统计和消息映射。';
      return;
    }
    elements.deleteHelp.textContent = '当前将删除 “'
      + buildRunTitle(run)
      + '” 的本地数据库记录。不会删除 Telegram 上已经创建的克隆群。';
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
      ? '将删除 “' + buildRunTitle(run) + '” 的本地数据库记录。请输入确认码：' + state.deleteConfirm
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
    elements.deleteStatus.textContent = '正在删除数据库记录...';
    try {
      await fetchJSON(
        '/api/admin/clone/runs/' + encodeURIComponent(state.runId),
        {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ confirm: confirmText })
        }
      );
      closeDeleteDialog(elements);
      window.location.assign('/admin/clone/runs/manage');
    } catch (error) {
      elements.deleteStatus.textContent = '删除失败：' + error.message;
    } finally {
      setElementDisabled(elements.deleteConfirmBtn, false);
      syncDeleteConfirmButton(elements);
    }
  }

  function syncDeleteButton(elements) {
    setElementDisabled(elements.deleteBtn, state.busy || !state.runId || !state.deleteConfirm);
    setElementDisabled(elements.detailRefreshBtn, state.busy || !state.runId);
  }

  function syncDeleteConfirmButton(elements) {
    var ok = String(elements.deleteConfirmInput.value || '').trim() === state.deleteConfirm;
    setElementDisabled(elements.deleteConfirmBtn, !ok);
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

  function syncMigrateLink(elements) {
    var href = state.runId
      ? buildRunMigrationHref({ run_id: state.runId })
      : '/admin/clone/migrate';
    elements.migrateLink.href = href;
    elements.migrateNavLink.href = href;
  }

  function setBusy(elements, busy) {
    state.busy = !!busy;
    syncDeleteButton(elements);
    syncMappingControls(elements);
  }

  function initializePageState(elements) {
    state.mappingOffset = 0;
    state.mappingLimit = MAPPING_PAGE_SIZE;
    elements.mappingStatusFilter.value = '';
    state.runId = getRunIdFromLocation();
    syncMigrateLink(elements);
  }

  function canResumeMigration(run) {
    return String((run && run.status) || '').trim().toLowerCase() === 'done'
      && !!(run && run.target_chat_id)
      && !!String((run && run.run_id) || '').trim();
  }

  function buildRunMigrationHref(run) {
    return '/admin/clone/migrate?run_id=' + encodeURIComponent(String(run.run_id || ''));
  }

  function hasPlanBlockingIssues(plan) {
    if (!plan) return false;
    var blocking = Array.isArray(plan.blocking_issues) ? plan.blocking_issues : [];
    return String(plan.status || '').trim().toLowerCase() === 'done' && blocking.length > 0;
  }

  function isMigrationErrored(migration) {
    return String((migration && migration.status) || '').trim().toLowerCase() === 'error';
  }

  function isPreviewRemaining(preview) {
    return Number((preview && preview.timeline_remaining) || 0) > 0;
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

  function formatDateTime(value) {
    var text = String(value || '').trim();
    if (!text) return '暂无';
    return text.replace('T', ' ').replace('+00:00', '');
  }

  function getRunStatusLabel(status) {
    var normalized = String(status || '').trim().toLowerCase();
    if (normalized === 'queued') return '排队中';
    if (normalized === 'running') return '执行中';
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
    return sharedFetchJSON(url, Object.assign({}, options || {}, {
      onUnauthorized: sessionController.handleUnauthorizedResponse
    }));
  }

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
