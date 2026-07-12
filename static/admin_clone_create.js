(function () {
  'use strict';

  var shared = window.AdminManageShared;
  var DATABASE_READ_TIMEOUT_MS = 20000;
  var state = {
    sourceChats: [],
    report: null,
    busy: false
  };

  document.addEventListener('DOMContentLoaded', async function () {
    var elements = getElements();
    if (!elements || !shared) return;
    initializeUI(elements);
    bindEvents(elements);
    await sessionController.checkAuth(elements);
  });

  function getElements() {
    var elements = {
      page: document.getElementById('admin-clone-page'),
      sourceStage: document.getElementById('admin-clone-source-stage'),
      createStage: document.getElementById('admin-clone-create-stage'),
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
      loginDialog: document.getElementById('admin-login-dialog'),
      loginStatus: document.getElementById('admin-login-status'),
      passwordInput: document.getElementById('admin-password-input'),
      loginConfirmBtn: document.getElementById('admin-login-confirm-btn')
    };
    var missing = Object.keys(elements).filter(function (key) { return !elements[key]; });
    if (missing.length) {
      console.warn('[admin_clone_create] Missing required elements:', missing.join(', '));
      return null;
    }
    return elements;
  }

  function initializeUI(elements) {
    renderSourceSummary(elements, null);
    renderReport(elements, null);
    setBusy(elements, false);
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
    elements.sortSelect.addEventListener('change', function () {
      loadSourceChats(elements);
    });
    elements.refreshBtn.addEventListener('click', function () {
      loadSourceChats(elements);
    });
    elements.sourceSelect.addEventListener('change', function () {
      handleSourceChange(elements);
    });
    elements.preflightBtn.addEventListener('click', function () {
      handlePreflightClick(elements);
    });
    elements.confirmInput.addEventListener('input', function () {
      syncStartButton(elements);
    });
    elements.targetTitleInput.addEventListener('input', function () {
      syncStartButton(elements);
    });
    elements.startBtn.addEventListener('click', function () {
      handleStartCloneClick(elements);
    });
    document.addEventListener('keydown', function (event) {
      if (!elements.loginDialog.hidden && event.key === 'Tab') {
        shared.trapFocusWithin(elements.loginDialog, event);
      }
    });
  }

  async function loadSourceChats(elements) {
    var previousValue = String(elements.sourceSelect.value || '');
    setBusy(elements, true);
    elements.sourceStatus.textContent = '正在读取可克隆源群组...';
    try {
      var payload = await fetchJSON(
        '/api/admin/clone/chats?sort=' + encodeURIComponent(String(elements.sortSelect.value || ''))
      );
      state.sourceChats = Array.isArray(payload.items) ? payload.items : [];
      renderSourceOptions(elements, previousValue);
      elements.sourceStatus.textContent = '共 ' + shared.formatNumber(state.sourceChats.length) + ' 个可选源群组。';
      handleSourceChange(elements);
    } catch (error) {
      state.sourceChats = [];
      renderSourceOptions(elements, '');
      handleSourceChange(elements);
      elements.sourceStatus.textContent = '读取源群组失败：' + error.message;
    } finally {
      setBusy(elements, false);
    }
  }

  function renderSourceOptions(elements, preferredValue) {
    elements.sourceSelect.textContent = '';
    var placeholder = document.createElement('option');
    placeholder.value = '';
    placeholder.textContent = state.sourceChats.length ? '请选择要克隆的源群组' : '暂无可选群组';
    elements.sourceSelect.appendChild(placeholder);
    state.sourceChats.forEach(function (item) {
      if (!item || item.chat_id === undefined || item.chat_id === null) return;
      var option = document.createElement('option');
      option.value = String(item.chat_id);
      option.textContent = buildChatOptionText(item);
      elements.sourceSelect.appendChild(option);
    });
    if (preferredValue && findSourceChat(preferredValue)) {
      elements.sourceSelect.value = preferredValue;
    }
  }

  function handleSourceChange(elements) {
    var selected = getSelectedSourceChat(elements);
    state.report = null;
    renderSourceSummary(elements, selected);
    renderReport(elements, null);
    if (selected) {
      elements.targetKindSelect.value = inferTargetKindFromSource(selected.chat_type);
      elements.preflightStatus.textContent = '已选择源群组，可以开始检查。';
    } else {
      elements.preflightStatus.textContent = '请选择源群组后开始检查。';
    }
    syncStageState(elements);
    syncStartButton(elements);
  }

  async function handlePreflightClick(elements) {
    var selected = getSelectedSourceChat(elements);
    if (!selected) {
      elements.preflightStatus.textContent = '请先选择源群组。';
      return;
    }
    setBusy(elements, true);
    state.report = null;
    renderReport(elements, null);
    elements.preflightStatus.textContent = '正在检查源群和第二账号配置...';
    try {
      var payload = await postJSON('/api/admin/clone/preflight', { chat_id: Number(selected.chat_id) });
      state.report = payload && payload.report ? payload.report : null;
      if (!state.report) throw new Error('检查响应缺少报告');
      renderReport(elements, state.report);
    } catch (error) {
      elements.preflightStatus.textContent = '检查失败：' + error.message;
    } finally {
      setBusy(elements, false);
    }
  }

  async function handleStartCloneClick(elements) {
    if (!isStartAllowed(elements)) {
      elements.preflightStatus.textContent = '请先完成检查并确认副本标题与确认码。';
      return;
    }
    var source = state.report.source || {};
    var chatId = Number(source.chat_id);
    if (!Number.isSafeInteger(chatId)) {
      elements.preflightStatus.textContent = '检查报告中的源群标识异常。';
      return;
    }
    setBusy(elements, true);
    elements.preflightStatus.textContent = '正在创建目标副本记录...';
    try {
      var payload = await postJSON('/api/admin/clone/jobs', {
        chat_id: chatId,
        target_title: elements.targetTitleInput.value,
        target_kind: elements.targetKindSelect.value,
        confirm: elements.confirmInput.value
      });
      var runId = String((((payload || {}).clone_run || {}).run_id) || '').trim();
      if (!runId) throw new Error('创建响应缺少克隆记录标识');
      elements.preflightStatus.textContent = '目标副本任务已创建，正在打开记录中心...';
      window.location.assign('/admin/clone/runs/detail?run_id=' + encodeURIComponent(runId));
    } catch (error) {
      elements.preflightStatus.textContent = '创建任务失败：' + error.message;
      setBusy(elements, false);
    }
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
    appendSummaryPair(elements.sourceSummary, '消息数', shared.formatNumber(item.message_count));
    appendSummaryPair(
      elements.sourceSummary,
      '媒体元信息',
      item.media_rows === undefined || item.media_rows === null
        ? '完成检查后统计'
        : shared.formatNumber(item.media_rows)
    );
    appendSummaryPair(elements.sourceSummary, '最后消息', shared.formatDateTime(item.last_message_at || item.last_seen_at));
  }

  function renderReport(elements, report) {
    elements.metricsList.textContent = '';
    elements.capabilities.textContent = '';
    elements.warnings.textContent = '';
    elements.recommendation.textContent = '';
    elements.confirmInput.value = '';
    elements.confirmHint.textContent = '完成检查后会自动填入操作确认码。';
    elements.targetTitleInput.value = '';
    if (!report) {
      syncStageState(elements);
      syncStartButton(elements);
      return;
    }
    var target = report.target || {};
    var source = report.source || {};
    var account = report.account || {};
    elements.targetTitleInput.value = String(target.default_title || '');
    elements.targetKindSelect.value = inferTargetKindFromSource(source.chat_type);
    elements.confirmInput.value = String(report.confirm || '');
    elements.confirmHint.textContent = '确认码已填入；创建后会自动进入该记录中心。';
    renderMetricCards(elements.metricsList, report.metrics || {});
    renderCapabilities(elements.capabilities, report.capabilities || []);
    renderWarnings(elements.warnings, report.warnings || []);
    renderRecommendation(elements.recommendation, report.recommendation || {});
    elements.preflightStatus.textContent = account.secondary_session_distinct
      ? '检查完成：可以创建目标副本。'
      : '检查完成：第二账号未就绪，暂时不能创建目标副本。';
    syncStageState(elements);
    syncStartButton(elements);
  }

  function renderMetricCards(container, metrics) {
    [
      ['total_messages', '消息总数', false],
      ['text_messages', '可重建文本', false],
      ['media_messages', '媒体消息', false],
      ['grouped_messages', '媒体组消息', false],
      ['media_metadata_coverage_percent', '媒体元信息覆盖', true],
      ['media_group_count', '媒体组数量', false],
      ['suspect_media_group_count', '疑似残缺媒体组', false],
      ['suspect_media_group_ratio_percent', '残缺媒体组占比', true]
    ].forEach(function (definition) {
      var card = document.createElement('li');
      var label = document.createElement('span');
      var value = document.createElement('strong');
      card.className = 'clone-stat-card';
      label.className = 'clone-stat-label';
      value.className = 'clone-stat-value';
      label.textContent = definition[1];
      value.textContent = definition[2]
        ? formatPercent(metrics[definition[0]])
        : shared.formatNumber(metrics[definition[0]]);
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

  function getSelectedSourceChat(elements) {
    return findSourceChat(elements.sourceSelect.value);
  }

  function findSourceChat(chatId) {
    var normalized = String(chatId || '').trim();
    return state.sourceChats.find(function (item) {
      return String((item && item.chat_id) || '') === normalized;
    }) || null;
  }

  function buildChatOptionText(item) {
    var title = String((item && item.chat_title) || (item && item.chat_id) || '').trim();
    return title + '（' + shared.formatNumber((item && item.message_count) || 0) + ' 条）';
  }

  function inferTargetKindFromSource(chatType) {
    var value = String(chatType || '').trim().toLowerCase();
    return value.indexOf('group') !== -1 || value === 'chat' ? 'megagroup' : 'channel';
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

  function getCapabilityStatusLabel(status) {
    if (status === 'ready') return '可执行';
    if (status === 'blocked') return '阻断';
    if (status === 'requires_source') return '依赖源群';
    if (status === 'deferred') return '后续阶段';
    if (status === 'empty') return '无数据';
    return status || '未知';
  }

  function formatPercent(value) {
    var number = Number(value || 0);
    return Number.isFinite(number) ? String(Math.round(number * 10) / 10) + '%' : '0%';
  }

  function syncStageState(elements) {
    elements.sourceStage.setAttribute('data-stage-state', state.report ? 'complete' : 'current');
    elements.createStage.setAttribute('data-stage-state', state.report ? 'current' : 'pending');
  }

  function setBusy(elements, busy) {
    state.busy = !!busy;
    var selected = getSelectedSourceChat(elements);
    shared.setElementDisabled(elements.sortSelect, state.busy);
    shared.setElementDisabled(elements.refreshBtn, state.busy);
    shared.setElementDisabled(elements.sourceSelect, state.busy);
    shared.setElementDisabled(elements.preflightBtn, state.busy || !selected);
    shared.setElementDisabled(elements.targetTitleInput, state.busy || !state.report);
    shared.setElementDisabled(elements.targetKindSelect, state.busy || !state.report);
    shared.setElementDisabled(elements.confirmInput, state.busy || !state.report);
    syncStartButton(elements);
    syncStageState(elements);
  }

  function syncStartButton(elements) {
    shared.setElementDisabled(elements.startBtn, !isStartAllowed(elements));
  }

  function isStartAllowed(elements) {
    if (state.busy || !state.report) return false;
    if (!((state.report.account || {}).secondary_session_distinct)) return false;
    if (String(elements.targetTitleInput.value || '').trim().length <= 0) return false;
    return String(elements.confirmInput.value || '').trim() === String(state.report.confirm || '').trim();
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

  async function postJSON(url, payload) {
    return shared.postJSON(url, payload, {
      onUnauthorized: sessionController.handleUnauthorizedResponse
    });
  }

  var sessionController = shared.createAdminSessionController({
    afterAuth: async function (elements) {
      await loadSourceChats(elements);
    },
    getElements: getElements,
    getPageElement: function (elements) {
      return elements && elements.page ? elements.page : null;
    }
  });
})();
