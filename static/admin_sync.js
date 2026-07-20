(function () {
  'use strict';

  var shared = window.AdminManageShared;
  var sharedFetchJSON = shared.fetchJSON;
  var formatDateTime = shared.formatDateTime;
  var formatNumber = shared.formatNumber;
  var setElementDisabled = shared.setElementDisabled;
  var trapFocusWithin = shared.trapFocusWithin;

  var LIVE_WINDOW_KEY = 'live';
  var LIVE_MESSAGES_LIMIT = 50;
  var LIVE_POLL_INTERVAL_MS = 15000;
  var SYNC_STATS_TIMEOUT_MS = 15000;
  var LIVE_MESSAGES_TIMEOUT_MS = 10000;
  var STORAGE_HEALTH_TIMEOUT_MS = 8000;
  var SYNC_STATS_SLOW_MS = 4000;
  var LIVE_MESSAGES_SLOW_MS = 3000;

  var syncState = {
    windows: [],
    selectedWindowKey: '',
    busy: false,
    liveItems: [],
    livePollTimerId: null,
    liveRequestSeq: 0,
    liveActiveRequestSeq: 0,
    storageRequestSeq: 0,
    chatRequestSeq: 0,
    chatItems: [],
    chatListBusy: false,
    health: null,
    storageHealth: null,
    scheduler: null,
    apiSignals: {
      stats: {
        lastDurationMs: 0,
        lastSlow: false,
        slowCount: 0,
        timeoutCount: 0,
        failureCount: 0,
        lastError: ''
      },
      live: {
        lastDurationMs: 0,
        lastSlow: false,
        slowCount: 0,
        timeoutCount: 0,
        failureCount: 0,
        lastError: ''
      }
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
      status: document.getElementById('admin-sync-status'),
      refreshBtn: document.getElementById('admin-sync-refresh-btn'),
      metricNote: document.getElementById('admin-sync-metric-note'),
      windowLabelValue: document.getElementById('admin-sync-window-label'),
      windowMessagesValue: document.getElementById('admin-sync-window-messages'),
      windowChatsValue: document.getElementById('admin-sync-window-chats'),
      latestCreatedAtValue: document.getElementById('admin-sync-latest-created-at'),
      windowSelect: document.getElementById('admin-sync-window-select'),
      windowGrid: document.getElementById('admin-sync-window-grid'),
      liveSection: document.getElementById('admin-sync-live-section'),
      liveStatus: document.getElementById('admin-sync-live-status'),
      liveList: document.getElementById('admin-sync-live-list'),
      healthPanel: document.getElementById('admin-sync-health-panel'),
      healthStatus: document.getElementById('admin-sync-health-status'),
      healthBanner: document.getElementById('admin-sync-health-banner'),
      healthReasons: document.getElementById('admin-sync-health-reasons'),
      healthActions: document.getElementById('admin-sync-health-actions'),
      diagnoseBtn: document.getElementById('admin-sync-diagnose-btn'),
      diagnoseResult: document.getElementById('admin-sync-diagnose-result'),
      storageHealthStatus: document.getElementById('admin-storage-health-status'),
      storageHealthBanner: document.getElementById('admin-storage-health-banner'),
      storageHealthMetrics: document.getElementById('admin-storage-health-metrics'),
      storageHealthReasons: document.getElementById('admin-storage-health-reasons'),
      storageHealthActions: document.getElementById('admin-storage-health-actions'),
      schedulerStatus: document.getElementById('admin-sync-scheduler-status'),
      schedulerGrid: document.getElementById('admin-sync-scheduler-grid'),
      accountGrid: document.getElementById('admin-sync-account-grid'),
      schedulerModel: document.getElementById('admin-sync-scheduler-model'),
      schedulerFailures: document.getElementById('admin-sync-scheduler-failures'),
      resetModelBtn: document.getElementById('admin-sync-reset-model-btn'),
      membershipSelect: document.getElementById('admin-sync-membership-select'),
      chatStatusSelect: document.getElementById('admin-sync-chat-status-select'),
      loadChatsBtn: document.getElementById('admin-sync-load-chats-btn'),
      chatList: document.getElementById('admin-sync-chat-list'),
      loginDialog: document.getElementById('admin-login-dialog'),
      loginStatus: document.getElementById('admin-login-status'),
      passwordInput: document.getElementById('admin-password-input'),
      loginConfirmBtn: document.getElementById('admin-login-confirm-btn')
    };

    var requiredKeys = [
      'status',
      'refreshBtn',
      'metricNote',
      'windowLabelValue',
      'windowMessagesValue',
      'windowChatsValue',
      'latestCreatedAtValue',
      'windowSelect',
      'windowGrid',
      'liveSection',
      'liveStatus',
      'liveList',
      'healthPanel',
      'healthStatus',
      'healthBanner',
      'healthReasons',
      'healthActions',
      'diagnoseBtn',
      'diagnoseResult',
      'storageHealthStatus',
      'storageHealthBanner',
      'storageHealthMetrics',
      'storageHealthReasons',
      'storageHealthActions',
      'schedulerStatus',
      'schedulerGrid',
      'accountGrid',
      'schedulerModel',
      'schedulerFailures',
      'resetModelBtn',
      'membershipSelect',
      'chatStatusSelect',
      'loadChatsBtn',
      'chatList',
      'loginDialog',
      'loginStatus',
      'passwordInput',
      'loginConfirmBtn'
    ];
    var missing = requiredKeys.filter(function (key) { return !elements[key]; });
    if (missing.length > 0) {
      console.warn('[admin_sync] Missing required elements:', missing.join(', '));
      return null;
    }
    return elements;
  }

  function initializeUI(elements) {
    elements.metricNote.textContent = '';
    elements.windowGrid.textContent = '';
    elements.liveList.textContent = '';
    elements.status.textContent = '正在读取消息同步统计...';
    elements.liveStatus.textContent = '正在读取最近入库消息...';
    elements.healthStatus.textContent = '正在检测同步链路...';
    elements.healthBanner.textContent = '等待健康检查结果...';
    elements.healthBanner.className = 'sync-health-banner is-neutral';
    elements.healthReasons.textContent = '';
    elements.healthActions.textContent = '';
    elements.diagnoseResult.textContent = '';
    elements.storageHealthStatus.textContent = '正在读取数据库容量状态...';
    elements.storageHealthBanner.textContent = '等待容量健康检查结果...';
    elements.storageHealthBanner.className = 'sync-health-banner is-neutral';
    elements.storageHealthMetrics.textContent = '';
    elements.storageHealthReasons.textContent = '';
    elements.storageHealthActions.textContent = '';
    elements.schedulerStatus.textContent = '正在读取调度状态...';
    elements.schedulerGrid.textContent = '';
    elements.accountGrid.textContent = '';
    elements.schedulerModel.textContent = '';
    elements.schedulerFailures.textContent = '';
    elements.chatList.textContent = '';
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
      loadSyncDashboard(elements);
    });
    elements.diagnoseBtn.addEventListener('click', function () {
      triggerSyncDiagnosis(elements);
    });
    elements.resetModelBtn.addEventListener('click', function () {
      resetSyncModel(elements);
    });
    elements.loadChatsBtn.addEventListener('click', function () {
      loadSchedulerChats(elements);
    });
    elements.membershipSelect.addEventListener('change', function () {
      loadSchedulerChats(elements);
    });
    elements.chatStatusSelect.addEventListener('change', function () {
      loadSchedulerChats(elements);
    });
    elements.windowSelect.addEventListener('change', function () {
      syncState.selectedWindowKey = String(elements.windowSelect.value || '');
      renderSelectedWindow(elements);
      renderWindowCards(elements);
      updateLiveSectionVisibility(elements);
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

  function setBusy(elements, isBusy) {
    syncState.busy = !!isBusy;
    setElementDisabled(elements.refreshBtn, syncState.busy);
    setElementDisabled(elements.windowSelect, syncState.busy);
    setElementDisabled(elements.diagnoseBtn, syncState.busy);
    setElementDisabled(elements.resetModelBtn, syncState.busy);
    setElementDisabled(elements.loadChatsBtn, syncState.busy || syncState.chatListBusy);
    if (elements.windowGrid && typeof elements.windowGrid.setAttribute === 'function') {
      elements.windowGrid.setAttribute('aria-busy', syncState.busy ? 'true' : 'false');
    }
  }

  function createTextElement(tagName, className, text) {
    var el = document.createElement(tagName);
    if (className) el.className = className;
    el.textContent = String(text || '');
    return el;
  }

  function isLiveWindowSelected() {
    return String(syncState.selectedWindowKey || '') === LIVE_WINDOW_KEY;
  }

  function clearLivePollTimer() {
    if (!syncState.livePollTimerId) {
      return;
    }
    window.clearTimeout(syncState.livePollTimerId);
    syncState.livePollTimerId = null;
  }

  function nextLiveRequestSeq() {
    syncState.liveRequestSeq += 1;
    syncState.liveActiveRequestSeq = syncState.liveRequestSeq;
    return syncState.liveActiveRequestSeq;
  }

  function isCurrentLiveRequest(requestSeq) {
    return Number(requestSeq) === Number(syncState.liveActiveRequestSeq);
  }

  function scheduleLivePoll(elements) {
    clearLivePollTimer();
    if (!isLiveWindowSelected()) {
      return;
    }
    syncState.livePollTimerId = window.setTimeout(function () {
      loadLiveMessages(elements, { silent: true });
    }, LIVE_POLL_INTERVAL_MS);
  }

  function getSelectedWindow() {
    var key = String(syncState.selectedWindowKey || '');
    return syncState.windows.find(function (item) {
      return String(item && item.window_key || '') === key;
    }) || syncState.windows[0] || null;
  }

  function renderWindowOptions(elements) {
    var selectedKey = String(syncState.selectedWindowKey || '');
    elements.windowSelect.textContent = '';
    syncState.windows.forEach(function (item, index) {
      var option = document.createElement('option');
      option.value = String(item.window_key || '');
      option.textContent = String(item.label || ('窗口 ' + String(index + 1)));
      if ((selectedKey && option.value === selectedKey) || (!selectedKey && index === 0)) {
        option.selected = true;
      }
      elements.windowSelect.appendChild(option);
    });
  }

  function renderSelectedWindow(elements) {
    var selectedWindow = getSelectedWindow();
    if (!selectedWindow) {
      elements.windowLabelValue.textContent = '暂无';
      elements.windowMessagesValue.textContent = '0';
      elements.windowChatsValue.textContent = '0';
      elements.latestCreatedAtValue.textContent = '暂无';
      elements.status.textContent = '暂无消息同步统计。';
      return;
    }

    elements.windowLabelValue.textContent = String(selectedWindow.label || '暂无');
    elements.windowMessagesValue.textContent = formatNumber(selectedWindow.message_count || 0);
    elements.windowChatsValue.textContent = formatNumber(selectedWindow.chat_count || 0);
    elements.latestCreatedAtValue.textContent = formatDateTime(selectedWindow.latest_created_at);

    if (Number(selectedWindow.message_count || 0) <= 0) {
      elements.status.textContent = String(selectedWindow.label || '当前窗口') + ' 内暂无新入库消息。';
      return;
    }

    elements.status.textContent =
      String(selectedWindow.label || '当前窗口')
      + ' 内入库 '
      + formatNumber(selectedWindow.message_count || 0)
      + ' 条消息，覆盖 '
      + formatNumber(selectedWindow.chat_count || 0)
      + ' 个群组。';
  }

  function buildWindowCard(item) {
    var card = document.createElement('article');
    card.className = 'sync-window-card';
    if (String(item.window_key || '') === String(syncState.selectedWindowKey || '')) {
      card.classList.add('is-active');
    }

    card.appendChild(createTextElement('h3', 'sync-window-title', item.label || '未知窗口'));

    var meta = document.createElement('div');
    meta.className = 'sync-window-meta';

    var messageStat = document.createElement('div');
    messageStat.className = 'sync-window-stat';
    messageStat.appendChild(createTextElement('span', 'sync-window-stat-label', '消息入库'));
    messageStat.appendChild(createTextElement('strong', 'sync-window-stat-value', formatNumber(item.message_count || 0)));
    meta.appendChild(messageStat);

    var chatStat = document.createElement('div');
    chatStat.className = 'sync-window-stat';
    chatStat.appendChild(createTextElement('span', 'sync-window-stat-label', '覆盖群组'));
    chatStat.appendChild(createTextElement('strong', 'sync-window-stat-value', formatNumber(item.chat_count || 0)));
    meta.appendChild(chatStat);

    card.appendChild(meta);

    card.appendChild(
      createTextElement(
        'p',
        'sync-window-time',
        (item.is_live ? '实时模式：' : '最早入库：') + (item.is_live ? '下方显示最近入库消息流' : formatDateTime(item.oldest_created_at))
      )
    );
    card.appendChild(
      createTextElement(
        'p',
        'sync-window-time',
        '最近入库：' + formatDateTime(item.latest_created_at)
      )
    );

    return card;
  }

  function renderWindowCards(elements) {
    elements.windowGrid.textContent = '';
    syncState.windows.forEach(function (item) {
      elements.windowGrid.appendChild(buildWindowCard(item));
    });
  }

  function normalizeSyncHealth(payload) {
    var health = payload && payload.health ? payload.health : {};
    var reasons = Array.isArray(health.reasons) ? health.reasons : [];
    var actions = Array.isArray(health.actions) ? health.actions : [];
    return {
      status: String(health.status || 'healthy'),
      checkedAt: String(health.checked_at || ''),
      latestMessageAgeSeconds: typeof health.latest_message_age_seconds === 'number'
        ? Number(health.latest_message_age_seconds)
        : null,
      reasons: reasons.map(function (item) {
        return {
          code: String(item && item.code || ''),
          severity: String(item && item.severity || 'warning'),
          message: String(item && item.message || '')
        };
      }).filter(function (item) {
        return !!item.message;
      }),
      actions: actions.map(function (item) { return String(item || ''); }).filter(Boolean),
      listener: health.listener || {}
    };
  }

  function signalApiOutcome(key, options) {
    var entry = syncState.apiSignals[key];
    if (!entry) return;
    var opts = options || {};
    if (typeof opts.durationMs === 'number') {
      entry.lastDurationMs = Math.max(0, Number(opts.durationMs));
    }
    if (opts.slow === true) {
      entry.lastSlow = true;
      entry.slowCount += 1;
    } else if (opts.success === true) {
      entry.lastSlow = false;
      entry.slowCount = 0;
    }
    if (opts.timeout === true) {
      entry.timeoutCount += 1;
      entry.failureCount += 1;
      entry.lastError = String(opts.errorMessage || '请求超时，请稍后重试');
      return;
    }
    if (opts.failed === true) {
      entry.failureCount += 1;
      entry.lastError = String(opts.errorMessage || '请求失败');
      return;
    }
      if (opts.success === true) {
        entry.lastError = '';
        entry.failureCount = 0;
        entry.timeoutCount = 0;
    }
  }

  function isTimeoutMessage(message) {
    return String(message || '').indexOf('请求超时') >= 0;
  }

  function buildApiHealthOverlay() {
    var reasons = [];
    var actions = [];
    ['stats', 'live'].forEach(function (key) {
      var entry = syncState.apiSignals[key];
      if (!entry) return;
      var label = key === 'stats' ? '同步统计接口' : '实时消息接口';
      if (entry.timeoutCount > 0) {
        reasons.push({
          code: key + '_timeout',
          severity: 'critical',
          message: label + '最近发生超时，可能存在 API 长等待或后端阻塞。'
        });
        actions.push('检查 ' + label + ' 的数据库查询、锁等待和服务负载。');
        return;
      }
      if (entry.failureCount > 0) {
        reasons.push({
          code: key + '_failure',
          severity: 'warning',
          message: label + '最近请求失败：' + String(entry.lastError || '未知错误')
        });
        actions.push('检查 ' + label + ' 返回状态和服务日志。');
        return;
      }
      if (entry.lastSlow === true) {
        reasons.push({
          code: key + '_slow',
          severity: 'warning',
          message: label + '响应偏慢，最近耗时约 ' + String(entry.lastDurationMs) + 'ms。'
        });
      }
    });
    return {
      reasons: reasons,
      actions: actions
    };
  }

  function renderSyncHealth(elements) {
    var health = syncState.health || {
      status: 'healthy',
      checkedAt: '',
      reasons: [],
      actions: []
    };
    var overlay = buildApiHealthOverlay();
    var allReasons = health.reasons.slice().concat(overlay.reasons);
    var allActions = health.actions.slice().concat(overlay.actions);
    var status = health.status || 'healthy';
    if (overlay.reasons.some(function (item) { return item.severity === 'critical'; })) {
      status = 'critical';
    } else if (
      status === 'healthy'
      && overlay.reasons.some(function (item) { return item.severity === 'warning'; })
    ) {
      status = 'warning';
    }

    elements.healthReasons.textContent = '';
    elements.healthActions.textContent = '';

    elements.healthBanner.className = 'sync-health-banner is-' + status;
    if (status === 'critical') {
      elements.healthBanner.textContent = '同步链路存在明确异常，建议立即执行诊断并检查监听状态。';
    } else if (status === 'warning') {
      elements.healthBanner.textContent = '同步链路存在风险信号，建议关注最近入库与接口时延。';
    } else {
      elements.healthBanner.textContent = '同步链路当前未发现明确异常。';
    }

    elements.healthStatus.textContent =
      '健康状态：' + status
      + (health.checkedAt ? '，检测时间 ' + formatDateTime(health.checkedAt) : '');

    if (!allReasons.length) {
      elements.healthReasons.appendChild(
        createTextElement('div', 'sync-health-item', '当前没有异常原因。')
      );
    } else {
      allReasons.forEach(function (item) {
        var box = createTextElement(
          'div',
          'sync-health-item is-' + String(item.severity || 'warning'),
          String(item.message || '')
        );
        elements.healthReasons.appendChild(box);
      });
    }

    var uniqueActions = [];
    allActions.forEach(function (item) {
      if (item && uniqueActions.indexOf(item) < 0) uniqueActions.push(item);
    });
    uniqueActions.forEach(function (item) {
      elements.healthActions.appendChild(
        createTextElement('div', 'sync-health-action', String(item))
      );
    });
  }

  function normalizeHealthStatus(value) {
    var status = String(value || '').toLowerCase();
    return ['healthy', 'warning', 'critical'].indexOf(status) >= 0 ? status : 'healthy';
  }

  function nullableNumber(value) {
    if (value === null || typeof value === 'undefined' || value === '') {
      return null;
    }
    var number = Number(value);
    return Number.isFinite(number) && number >= 0 ? number : null;
  }

  function formatByteSize(value) {
    var bytes = nullableNumber(value);
    if (bytes === null) return '未知';
    var units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
    var unitIndex = 0;
    while (bytes >= 1024 && unitIndex < units.length - 1) {
      bytes /= 1024;
      unitIndex += 1;
    }
    var precision = unitIndex === 0 || bytes >= 100 ? 0 : 1;
    return bytes.toFixed(precision) + ' ' + units[unitIndex];
  }

  function formatMetricCount(value, source) {
    var count = nullableNumber(value);
    if (count === null) return '未知';
    var prefix = String(source || '') === 'sqlite_stat1' ? '约 ' : '';
    return prefix + formatNumber(count);
  }

  function normalizeStorageHealth(payload) {
    var database = payload && payload.database && typeof payload.database === 'object'
      ? payload.database
      : {};
    var counts = payload && payload.counts && typeof payload.counts === 'object'
      ? payload.counts
      : {};
    var sources = counts.sources && typeof counts.sources === 'object' ? counts.sources : {};
    var indexes = payload && payload.indexes && typeof payload.indexes === 'object'
      ? payload.indexes
      : {};
    var validation = counts.manticore_validation
      && typeof counts.manticore_validation === 'object'
      ? counts.manticore_validation
      : {};
    var maintenance = payload && payload.maintenance && typeof payload.maintenance === 'object'
      ? payload.maintenance
      : {};
    var lastRecordedJob = maintenance.last_recorded_job
      && typeof maintenance.last_recorded_job === 'object'
      ? maintenance.last_recorded_job
      : null;
    var reasons = Array.isArray(payload && payload.reasons) ? payload.reasons : [];
    var actions = Array.isArray(payload && payload.actions) ? payload.actions : [];
    return {
      status: normalizeHealthStatus(payload && payload.status),
      checkedAt: String(payload && payload.checked_at || ''),
      database: {
        mainBytes: nullableNumber(database.main_bytes),
        walBytes: nullableNumber(database.wal_bytes),
        shmBytes: nullableNumber(database.shm_bytes),
        pageCount: nullableNumber(database.page_count),
        freelistCount: nullableNumber(database.freelist_count),
        freelistBytes: nullableNumber(database.freelist_bytes),
        journalMode: String(database.journal_mode || ''),
        diskFreeBytes: nullableNumber(database.disk_free_bytes),
        compactionRequiredBytes: nullableNumber(database.compaction_required_bytes),
        canCompactSafely: database.can_compact_safely === true
          ? true
          : (database.can_compact_safely === false ? false : null)
      },
      counts: {
        messageCount: nullableNumber(counts.message_count),
        mediaGroupCount: nullableNumber(counts.media_group_count),
        manticoreOutboxPending: nullableNumber(counts.manticore_outbox_pending),
        manticoreOutboxFailed: nullableNumber(counts.manticore_outbox_failed),
        manticoreOutboxMaxAttempts: nullableNumber(counts.manticore_outbox_max_attempts),
        manticoreOutboxOldestFailedAt: String(counts.manticore_outbox_oldest_failed_at || ''),
        manticoreOutboxLastError: String(counts.manticore_outbox_last_error || ''),
        manticoreValidation: {
          lastValidatedAt: String(validation.last_validated_at || ''),
          sqliteDocumentCount: nullableNumber(validation.sqlite_document_count),
          manticoreDocumentCount: nullableNumber(validation.manticore_document_count),
          outboxPending: nullableNumber(validation.outbox_pending),
          lastError: String(validation.last_error || '')
        },
        sources: {
          mediaGroupCount: String(sources.media_group_count || ''),
          manticoreOutboxPending: String(sources.manticore_outbox_pending || ''),
          manticoreOutboxFailed: String(sources.manticore_outbox_failed || '')
        }
      },
      indexes: {
        manticoreReady: indexes.manticore_ready === true
      },
      maintenance: {
        lastRecordedJob: lastRecordedJob ? {
          jobType: String(lastRecordedJob.job_type || ''),
          status: String(lastRecordedJob.status || ''),
          updatedAt: String(lastRecordedJob.updated_at || '')
        } : null
      },
      reasons: reasons.map(function (item) {
        return {
          severity: normalizeHealthStatus(item && item.severity),
          message: String(item && item.message || '')
        };
      }).filter(function (item) {
        return !!item.message;
      }),
      actions: actions.map(function (item) {
        return String(item || '');
      }).filter(Boolean)
    };
  }

  function createStorageMetric(label, value, note) {
    var item = document.createElement('div');
    item.className = 'sync-storage-metric';
    item.appendChild(createTextElement('span', 'sync-storage-metric-label', label));
    item.appendChild(createTextElement('strong', 'sync-storage-metric-value', value));
    if (note) {
      item.appendChild(createTextElement('span', 'sync-storage-metric-note', note));
    }
    return item;
  }

  function renderStorageHealth(elements) {
    var health = syncState.storageHealth;
    if (!health) return;

    var database = health.database;
    var counts = health.counts;
    elements.storageHealthMetrics.textContent = '';
    elements.storageHealthReasons.textContent = '';
    elements.storageHealthActions.textContent = '';
    elements.storageHealthBanner.className = 'sync-health-banner is-' + health.status;

    if (health.status === 'critical') {
      elements.storageHealthBanner.textContent = '数据库容量或检索维护存在严重风险，应优先处理。';
    } else if (health.status === 'warning') {
      elements.storageHealthBanner.textContent = '数据库容量或检索维护出现预警信号，需要安排观察或维护。';
    } else {
      elements.storageHealthBanner.textContent = '数据库容量、WAL 和搜索索引当前未发现明确风险。';
    }

    [
      ['主库', formatByteSize(database.mainBytes), '页数 ' + formatMetricCount(database.pageCount)],
      ['WAL', formatByteSize(database.walBytes), database.journalMode || '未知日志模式'],
      ['SHM', formatByteSize(database.shmBytes), '共享内存 sidecar'],
      ['空闲页', formatByteSize(database.freelistBytes), formatMetricCount(database.freelistCount) + ' 页'],
      ['磁盘余量', formatByteSize(database.diskFreeBytes), database.canCompactSafely === false ? '不足以安全压缩' : '数据库所在磁盘'],
      ['消息', formatMetricCount(counts.messageCount), '来自群组消息汇总'],
      ['媒体组', formatMetricCount(counts.mediaGroupCount, counts.sources.mediaGroupCount), '媒体组索引'],
      ['Manticore 队列', formatMetricCount(counts.manticoreOutboxPending, counts.sources.manticoreOutboxPending), '等待后台同步'],
      ['Manticore 失败', formatMetricCount(counts.manticoreOutboxFailed, counts.sources.manticoreOutboxFailed), counts.manticoreOutboxLastError || '无失败任务'],
      ['索引文档', formatMetricCount(counts.manticoreValidation.manticoreDocumentCount), 'SQLite ' + formatMetricCount(counts.manticoreValidation.sqliteDocumentCount)],
      ['Manticore', health.indexes.manticoreReady ? '就绪' : '未就绪', '全文搜索索引']
    ].forEach(function (metric) {
      elements.storageHealthMetrics.appendChild(
        createStorageMetric(metric[0], metric[1], metric[2])
      );
    });

    if (!health.reasons.length) {
      elements.storageHealthReasons.appendChild(
        createTextElement('div', 'sync-health-item', '当前没有容量或索引维护异常。')
      );
    } else {
      health.reasons.forEach(function (item) {
        elements.storageHealthReasons.appendChild(
          createTextElement(
            'div',
            'sync-health-item is-' + item.severity,
            item.message
          )
        );
      });
    }

    health.actions.forEach(function (item) {
      elements.storageHealthActions.appendChild(
        createTextElement('div', 'sync-health-action', item)
      );
    });

    var maintenanceParts = [];
    if (counts.manticoreValidation.lastValidatedAt) {
      maintenanceParts.push(
        '最近索引校验 ' + formatDateTime(counts.manticoreValidation.lastValidatedAt)
      );
    }
    if (health.maintenance.lastRecordedJob) {
      maintenanceParts.push(
        '最近维护任务 ' + health.maintenance.lastRecordedJob.jobType
        + ' / ' + health.maintenance.lastRecordedJob.status
        + (health.maintenance.lastRecordedJob.updatedAt
          ? ' / ' + formatDateTime(health.maintenance.lastRecordedJob.updatedAt)
          : '')
      );
    }
    var statusText = maintenanceParts.length
      ? maintenanceParts.join('；')
      : '轻量状态：文件大小、SQLite 元数据和缓存/统计计数；不执行完整性校验。';
    elements.storageHealthStatus.textContent = statusText
      + (health.checkedAt ? '；状态读取 ' + formatDateTime(health.checkedAt) : '');
  }

  function formatDurationSeconds(value) {
    var seconds = Math.max(0, Number(value || 0));
    if (!seconds) return '0 秒';
    if (seconds < 60) return String(Math.round(seconds)) + ' 秒';
    if (seconds < 3600) return String(Math.round(seconds / 60)) + ' 分钟';
    return String(Math.round(seconds / 3600)) + ' 小时';
  }

  function normalizeCountList(items, keyName) {
    return (Array.isArray(items) ? items : []).map(function (item) {
      return {
        key: String(item && item[keyName] || ''),
        count: Number(item && item.count || 0)
      };
    }).filter(function (item) {
      return !!item.key;
    });
  }

  function normalizeScheduler(payload) {
    var scheduler = payload && payload.scheduler ? payload.scheduler : {};
    var model = scheduler.model || {};
    var recent = scheduler.recent || {};
    var failures = Array.isArray(scheduler.recent_failures) ? scheduler.recent_failures : [];
    return {
      enabled: scheduler.enabled === true,
      aiEnabled: scheduler.ai_enabled === true,
      aiShadow: scheduler.ai_shadow === true,
      aiAutoPromoteEnabled: scheduler.ai_auto_promote_enabled === true,
      effectiveModelMode: String(scheduler.effective_model_mode || ''),
      modelCanTakeOver: scheduler.model_can_take_over === true,
      pendingCount: Number(scheduler.pending_count || 0),
      dueCount: Number(scheduler.due_count || 0),
      inFlightCount: Number(scheduler.in_flight_count || 0),
      learningEventCount: Number(scheduler.learning_event_count || 0),
      outcomeSampleCount: Number(scheduler.outcome_sample_count || 0),
      avgQuietDelaySeconds: Number(scheduler.avg_quiet_delay_seconds || 0),
      nextDueAt: String(scheduler.next_due_at || ''),
      coalescedEventCount: Number(scheduler.coalesced_event_count || 0),
      membershipCounts: normalizeCountList(scheduler.membership_counts, 'scope'),
      statusCounts: normalizeCountList(scheduler.status_counts, 'status'),
      accounts: Array.isArray(scheduler.accounts) ? scheduler.accounts : [],
      accountCapacity: scheduler.account_capacity && typeof scheduler.account_capacity === 'object'
        ? scheduler.account_capacity
        : {},
      backpressure: scheduler.backpressure && typeof scheduler.backpressure === 'object'
        ? scheduler.backpressure
        : {},
      model: {
        backend: String(model.backend || ''),
        modelVersion: String(model.model_version || ''),
        trainedAt: String(model.trained_at || ''),
        sampleCount: Number(model.sample_count || 0),
        artifactPath: String(model.artifact_path || ''),
        state: model.state && typeof model.state === 'object' ? model.state : {},
        metrics: model.metrics && typeof model.metrics === 'object' ? model.metrics : {}
      },
      recent: {
        updateCount: Number(recent.update_count || 0),
        addedMessageCount: Number(recent.added_message_count || 0),
        avgWaitSeconds: Number(recent.avg_wait_seconds || 0),
        failureCount: Number(recent.failure_count || 0)
      },
      failures: failures.map(function (item) {
        return {
          chatId: Number(item && item.chat_id || 0),
          eventType: String(item && item.event_type || ''),
          reason: String(item && item.reason || ''),
          sourceAccount: String(item && item.source_account || ''),
          failureType: String(item && item.failure_type || ''),
          createdAt: String(item && item.created_at || '')
        };
      })
    };
  }

  function schedulerScopeLabel(scope) {
    var labels = {
      none_joined: '公开可探测',
      both_joined: '双账号已加入',
      single_joined_primary: '主账号已加入',
      single_joined_secondary: '第二账号已加入',
      unobservable: '不可观察',
      unknown: '未知'
    };
    return labels[String(scope || '')] || String(scope || '未知');
  }

  function schedulerStatusLabel(status) {
    var labels = {
      idle: '空闲',
      pending: '等待入库',
      updating: '正在入库',
      backoff: '冷却中',
      quarantined: '隔离',
      unobservable: '不可观察',
      deleted: '已删除'
    };
    return labels[String(status || '')] || String(status || '未知');
  }

  function createSchedulerStat(label, value) {
    var item = document.createElement('div');
    item.className = 'sync-scheduler-stat';
    item.appendChild(createTextElement('span', '', label));
    item.appendChild(createTextElement('strong', '', value));
    return item;
  }

  function accountLabel(key) {
    var labels = {
      primary: '主账号',
      secondary: '第二账号'
    };
    return labels[String(key || '')] || String(key || '账号');
  }

  function renderAccountGrid(elements, scheduler) {
    elements.accountGrid.textContent = '';
    var accounts = scheduler.accounts || [];
    if (!accounts.length) {
      elements.accountGrid.appendChild(
        createTextElement('div', 'sync-account-item', '暂无账号运行状态。')
      );
      return;
    }
    accounts.forEach(function (account) {
      var cooldownSeconds = Number(account && account.cooldown_seconds || 0);
      var item = document.createElement('div');
      item.className = 'sync-account-item' + (cooldownSeconds > 0 ? ' is-cooling' : '');
      item.appendChild(
        createTextElement(
          'strong',
          '',
          String(account && account.label || accountLabel(account && account.key))
        )
      );
      item.appendChild(
        createTextElement(
          'span',
          '',
          cooldownSeconds > 0
            ? '冷却中，剩余约 ' + formatDurationSeconds(cooldownSeconds)
            : (account && account.connected ? '监听已连接' : '可用于任务调度')
        )
      );
      if (account && account.last_error) {
        item.appendChild(createTextElement('span', '', '最近异常：' + String(account.last_error)));
      }
      elements.accountGrid.appendChild(item);
    });
  }

  function renderSyncScheduler(elements) {
    var scheduler = syncState.scheduler || normalizeScheduler({});
    elements.schedulerGrid.textContent = '';
    elements.accountGrid.textContent = '';
    elements.schedulerFailures.textContent = '';

    elements.schedulerStatus.textContent = scheduler.enabled
      ? '实时调度已启用，事件会先合并为等待入库任务，再按到期时间拉取。'
      : '智能调度未启用，当前仍使用兼容同步路径。';

    [
      ['等待入库', formatNumber(scheduler.pendingCount)],
      ['到期任务', formatNumber(scheduler.dueCount)],
      ['正在入库', formatNumber(scheduler.inFlightCount)],
      ['平均延迟', formatDurationSeconds(scheduler.avgQuietDelaySeconds)],
      ['合并事件', formatNumber(scheduler.coalescedEventCount)],
      ['24h 新增', formatNumber(scheduler.recent.addedMessageCount)]
    ].forEach(function (entry) {
      elements.schedulerGrid.appendChild(createSchedulerStat(entry[0], entry[1]));
    });
    renderAccountGrid(elements, scheduler);

    var scopeText = scheduler.membershipCounts.length
      ? scheduler.membershipCounts.map(function (item) {
        return schedulerScopeLabel(item.key) + ' ' + formatNumber(item.count);
      }).join('，')
      : '暂无状态样本';
    var statusText = scheduler.statusCounts.length
      ? scheduler.statusCounts.map(function (item) {
        return schedulerStatusLabel(item.key) + ' ' + formatNumber(item.count);
      }).join('，')
      : '暂无任务状态';
    var modelText = [
      '状态分布：' + scopeText,
      '任务状态：' + statusText,
      '模型模式：' + (scheduler.effectiveModelMode || (scheduler.aiEnabled ? '仅观察' : '已关闭'))
        + ' / ' + (scheduler.model.backend || 'none')
        + (scheduler.model.state.mode ? ' / ' + String(scheduler.model.state.mode) : '')
        + '，训练样本 ' + formatNumber(scheduler.outcomeSampleCount || scheduler.model.sampleCount)
        + '，学习事件 ' + formatNumber(scheduler.learningEventCount)
        + (scheduler.model.trainedAt ? '，训练 ' + formatDateTime(scheduler.model.trainedAt) : '')
    ];
    if (scheduler.aiEnabled) {
      var readyRuns = Number(scheduler.model.state.consecutive_ready_count || 0);
      var requiredReadyRuns = Number(scheduler.model.state.required_consecutive_ready_count || 0);
      var validationAccuracy = Number(scheduler.model.metrics.validation_delay_accuracy || 0);
      modelText.push(
        '就绪：' + (scheduler.model.state.ready === true ? '是' : '否')
          + (requiredReadyRuns > 0 ? '，连续达标 ' + formatNumber(readyRuns) + '/' + formatNumber(requiredReadyRuns) : '')
          + (validationAccuracy > 0 ? '，验证准确率 ' + String(Math.round(validationAccuracy * 100)) + '%' : '')
      );
    }
    if (scheduler.nextDueAt) {
      modelText.push('下一次到期：' + formatDateTime(scheduler.nextDueAt));
    }
    elements.schedulerModel.textContent = modelText.join('。');

    renderSchedulerChats(elements);

    if (!scheduler.failures.length) {
      elements.schedulerFailures.appendChild(
        createTextElement('div', 'sync-scheduler-failure', '最近没有调度失败。')
      );
      return;
    }
    scheduler.failures.slice(0, 5).forEach(function (failure) {
      elements.schedulerFailures.appendChild(
        createTextElement(
          'div',
          'sync-scheduler-failure',
          'Chat ' + String(failure.chatId)
            + '：' + (failure.failureType || 'failed')
            + (failure.sourceAccount ? ' / ' + failure.sourceAccount : '')
            + (failure.createdAt ? ' / ' + formatDateTime(failure.createdAt) : '')
        )
      );
    });
  }

  function renderSchedulerChats(elements) {
    elements.chatList.textContent = '';
    if (!syncState.chatItems.length) {
      elements.chatList.appendChild(
        createTextElement('div', 'sync-chat-row', '暂无调度群组列表。')
      );
      return;
    }
    syncState.chatItems.slice(0, 20).forEach(function (item) {
      var row = document.createElement('div');
      row.className = 'sync-chat-row';
      var main = document.createElement('div');
      main.className = 'sync-chat-main';
      main.appendChild(
        createTextElement(
          'strong',
          '',
          String(item.chatTitle || ('Chat ' + String(item.chatId || 0)))
        )
      );
      main.appendChild(
        createTextElement(
          'span',
          '',
          schedulerStatusLabel(item.status)
            + ' / ' + schedulerScopeLabel(item.membershipScope)
            + (item.dueAt ? ' / 到期 ' + formatDateTime(item.dueAt) : '')
        )
      );
      row.appendChild(main);
      var probeBtn = document.createElement('button');
      probeBtn.className = 'btn';
      probeBtn.type = 'button';
      probeBtn.textContent = '单群诊断';
      probeBtn.addEventListener('click', function () {
        triggerChatProbe(elements, item.chatId);
      });
      row.appendChild(probeBtn);
      elements.chatList.appendChild(row);
    });
  }

  function normalizeSchedulerChatsPayload(payload) {
    var items = Array.isArray(payload && payload.items) ? payload.items : [];
    return items.map(function (item) {
      return {
        chatId: Number(item && item.chat_id || 0),
        chatTitle: String(item && item.chat_title || ''),
        membershipScope: String(item && item.membership_scope || ''),
        status: String(item && item.status || ''),
        dueAt: String(item && item.due_at || ''),
        eventCount: Number(item && item.event_count || 0),
        sourceAccount: String(item && item.last_source_account || item && item.source_accounts || '')
      };
    }).filter(function (item) {
      return item.chatId > 0;
    });
  }

  async function loadSchedulerChats(elements) {
    var requestSeq = ++syncState.chatRequestSeq;
    syncState.chatListBusy = true;
    setBusy(elements, syncState.busy);
    elements.chatList.textContent = '正在读取调度群组...';
    try {
      var params = new URLSearchParams({
        limit: '20',
        offset: '0'
      });
      if (elements.membershipSelect.value) {
        params.set('membership', elements.membershipSelect.value);
      }
      if (elements.chatStatusSelect.value) {
        params.set('status', elements.chatStatusSelect.value);
      }
      syncState.chatItems = normalizeSchedulerChatsPayload(
        await fetchJSON('/api/admin/sync/chats?' + params.toString(), { timeoutMs: SYNC_STATS_TIMEOUT_MS })
      );
      if (requestSeq !== syncState.chatRequestSeq) return;
      renderSchedulerChats(elements);
    } catch (error) {
      if (requestSeq !== syncState.chatRequestSeq) return;
      elements.chatList.textContent = '读取调度群组失败：' + error.message;
    } finally {
      if (requestSeq !== syncState.chatRequestSeq) return;
      syncState.chatListBusy = false;
      setBusy(elements, syncState.busy);
    }
  }

  async function triggerChatProbe(elements, chatId) {
    if (!chatId) return;
    try {
      await fetchJSON('/api/admin/sync/chats/' + encodeURIComponent(String(chatId)) + '/probe', {
        method: 'POST',
        timeoutMs: 20000
      });
      await loadSyncStats(elements);
      await loadSchedulerChats(elements);
    } catch (error) {
      elements.schedulerStatus.textContent = '单群诊断失败：' + error.message;
    }
  }

  function normalizeSyncPayload(payload) {
    var windows = Array.isArray(payload && payload.windows) ? payload.windows : [];
    return {
      generatedAt: String(payload && payload.generated_at || ''),
      latestMessageCreatedAt: String(payload && payload.latest_message_created_at || ''),
      metricNote: String(payload && payload.metric_note || ''),
      health: normalizeSyncHealth(payload),
      scheduler: normalizeScheduler(payload),
      windows: windows.map(function (item) {
        return {
          window_key: String(item && item.window_key || ''),
          label: String(item && item.label || ''),
          seconds: Number(item && item.seconds || 0),
          is_live: item && item.is_live === true,
          message_count: Number(item && item.message_count || 0),
          chat_count: Number(item && item.chat_count || 0),
          oldest_created_at: String(item && item.oldest_created_at || ''),
          latest_created_at: String(item && item.latest_created_at || '')
        };
      }).filter(function (item) {
        return !!item.window_key;
      }),
      defaultWindowKey: String(payload && payload.default_window_key || '')
    };
  }

  function normalizeLiveMessagesPayload(payload) {
    var items = Array.isArray(payload && payload.items) ? payload.items : [];
    return {
      generatedAt: String(payload && payload.generated_at || ''),
      items: items.map(function (item) {
        return {
          pk: Number(item && item.pk || 0),
          chat_id: Number(item && item.chat_id || 0),
          chat_title: String(item && item.chat_title || ''),
          chat_username: String(item && item.chat_username || ''),
          chat_type: String(item && item.chat_type || ''),
          message_id: Number(item && item.message_id || 0),
          msg_type: String(item && item.msg_type || 'TEXT'),
          msg_date_text: String(item && item.msg_date_text || ''),
          created_at: String(item && item.created_at || ''),
          content_preview: String(item && item.content_preview || '')
        };
      }).filter(function (item) {
        return item.chat_id !== 0 && item.message_id > 0;
      })
    };
  }

  function buildOpenLink(item) {
    var params = new URLSearchParams({
      chat_id: String(item.chat_id || 0),
      message_id: String(item.message_id || 0)
    });
    return '/open/telegram?' + params.toString();
  }

  function renderLiveEmpty(elements, message) {
    elements.liveList.textContent = '';
    elements.liveList.appendChild(
      createTextElement('p', 'sync-live-empty', message || '暂无最近入库消息。')
    );
  }

  function buildLiveItem(item) {
    var article = document.createElement('article');
    article.className = 'sync-live-item';

    var head = document.createElement('div');
    head.className = 'sync-live-head';

    var chatWrap = document.createElement('div');
    chatWrap.className = 'sync-live-chat';
    chatWrap.appendChild(
      createTextElement(
        'strong',
        'sync-live-chat-title',
        item.chat_title || ('Chat ' + String(item.chat_id || ''))
      )
    );

    var metaParts = [
      'chat_id=' + String(item.chat_id || 0),
      'message_id=' + String(item.message_id || 0),
      item.msg_type || 'TEXT'
    ];
    if (item.msg_date_text) {
      metaParts.push('消息时间 ' + formatDateTime(item.msg_date_text));
    }
    chatWrap.appendChild(
      createTextElement('div', 'sync-live-chat-meta', metaParts.join(' · '))
    );
    head.appendChild(chatWrap);

    var openLink = document.createElement('a');
    openLink.className = 'sync-live-open';
    openLink.href = buildOpenLink(item);
    openLink.textContent = '打开消息';
    head.appendChild(openLink);

    article.appendChild(head);
    article.appendChild(
      createTextElement(
        'p',
        'sync-live-content',
        item.content_preview || '[' + String(item.msg_type || 'TEXT') + ']'
      )
    );

    var foot = document.createElement('div');
    foot.className = 'sync-live-foot';
    foot.appendChild(
      createTextElement('span', '', '入库时间：' + formatDateTime(item.created_at))
    );
    if (item.chat_username) {
      foot.appendChild(
        createTextElement('span', '', '@' + String(item.chat_username || '').replace(/^@+/, ''))
      );
    }
    article.appendChild(foot);
    return article;
  }

  function renderLiveMessages(elements) {
    elements.liveList.textContent = '';
    if (!syncState.liveItems.length) {
      renderLiveEmpty(elements, '当前暂无最近入库消息。');
      return;
    }
    syncState.liveItems.forEach(function (item) {
      elements.liveList.appendChild(buildLiveItem(item));
    });
  }

  function updateLiveSectionVisibility(elements, options) {
    var opts = options || {};
    var showLive = isLiveWindowSelected();
    elements.liveSection.hidden = !showLive;
    if (!showLive) {
      clearLivePollTimer();
      return;
    }
    renderLiveMessages(elements);
    if (opts.skipReload) {
      scheduleLivePoll(elements);
      return;
    }
    loadLiveMessages(elements, { silent: true });
  }

  async function loadLiveMessages(elements, options) {
    var opts = options || {};
    var requestSeq = nextLiveRequestSeq();
    var shouldRender = opts.forceRender === true || isLiveWindowSelected();

    if (!shouldRender) {
      clearLivePollTimer();
      return;
    }

    if (!opts.silent) {
      elements.liveStatus.textContent = '正在读取最近入库消息...';
    }

    try {
      var startedAt = Date.now();
      var payload = normalizeLiveMessagesPayload(
        await fetchJSON(
          '/api/admin/sync/messages?limit=' + encodeURIComponent(String(LIVE_MESSAGES_LIMIT)),
          { timeoutMs: LIVE_MESSAGES_TIMEOUT_MS }
        )
      );
      var elapsedMs = Date.now() - startedAt;
      signalApiOutcome('live', {
        success: true,
        durationMs: elapsedMs,
        slow: elapsedMs >= LIVE_MESSAGES_SLOW_MS
      });
      if (!isCurrentLiveRequest(requestSeq)) {
        return;
      }
      syncState.liveItems = payload.items;
      if (isLiveWindowSelected()) {
        renderLiveMessages(elements);
      }
      if (!payload.items.length) {
        elements.liveStatus.textContent = '实时模式下暂无最近入库消息。';
      } else {
        elements.liveStatus.textContent =
          '最近已入库 ' + formatNumber(payload.items.length) + ' 条消息，数据生成时间 ' + formatDateTime(payload.generatedAt) + '。';
      }
    } catch (error) {
      signalApiOutcome('live', {
        failed: !isTimeoutMessage(error && error.message),
        timeout: isTimeoutMessage(error && error.message),
        errorMessage: error && error.message
      });
      if (!isCurrentLiveRequest(requestSeq)) {
        return;
      }
      if (isLiveWindowSelected() || !opts.silent) {
        elements.liveStatus.textContent = '读取最近入库消息失败：' + error.message;
      }
    } finally {
      if (isCurrentLiveRequest(requestSeq)) {
        renderSyncHealth(elements);
        scheduleLivePoll(elements);
      }
    }
  }

  async function loadSyncStats(elements) {
    setBusy(elements, true);
    try {
      var startedAt = Date.now();
      var payload = normalizeSyncPayload(
        await fetchJSON('/api/admin/sync/stats', { timeoutMs: SYNC_STATS_TIMEOUT_MS })
      );
      var elapsedMs = Date.now() - startedAt;
      signalApiOutcome('stats', {
        success: true,
        durationMs: elapsedMs,
        slow: elapsedMs >= SYNC_STATS_SLOW_MS
      });
      syncState.windows = payload.windows;
      syncState.health = payload.health;
      syncState.scheduler = payload.scheduler;
      if (!syncState.windows.length) {
        syncState.selectedWindowKey = '';
      } else if (
        !syncState.selectedWindowKey
        || !syncState.windows.some(function (item) {
          return item.window_key === syncState.selectedWindowKey;
        })
      ) {
        syncState.selectedWindowKey = String(
          payload.defaultWindowKey
          || (syncState.windows[0] && syncState.windows[0].window_key)
          || ''
        );
      }

      renderWindowOptions(elements);
      renderSelectedWindow(elements);
      renderWindowCards(elements);
      renderSyncHealth(elements);
      renderSyncScheduler(elements);
      updateLiveSectionVisibility(elements, { skipReload: true });

      elements.metricNote.textContent = payload.metricNote
        ? payload.metricNote + ' 数据生成时间：' + formatDateTime(payload.generatedAt) + '。'
        : '数据生成时间：' + formatDateTime(payload.generatedAt) + '。';

      if (!payload.windows.length) {
        elements.status.textContent = '暂无消息同步统计。';
      } else {
        elements.latestCreatedAtValue.textContent = formatDateTime(payload.latestMessageCreatedAt);
      }
      return true;
    } catch (error) {
      signalApiOutcome('stats', {
        failed: !isTimeoutMessage(error && error.message),
        timeout: isTimeoutMessage(error && error.message),
        errorMessage: error && error.message
      });
      elements.status.textContent = '读取消息同步统计失败：' + error.message;
      renderSyncHealth(elements);
      renderSyncScheduler(elements);
      clearLivePollTimer();
      return false;
    } finally {
      setBusy(elements, false);
    }
  }

  async function loadStorageHealth(elements) {
    var requestSeq = ++syncState.storageRequestSeq;
    elements.storageHealthStatus.textContent = '正在读取数据库容量状态...';
    try {
      var storageHealth = normalizeStorageHealth(
        await fetchJSON('/api/admin/storage-health', {
          timeoutMs: STORAGE_HEALTH_TIMEOUT_MS
        })
      );
      if (requestSeq !== syncState.storageRequestSeq) return;
      syncState.storageHealth = storageHealth;
      renderStorageHealth(elements);
    } catch (error) {
      if (requestSeq !== syncState.storageRequestSeq) return;
      elements.storageHealthBanner.className = 'sync-health-banner is-warning';
      elements.storageHealthBanner.textContent = '读取数据库容量状态失败。';
      elements.storageHealthStatus.textContent = '读取数据库容量状态失败：' + error.message;
      elements.storageHealthMetrics.textContent = '';
      elements.storageHealthReasons.textContent = '';
      elements.storageHealthActions.textContent = '';
    }
  }

  async function loadSyncDashboard(elements) {
    var statsLoaded = await loadSyncStats(elements);
    if (!statsLoaded) {
      return;
    }
    loadStorageHealth(elements);
    if (!syncState.liveItems.length || isLiveWindowSelected()) {
      loadLiveMessages(elements, {
        silent: false,
        forceRender: true
      });
    }
    loadSchedulerChats(elements);
  }

  function primeSyncDashboard(elements) {
    loadSyncDashboard(elements).catch(function (error) {
      elements.status.textContent = '读取消息同步统计失败：' + error.message;
      renderSyncHealth(elements);
      renderSyncScheduler(elements);
    });
  }

  async function triggerSyncDiagnosis(elements) {
    elements.diagnoseResult.textContent = '正在执行即时诊断...';
    setElementDisabled(elements.diagnoseBtn, true);
    try {
      var payload = await fetchJSON('/api/admin/sync/diagnose', {
        method: 'POST',
        timeoutMs: 20000
      });
      var items = Array.isArray(payload && payload.items) ? payload.items : [];
      var parts = [String(payload && payload.message || '诊断完成')];
      if (items.length) {
        parts.push(items.map(function (item) {
          var title = String(item.chat_title || ('Chat ' + String(item.chat_id || 0)));
          var status = String(item.status || 'unknown');
          return title + '：' + status;
        }).join('；'));
      }
      elements.diagnoseResult.textContent = parts.join(' ');
      await loadSyncStats(elements);
      if (isLiveWindowSelected()) {
        loadLiveMessages(elements, { silent: false, forceRender: true });
      }
    } catch (error) {
      elements.diagnoseResult.textContent = '执行即时诊断失败：' + error.message;
      renderSyncHealth(elements);
    } finally {
      setElementDisabled(elements.diagnoseBtn, syncState.busy);
    }
  }

  async function resetSyncModel(elements) {
    if (!window.confirm('确认重置同步模型？实时调度会继续使用启发式策略，模型重新积累样本后再训练。')) {
      return;
    }
    elements.schedulerStatus.textContent = '正在重置模型...';
    setElementDisabled(elements.resetModelBtn, true);
    try {
      await fetchJSON('/api/admin/sync/model/reset', {
        method: 'POST',
        timeoutMs: 15000
      });
      elements.schedulerStatus.textContent = '模型已重置，当前调度使用启发式策略。';
      await loadSyncStats(elements);
    } catch (error) {
      elements.schedulerStatus.textContent = '重置模型失败：' + error.message;
    } finally {
      setElementDisabled(elements.resetModelBtn, syncState.busy);
    }
  }

  async function fetchJSON(url, options) {
    return sharedFetchJSON(url, Object.assign({}, options || {}, {
      onUnauthorized: sessionController.handleUnauthorizedResponse
    }));
  }

  var sessionController = shared.createAdminSessionController({
    afterAuth: async function (elements, context) {
      primeSyncDashboard(elements);
    },
    getElements: getElements,
    getPageElement: function () {
      return document.getElementById('admin-sync-page');
    }
  });
})();
