(function () {
  'use strict';

  var shared = window.AdminManageShared;
  var appendLog = shared.appendLog;
  var clearLogs = shared.clearLogs;
  var ensurePlaceholder = shared.ensurePlaceholder;
  var sharedFetchJSON = shared.fetchJSON;
  var getCreatedJobId = shared.getCreatedJobId;
  var setDialogOpenState = shared.setDialogOpenState;
  var setElementDisabled = shared.setElementDisabled;
  var setPageInteractionState = shared.setPageInteractionState;
  var syncClearLogsButtonVisibility = shared.syncClearLogsButtonVisibility;
  var trapFocusWithin = shared.trapFocusWithin;

  var JOB_POLL_INTERVAL_MS = 3000;
  var JOB_POLL_RETRY_MAX_COUNT = 20;
  var JOB_POLL_RETRY_BASE_MS = 3000;
  var LIST_INITIAL_RENDER_BATCH_SIZE = 40;
  var LIST_RENDER_BATCH_SIZE = 40;
  var LIST_RENDER_STATUS_INTERVAL_MS = 250;

  var listRenderTokens = {
    channels: 0,
    missing: 0,
    absent: 0,
    restricted: 0
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
    errorMessage: ''
  };

  var authState = {
    authenticated: false,
    logoutTimer: null
  };

  var restrictedState = {
    items: [],
    filterValue: '__all__'
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
      scanRestrictedBtn: document.getElementById('admin-scan-restricted-btn'),
      refreshRestrictedBtn: document.getElementById('admin-refresh-restricted-btn'),
      restrictedFilterSelect: document.getElementById('admin-restricted-filter-select'),
      restrictedListToggleBtn: document.getElementById('admin-restricted-list-toggle-btn'),
      restrictedStatus: document.getElementById('admin-restricted-status'),
      restrictedList: document.getElementById('admin-restricted-list'),
      logContainer: document.getElementById('admin-channel-log-container'),
      clearLogsBtn: document.getElementById('admin-clear-channel-logs-btn'),
      loginDialog: document.getElementById('admin-login-dialog'),
      loginStatus: document.getElementById('admin-login-status'),
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
      'scanRestrictedBtn',
      'refreshRestrictedBtn',
      'restrictedFilterSelect',
      'restrictedListToggleBtn',
      'restrictedStatus',
      'restrictedList',
      'logContainer',
      'clearLogsBtn',
      'loginDialog',
      'loginStatus',
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
    elements.refreshRestrictedBtn.addEventListener('click', function () {
      loadRestrictedChannels(elements);
    });
    elements.restrictedFilterSelect.addEventListener('change', function () {
      restrictedState.filterValue = elements.restrictedFilterSelect.value || '__all__';
      renderRestrictedChannels(elements);
    });
    elements.restrictedListToggleBtn.addEventListener('click', function () {
      toggleListArea(elements.restrictedListToggleBtn, elements.restrictedList);
    });
    elements.scanRestrictedBtn.addEventListener('click', function () {
      handleScanRestrictedClick(elements);
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

  async function checkAuth(elements) {
    try {
      var data = await fetchJSON('/api/admin/auth/check');
      if (data.authenticated) {
        authState.authenticated = true;
        closeLoginDialog(elements);
        setupAutoLogout(elements, data.remaining);
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
        appendLog(elements, '认证成功，已进入频道管理');
        await loadInitialData(elements);
      }
    } catch (error) {
      setLoginStatus(elements, '认证失败：' + error.message);
      elements.passwordInput.focus();
    } finally {
      setElementDisabled(elements.loginConfirmBtn, false);
    }
  }

  function setupAutoLogout(elements, seconds) {
    if (authState.logoutTimer) window.clearTimeout(authState.logoutTimer);
    if (seconds <= 0) return;
    authState.logoutTimer = window.setTimeout(function () {
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
    setPageInteractionState(document.getElementById('admin-channels-page'), false);
  }

  function closeLoginDialog(elements) {
    setLoginStatus(elements, '');
    setDialogOpenState(elements.loginDialog, false, { skipFocusRestore: true });
    setPageInteractionState(document.getElementById('admin-channels-page'), true);
  }

  async function loadInitialData(elements) {
    await loadChannels(elements);
    await loadMissingChannels(elements);
    await loadAbsentChannels(elements);
    await loadRestrictedChannels(elements);
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

  function itemLastMessageAt(item) {
    return (item && item.last_message_at) || (item && item.last_seen_at) || '';
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

  function setListRenderBusy(container, isBusy) {
    if (!container || typeof container.setAttribute !== 'function') return;
    container.setAttribute('aria-busy', isBusy ? 'true' : 'false');
  }

  function nextListRenderToken(key) {
    listRenderTokens[key] = (listRenderTokens[key] || 0) + 1;
    return listRenderTokens[key];
  }

  function stopListRendering(key, container) {
    nextListRenderToken(key);
    setListRenderBusy(container, false);
  }

  function isListRenderCurrent(key, token) {
    return listRenderTokens[key] === token;
  }

  function scheduleListRender(callback) {
    if (typeof window.requestIdleCallback === 'function') {
      window.requestIdleCallback(callback, { timeout: 120 });
      return;
    }
    window.setTimeout(callback, 16);
  }

  function renderItemsInBatches(options) {
    var key = options.key;
    var container = options.container;
    var items = Array.isArray(options.items) ? options.items : [];
    var createItem = options.createItem;
    var statusElement = options.statusElement;
    var progressText = options.progressText;
    var doneText = options.doneText;
    var token = nextListRenderToken(key);
    var total = items.length;
    var index = 0;
    var lastStatusUpdateAt = 0;

    container.textContent = '';
    setListRenderBusy(container, true);

    function setProgressStatus(force) {
      if (!statusElement || typeof progressText !== 'function') return;
      var now = Date.now();
      if (!force && index < total && now - lastStatusUpdateAt < LIST_RENDER_STATUS_INTERVAL_MS) {
        return;
      }
      lastStatusUpdateAt = now;
      statusElement.textContent = progressText(index, total);
    }

    function appendBatch() {
      if (!isListRenderCurrent(key, token)) return;

      var fragment = document.createDocumentFragment();
      var batchSize = index === 0 ? LIST_INITIAL_RENDER_BATCH_SIZE : LIST_RENDER_BATCH_SIZE;
      var end = Math.min(index + batchSize, total);
      while (index < end) {
        fragment.appendChild(createItem(items[index], index));
        index += 1;
      }
      container.appendChild(fragment);

      if (index < total) {
        setProgressStatus(false);
        scheduleListRender(appendBatch);
        return;
      }

      setListRenderBusy(container, false);
      if (statusElement && typeof doneText === 'function') {
        statusElement.textContent = doneText(total);
      }
    }

    appendBatch();
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

  function getChannelActionLabel(item) {
    return String(
      (item && item.chat_title)
      || (item && item.title)
      || (item && item.chat_username && ('@' + item.chat_username))
      || (item && item.chat_id && ('Chat ' + item.chat_id))
      || '该群组或频道'
    ).trim();
  }

  function createChannelActions(item, elements, options) {
    var actions = document.createElement('div');
    var actionOptions = options || {};
    var channelLabel = getChannelActionLabel(item);
    actions.className = 'channel-actions';

    if (item.telegram_app_link) {
      var appLink = document.createElement('a');
      appLink.href = item.telegram_app_link;
      appLink.textContent = '打开客户端';
      appLink.setAttribute('aria-label', '使用 Telegram 客户端打开 ' + channelLabel);
      actions.appendChild(appLink);
    }

    if (item.telegram_web_link) {
      var webLink = document.createElement('a');
      webLink.href = item.telegram_web_link;
      webLink.target = '_blank';
      webLink.rel = 'noopener noreferrer';
      webLink.textContent = '网页入口';
      webLink.setAttribute('aria-label', '在新标签页打开 ' + channelLabel + ' 的 Telegram 网页入口');
      actions.appendChild(webLink);
    }

    var copyBtn = document.createElement('button');
    copyBtn.type = 'button';
    copyBtn.textContent = '复制信息';
    copyBtn.setAttribute('aria-label', '复制 ' + channelLabel + ' 的群组或频道信息');
    copyBtn.addEventListener('click', function () {
      copyChannelInfo(item, elements);
    });
    actions.appendChild(copyBtn);

    if (actionOptions.allowDelete) {
      var deleteBtn = document.createElement('button');
      deleteBtn.type = 'button';
      deleteBtn.className = 'danger-action';
      deleteBtn.textContent = '删除数据';
      deleteBtn.setAttribute('aria-label', '从数据库删除 ' + channelLabel + ' 的全部数据');
      deleteBtn.addEventListener('click', function () {
        handleDeleteChannelData(elements, item);
      });
      actions.appendChild(deleteBtn);
    }
    return actions;
  }

  function renderChannels(elements, channels) {
    stopListRendering('channels', elements.channelList);
    elements.channelList.textContent = '';
    if (!Array.isArray(channels) || channels.length === 0) {
      var empty = document.createElement('div');
      empty.className = 'empty-box';
      empty.textContent = '暂无已入库群组或频道。';
      elements.channelList.appendChild(empty);
      elements.channelCount.textContent = '共 0 个群组/频道。';
      return;
    }

    renderItemsInBatches({
      key: 'channels',
      container: elements.channelList,
      items: channels,
      statusElement: elements.channelCount,
      progressText: function (visible, total) {
        return '正在显示 ' + visible + '/' + total + ' 个群组/频道...';
      },
      doneText: function (total) {
        return '共 ' + total + ' 个群组/频道。';
      },
      createItem: function (channel) {
        var parts = [];
        if (channel.chat_username) parts.push('@' + channel.chat_username);
        if (channel.chat_type) parts.push(channel.chat_type);
        return createChannelRecordItem({
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
          actions: createChannelActions(channel, elements, { allowDelete: true }),
          note: channel.has_public_link
            ? ''
            : '私有群组通常没有稳定网页入口；客户端链接不可用时可复制信息后在 Telegram 中定位。'
        });
      }
    });
  }

  async function loadChannels(elements) {
    stopListRendering('channels', elements.channelList);
    elements.channelCount.textContent = '正在读取列表...';
    try {
      var data = await fetchJSON(
        '/api/admin/channels?sort=' + encodeURIComponent(elements.sortSelect.value)
      );
      if (!data.ok) throw new Error(data.error || '读取失败');
      renderChannels(elements, data.channels || []);
    } catch (error) {
      stopListRendering('channels', elements.channelList);
      elements.channelCount.textContent = '读取列表失败：' + error.message;
    }
  }

  function renderMissingChannels(elements, items) {
    stopListRendering('missing', elements.missingList);
    elements.missingList.textContent = '';
    if (!Array.isArray(items) || items.length === 0) {
      var empty = document.createElement('div');
      empty.className = 'empty-box';
      empty.textContent = '暂无未入库扫描结果。';
      elements.missingList.appendChild(empty);
      elements.missingStatus.textContent = '暂无未入库扫描结果，可点击“扫描当前账号”。';
      return;
    }

    renderItemsInBatches({
      key: 'missing',
      container: elements.missingList,
      items: items,
      statusElement: elements.missingStatus,
      progressText: function (visible, total) {
        return '正在显示 ' + visible + '/' + total + ' 个未入库扫描结果...';
      },
      doneText: function (total) {
        return '发现 ' + total + ' 个已加入但未入库的群组/频道。';
      },
      createItem: function (item) {
        var metaParts = [];
        if (item.chat_username) metaParts.push('@' + item.chat_username);
        if (item.chat_type) metaParts.push(item.chat_type);

        return createChannelRecordItem({
          title: item.chat_title || ('Chat ' + item.chat_id),
          subtitle: metaParts.join(' | '),
          metrics: [
            { label: 'chat_id', value: String(item.chat_id) },
            { label: '最后消息', value: formatDateTime(itemLastMessageAt(item)) }
          ],
          meta: [
            { label: 'chat_id', value: String(item.chat_id) },
            { label: '用户名', value: item.chat_username ? '@' + item.chat_username : '' },
            { label: '类型', value: item.chat_type || '' },
            { label: '扫描', value: formatDateTime(item.scanned_at) },
          ],
          actions: createChannelActions(item, elements),
          note: item.has_public_link
            ? ''
            : '私有群组通常没有稳定网页入口；客户端链接不可用时可复制信息后在 Telegram 中定位。'
        });
      }
    });
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
    stopListRendering('missing', elements.missingList);
    elements.missingStatus.textContent = '正在读取扫描结果...';
    try {
      var data = await fetchJSON('/api/admin/channels/missing');
      if (!data.ok) throw new Error(data.error || '读取失败');
      renderMissingChannels(elements, data.items || []);
    } catch (error) {
      stopListRendering('missing', elements.missingList);
      elements.missingStatus.textContent = '读取扫描结果失败：' + error.message;
    }
  }

  function renderAbsentChannels(elements, items) {
    stopListRendering('absent', elements.absentList);
    elements.absentList.textContent = '';
    if (!Array.isArray(items) || items.length === 0) {
      var empty = document.createElement('div');
      empty.className = 'empty-box';
      empty.textContent = '暂无账号外数据库扫描结果。';
      elements.absentList.appendChild(empty);
      elements.absentStatus.textContent = '暂无账号外数据库扫描结果，可点击“扫描账号外数据”。';
      return;
    }

    renderItemsInBatches({
      key: 'absent',
      container: elements.absentList,
      items: items,
      statusElement: elements.absentStatus,
      progressText: function (visible, total) {
        return '正在显示 ' + visible + '/' + total + ' 个账号外数据库扫描结果...';
      },
      doneText: function (total) {
        return '发现 ' + total + ' 个数据库中存在但账号未加入或不可用的群组/频道。';
      },
      createItem: function (item) {
        var metaParts = [];
        if (item.chat_username) metaParts.push('@' + item.chat_username);
        if (item.chat_type) metaParts.push(item.chat_type);

        return createChannelRecordItem({
          title: item.chat_title || ('Chat ' + item.chat_id),
          subtitle: metaParts.join(' | '),
          metrics: [
            { label: '消息数', value: formatNumber(item.message_count) },
            { label: '最后消息', value: formatDateTime(itemLastMessageAt(item)) },
            { label: '扫描', value: formatDateTime(item.scanned_at) }
          ],
          meta: [
            { label: 'chat_id', value: String(item.chat_id) },
            { label: '用户名', value: item.chat_username ? '@' + item.chat_username : '' },
            { label: '类型', value: item.chat_type || '' },
            { label: '原因', value: item.scan_reason || '账号未加入' },
            { label: '入库更新', value: formatDateTime(item.last_seen_at) },
          ],
          actions: createChannelActions(item, elements, { allowDelete: true }),
          note: item.scan_reason && item.scan_reason !== '账号未加入'
            ? item.scan_reason
            : (
                item.has_public_link
                  ? ''
                  : '私有群组通常没有稳定网页入口；删除前可复制信息核对目标。'
              )
        });
      }
    });
  }

  async function loadAbsentChannels(elements) {
    stopListRendering('absent', elements.absentList);
    elements.absentStatus.textContent = '正在读取扫描结果...';
    try {
      var data = await fetchJSON('/api/admin/channels/absent');
      if (!data.ok) throw new Error(data.error || '读取失败');
      renderAbsentChannels(elements, data.items || []);
    } catch (error) {
      stopListRendering('absent', elements.absentList);
      elements.absentStatus.textContent = '读取扫描结果失败：' + error.message;
    }
  }

  function buildRestrictedNote(item) {
    var parts = [];
    if (item.restriction_text) parts.push(item.restriction_text);
    if (item.restriction_platforms) parts.push('平台：' + item.restriction_platforms);
    if (item.restriction_reasons) parts.push('原因：' + item.restriction_reasons);
    if (item.risk_flags) parts.push('标记：' + item.risk_flags);
    return parts.join(' | ');
  }

  function splitRestrictionTokens(value) {
    var text = String(value || '').trim();
    if (!text) return [''];
    return text.split(/[、,，;；|/]+/).map(function (part) {
      return part.trim();
    }).filter(Boolean);
  }

  function buildRestrictionFilterKey(platform, reason) {
    return String(platform || '').trim().toLowerCase() + '\u0001' + String(reason || '').trim().toLowerCase();
  }

  function displayRestrictionPlatform(platform) {
    var raw = String(platform || '').trim();
    var normalized = raw.toLowerCase();
    if (!normalized) return '未标明平台';
    if (normalized === 'all') return '全部平台';
    if (normalized === 'ios') return 'iOS/苹果';
    if (normalized === 'apple') return 'Apple/苹果';
    return raw;
  }

  function displayRestrictionReason(reason) {
    var raw = String(reason || '').trim();
    var normalized = raw.toLowerCase();
    if (!normalized) return '未标明原因';
    if (normalized === 'porn') return '色情';
    if (normalized === 'terms' || normalized === 'tos') return '违反条款';
    if (normalized === 'copyright') return '版权';
    return raw;
  }

  function buildRestrictionFilterLabel(platform, reason) {
    return displayRestrictionPlatform(platform) + ' / ' + displayRestrictionReason(reason);
  }

  function getRestrictionFilterPairs(item) {
    var platforms = splitRestrictionTokens(item && item.restriction_platforms);
    var reasons = splitRestrictionTokens(item && item.restriction_reasons);
    var pairs = [];
    platforms.forEach(function (platform) {
      reasons.forEach(function (reason) {
        pairs.push({
          key: buildRestrictionFilterKey(platform, reason),
          label: buildRestrictionFilterLabel(platform, reason)
        });
      });
    });
    return pairs.length > 0
      ? pairs
      : [{ key: buildRestrictionFilterKey('', ''), label: buildRestrictionFilterLabel('', '') }];
  }

  function updateRestrictedFilterOptions(elements) {
    var currentValue = restrictedState.filterValue || '__all__';
    var countsByKey = {};
    var labelsByKey = {};

    restrictedState.items.forEach(function (item) {
      var seenForItem = {};
      getRestrictionFilterPairs(item).forEach(function (pair) {
        if (seenForItem[pair.key]) return;
        seenForItem[pair.key] = true;
        countsByKey[pair.key] = (countsByKey[pair.key] || 0) + 1;
        labelsByKey[pair.key] = pair.label;
      });
    });

    elements.restrictedFilterSelect.textContent = '';
    var allOption = document.createElement('option');
    allOption.value = '__all__';
    allOption.textContent = '全部类型（' + restrictedState.items.length + '）';
    elements.restrictedFilterSelect.appendChild(allOption);

    Object.keys(countsByKey).sort(function (a, b) {
      var countDelta = countsByKey[b] - countsByKey[a];
      if (countDelta !== 0) return countDelta;
      return labelsByKey[a].localeCompare(labelsByKey[b]);
    }).forEach(function (key) {
      var option = document.createElement('option');
      option.value = key;
      option.textContent = labelsByKey[key] + '（' + countsByKey[key] + '）';
      elements.restrictedFilterSelect.appendChild(option);
    });

    if (countsByKey[currentValue] || currentValue === '__all__') {
      restrictedState.filterValue = currentValue;
    } else {
      restrictedState.filterValue = '__all__';
    }
    elements.restrictedFilterSelect.value = restrictedState.filterValue;
  }

  function filterRestrictedItems(items) {
    var filterValue = restrictedState.filterValue || '__all__';
    if (filterValue === '__all__') return items;
    return items.filter(function (item) {
      return getRestrictionFilterPairs(item).some(function (pair) {
        return pair.key === filterValue;
      });
    });
  }

  function renderRestrictedChannels(elements) {
    var items = filterRestrictedItems(restrictedState.items);
    stopListRendering('restricted', elements.restrictedList);
    elements.restrictedList.textContent = '';
    if (!Array.isArray(restrictedState.items) || restrictedState.items.length === 0) {
      var empty = document.createElement('div');
      empty.className = 'empty-box';
      empty.textContent = '暂无内容限制/风险标记扫描结果。';
      elements.restrictedList.appendChild(empty);
      elements.restrictedStatus.textContent = '暂无内容限制/风险标记扫描结果，可点击“扫描限制标记”。';
      return;
    }
    if (items.length === 0) {
      var noMatch = document.createElement('div');
      noMatch.className = 'empty-box';
      noMatch.textContent = '当前筛选条件下没有匹配结果。';
      elements.restrictedList.appendChild(noMatch);
      elements.restrictedStatus.textContent = '当前类型 0 个，共 ' + restrictedState.items.length + ' 个内容限制/风险标记结果。';
      return;
    }

    renderItemsInBatches({
      key: 'restricted',
      container: elements.restrictedList,
      items: items,
      statusElement: elements.restrictedStatus,
      progressText: function (visible, total) {
        if ((restrictedState.filterValue || '__all__') === '__all__') {
          return '正在显示 ' + visible + '/' + total + ' 个内容限制/风险标记结果...';
        }
        return '正在显示当前类型 ' + visible + '/' + total + ' 个，共 '
          + restrictedState.items.length
          + ' 个内容限制/风险标记结果...';
      },
      doneText: function (total) {
        if ((restrictedState.filterValue || '__all__') === '__all__') {
          return '发现 ' + total + ' 个带 Telegram 内容限制/风险标记的群组/频道。';
        }
        return '当前类型 ' + total + ' 个，共 '
          + restrictedState.items.length
          + ' 个内容限制/风险标记结果。';
      },
      createItem: function (item) {
        var metaParts = [];
        if (item.chat_username) metaParts.push('@' + item.chat_username);
        if (item.chat_type) metaParts.push(item.chat_type);
        if (item.risk_flags) metaParts.push(item.risk_flags);

        return createChannelRecordItem({
          title: item.chat_title || ('Chat ' + item.chat_id),
          subtitle: metaParts.join(' | '),
          metrics: [
            { label: 'chat_id', value: String(item.chat_id) },
            { label: '最后消息', value: formatDateTime(itemLastMessageAt(item)) }
          ],
          meta: [
            { label: 'chat_id', value: String(item.chat_id) },
            { label: '用户名', value: item.chat_username ? '@' + item.chat_username : '' },
            { label: '类型', value: item.chat_type || '' },
            { label: '平台', value: item.restriction_platforms || '' },
            { label: '原因', value: item.restriction_reasons || '' },
            { label: '标记', value: item.risk_flags || '' },
            { label: '扫描', value: formatDateTime(item.scanned_at) },
          ],
          actions: createChannelActions(item, elements, { allowDelete: true }),
          note: buildRestrictedNote(item)
        });
      }
    });
  }

  async function loadRestrictedChannels(elements) {
    stopListRendering('restricted', elements.restrictedList);
    elements.restrictedStatus.textContent = '正在读取扫描结果...';
    try {
      var data = await fetchJSON('/api/admin/channels/restricted');
      if (!data.ok) throw new Error(data.error || '读取失败');
      restrictedState.items = Array.isArray(data.items) ? data.items : [];
      updateRestrictedFilterOptions(elements);
      renderRestrictedChannels(elements);
    } catch (error) {
      stopListRendering('restricted', elements.restrictedList);
      elements.restrictedStatus.textContent = '读取扫描结果失败：' + error.message;
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
    if (!window.confirm('确认扫描数据库中存在但当前账号未加入或不可用的群组或频道？')) {
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

  async function handleScanRestrictedClick(elements) {
    if (!window.confirm('确认扫描当前 Telegram 账号中带内容限制或风险标记的群组或频道？')) {
      appendLog(elements, '已取消扫描');
      return;
    }
    try {
      var payload = await fetchJSON('/api/admin/channels/restricted/scan', {
        method: 'POST'
      });
      var jobId = getCreatedJobId(payload);
      appendLog(elements, '扫描任务已创建：' + jobId);
      startJobPolling(elements, jobId, {
        doneMessage: '扫描任务执行完成',
        errorMessage: '扫描任务执行失败，请检查日志',
        onDone: function () {
          return loadRestrictedChannels(elements);
        }
      });
    } catch (error) {
      appendLog(elements, '创建扫描任务失败：' + error.message);
    }
  }

  async function handleDeleteChannelData(elements, item) {
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
          await loadRestrictedChannels(elements);
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
    setElementDisabled(elements.scanRestrictedBtn, disabled);
    setElementDisabled(elements.refreshRestrictedBtn, disabled);
    setElementDisabled(elements.restrictedFilterSelect, disabled);
    setElementDisabled(elements.restrictedListToggleBtn, disabled);
    if (elements.logContainer && typeof elements.logContainer.setAttribute === 'function') {
      elements.logContainer.setAttribute('aria-busy', disabled ? 'true' : 'false');
    }
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
})();
