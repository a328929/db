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
  var formatDateTime = shared.formatDateTime;
  var formatNumber = shared.formatNumber;

  var JOB_POLL_INTERVAL_MS = 3000;
  var JOB_POLL_RETRY_MAX_COUNT = 20;
  var JOB_POLL_RETRY_BASE_MS = 3000;
  var LIST_INITIAL_RENDER_BATCH_SIZE = 40;
  var LIST_RENDER_BATCH_SIZE = 40;
  var LIST_RENDER_STATUS_INTERVAL_MS = 250;

  var listRenderTokens = {
    channels: 0,
    missing: 0,
    restricted: 0
  };
  var listRequestTokens = {
    channels: 0,
    missing: 0,
    restricted: 0
  };

  var channelState = {
    items: [],
    membershipFilter: '__all__'
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

  var restrictedState = {
    items: [],
    filterValue: '__all__'
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
      sortSelect: document.getElementById('admin-channel-sort-select'),
      membershipFilterSelect: document.getElementById('admin-channel-membership-filter'),
      refreshChannelsBtn: document.getElementById('admin-channel-refresh-btn'),
      channelListToggleBtn: document.getElementById('admin-channel-list-toggle-btn'),
      channelCount: document.getElementById('admin-channel-count'),
      channelList: document.getElementById('admin-channel-list'),
      scanMissingBtn: document.getElementById('admin-scan-missing-btn'),
      refreshMissingBtn: document.getElementById('admin-refresh-missing-btn'),
      missingListToggleBtn: document.getElementById('admin-missing-list-toggle-btn'),
      missingStatus: document.getElementById('admin-missing-status'),
      missingList: document.getElementById('admin-missing-list'),
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
      'membershipFilterSelect',
      'refreshChannelsBtn',
      'channelListToggleBtn',
      'channelCount',
      'channelList',
      'scanMissingBtn',
      'refreshMissingBtn',
      'missingListToggleBtn',
      'missingStatus',
      'missingList',
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
    elements.sortSelect.addEventListener('change', function () {
      loadChannels(elements);
    });
    elements.membershipFilterSelect.addEventListener('change', function () {
      channelState.membershipFilter = elements.membershipFilterSelect.value || '__all__';
      renderChannels(elements, channelState.items);
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

  async function loadInitialData(elements) {
    await loadChannels(elements);
    await loadMissingChannels(elements);
    await loadRestrictedChannels(elements);
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

  function nextListRequestToken(key) {
    listRequestTokens[key] = (listRequestTokens[key] || 0) + 1;
    return listRequestTokens[key];
  }

  function isListRequestCurrent(key, token) {
    return listRequestTokens[key] === token;
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
    var getGroup = options.getGroup;
    var createGroupHeader = options.createGroupHeader;
    var token = nextListRenderToken(key);
    var total = items.length;
    var index = 0;
    var lastStatusUpdateAt = 0;
    var previousGroup;

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
        var item = items[index];
        var group = typeof getGroup === 'function' ? getGroup(item, index) : undefined;
        if (
          typeof createGroupHeader === 'function'
          && (index === 0 || group !== previousGroup)
        ) {
          fragment.appendChild(createGroupHeader(group));
        }
        previousGroup = group;
        fragment.appendChild(createItem(item, index));
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

  function schedulerMembershipLabel(scope) {
    var labels = {
      none_joined: 'A 未加入',
      both_joined: 'B 双账号',
      single_joined_primary: 'C 主账号',
      single_joined_secondary: 'C 第二账号',
      unobservable: '不可观察',
      unknown: '未知'
    };
    return labels[String(scope || '')] || String(scope || '未刷新');
  }

  function schedulerStatusLabel(status) {
    var labels = {
      idle: '空闲',
      pending: '待拉取',
      updating: '执行中',
      backoff: '冷却',
      quarantined: '隔离',
      unobservable: '不可观察',
      deleted: '已删除'
    };
    return labels[String(status || '')] || String(status || '未刷新');
  }

  function schedulerAccountLabel(value) {
    var text = String(value || '').trim();
    if (!text) return '';
    return text.split(',').map(function (part) {
      var item = part.trim();
      if (item === 'primary') return '主账号';
      if (item === 'secondary') return '第二账号';
      return item;
    }).filter(Boolean).join(', ');
  }

  function schedulerNextTime(channel) {
    var updateAt = String(channel.sync_next_update_at || '');
    var probeAt = String(channel.sync_next_probe_at || '');
    if (updateAt) return '拉取 ' + formatDateTime(updateAt);
    if (probeAt) return '探测 ' + formatDateTime(probeAt);
    return '';
  }

  function filterChannelsByMembership(channels) {
    var filterValue = channelState.membershipFilter || '__all__';
    if (filterValue === '__all__') return channels;
    return channels.filter(function (channel) {
      return String(channel && channel.sync_membership_scope || '') === filterValue;
    });
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

    if (actionOptions.allowProbe) {
      var probeBtn = document.createElement('button');
      probeBtn.type = 'button';
      probeBtn.textContent = '调度诊断';
      probeBtn.setAttribute('aria-label', '对 ' + channelLabel + ' 执行即时调度诊断');
      probeBtn.addEventListener('click', function () {
        handleProbeChannelSchedule(elements, item, probeBtn);
      });
      actions.appendChild(probeBtn);
    }

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
    var allChannels = Array.isArray(channels) ? channels : [];
    var visibleChannels = filterChannelsByMembership(allChannels);
    stopListRendering('channels', elements.channelList);
    elements.channelList.textContent = '';
    if (!allChannels.length) {
      var empty = document.createElement('div');
      empty.className = 'empty-box';
      empty.textContent = '暂无已入库群组或频道。';
      elements.channelList.appendChild(empty);
      elements.channelCount.textContent = '共 0 个群组/频道。';
      return;
    }
    if (!visibleChannels.length) {
      var noMatch = document.createElement('div');
      noMatch.className = 'empty-box';
      noMatch.textContent = '当前调度状态筛选下没有匹配结果。';
      elements.channelList.appendChild(noMatch);
      elements.channelCount.textContent = '当前筛选 0 个，共 ' + allChannels.length + ' 个群组/频道。';
      return;
    }

    renderItemsInBatches({
      key: 'channels',
      container: elements.channelList,
      items: visibleChannels,
      statusElement: elements.channelCount,
      progressText: function (visible, total) {
        return '正在显示 ' + visible + '/' + total + ' 个群组/频道...';
      },
      doneText: function (total) {
        if ((channelState.membershipFilter || '__all__') === '__all__') {
          return '共 ' + total + ' 个群组/频道。';
        }
        return '当前筛选 ' + total + ' 个，共 ' + allChannels.length + ' 个群组/频道。';
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
            { label: '最后消息', value: formatDateTime(channel.last_message_at) },
            { label: '调度', value: schedulerStatusLabel(channel.sync_status) }
          ],
          meta: [
            { label: 'chat_id', value: String(channel.chat_id) },
            { label: '用户名', value: channel.chat_username ? '@' + channel.chat_username : '' },
            { label: '类型', value: channel.chat_type || '' },
            { label: '状态', value: schedulerMembershipLabel(channel.sync_membership_scope) },
            { label: '可用账号', value: schedulerAccountLabel(channel.sync_source_accounts || channel.sync_last_source_account) },
            { label: '下次任务', value: schedulerNextTime(channel) },
            { label: '优先级', value: Number(channel.sync_priority_score || 0).toFixed(1) },
            { label: '隔离', value: channel.sync_quarantine_reason || channel.sync_last_probe_status || '' },
          ],
          actions: createChannelActions(channel, elements, {
            allowDelete: true,
            allowProbe: true
          }),
          note: channel.has_public_link
            ? ''
            : '私有群组通常没有稳定网页入口；客户端链接不可用时可复制信息后在 Telegram 中定位。'
        });
      }
    });
  }

  async function loadChannels(elements) {
    var requestToken = nextListRequestToken('channels');
    stopListRendering('channels', elements.channelList);
    elements.channelCount.textContent = '正在读取列表...';
    try {
      var data = await fetchJSON(
        '/api/admin/channels?sort=' + encodeURIComponent(elements.sortSelect.value)
      );
      if (!isListRequestCurrent('channels', requestToken)) return;
      if (!data.ok) throw new Error(data.error || '读取失败');
      channelState.items = Array.isArray(data.channels) ? data.channels : [];
      renderChannels(elements, channelState.items);
    } catch (error) {
      if (!isListRequestCurrent('channels', requestToken)) return;
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
      elements.missingStatus.textContent = '暂无未入库扫描结果，可点击“扫描已配置账号”。';
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
        return '发现 ' + total + ' 个已配置账号已加入但未入库的群组/频道。';
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
            { label: '状态', value: item.unavailable_reason ? '已加入但当前不可访问' : '已加入' },
            { label: '扫描', value: formatDateTime(item.scanned_at) },
          ],
          actions: createChannelActions(item, elements),
          note: item.unavailable_reason
            ? item.unavailable_reason
            : (
                item.has_public_link
                  ? ''
                  : '私有群组通常没有稳定网页入口；客户端链接不可用时可复制信息后在 Telegram 中定位。'
              )
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
    var requestToken = nextListRequestToken('missing');
    stopListRendering('missing', elements.missingList);
    elements.missingStatus.textContent = '正在读取扫描结果...';
    try {
      var data = await fetchJSON('/api/admin/channels/missing');
      if (!isListRequestCurrent('missing', requestToken)) return;
      if (!data.ok) throw new Error(data.error || '读取失败');
      renderMissingChannels(elements, data.items || []);
    } catch (error) {
      if (!isListRequestCurrent('missing', requestToken)) return;
      stopListRendering('missing', elements.missingList);
      elements.missingStatus.textContent = '读取扫描结果失败：' + error.message;
    }
  }

  function buildRestrictedNote(item) {
    var parts = [];
    if (Number(item.database_match_ambiguous || 0) === 1) {
      parts.push('数据库存在多个等价 chat_id，删除和调度诊断已禁用');
    }
    if (item.restriction_text) parts.push(item.restriction_text);
    if (item.restriction_platforms) parts.push('平台：' + item.restriction_platforms);
    if (item.restriction_reasons) parts.push('原因：' + item.restriction_reasons);
    if (item.risk_flags) parts.push('标记：' + item.risk_flags);
    return parts.join(' | ');
  }

  function restrictedDatabaseLabel(item) {
    if (Number(item && item.database_match_ambiguous || 0) === 1) {
      return 'ID 冲突';
    }
    return Number(item && item.in_database || 0) === 1 ? '已入库' : '未入库';
  }

  function restrictedMembershipLabel(scope) {
    return normalizeRestrictedMembershipScope(scope) === 'public_unjoined'
      ? '账号未加入'
      : '账号已加入';
  }

  function normalizeRestrictedMembershipScope(scope) {
    return String(scope || '') === 'public_unjoined' ? 'public_unjoined' : 'joined';
  }

  function orderRestrictedItemsByMembership(items) {
    var joinedItems = [];
    var publicUnjoinedItems = [];
    (Array.isArray(items) ? items : []).forEach(function (item) {
      if (normalizeRestrictedMembershipScope(item && item.membership_scope) === 'public_unjoined') {
        publicUnjoinedItems.push(item);
        return;
      }
      joinedItems.push(item);
    });
    return joinedItems.concat(publicUnjoinedItems);
  }

  function countRestrictedItemsByMembership(items) {
    var counts = {
      joined: 0,
      public_unjoined: 0
    };
    (Array.isArray(items) ? items : []).forEach(function (item) {
      var scope = normalizeRestrictedMembershipScope(item && item.membership_scope);
      counts[scope] += 1;
    });
    return counts;
  }

  function restrictedMembershipGroupLabel(scope) {
    return normalizeRestrictedMembershipScope(scope) === 'public_unjoined'
      ? '数据库已入库、账号未加入的公开群组/频道'
      : '已加入账号的群组/频道';
  }

  function createRestrictedMembershipGroupHeader(scope, count) {
    var heading = document.createElement('h3');
    heading.className = 'restricted-membership-heading';
    heading.textContent = restrictedMembershipGroupLabel(scope) + '（' + count + '）';
    return heading;
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
    var items = orderRestrictedItemsByMembership(
      filterRestrictedItems(restrictedState.items)
    );
    var membershipCounts = countRestrictedItemsByMembership(items);
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
      getGroup: function (item) {
        return normalizeRestrictedMembershipScope(item && item.membership_scope);
      },
      createGroupHeader: function (scope) {
        return createRestrictedMembershipGroupHeader(scope, membershipCounts[scope] || 0);
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
            { label: '账号状态', value: restrictedMembershipLabel(item.membership_scope) },
            { label: '数据库', value: restrictedDatabaseLabel(item) },
            { label: '扫描', value: formatDateTime(item.scanned_at) },
          ],
          actions: createChannelActions(item, elements, {
            allowDelete: Number(item.in_database || 0) === 1,
            allowProbe: Number(item.in_database || 0) === 1
          }),
          note: buildRestrictedNote(item)
        });
      }
    });
  }

  async function loadRestrictedChannels(elements) {
    var requestToken = nextListRequestToken('restricted');
    stopListRendering('restricted', elements.restrictedList);
    elements.restrictedStatus.textContent = '正在读取扫描结果...';
    try {
      var data = await fetchJSON('/api/admin/channels/restricted');
      if (!isListRequestCurrent('restricted', requestToken)) return;
      if (!data.ok) throw new Error(data.error || '读取失败');
      restrictedState.items = Array.isArray(data.items) ? data.items : [];
      updateRestrictedFilterOptions(elements);
      renderRestrictedChannels(elements);
    } catch (error) {
      if (!isListRequestCurrent('restricted', requestToken)) return;
      stopListRendering('restricted', elements.restrictedList);
      elements.restrictedStatus.textContent = '读取扫描结果失败：' + error.message;
    }
  }

  async function handleScanMissingClick(elements) {
    if (!window.confirm('确认扫描已配置 Telegram 账号中已加入但未入库的群组或频道？')) {
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

  async function handleScanRestrictedClick(elements) {
    if (!window.confirm('确认扫描账号已加入及数据库中可解析的公开群组/频道风险标记？')) {
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
          await loadRestrictedChannels(elements);
        }
      });
    } catch (error) {
      appendLog(elements, '创建删除任务失败：' + error.message);
    }
  }

  async function handleProbeChannelSchedule(elements, item, button) {
    var chatId = Number(item && item.chat_id);
    if (!Number.isInteger(chatId) || chatId === 0) {
      appendLog(elements, '无法诊断：chat_id 非法');
      return;
    }

    setElementDisabled(button, true);
    appendLog(elements, '开始即时调度诊断：' + (item.chat_title || ('Chat ' + chatId)));
    try {
      var payload = await fetchJSON('/api/admin/sync/chats/' + encodeURIComponent(String(chatId)) + '/probe', {
        method: 'POST'
      });
      if (!payload.ok) {
        throw new Error(payload.message || payload.error || '诊断失败');
      }

      var firstItem = Array.isArray(payload.items) && payload.items.length > 0
        ? payload.items[0]
        : {};
      var details = [
        '状态：' + (firstItem.status || '未知'),
        '远端：' + formatNumber(firstItem.remote_last_id),
        '本地：' + formatNumber(firstItem.local_last_id),
        '冷却：' + formatNumber(firstItem.cooldown_seconds) + ' 秒'
      ];
      appendLog(elements, (payload.message || '即时调度诊断完成') + '，' + details.join('，'));
      await loadChannels(elements);
    } catch (error) {
      appendLog(elements, '即时调度诊断失败：' + error.message);
    } finally {
      setElementDisabled(button, false);
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
    setElementDisabled(elements.sortSelect, disabled);
    setElementDisabled(elements.membershipFilterSelect, disabled);
    setElementDisabled(elements.refreshChannelsBtn, disabled);
    setElementDisabled(elements.channelListToggleBtn, disabled);
    setElementDisabled(elements.scanMissingBtn, disabled);
    setElementDisabled(elements.refreshMissingBtn, disabled);
    setElementDisabled(elements.missingListToggleBtn, disabled);
    setElementDisabled(elements.scanRestrictedBtn, disabled);
    setElementDisabled(elements.refreshRestrictedBtn, disabled);
    setElementDisabled(elements.restrictedFilterSelect, disabled);
    setElementDisabled(elements.restrictedListToggleBtn, disabled);
    [elements.channelList, elements.missingList, elements.restrictedList].forEach(function (list) {
      if (!list || typeof list.querySelectorAll !== 'function') return;
      list.querySelectorAll('button').forEach(function (button) {
        setElementDisabled(button, disabled);
      });
    });
    if (elements.logContainer && typeof elements.logContainer.setAttribute === 'function') {
      elements.logContainer.setAttribute('aria-busy', disabled ? 'true' : 'false');
    }
  }

  async function fetchJSON(url, options) {
    return sharedFetchJSON(url, Object.assign({}, options || {}, {
      onUnauthorized: sessionController.handleUnauthorizedResponse
    }));
  }

  var sessionController = shared.createAdminSessionController({
    afterAuth: async function (elements, context) {
      await loadInitialData(elements);
      if (context.reason === 'login') {
        appendLog(elements, '认证成功，已进入频道管理');
      }
    },
    getElements: getElements,
    getPageElement: function () {
      return document.getElementById('admin-channels-page');
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
    onStop: function (state) {
      state.onDone = null;
      state.doneMessage = '';
      state.errorMessage = '';
    },
    setBusy: function (elements, isBusy) {
      if (elements) {
        setBusy(elements, isBusy);
      }
    },
    setInitialState: function (state, options) {
      var pollOptions = options || {};
      state.onDone = typeof pollOptions.onDone === 'function' ? pollOptions.onDone : null;
      state.doneMessage = pollOptions.doneMessage || '任务执行完成';
      state.errorMessage = pollOptions.errorMessage || '任务执行失败，请检查日志';
    }
  });
})();
