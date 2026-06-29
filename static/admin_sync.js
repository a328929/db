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

  var syncState = {
    windows: [],
    selectedWindowKey: '',
    busy: false,
    liveItems: [],
    livePollTimerId: null,
    liveRequestSeq: 0,
    liveActiveRequestSeq: 0
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

  function normalizeSyncPayload(payload) {
    var windows = Array.isArray(payload && payload.windows) ? payload.windows : [];
    return {
      generatedAt: String(payload && payload.generated_at || ''),
      latestMessageCreatedAt: String(payload && payload.latest_message_created_at || ''),
      metricNote: String(payload && payload.metric_note || ''),
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
      var payload = normalizeLiveMessagesPayload(
        await fetchJSON('/api/admin/sync/messages?limit=' + encodeURIComponent(String(LIVE_MESSAGES_LIMIT)))
      );
      if (!isCurrentLiveRequest(requestSeq)) {
        return;
      }
      syncState.liveItems = payload.items;
      if (isLiveWindowSelected()) {
        renderLiveMessages(elements);
      }
      if (!payload.items.length) {
        elements.liveStatus.textContent = '时时模式下暂无最近入库消息。';
      } else {
        elements.liveStatus.textContent =
          '最近已入库 ' + formatNumber(payload.items.length) + ' 条消息，数据生成时间 ' + formatDateTime(payload.generatedAt) + '。';
      }
    } catch (error) {
      if (!isCurrentLiveRequest(requestSeq)) {
        return;
      }
      if (!opts.silent) {
        elements.liveStatus.textContent = '读取最近入库消息失败：' + error.message;
      }
    } finally {
      if (isCurrentLiveRequest(requestSeq)) {
        scheduleLivePoll(elements);
      }
    }
  }

  async function loadSyncStats(elements) {
    setBusy(elements, true);
    try {
      var payload = normalizeSyncPayload(await fetchJSON('/api/admin/sync/stats'));
      syncState.windows = payload.windows;
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
      updateLiveSectionVisibility(elements, { skipReload: true });

      elements.metricNote.textContent = payload.metricNote
        ? payload.metricNote + ' 数据生成时间：' + formatDateTime(payload.generatedAt) + '。'
        : '数据生成时间：' + formatDateTime(payload.generatedAt) + '。';

      if (!payload.windows.length) {
        elements.status.textContent = '暂无消息同步统计。';
      } else {
        elements.latestCreatedAtValue.textContent = formatDateTime(payload.latestMessageCreatedAt);
      }
    } catch (error) {
      elements.status.textContent = '读取消息同步统计失败：' + error.message;
      clearLivePollTimer();
    } finally {
      setBusy(elements, false);
    }
  }

  async function loadSyncDashboard(elements) {
    var statsTask = loadSyncStats(elements);
    var shouldPrimeLive = !syncState.liveItems.length || isLiveWindowSelected();
    if (!shouldPrimeLive) {
      await statsTask;
      return;
    }

    await Promise.all([
      statsTask,
      loadLiveMessages(elements, {
        silent: false,
        forceRender: true
      })
    ]);
  }

  async function fetchJSON(url, options) {
    return sharedFetchJSON(url, Object.assign({}, options || {}, {
      onUnauthorized: sessionController.handleUnauthorizedResponse
    }));
  }

  var sessionController = shared.createAdminSessionController({
    afterAuth: async function (elements, context) {
      await loadSyncDashboard(elements);
    },
    getElements: getElements,
    getPageElement: function () {
      return document.getElementById('admin-sync-page');
    }
  });
})();
