(function () {
  'use strict';

  var adminCsrfToken = '';

  function setAdminCsrfToken(token) {
    adminCsrfToken = typeof token === 'string' ? token : '';
  }

  function getAdminCsrfToken() {
    return adminCsrfToken;
  }

  function isAdminWriteRequest(url, method) {
    var normalizedMethod = String(method || 'GET').toUpperCase();
    if (['POST', 'PUT', 'PATCH', 'DELETE'].indexOf(normalizedMethod) === -1) {
      return false;
    }
    var normalizedUrl = String(url || '');
    if (normalizedUrl.indexOf('/api/admin/') !== 0) {
      return false;
    }
    return normalizedUrl !== '/api/admin/auth/login';
  }

  function buildFetchHeaders(url, requestOptions) {
    var headers = Object.assign({}, requestOptions.headers || {});
    var method = requestOptions.method || 'GET';
    if (isAdminWriteRequest(url, method) && adminCsrfToken) {
      headers['X-CSRF-Token'] = adminCsrfToken;
    }
    return headers;
  }

  function normalizeChats(payload) {
    function normalizeChatItem(chat) {
      if (!chat || typeof chat !== 'object') {
        return null;
      }

      var chatId = chat.chat_id;
      if (chatId === undefined || chatId === null) {
        return null;
      }

      var chatTitle = (typeof chat.chat_title === 'string' && chat.chat_title.trim())
        ? chat.chat_title.trim()
        : String(chatId);

      return {
        chat_id: chatId,
        chat_title: chatTitle,
        message_count: chat.message_count
      };
    }

    function normalizeChatList(items) {
      return items.map(normalizeChatItem).filter(function (item) {
        return !!item;
      });
    }

    if (Array.isArray(payload)) {
      return normalizeChatList(payload);
    }
    if (payload && Array.isArray(payload.items)) {
      return normalizeChatList(payload.items);
    }
    if (payload && Array.isArray(payload.chats)) {
      return normalizeChatList(payload.chats);
    }
    return [];
  }

  function buildChatOptionText(chat, fallbackId) {
    var title = (typeof chat.chat_title === 'string' && chat.chat_title.trim())
      ? chat.chat_title.trim()
      : fallbackId;
    var count = chat.message_count;

    return (count === undefined || count === null) ? title : title + '（' + String(count) + '）';
  }

  function renderChatOptions(selectElement, chats) {
    selectElement.textContent = '';

    var noneOption = document.createElement('option');
    noneOption.value = 'none';
    noneOption.textContent = '无';
    selectElement.appendChild(noneOption);

    var allOption = document.createElement('option');
    allOption.value = 'all';
    allOption.textContent = '全部';
    allOption.selected = true;
    selectElement.appendChild(allOption);

    chats.forEach(function (chat) {
      if (!chat || chat.chat_id === undefined || chat.chat_id === null) {
        return;
      }
      var option = document.createElement('option');
      var chatId = String(chat.chat_id);
      option.value = chatId;
      option.textContent = buildChatOptionText(chat, chatId);
      selectElement.appendChild(option);
    });
  }

  function pickFirstText() {
    for (var i = 0; i < arguments.length; i += 1) {
      var value = arguments[i];
      if (typeof value === 'string' && value.trim()) {
        return value.trim();
      }
      if (typeof value === 'number') {
        return String(value);
      }
    }
    return '';
  }

  function pickFirstNumber() {
    for (var i = 0; i < arguments.length; i += 1) {
      var value = arguments[i];
      if (typeof value === 'number' && Number.isFinite(value)) {
        return String(value);
      }
      if (typeof value === 'string' && value.trim() !== '' && !Number.isNaN(Number(value))) {
        return String(Number(value));
      }
    }
    return '0';
  }

  function getSelectedOptionLabel(selectElement, value) {
    var option = Array.prototype.find.call(selectElement.options, function (opt) {
      return opt.value === value;
    });
    return option ? option.textContent : '';
  }

  function setPageInteractionState(pageElement, interactive) {
    if (!pageElement || !pageElement.style) {
      return;
    }
    pageElement.style.opacity = interactive ? '1' : '0.1';
    pageElement.style.pointerEvents = interactive ? 'auto' : 'none';
    if (interactive) {
      pageElement.removeAttribute('aria-hidden');
      pageElement.removeAttribute('inert');
      pageElement.inert = false;
      return;
    }
    pageElement.setAttribute('aria-hidden', 'true');
    pageElement.setAttribute('inert', '');
    pageElement.inert = true;
  }

  function getVisibleDialog(dialogs) {
    if (!Array.isArray(dialogs)) {
      return null;
    }
    for (var i = 0; i < dialogs.length; i += 1) {
      var dialog = dialogs[i];
      if (dialog && !dialog.hidden) {
        return dialog;
      }
    }
    return null;
  }

  function getFocusableElements(container) {
    if (!container || typeof container.querySelectorAll !== 'function') {
      return [];
    }

    var selector = [
      'a[href]',
      'button',
      'input',
      'select',
      'textarea',
      '[tabindex]'
    ].join(', ');

    var nodes = Array.prototype.slice.call(container.querySelectorAll(selector));
    return nodes.filter(function (node) {
      if (!node || node.disabled) {
        return false;
      }
      if (node.hidden) {
        return false;
      }
      if (node.getAttribute('aria-hidden') === 'true') {
        return false;
      }
      if (node.getAttribute('tabindex') === '-1') {
        return false;
      }
      return node.getClientRects().length > 0;
    });
  }

  function trapFocusWithin(container, event) {
    if (!container || !event) {
      return;
    }

    var focusable = getFocusableElements(container);
    if (!focusable.length) {
      event.preventDefault();
      return;
    }

    var activeElement = document.activeElement;
    var first = focusable[0];
    var last = focusable[focusable.length - 1];
    var isShift = !!event.shiftKey;

    if (focusable.indexOf(activeElement) === -1) {
      event.preventDefault();
      (isShift ? last : first).focus();
      return;
    }

    if (!isShift && activeElement === last) {
      event.preventDefault();
      first.focus();
      return;
    }

    if (isShift && activeElement === first) {
      event.preventDefault();
      last.focus();
    }
  }

  function setDialogOpenState(dialog, isOpen, options) {
    if (!dialog) {
      return;
    }

    var opts = options || {};
    dialog.hidden = !isOpen;

    if (isOpen) {
      if (opts.focusElement && typeof opts.focusElement.focus === 'function') {
        opts.focusElement.focus();
      }
      return;
    }

    if (opts.skipFocusRestore) {
      return;
    }
    if (opts.restoreFocusElement && typeof opts.restoreFocusElement.focus === 'function') {
      opts.restoreFocusElement.focus();
    }
  }

  function setLoginStatus(elements, message) {
    if (!elements || !elements.loginStatus) {
      return;
    }
    elements.loginStatus.textContent = String(message || '');
  }

  function createAdminSessionController(options) {
    var opts = options || {};
    var afterAuth = typeof opts.afterAuth === 'function' ? opts.afterAuth : null;
    var getElements = typeof opts.getElements === 'function'
      ? opts.getElements
      : function () { return null; };
    var logoutTimerId = null;

    function getPageElement(elements) {
      if (typeof opts.getPageElement === 'function') {
        return opts.getPageElement(elements);
      }
      return elements && elements.page ? elements.page : null;
    }

    function clearLogoutTimer() {
      if (!logoutTimerId) {
        return;
      }
      window.clearTimeout(logoutTimerId);
      logoutTimerId = null;
    }

    function openLoginDialog(elements) {
      setDialogOpenState(elements && elements.loginDialog, true, {
        focusElement: elements && elements.passwordInput
      });
      setPageInteractionState(getPageElement(elements), false);
    }

    function closeLoginDialog(elements) {
      setLoginStatus(elements, '');
      setDialogOpenState(elements && elements.loginDialog, false, {
        skipFocusRestore: true
      });
      setPageInteractionState(getPageElement(elements), true);
    }

    function showExpiredSession(elements) {
      setLoginStatus(elements, '会话已过期，请重新登录。');
      openLoginDialog(elements);
    }

    function scheduleAutoLogout(seconds) {
      clearLogoutTimer();
      if (seconds <= 0) {
        return;
      }
      logoutTimerId = window.setTimeout(function () {
        showExpiredSession(getElements());
      }, seconds * 1000);
    }

    async function runAfterAuth(elements, reason, payload) {
      if (!afterAuth) {
        return;
      }
      await afterAuth(elements, {
        reason: reason,
        payload: payload
      });
    }

    async function checkAuth(elements) {
      var data = null;
      try {
        data = await fetchJSON('/api/admin/auth/check');
      } catch (_error) {
        openLoginDialog(elements);
        return;
      }
      if (!data || !data.authenticated) {
        openLoginDialog(elements);
        return;
      }
      closeLoginDialog(elements);
      scheduleAutoLogout(data.remaining);
      await runAfterAuth(elements, 'check', data);
    }

    async function handleLogin(elements) {
      var password = elements && elements.passwordInput ? elements.passwordInput.value : '';
      if (!password) {
        setLoginStatus(elements, '请输入管理员密码。');
        if (elements && elements.passwordInput) {
          elements.passwordInput.focus();
        }
        return;
      }

      setLoginStatus(elements, '');
      setElementDisabled(elements && elements.loginConfirmBtn, true);
      try {
        var data = await fetchJSON('/api/admin/auth/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ password: password })
        });
        if (!data || !data.ok) {
          throw new Error('登录失败');
        }
        if (elements && elements.passwordInput) {
          elements.passwordInput.value = '';
        }
        closeLoginDialog(elements);
        scheduleAutoLogout(data.expiry_duration);
        await runAfterAuth(elements, 'login', data);
      } catch (error) {
        setLoginStatus(elements, '认证失败：' + error.message);
        if (elements && elements.passwordInput) {
          elements.passwordInput.focus();
        }
      } finally {
        setElementDisabled(elements && elements.loginConfirmBtn, false);
      }
    }

    function handleUnauthorizedResponse() {
      clearLogoutTimer();
      showExpiredSession(getElements());
    }

    return {
      checkAuth: checkAuth,
      closeLoginDialog: closeLoginDialog,
      handleLogin: handleLogin,
      handleUnauthorizedResponse: handleUnauthorizedResponse,
      openLoginDialog: openLoginDialog,
      setLoginStatus: setLoginStatus
    };
  }

  function createAdminJobPollController(options) {
    var opts = options || {};
    var fetchJSON = typeof opts.fetchJSON === 'function' ? opts.fetchJSON : null;
    var appendLog = typeof opts.appendLog === 'function' ? opts.appendLog : function () {};
    var getElements = typeof opts.getElements === 'function'
      ? opts.getElements
      : function () { return null; };
    var setBusy = typeof opts.setBusy === 'function' ? opts.setBusy : function () {};
    var onSnapshot = typeof opts.onSnapshot === 'function' ? opts.onSnapshot : null;
    var onDone = typeof opts.onDone === 'function' ? opts.onDone : null;
    var onError = typeof opts.onError === 'function' ? opts.onError : null;
    var setInitialState = typeof opts.setInitialState === 'function'
      ? opts.setInitialState
      : function () {};
    var onStop = typeof opts.onStop === 'function' ? opts.onStop : null;
    var getDoneMessage = typeof opts.getDoneMessage === 'function'
      ? opts.getDoneMessage
      : function (state) { return state.doneMessage || '任务执行完成'; };
    var getErrorMessage = typeof opts.getErrorMessage === 'function'
      ? opts.getErrorMessage
      : function (state) { return state.errorMessage || '任务执行失败，请检查日志'; };
    var shouldContinue = typeof opts.shouldContinue === 'function'
      ? opts.shouldContinue
      : function () { return true; };
    var resolveSnapshot = typeof opts.resolveSnapshot === 'function'
      ? opts.resolveSnapshot
      : function (snapshotPayload) {
          var snapshot = snapshotPayload && snapshotPayload.job ? snapshotPayload.job : null;
          var status = snapshot && typeof snapshot.status === 'string' ? snapshot.status.trim() : '';
          if (!snapshot || !status) {
            return {
              errorMessage: '任务状态响应异常，已停止轮询',
              snapshot: null,
              status: ''
            };
          }
          return {
            errorMessage: '',
            snapshot: snapshot,
            status: status
          };
        };
    var onStart = typeof opts.onStart === 'function' ? opts.onStart : null;
    var intervalMs = Math.max(250, Number(opts.intervalMs) || 3000);
    var retryBaseMs = Math.max(250, Number(opts.retryBaseMs) || 3000);
    var retryMaxCount = Math.max(1, Number(opts.retryMaxCount) || 20);

    function stop(state, expectedToken) {
      if (typeof expectedToken === 'number' && state.pollToken !== expectedToken) {
        return;
      }
      if (state.timerId) {
        window.clearTimeout(state.timerId);
      }
      state.timerId = null;
      state.isPolling = false;
      if (typeof onStop === 'function') {
        onStop(state);
      }
      setBusy(getElements(), false);
    }

    function isActive(state, jobId, pollToken) {
      return state.isPolling
        && state.jobId === jobId
        && state.pollToken === pollToken;
    }

    function schedule(state, jobId, pollToken, delayMs) {
      if (!isActive(state, jobId, pollToken)) {
        return;
      }
      state.timerId = window.setTimeout(function () {
        poll(state);
      }, Math.max(250, Number(delayMs) || intervalMs));
    }

    async function poll(state) {
      if (!state.isPolling || !state.jobId || !fetchJSON) {
        return;
      }

      var jobId = state.jobId;
      var pollToken = state.pollToken;
      if (!shouldContinue(state, {
        appendLog: appendLog,
        stop: function () { stop(state, pollToken); }
      })) {
        return;
      }
      try {
        var logsPayload = await fetchJSON(
          '/api/admin/jobs/'
            + encodeURIComponent(jobId)
            + '/logs?after_seq='
            + encodeURIComponent(String(state.lastSeq || 0))
        );
        if (!isActive(state, jobId, pollToken)) {
          return;
        }

        var logs = logsPayload && Array.isArray(logsPayload.logs) ? logsPayload.logs : [];
        logs.forEach(function (line) {
          if (!line || typeof line.message !== 'string') {
            return;
          }
          appendLog(line.message);
          if (typeof line.seq === 'number' && Number.isFinite(line.seq)) {
            state.lastSeq = Math.max(state.lastSeq, line.seq);
          }
        });

        var snapshotPayload = await fetchJSON('/api/admin/jobs/' + encodeURIComponent(jobId));
        if (!isActive(state, jobId, pollToken)) {
          return;
        }

        var snapshotInfo = resolveSnapshot(snapshotPayload, state);
        var snapshot = snapshotInfo && snapshotInfo.snapshot ? snapshotInfo.snapshot : null;
        var status = snapshotInfo && typeof snapshotInfo.status === 'string' ? snapshotInfo.status.trim() : '';
        if (!snapshot || !status) {
          appendLog(
            snapshotInfo && snapshotInfo.errorMessage
              ? snapshotInfo.errorMessage
              : '任务状态响应异常，已停止轮询'
          );
          stop(state, pollToken);
          return;
        }

        state.retryCount = 0;
        if (typeof onSnapshot === 'function') {
          onSnapshot(snapshot, state);
        }
        var progressState = buildSnapshotProgressMessage(snapshot);
        if (progressState.key && progressState.key !== state.lastProgressKey) {
          state.lastProgressKey = progressState.key;
          if (progressState.message) {
            appendLog(progressState.message);
          }
        }

        if (status === 'done') {
          appendLog(getDoneMessage(state, snapshot));
          stop(state, pollToken);
          if (typeof onDone === 'function') {
            await onDone(snapshot, state);
          }
          return;
        }
        if (status === 'error') {
          appendLog(getErrorMessage(state, snapshot));
          stop(state, pollToken);
          if (typeof onError === 'function') {
            await onError(snapshot, state);
          }
          return;
        }
      } catch (error) {
        if (!isActive(state, jobId, pollToken)) {
          return;
        }
        state.retryCount += 1;
        if (state.retryCount > retryMaxCount) {
          appendLog('任务日志轮询失败次数过多，已停止轮询：' + error.message);
          stop(state, pollToken);
          return;
        }
        appendLog(
          '任务日志轮询失败，稍后自动重试（'
            + state.retryCount
            + '/'
            + retryMaxCount
            + '）：'
            + error.message
        );
        schedule(state, jobId, pollToken, retryBaseMs * Math.min(state.retryCount, 5));
        return;
      }
      schedule(state, jobId, pollToken, intervalMs);
    }

    function start(state, jobId, options) {
      var normalizedJobId = String(jobId || '').trim();
      if (!normalizedJobId) {
        return;
      }
      stop(state);
      state.pollToken += 1;
      state.jobId = normalizedJobId;
      state.lastSeq = 0;
      state.retryCount = 0;
      state.lastProgressKey = '';
      state.isPolling = true;
      setInitialState(state, options || {});
      if (typeof onStart === 'function') {
        onStart(state, options || {});
      }
      setBusy(getElements(), true);
      poll(state);
    }

    return {
      isActive: isActive,
      schedule: schedule,
      start: start,
      stop: stop
    };
  }

  function isAllScopeValue(value) {
    return String(value || '').trim() === 'all';
  }

  function isNoneScopeValue(value) {
    return String(value || '').trim() === 'none';
  }

  function isChatScopeValue(value) {
    var normalized = String(value || '').trim();
    return !!normalized && normalized !== 'none' && normalized !== 'all';
  }

  function getCurrentTargetInfo(elements) {
    var selectElement = elements && elements.scopeSelect;
    var scopeValue = selectElement ? String(selectElement.value || '') : '';
    var label = selectElement ? getSelectedOptionLabel(selectElement, scopeValue) : '';
    var trimmedLabel = typeof label === 'string' ? label.trim() : '';

    return {
      scopeValue: scopeValue,
      chatId: isChatScopeValue(scopeValue) ? scopeValue : '',
      label: trimmedLabel,
      isNone: isNoneScopeValue(scopeValue),
      isAll: isAllScopeValue(scopeValue),
      isChat: isChatScopeValue(scopeValue)
    };
  }

  function getRequiredIntegerChatId(target, errorMessage) {
    var chatIdNumber = Number(target && target.chatId);
    if (!Number.isFinite(chatIdNumber) || !Number.isInteger(chatIdNumber)) {
      throw new Error(errorMessage || 'chat_id 参数非法');
    }
    return chatIdNumber;
  }

  function getTargetScopeLabel(target) {
    if (!target) {
      return '';
    }
    return target.isAll ? '全部数据' : (target.label || target.chatId);
  }

  function buildNamedConfirmText(defaultText, prefixText, label) {
    if (label) {
      return String(prefixText || defaultText) + label + '？';
    }
    return String(defaultText || '');
  }

  function buildScopeRequestPayload(target, invalidChatIdMessage) {
    if (target.isAll) {
      return { scope: 'all' };
    }
    return {
      scope: 'chat',
      chat_id: getRequiredIntegerChatId(target, invalidChatIdMessage)
    };
  }

  function getConfirmationTarget(payload) {
    return payload.scope === 'all' ? 'all' : String(payload.chat_id);
  }

  function getCreatedJobId(payload) {
    var job = payload && payload.job ? payload.job : null;
    var jobId = job && job.job_id ? String(job.job_id) : '';
    if (!jobId) {
      throw new Error('任务创建成功但缺少 job_id');
    }
    return jobId;
  }

  function setStatsLineText(valueElement, prefixText, suffixText) {
    if (!valueElement || !valueElement.parentNode) {
      return;
    }

    var parent = valueElement.parentNode;
    var prefixNode = valueElement.previousSibling;
    if (!prefixNode || prefixNode.nodeType !== Node.TEXT_NODE) {
      if (prefixNode && prefixNode.nodeType === Node.ELEMENT_NODE) {
        prefixNode.textContent = '';
      }
      prefixNode = document.createTextNode('');
      parent.insertBefore(prefixNode, valueElement);
    }

    var suffixNode = valueElement.nextSibling;
    if (!suffixNode || suffixNode.nodeType !== Node.TEXT_NODE) {
      if (suffixNode && suffixNode.nodeType === Node.ELEMENT_NODE) {
        suffixNode.textContent = '';
      }
      suffixNode = document.createTextNode('');
      parent.insertBefore(suffixNode, valueElement.nextSibling);
    }

    prefixNode.nodeValue = String(prefixText || '');
    suffixNode.nodeValue = String(suffixText || '');
  }

  function getProgressStageLabel(stage) {
    var normalized = String(stage || '').trim().toLowerCase();
    if (normalized === 'updating') return '更新中';
    if (normalized === 'finalizing') return '整理中';
    if (normalized === 'fetching') return '抓取中';
    if (normalized === 'done') return '完成';
    if (normalized === 'error') return '失败';
    if (normalized === 'queued') return '排队中';
    if (normalized === 'running') return '执行中';
    return normalized || '执行中';
  }

  function buildSnapshotProgressMessage(snapshot) {
    var progress = snapshot && snapshot.progress && typeof snapshot.progress === 'object'
      ? snapshot.progress
      : null;
    if (!progress) {
      return { key: '', message: '' };
    }

    var current = typeof progress.current === 'number' && Number.isFinite(progress.current)
      ? Math.max(0, progress.current)
      : 0;
    var total = typeof progress.total === 'number' && Number.isFinite(progress.total)
      ? Math.max(0, progress.total)
      : null;
    var normalizedStage = String(progress.stage || '').trim().toLowerCase();
    var stage = getProgressStageLabel(progress.stage);
    var key = stage + '|' + String(current) + '|' + String(total === null ? '' : total);

    if (total !== null && total > 0) {
      if (normalizedStage === 'error') {
        return {
          key: key,
          message: '[进度] 已处理 ' + current + '/' + total + '，任务失败'
        };
      }
      if (normalizedStage === 'finalizing') {
        return {
          key: key,
          message: '[进度] 已处理 ' + current + '/' + total + '，正在整理结果'
        };
      }
      if (normalizedStage === 'done') {
        return {
          key: key,
          message: '[进度] 已处理 ' + current + '/' + total
        };
      }
      return {
        key: key,
        message: '[进度] 已处理 ' + current + '/' + total
      };
    }
    if (current > 0 || stage !== '排队中') {
      return {
        key: key,
        message: '[进度] ' + stage + ' ' + current
      };
    }
    return { key: key, message: '' };
  }

  function buildDialogTargetPreview(value) {
    var text = String(value || '').trim();
    if (text.length <= 40) {
      return text;
    }
    return text.slice(0, 40) + '…';
  }

  function setElementDisabled(element, disabled) {
    if (!element || typeof element.disabled === 'undefined') {
      return;
    }
    element.disabled = !!disabled;
  }

  function setElementHidden(element, hidden) {
    if (!element) {
      return;
    }
    element.hidden = hidden;
  }

  function getLogList(container) {
    if (!container) {
      return null;
    }

    var list = container.querySelector('[data-log-list="true"]');
    if (list) {
      return list;
    }

    list = document.createElement('ol');
    list.className = 'admin-log-list';
    list.setAttribute('data-log-list', 'true');
    list.setAttribute('role', 'list');
    container.appendChild(list);
    return list;
  }

  function ensurePlaceholder(container) {
    var list = getLogList(container);
    if (!list || list.querySelector('[data-placeholder="true"]')) {
      return;
    }

    var placeholder = document.createElement('li');
    placeholder.className = 'admin-log-placeholder';
    placeholder.setAttribute('role', 'listitem');
    placeholder.setAttribute('tabindex', '0');
    placeholder.textContent = '暂无日志';
    placeholder.setAttribute('data-placeholder', 'true');
    list.appendChild(placeholder);
  }

  function removePlaceholder(container) {
    var list = getLogList(container);
    if (!list) {
      return;
    }
    var placeholder = list.querySelector('[data-placeholder="true"]');
    if (placeholder) {
      placeholder.remove();
    } else if (list.textContent.trim() === '暂无日志') {
      list.textContent = '';
    }
  }

  function syncClearLogsButtonVisibility(elements) {
    if (!elements || !elements.logContainer || !elements.clearLogsBtn) {
      return;
    }

    var list = getLogList(elements.logContainer);
    if (!list) {
      elements.clearLogsBtn.hidden = true;
      return;
    }

    var hasLogs = Array.prototype.some.call(list.children, function (node) {
      if (!node || typeof node.getAttribute !== 'function') {
        return false;
      }
      if (node.getAttribute('data-placeholder') === 'true') {
        return false;
      }
      return !!String(node.textContent || '').trim();
    });

    elements.clearLogsBtn.hidden = !hasLogs;
  }

  function isLogContainerPinnedToBottom(container) {
    if (!container) {
      return true;
    }
    var remaining = container.scrollHeight - container.scrollTop - container.clientHeight;
    return remaining <= 24;
  }

  function logMessageKind(text) {
    var normalized = String(text || '').trim();
    if (!normalized) return 'info';
    if (normalized.indexOf('[进度]') === 0) return 'progress';
    if (/^账号执行统计：/.test(normalized)) return 'summary';
    if (/^(全部群组增量采集|扫描完成：|删除零消息群组完成：|零消息群组删除完成：)/.test(normalized)) {
      return 'summary';
    }
    if (/^\[\d+\/\d+\]/.test(normalized)) {
      if (/(失败|错误|异常|暂缓)/.test(normalized)) return 'error';
      if (/(冷却|等待|重试|切换|停止启动剩余群组)/.test(normalized)) return 'warning';
      if (/(新增|连接成功|启用|已切换账号完成采集|导入目标)/.test(normalized)) return 'step';
      return 'info';
    }
    if (/(失败|错误|异常|非法|不存在|无法|未授权|已中止)/.test(normalized)) return 'error';
    if (/(冷却|FloodWait|等待约|停止启动剩余群组|已收到停止请求|已请求停止|继续监控)/.test(normalized)) {
      return 'warning';
    }
    if (/(已创建|已接收|执行完成|认证成功|预检完成|恢复完成|删除完成|扫描任务已创建|恢复任务已创建)/.test(normalized)) {
      return 'success';
    }
    return 'info';
  }

  function splitLogPrefix(text) {
    var normalized = String(text || '');
    var progressMatch = normalized.match(/^(\[进度\])\s*(.*)$/);
    if (progressMatch) {
      return {
        prefix: progressMatch[1],
        message: progressMatch[2] || '',
        kind: 'progress'
      };
    }

    var stepMatch = normalized.match(/^(\[\d+\/\d+\])\s*(.*)$/);
    if (stepMatch) {
      return {
        prefix: stepMatch[1],
        message: stepMatch[2] || '',
        kind: logMessageKind(normalized)
      };
    }

    return {
      prefix: '',
      message: normalized,
      kind: logMessageKind(normalized)
    };
  }

  function buildLogEntry(message, options) {
    var parsed = splitLogPrefix(message);
    var line = document.createElement('li');
    line.className = 'admin-log-entry admin-log-entry--' + parsed.kind;
    line.setAttribute('role', 'listitem');
    line.setAttribute('data-log-kind', parsed.kind);
    line.setAttribute('tabindex', '0');
    if (options && options.liveProgress) {
      line.setAttribute('data-log-live', 'true');
    }

    if (parsed.prefix) {
      var prefix = document.createElement('span');
      prefix.className = 'admin-log-prefix';
      prefix.textContent = parsed.prefix;
      line.appendChild(prefix);
    }

    var text = document.createElement('span');
    text.className = 'admin-log-message';
    text.textContent = parsed.message || String(message || '');
    line.appendChild(text);
    return line;
  }

  function upsertProgressEntry(container, message) {
    var list = getLogList(container);
    if (!list) {
      return;
    }

    var existing = list.querySelector('[data-log-kind="progress"][data-log-live="true"]');
    if (existing) {
      var replacement = buildLogEntry(message, { liveProgress: true });
      list.replaceChild(replacement, existing);
      list.appendChild(replacement);
      return;
    }
    list.appendChild(buildLogEntry(message, { liveProgress: true }));
  }

  function appendLog(elements, message) {
    if (!message) {
      return;
    }

    var container = elements && elements.logContainer;
    if (!container) {
      return;
    }
    var list = getLogList(container);
    if (!list) {
      return;
    }
    var text = String(message);
    var shouldStickToBottom = isLogContainerPinnedToBottom(container);

    removePlaceholder(container);
    if (text.indexOf('[进度]') === 0) {
      upsertProgressEntry(container, text);
    } else {
      list.appendChild(buildLogEntry(text));
    }

    if (shouldStickToBottom) {
      container.scrollTop = container.scrollHeight;
    }
    syncClearLogsButtonVisibility(elements);
  }

  function clearLogs(elements) {
    var list = getLogList(elements.logContainer);
    if (list) {
      list.textContent = '';
    } else {
      elements.logContainer.textContent = '';
    }
    ensurePlaceholder(elements.logContainer);
    syncClearLogsButtonVisibility(elements);
  }

  async function readResponseErrorPayload(response) {
    try {
      var payload = await response.json();
      return payload && typeof payload === 'object' ? payload : {};
    } catch (_ignoreErrorPayload) {
      return {};
    }
  }

  function buildResponseErrorMessage(response, payload) {
    var serverMessage = '';
    if (payload && typeof payload.error === 'string' && payload.error.trim()) {
      serverMessage = payload.error.trim();
    }
    if (serverMessage) {
      return serverMessage;
    }
    if (response.status === 429) {
      return '请求太快了，请稍等一会儿再试';
    }
    if (response.status === 500 || response.status === 503) {
      return '系统异常，请 15 秒后再试';
    }
    return '操作失败 (HTTP ' + response.status + ')';
  }

  async function fetchJSON(url, options) {
    var requestOptions = options || {};
    var onUnauthorized = requestOptions.onUnauthorized;

    var response;
    try {
      response = await fetch(url, {
        method: requestOptions.method || 'GET',
        headers: buildFetchHeaders(url, requestOptions),
        body: requestOptions.body
      });
    } catch (_networkError) {
      throw new Error('网络请求失败');
    }

    if (!response.ok) {
      if (response.status === 401) {
        if (typeof onUnauthorized === 'function') {
          onUnauthorized();
        }
        throw new Error('未授权，请先登录');
      }

      throw new Error(buildResponseErrorMessage(
        response,
        await readResponseErrorPayload(response)
      ));
    }

    try {
      var payload = await response.json();
      if (payload && typeof payload.csrf_token === 'string') {
        setAdminCsrfToken(payload.csrf_token);
      }
      return payload;
    } catch (_parseError) {
      throw new Error('响应 JSON 解析失败');
    }
  }

  async function postJSON(url, payload, options) {
    var opts = options || {};
    return fetchJSON(url, {
      method: 'POST',
      headers: Object.assign(
        { 'Content-Type': 'application/json' },
        opts.headers || {}
      ),
      body: JSON.stringify(payload),
      onUnauthorized: opts.onUnauthorized
    });
  }

  window.AdminManageShared = {
    appendLog: appendLog,
    buildDialogTargetPreview: buildDialogTargetPreview,
    buildChatOptionText: buildChatOptionText,
    buildNamedConfirmText: buildNamedConfirmText,
    buildScopeRequestPayload: buildScopeRequestPayload,
    buildSnapshotProgressMessage: buildSnapshotProgressMessage,
    clearLogs: clearLogs,
    ensurePlaceholder: ensurePlaceholder,
    fetchJSON: fetchJSON,
    createAdminSessionController: createAdminSessionController,
    createAdminJobPollController: createAdminJobPollController,
    getAdminCsrfToken: getAdminCsrfToken,
    getConfirmationTarget: getConfirmationTarget,
    getCreatedJobId: getCreatedJobId,
    getFocusableElements: getFocusableElements,
    getCurrentTargetInfo: getCurrentTargetInfo,
    getProgressStageLabel: getProgressStageLabel,
    getRequiredIntegerChatId: getRequiredIntegerChatId,
    getSelectedOptionLabel: getSelectedOptionLabel,
    getTargetScopeLabel: getTargetScopeLabel,
    getVisibleDialog: getVisibleDialog,
    isAllScopeValue: isAllScopeValue,
    isChatScopeValue: isChatScopeValue,
    isNoneScopeValue: isNoneScopeValue,
    normalizeChats: normalizeChats,
    pickFirstNumber: pickFirstNumber,
    pickFirstText: pickFirstText,
    postJSON: postJSON,
    removePlaceholder: removePlaceholder,
    renderChatOptions: renderChatOptions,
    setDialogOpenState: setDialogOpenState,
    setElementDisabled: setElementDisabled,
    setElementHidden: setElementHidden,
    setLoginStatus: setLoginStatus,
    setStatsLineText: setStatsLineText,
    setPageInteractionState: setPageInteractionState,
    setAdminCsrfToken: setAdminCsrfToken,
    syncClearLogsButtonVisibility: syncClearLogsButtonVisibility,
    trapFocusWithin: trapFocusWithin
  };
})();
