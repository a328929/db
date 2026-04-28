(function () {
  'use strict';

  function normalizeChats(payload) {
    function normalizeChatItem(chat) {
      if (!chat || typeof chat !== 'object') {
        return null;
      }

      var chatId = chat.chat_id;
      if (chatId === undefined || chatId === null) {
        return null;
      }

      var chatTitle = '';
      if (typeof chat.chat_title === 'string' && chat.chat_title.trim()) {
        chatTitle = chat.chat_title.trim();
      } else if (typeof chat.chat_name === 'string' && chat.chat_name.trim()) {
        chatTitle = chat.chat_name.trim();
      } else if (typeof chat.title === 'string' && chat.title.trim()) {
        chatTitle = chat.title.trim();
      } else {
        chatTitle = String(chatId);
      }

      var messageCount = chat.message_count;
      if (messageCount === undefined || messageCount === null) {
        messageCount = chat.msg_count;
      }

      return {
        chat_id: chatId,
        chat_title: chatTitle,
        message_count: messageCount
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
    selectElement.innerHTML = '';

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

  function ensurePlaceholder(container) {
    if (container.querySelector('[data-placeholder="true"]')) {
      return;
    }

    var placeholder = document.createElement('p');
    placeholder.textContent = '暂无日志';
    placeholder.setAttribute('data-placeholder', 'true');
    container.appendChild(placeholder);
  }

  function removePlaceholder(container) {
    var placeholder = container.querySelector('[data-placeholder="true"]');
    if (placeholder) {
      placeholder.remove();
    } else if (container.textContent.trim() === '暂无日志') {
      container.textContent = '';
    }
  }

  function syncClearLogsButtonVisibility(elements) {
    if (!elements || !elements.logContainer || !elements.clearLogsBtn) {
      return;
    }

    var hasLogs = Array.prototype.some.call(elements.logContainer.children, function (node) {
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

  function appendLog(elements, message) {
    if (!message) {
      return;
    }

    removePlaceholder(elements.logContainer);

    var line = document.createElement('p');
    line.textContent = String(message);
    elements.logContainer.appendChild(line);

    elements.logContainer.scrollTop = elements.logContainer.scrollHeight;
    syncClearLogsButtonVisibility(elements);
  }

  function clearLogs(elements) {
    elements.logContainer.textContent = '';
    ensurePlaceholder(elements.logContainer);
    syncClearLogsButtonVisibility(elements);
  }

  window.AdminManageShared = {
    appendLog: appendLog,
    buildChatOptionText: buildChatOptionText,
    clearLogs: clearLogs,
    ensurePlaceholder: ensurePlaceholder,
    getFocusableElements: getFocusableElements,
    getCurrentTargetInfo: getCurrentTargetInfo,
    getSelectedOptionLabel: getSelectedOptionLabel,
    getVisibleDialog: getVisibleDialog,
    isAllScopeValue: isAllScopeValue,
    isChatScopeValue: isChatScopeValue,
    isNoneScopeValue: isNoneScopeValue,
    normalizeChats: normalizeChats,
    pickFirstNumber: pickFirstNumber,
    pickFirstText: pickFirstText,
    removePlaceholder: removePlaceholder,
    renderChatOptions: renderChatOptions,
    setDialogOpenState: setDialogOpenState,
    setElementDisabled: setElementDisabled,
    setElementHidden: setElementHidden,
    setPageInteractionState: setPageInteractionState,
    syncClearLogsButtonVisibility: syncClearLogsButtonVisibility,
    trapFocusWithin: trapFocusWithin
  };
})();
