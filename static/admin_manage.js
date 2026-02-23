(function () {
  'use strict';

  document.addEventListener('DOMContentLoaded', function () {
    var elements = getElements();
    if (!elements) {
      return;
    }

    initializeUI(elements);
    bindEvents(elements);
    loadInitialReadOnlyData(elements);

    window.AdminManageUI = {
      appendLog: function (message) {
        appendLog(elements, message);
      },
      clearLogs: function () {
        clearLogs(elements);
      }
    };
  });

  function getElements() {
    var elements = {
      scopeSelect: document.getElementById('admin-scope-select'),
      incrementalCheckbox: document.getElementById('admin-incremental-checkbox'),
      incrementalLabel: document.querySelector('label[for="admin-incremental-checkbox"]'),
      startUpdateBtn: document.getElementById('admin-start-update-btn'),
      deleteDataBtn: document.getElementById('admin-delete-data-btn'),
      logContainer: document.getElementById('admin-log-container'),
      clearLogsBtn: document.getElementById('admin-clear-logs-btn'),
      openAddDialogBtn: document.getElementById('admin-open-add-dialog-btn'),
      dialog: document.getElementById('admin-add-target-dialog'),
      dialogInput: document.getElementById('admin-target-input'),
      dialogCancelBtn: document.getElementById('admin-dialog-cancel-btn'),
      dialogConfirmBtn: document.getElementById('admin-dialog-confirm-btn'),
      statScope: document.getElementById('admin-stat-scope'),
      statMessages: document.getElementById('admin-stat-messages')
    };

    var requiredKeys = [
      'scopeSelect',
      'incrementalCheckbox',
      'incrementalLabel',
      'startUpdateBtn',
      'deleteDataBtn',
      'logContainer',
      'clearLogsBtn',
      'openAddDialogBtn',
      'dialog',
      'dialogInput',
      'dialogCancelBtn',
      'dialogConfirmBtn',
      'statScope',
      'statMessages'
    ];

    var missing = requiredKeys.filter(function (key) {
      return !elements[key];
    });

    if (missing.length > 0) {
      console.warn('[admin_manage] Missing required elements:', missing.join(', '));
      return null;
    }

    return elements;
  }

  function initializeUI(elements) {
    updateControlVisibility(elements);
    ensurePlaceholder(elements.logContainer);
    elements.clearLogsBtn.hidden = true;
    closeDialog(elements, { skipFocusRestore: true });
  }

  function bindEvents(elements) {
    elements.scopeSelect.addEventListener('change', function () {
      updateControlVisibility(elements);
      loadStatsByCurrentSelection(elements);
    });

    elements.incrementalCheckbox.addEventListener('change', function () {
      updateControlVisibility(elements);
    });

    elements.clearLogsBtn.addEventListener('click', function () {
      clearLogs(elements);
    });

    elements.startUpdateBtn.addEventListener('click', function () {
      handleStartUpdateClick(elements);
    });

    elements.deleteDataBtn.addEventListener('click', function () {
      handleDeleteDataClick(elements);
    });

    elements.openAddDialogBtn.addEventListener('click', function () {
      openDialog(elements);
    });

    elements.dialogCancelBtn.addEventListener('click', function () {
      closeDialog(elements);
    });

    elements.dialogConfirmBtn.addEventListener('click', function () {
      handleDialogConfirm(elements);
    });

    elements.dialogInput.addEventListener('keydown', function (event) {
      if (event.key !== 'Enter' || elements.dialog.hidden) {
        return;
      }
      event.preventDefault();
      handleDialogConfirm(elements);
    });

    document.addEventListener('keydown', function (event) {
      if (event.key === 'Escape' && !elements.dialog.hidden) {
        closeDialog(elements);
      }
    });
  }

  async function loadInitialReadOnlyData(elements) {
    await loadChatsIntoSelect(elements);
    await loadStatsByCurrentSelection(elements);
  }

  async function loadChatsIntoSelect(elements) {
    try {
      var data = await fetchJSON('/api/admin/chats');
      var chats = normalizeChats(data);
      renderChatOptions(elements.scopeSelect, chats);
    } catch (error) {
      appendLog(elements, '读取群组列表失败：' + error.message);
    }
  }

  function normalizeChats(payload) {
    if (Array.isArray(payload)) {
      return payload;
    }
    if (payload && Array.isArray(payload.items)) {
      return payload.items;
    }
    if (payload && Array.isArray(payload.chats)) {
      return payload.chats;
    }
    return [];
  }

  function renderChatOptions(selectElement, chats) {
    selectElement.innerHTML = '';

    var defaultOption = document.createElement('option');
    defaultOption.value = 'none';
    defaultOption.textContent = '无';
    defaultOption.selected = true;
    selectElement.appendChild(defaultOption);

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

  function buildChatOptionText(chat, fallbackId) {
    var title = '';
    if (typeof chat.chat_name === 'string' && chat.chat_name.trim()) {
      title = chat.chat_name.trim();
    } else if (typeof chat.title === 'string' && chat.title.trim()) {
      title = chat.title.trim();
    } else {
      title = fallbackId;
    }

    var count = chat.message_count;
    if (count === undefined || count === null) {
      count = chat.msg_count;
    }

    return (count === undefined || count === null) ? title : title + '（' + String(count) + '）';
  }

  async function loadStatsByCurrentSelection(elements) {
    var selectedChatId = elements.scopeSelect.value;
    var statsPath = '/api/admin/stats';

    if (selectedChatId && selectedChatId !== 'none') {
      statsPath += '?chat_id=' + encodeURIComponent(selectedChatId);
    }

    try {
      var data = await fetchJSON(statsPath);
      applyStatsToHeader(elements, data, selectedChatId);
    } catch (error) {
      appendLog(elements, '读取统计信息失败：' + error.message);
    }
  }

  function applyStatsToHeader(elements, payload, selectedChatId) {
    var data = payload && payload.data ? payload.data : payload;

    if (selectedChatId && selectedChatId !== 'none') {
      elements.statScope.textContent = pickFirstText(
        data && data.chat_name,
        data && data.chat_title,
        getSelectedOptionLabel(elements.scopeSelect, selectedChatId),
        selectedChatId
      );
      elements.statMessages.textContent = pickFirstNumber(
        data && data.message_count,
        data && data.msg_count,
        0
      );
      return;
    }

    elements.statScope.textContent = pickFirstNumber(
      data && data.chat_count,
      data && data.scope_count,
      data && data.total_chats,
      data && data.count,
      0
    );

    elements.statMessages.textContent = pickFirstNumber(
      data && data.message_count,
      data && data.total_messages,
      data && data.msg_count,
      0
    );
  }

  function getSelectedOptionLabel(selectElement, value) {
    var option = Array.prototype.find.call(selectElement.options, function (opt) {
      return opt.value === value;
    });
    return option ? option.textContent : '';
  }

  function getCurrentTargetInfo(elements) {
    var selectElement = elements && elements.scopeSelect;
    var chatId = selectElement ? String(selectElement.value || '') : '';
    var label = selectElement ? getSelectedOptionLabel(selectElement, chatId) : '';
    var trimmedLabel = typeof label === 'string' ? label.trim() : '';

    return {
      chatId: chatId,
      label: trimmedLabel,
      isNone: !chatId || chatId === 'none'
    };
  }

  function handleStartUpdateClick(elements) {
    var target = getCurrentTargetInfo(elements);
    if (target.isNone) {
      appendLog(elements, '未选择群组/频道');
      return;
    }

    if (!elements.incrementalCheckbox || !elements.incrementalCheckbox.checked) {
      appendLog(elements, '未启用增量更新，无法执行更新');
      return;
    }

    if (!window.confirm('确认执行增量更新？')) {
      appendLog(elements, '已取消更新操作');
      return;
    }

    appendLog(elements, '开始执行增量更新（占位）');
    appendLog(elements, '目标：' + (target.label || target.chatId) + '（ID: ' + target.chatId + '）');
    appendLog(elements, '更新任务已提交（占位）');
  }

  function handleDeleteDataClick(elements) {
    var target = getCurrentTargetInfo(elements);
    if (target.isNone) {
      appendLog(elements, '未选择群组/频道');
      return;
    }

    var confirmText = '确认删除该群/频道数据？';
    if (target.label) {
      confirmText = '确认删除该群/频道数据：' + target.label + '？';
    }

    if (!window.confirm(confirmText)) {
      appendLog(elements, '已取消删除操作');
      return;
    }

    appendLog(elements, '开始删除群/频道数据（占位）');
    appendLog(elements, '目标：' + (target.label || target.chatId) + '（ID: ' + target.chatId + '）');
    appendLog(elements, '删除任务已提交（占位）');
  }

  async function fetchJSON(url) {
    var response;
    try {
      response = await fetch(url, {
        method: 'GET',
        headers: {
          Accept: 'application/json'
        }
      });
    } catch (networkError) {
      throw new Error('网络请求失败');
    }

    if (!response.ok) {
      throw new Error('HTTP ' + response.status);
    }

    try {
      return await response.json();
    } catch (parseError) {
      throw new Error('响应 JSON 解析失败');
    }
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

  function updateControlVisibility(elements) {
    var hasSelectedTarget = elements.scopeSelect.value !== 'none';

    setElementHidden(elements.incrementalCheckbox, !hasSelectedTarget);
    setElementHidden(elements.incrementalLabel, !hasSelectedTarget);

    if (!hasSelectedTarget) {
      elements.startUpdateBtn.hidden = true;
      elements.deleteDataBtn.hidden = true;
      return;
    }

    elements.deleteDataBtn.hidden = false;
    elements.startUpdateBtn.hidden = !elements.incrementalCheckbox.checked;
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
    elements.clearLogsBtn.hidden = false;
  }

  function clearLogs(elements) {
    elements.logContainer.textContent = '';
    ensurePlaceholder(elements.logContainer);
    elements.clearLogsBtn.hidden = true;
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

  function openDialog(elements) {
    elements.dialog.hidden = false;
    elements.dialogInput.focus();
  }

  function closeDialog(elements, options) {
    var opts = options || {};
    elements.dialog.hidden = true;

    if (!opts.skipFocusRestore) {
      elements.openAddDialogBtn.focus();
    }
  }

  function buildDialogTargetPreview(value) {
    var text = String(value || '').trim();
    if (text.length <= 40) {
      return text;
    }
    return text.slice(0, 40) + '…';
  }

  function handleDialogConfirm(elements) {
    var value = elements && elements.dialogInput ? elements.dialogInput.value.trim() : '';

    if (!value) {
      appendLog(elements, '请输入群组名称或链接');
      return;
    }

    var preview = buildDialogTargetPreview(value);
    if (!window.confirm('确认新增/更新抓取目标：' + preview + '？')) {
      appendLog(elements, '已取消新增抓取目标');
      return;
    }

    appendLog(elements, '已接收抓取目标：' + value);
    appendLog(elements, '正在创建抓取任务（占位）');
    appendLog(elements, '详细抓取日志将在此处显示（占位）');
    elements.dialogInput.value = '';
    closeDialog(elements);
  }

  function setElementHidden(element, hidden) {
    if (!element) {
      return;
    }
    element.hidden = hidden;
  }
})();
