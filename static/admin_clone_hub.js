(function () {
  'use strict';

  var shared = window.AdminManageShared;
  var state = { busy: false };

  document.addEventListener('DOMContentLoaded', function () {
    var elements = getElements();
    if (!elements || !shared) {
      return;
    }
    elements.refreshBtn.addEventListener('click', function () {
      loadWorkbench(elements);
    });
    loadWorkbench(elements);
  });

  function getElements() {
    var elements = {
      page: document.getElementById('admin-clone-hub-page'),
      focusTitle: document.getElementById('cloneWorkbenchFocusTitle'),
      focusDescription: document.getElementById('admin-clone-hub-focus-description'),
      focusContext: document.getElementById('admin-clone-hub-focus-context'),
      focusAction: document.getElementById('admin-clone-hub-focus-action'),
      summary: document.getElementById('admin-clone-hub-summary'),
      refreshBtn: document.getElementById('admin-clone-hub-refresh-btn'),
      createStep: document.getElementById('admin-clone-hub-step-create'),
      manageStep: document.getElementById('admin-clone-hub-step-manage')
    };
    var required = Object.keys(elements).filter(function (key) {
      return !elements[key];
    });
    if (required.length) {
      console.warn('[admin_clone_hub] Missing required elements:', required.join(', '));
      return null;
    }
    return elements;
  }

  async function loadWorkbench(elements) {
    setBusy(elements, true);
    try {
      var payload = await shared.fetchJSON('/api/admin/clone/workbench', {
        onUnauthorized: redirectToLogin
      });
      renderWorkbench(elements, payload || {});
    } catch (error) {
      elements.focusTitle.textContent = '暂时无法读取工作状态';
      elements.focusDescription.textContent = '请刷新后重试。' + (error && error.message ? ' ' + error.message : '');
      elements.focusContext.hidden = true;
      elements.focusAction.textContent = '';
    } finally {
      setBusy(elements, false);
    }
  }

  function renderWorkbench(elements, payload) {
    var focus = payload && payload.focus && typeof payload.focus === 'object'
      ? payload.focus
      : {};
    var summary = payload && payload.summary && typeof payload.summary === 'object'
      ? payload.summary
      : {};

    elements.focusTitle.textContent = String(focus.title || '从创建副本开始');
    elements.focusDescription.textContent = String(
      focus.description || '选择数据库中的源群，完成检查后创建目标副本。'
    );
    renderFocusContext(elements.focusContext, focus.run);
    renderFocusAction(elements.focusAction, focus.action);
    renderSummary(elements.summary, summary);
    renderWorkflowSteps(elements, focus);
  }

  function renderFocusContext(container, run) {
    container.textContent = '';
    if (!run || typeof run !== 'object' || !run.label) {
      container.hidden = true;
      return;
    }
    var label = document.createElement('span');
    var value = document.createElement('strong');
    label.textContent = '当前记录';
    value.textContent = String(run.label);
    container.appendChild(label);
    container.appendChild(value);
    container.hidden = false;
  }

  function renderFocusAction(container, action) {
    container.textContent = '';
    if (!action || typeof action !== 'object' || !action.href || !action.label) {
      return;
    }
    var link = document.createElement('a');
    link.className = 'btn primary';
    link.href = String(action.href);
    link.textContent = String(action.label);
    container.appendChild(link);
  }

  function renderSummary(container, summary) {
    container.textContent = '';
    [
      ['全部记录', summary.total],
      ['创建中', summary.creating],
      ['删除中', summary.deleting],
      ['已创建', summary.created],
      ['创建失败', summary.failed]
    ].forEach(function (item) {
      var wrap = document.createElement('div');
      var term = document.createElement('dt');
      var detail = document.createElement('dd');
      term.textContent = item[0];
      detail.textContent = formatNumber(item[1]);
      wrap.appendChild(term);
      wrap.appendChild(detail);
      container.appendChild(wrap);
    });
  }

  function renderWorkflowSteps(elements, focus) {
    var states = {
      create: 'ready',
      manage: 'available'
    };
    var currentStep = String(focus.step || 'create');
    var currentState = String(focus.state || 'ready');

    if (currentStep === 'migrate') {
      states.create = 'complete';
      states.manage = currentState;
    } else if (currentStep === 'manage') {
      states.create = 'complete';
      states.manage = currentState;
    } else {
      states.create = currentState;
    }

    setWorkflowStepState(elements.createStep, states.create);
    setWorkflowStepState(elements.manageStep, states.manage);
  }

  function setWorkflowStepState(element, value) {
    if (!element) {
      return;
    }
    element.setAttribute('data-workflow-state', String(value || 'pending'));
  }

  function formatNumber(value) {
    var number = Number(value || 0);
    if (!Number.isFinite(number)) {
      return '0';
    }
    return String(Math.trunc(number)).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  }

  function setBusy(elements, busy) {
    state.busy = !!busy;
    elements.refreshBtn.disabled = state.busy;
    elements.page.setAttribute('aria-busy', state.busy ? 'true' : 'false');
  }

  function redirectToLogin() {
    var next = String(window.location.pathname || '/admin/clone')
      + String(window.location.search || '');
    window.location.assign('/admin/login?next=' + encodeURIComponent(next));
  }
})();
