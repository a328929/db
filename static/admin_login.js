(function () {
  'use strict';

  var DEFAULT_NEXT_PATH = '/admin/manage';
  var ALLOWED_NEXT_PATHS = [
    '/admin/manage',
    '/admin/channels',
    '/admin/clone',
    '/admin/clone/create',
    '/admin/clone/migrate',
    '/admin/clone/runs/manage',
    '/admin/clone/runs/detail',
    '/admin/recovery'
  ];

  document.addEventListener('DOMContentLoaded', function () {
    var elements = getElements();
    if (!elements) {
      return;
    }
    elements.passwordInput.focus();
    bindEvents(elements);
    checkExistingSession(elements);
  });

  function getElements() {
    var elements = {
      page: document.getElementById('admin-login-page'),
      status: document.getElementById('admin-login-status'),
      passwordInput: document.getElementById('admin-password-input'),
      confirmBtn: document.getElementById('admin-login-confirm-btn')
    };
    if (!elements.page || !elements.status || !elements.passwordInput || !elements.confirmBtn) {
      return null;
    }
    return elements;
  }

  function bindEvents(elements) {
    elements.confirmBtn.addEventListener('click', function () {
      handleLogin(elements);
    });
    elements.passwordInput.addEventListener('keydown', function (event) {
      if (event.key !== 'Enter') {
        return;
      }
      event.preventDefault();
      handleLogin(elements);
    });
  }

  function normalizeNextPath(rawNextPath) {
    var nextPath = String(rawNextPath || '').trim();
    if (!nextPath || nextPath.indexOf('//') === 0 || nextPath.indexOf('://') !== -1) {
      return DEFAULT_NEXT_PATH;
    }
    var queryIndex = nextPath.indexOf('?');
    var pathOnly = queryIndex === -1 ? nextPath : nextPath.slice(0, queryIndex);
    if (ALLOWED_NEXT_PATHS.indexOf(pathOnly) === -1) {
      return DEFAULT_NEXT_PATH;
    }
    return nextPath;
  }

  function getNextPath(elements) {
    return normalizeNextPath(elements.page.getAttribute('data-next-path'));
  }

  function setStatus(elements, message) {
    elements.status.textContent = String(message || '');
  }

  function setBusy(elements, busy) {
    elements.confirmBtn.disabled = !!busy;
    elements.passwordInput.disabled = !!busy;
  }

  async function fetchJSON(url, options) {
    var response;
    try {
      response = await fetch(url, options || {});
    } catch (_networkError) {
      throw new Error('网络请求失败');
    }

    var payload = {};
    try {
      payload = await response.json();
    } catch (_parseError) {
      payload = {};
    }

    if (!response.ok) {
      if (payload && typeof payload.error === 'string' && payload.error.trim()) {
        throw new Error(payload.error.trim());
      }
      if (response.status === 429) {
        throw new Error('登录失败次数过多，请稍后再试');
      }
      throw new Error('登录失败 (HTTP ' + response.status + ')');
    }
    return payload;
  }

  async function checkExistingSession(elements) {
    try {
      var payload = await fetchJSON('/api/admin/auth/check');
      if (payload && payload.authenticated) {
        window.location.replace(getNextPath(elements));
      }
    } catch (_ignoreAuthCheckError) {
      // Keep the explicit login form visible when auth check cannot complete.
    }
  }

  async function handleLogin(elements) {
    var password = elements.passwordInput.value;
    if (!password) {
      setStatus(elements, '请输入管理员密码。');
      elements.passwordInput.focus();
      return;
    }

    setStatus(elements, '');
    setBusy(elements, true);
    try {
      var payload = await fetchJSON('/api/admin/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: password })
      });
      if (payload && payload.ok) {
        window.location.assign(getNextPath(elements));
        return;
      }
      throw new Error('登录失败');
    } catch (error) {
      setStatus(elements, '认证失败：' + error.message);
      elements.passwordInput.focus();
    } finally {
      setBusy(elements, false);
    }
  }
})();
