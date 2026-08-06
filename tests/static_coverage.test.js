const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const ROOT = path.resolve(__dirname, '..');

function createElement(tagName = '') {
  return {
    tagName,
    className: '',
    textContent: '',
    children: [],
    dataset: {},
    value: '',
    appendChild(child) {
      this.children.push(child);
      return child;
    },
    addEventListener() {},
    getAttribute() {
      return null;
    },
    removeAttribute() {},
    setAttribute() {}
  };
}

function createSandbox() {
  const sandbox = {
    Array,
    Boolean,
    Error,
    Intl,
    Math,
    Number,
    Object,
    Promise,
    String,
    URLSearchParams,
    clearTimeout,
    console,
    document: {
      addEventListener() {},
      createElement,
      getElementById() {
        return null;
      }
    },
    setTimeout
  };
  sandbox.window = sandbox;
  return sandbox;
}

function loadScript(relativePath, sandbox, hookSource = '') {
  const filename = path.join(ROOT, relativePath);
  let source = fs.readFileSync(filename, 'utf8');
  if (hookSource) {
    const closingMarker = '\n})();';
    const closingIndex = source.lastIndexOf(closingMarker);
    if (closingIndex < 0) {
      throw new Error(`Unable to add test hooks to ${relativePath}`);
    }
    source = `${source.slice(0, closingIndex)}\n${hookSource}${source.slice(closingIndex)}`;
  }
  vm.runInNewContext(source, sandbox, { filename });
}

test('shared admin helpers exercise the modernized string and log handling', () => {
  const sandbox = createSandbox();
  sandbox.TgHarvestDateUtils = { formatDateTime(value) { return String(value || ''); } };
  loadScript(
    'static/admin_manage_shared.js',
    sandbox,
    'window.__adminManageSharedCoverageHooks = {'
      + ' isAdminWriteRequest: isAdminWriteRequest,'
      + ' buildFetchHeaders: buildFetchHeaders,'
      + ' logMessageKind: logMessageKind,'
      + ' splitLogPrefix: splitLogPrefix'
      + ' };'
  );

  const shared = sandbox.AdminManageShared;
  const hooks = sandbox.__adminManageSharedCoverageHooks;
  shared.setAdminCsrfToken('csrf-token');

  assert.equal(hooks.isAdminWriteRequest('/api/admin/jobs', 'POST'), true);
  assert.equal(hooks.isAdminWriteRequest('/api/admin/auth/login', 'POST'), false);
  assert.equal(hooks.isAdminWriteRequest('/api/public/items', 'POST'), false);
  assert.equal(hooks.isAdminWriteRequest('/api/admin/jobs', 'GET'), false);
  assert.equal(
    hooks.buildFetchHeaders('/api/admin/jobs', { method: 'POST', headers: { Accept: 'application/json' } })['X-CSRF-Token'],
    'csrf-token'
  );
  assert.equal(shared.normalizeChats({ items: [{ chat_id: 7, chat_title: 'Source' }] })[0].chat_id, 7);
  assert.equal(shared.normalizeChats({ chats: [{ chat_id: 8 }] })[0].chat_id, 8);
  assert.equal(shared.formatNumber(1234567), '1,234,567');
  assert.equal(hooks.logMessageKind('[进度] importing'), 'progress');
  assert.equal(hooks.splitLogPrefix('[进度] importing').message, 'importing');
  assert.equal(hooks.splitLogPrefix('[1/2] importing').message, 'importing');
});

test('clone creation helpers exercise optional access and request option forwarding', async () => {
  const sandbox = createSandbox();
  let capturedSessionOptions;
  let capturedRequest;
  const shared = {
    createAdminSessionController(options) {
      capturedSessionOptions = options;
      return {
        checkAuth() {},
        handleLogin() {},
        handleUnauthorizedResponse() {}
      };
    },
    fetchJSON(url, options) {
      capturedRequest = { url, options };
      return Promise.resolve({ ok: true });
    },
    formatDateTime(value) {
      return String(value || '');
    },
    formatNumber(value) {
      return String(value || 0);
    },
    postJSON() {
      return Promise.resolve({});
    },
    setElementDisabled() {},
    trapFocusWithin() {}
  };
  sandbox.AdminManageShared = shared;
  loadScript(
    'static/admin_clone_create.js',
    sandbox,
    'window.__adminCloneCreateCoverageHooks = {'
      + ' state: state,'
      + ' buildChatOptionText: buildChatOptionText,'
      + ' fetchJSON: fetchJSON,'
      + ' findSourceChat: findSourceChat,'
      + ' inferTargetKindFromSource: inferTargetKindFromSource,'
      + ' isStartAllowed: isStartAllowed,'
      + ' renderCapabilities: renderCapabilities,'
      + ' renderRecommendation: renderRecommendation,'
      + ' syncStageState: syncStageState'
      + ' };'
  );

  const hooks = sandbox.__adminCloneCreateCoverageHooks;
  const capabilities = createElement('section');
  hooks.renderCapabilities(capabilities, [
    { status: 'requires_source', label: 'Source access', detail: 'Available' }
  ]);
  assert.equal(capabilities.children[0].children[0].children[1].className, 'clone-capability-status is-requires-source');

  const recommendation = createElement('p');
  hooks.renderRecommendation(recommendation, {
    level: 'ready',
    mode: 'clone',
    summary: 'Proceed'
  });
  assert.equal(recommendation.textContent, '评估 ready / clone：Proceed');

  hooks.state.sourceChats = [{ chat_id: 9, chat_title: 'Source', message_count: 12 }];
  assert.equal(hooks.findSourceChat('9').chat_id, 9);
  assert.equal(hooks.buildChatOptionText(hooks.state.sourceChats[0]), 'Source（12 条）');
  assert.equal(hooks.inferTargetKindFromSource('megagroup'), 'megagroup');
  assert.equal(hooks.inferTargetKindFromSource('channel'), 'channel');

  const elements = {
    sourceStage: createElement('section'),
    createStage: createElement('section'),
    targetTitleInput: { value: 'Clone' },
    confirmInput: { value: 'CONFIRM' }
  };
  hooks.state.report = null;
  hooks.syncStageState(elements);
  assert.equal(elements.sourceStage.dataset.stageState, 'current');
  hooks.state.report = { account: { secondary_session_distinct: true }, confirm: 'CONFIRM' };
  hooks.syncStageState(elements);
  assert.equal(elements.sourceStage.dataset.stageState, 'complete');
  assert.equal(elements.createStage.dataset.stageState, 'current');
  assert.equal(hooks.isStartAllowed(elements), true);

  await hooks.fetchJSON('/api/admin/clone/chats', {});
  assert.equal(capturedRequest.url, '/api/admin/clone/chats');
  assert.equal(capturedRequest.options.timeoutMs, 20000);
  assert.equal(typeof capturedRequest.options.onUnauthorized, 'function');
  assert.equal(capturedSessionOptions.getPageElement(null), null);
  const page = createElement('main');
  assert.equal(capturedSessionOptions.getPageElement({ page }), page);
});
