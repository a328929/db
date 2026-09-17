import subprocess
from pathlib import Path


def test_stale_scheduler_response_cannot_replace_current_state():
    script = r"""
    const fs = require('node:fs');
    const vm = require('node:vm');
    const assert = require('node:assert/strict');
    const pending = [];
    const sandbox = {
      window: {AdminManageShared: {
        fetchJSON: () => new Promise(resolve => pending.push(resolve)),
        setElementDisabled() {}, createAdminSessionController: () => ({})
      }}, document: {addEventListener() {}}, URLSearchParams
    };
    const source = fs.readFileSync('static/admin_sync.js', 'utf8');
    const end = source.lastIndexOf('})();');
    const expose = `
      renderSchedulerChats = function () {};
      window.testHooks = {load: loadSchedulerChats, state: syncState};
    `;
    vm.runInNewContext(source.slice(0,end) + expose + source.slice(end), sandbox);
    (async () => {
      const elements = {chatList:{}, membershipSelect:{value:'first'}, chatStatusSelect:{value:''}};
      const oldRequest = sandbox.window.testHooks.load(elements);
      elements.membershipSelect.value = 'second';
      const newRequest = sandbox.window.testHooks.load(elements);
      pending[1]({items:[{chat_id:2}]});
      await newRequest;
      pending[0]({items:[{chat_id:1}]});
      await oldRequest;
      assert.equal(sandbox.window.testHooks.state.chatItems[0].chatId, 2);
      assert.equal(sandbox.window.testHooks.state.chatListBusy, false);
    })().catch(error => { console.error(error); process.exitCode = 1; });
    """
    result = subprocess.run(
        ["node", "-e", script],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
