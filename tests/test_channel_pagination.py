import sqlite3
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from flask import Flask

from tg_harvest.admin_jobs.channel_inventory import _scan_missing_chat_rows
from tg_harvest.storage.channel_management import list_database_channel_page
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema
from tg_harvest.web.routes.channels import register_channel_routes


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    create_schema(connection, detect_sqlite_features(connection))
    connection.executemany(
        "INSERT INTO chats(chat_id,chat_title,message_count) VALUES (?,?,?)",
        [(i, f"Chat {i}", i) for i in range(1, 251)],
    )
    connection.execute(
        "INSERT INTO sync_chat_state(chat_id,membership_scope) VALUES (250,'both_joined')"
    )
    connection.commit()
    yield connection
    connection.close()


def test_channel_pages_have_stable_order_and_filter_globally(conn):
    first = list_database_channel_page(conn, sort="message_count_desc", limit=100)
    second = list_database_channel_page(
        conn, sort="message_count_desc", limit=100, offset=100
    )
    assert first["total"] == 250
    assert [row["chat_id"] for row in first["channels"]] == list(range(250, 150, -1))
    assert [row["chat_id"] for row in second["channels"]] == list(range(150, 50, -1))
    unknown = list_database_channel_page(
        conn, sort="message_count_desc", membership="unknown", limit=100
    )
    assert unknown["total"] == 249
    assert unknown["all_count"] == 250
    assert unknown["channels"][0]["chat_id"] == 249
    clamped = list_database_channel_page(
        conn, sort="updated_desc", membership="both_joined", offset=1000
    )
    assert clamped["offset"] == 0
    assert len(clamped["channels"]) == 1


def test_paginated_channel_route_validates_and_returns_links(conn):
    class BorrowedConnection:
        def __getattr__(self, name):
            return getattr(conn, name)

        def close(self):
            pass

    app = Flask(__name__)
    register_channel_routes(
        app,
        logger=Mock(),
        get_conn_fn=BorrowedConnection,
        build_telegram_chat_link_bundle_fn=lambda *args, **kwargs: SimpleNamespace(
            app_link="tg://test", web_link=""
        ),
    )
    with patch("tg_harvest.web.auth.is_authenticated", return_value=True):
        client = app.test_client()
        response = client.get(
            "/api/admin/channels?limit=40&offset=40&membership=unknown"
        )
        assert response.status_code == 200
        payload = response.get_json()
        assert payload["total"] == 249
        assert len(payload["channels"]) == 40
        assert payload["channels"][0]["telegram_app_link"] == "tg://test"
        for query in (
            "limit=0",
            "limit=201",
            "limit=10&offset=-1",
            "limit=10&membership=invalid",
        ):
            assert client.get("/api/admin/channels?" + query).status_code == 400


def test_incomplete_account_scan_cannot_replace_previous_snapshot():
    with (
        patch(
            "tg_harvest.admin_jobs.channel_inventory.call_with_conn", return_value=set()
        ),
        patch(
            "tg_harvest.admin_jobs.channel_inventory._scan_account_rows",
            side_effect=[[], None],
        ),
        pytest.raises(RuntimeError, match="已保留上次完整扫描结果"),
    ):
        _scan_missing_chat_rows(
            cfg=SimpleNamespace(
                session_name="main", secondary_session_name="secondary"
            ),
            get_conn_fn=lambda: None,
            admin_job_append_log_fn=lambda *args: None,
            job_id="test",
        )


def test_page_number_tracks_rendered_data_and_failed_requests_roll_back():
    script = r"""
    const fs = require('node:fs');
    const vm = require('node:vm');
    const assert = require('node:assert/strict');
    const pending = [];
    const sandbox = {
      document: {addEventListener() {}}, URLSearchParams,
      window: {AdminManageShared: {
        fetchJSON: () => new Promise((resolve,reject) => pending.push({resolve,reject})),
        setElementDisabled(element, disabled) { if (element) element.disabled = disabled; },
        createAdminSessionController: () => ({}), createAdminJobPollController: () => ({})
      }}
    };
    const source = fs.readFileSync('static/admin_channels.js', 'utf8');
    const end = source.lastIndexOf('})();');
    const expose = `
      renderChannels = function(elements, items) { window.rendered = items; };
      window.hooks = {load:loadChannels, state:channelState};
    `;
    vm.runInNewContext(source.slice(0,end) + expose + source.slice(end), sandbox);
    (async () => {
      const hooks = sandbox.window.hooks;
      Object.assign(hooks.state, {total:250, allCount:250, offset:100});
      const elements = {
        sortSelect:{value:'message_count_desc'}, channelCount:{},
        channelList:{setAttribute(name,value) { this[name] = value; }},
        prevPage:{}, nextPage:{}, pageStatus:{}
      };
      const failed = hooks.load(elements);
      assert.equal(elements.pageStatus.textContent, '1 / 3');
      assert.equal(elements.nextPage.disabled, true);
      assert.equal(elements.channelList['aria-busy'], 'true');
      pending[0].reject(new Error('network unavailable'));
      await failed;
      assert.equal(hooks.state.offset, 0);
      assert.equal(elements.pageStatus.textContent, '1 / 3');
      assert.equal(elements.channelList['aria-busy'], 'false');
      hooks.state.offset = 100;
      const success = hooks.load(elements);
      pending[1].resolve({ok:true,channels:[{chat_id:150}],total:250,all_count:250,offset:100});
      await success;
      assert.equal(elements.pageStatus.textContent, '2 / 3');
      assert.equal(sandbox.window.rendered[0].chat_id, 150);
    })().catch(error => { console.error(error); process.exitCode = 1; });
    """
    result = subprocess.run(
        ["node", "-e", script],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
