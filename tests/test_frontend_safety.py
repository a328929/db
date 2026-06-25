import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent


class FrontendSafetyTests(unittest.TestCase):
    def test_templates_load_shared_display_helpers(self) -> None:
        index_template = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
        context_template = (ROOT / "templates" / "context.html").read_text(encoding="utf-8")
        self.assertIn("display_helpers.js", index_template)
        self.assertIn("display_helpers.js", context_template)

    def test_admin_template_loads_shared_admin_helpers(self) -> None:
        template = (ROOT / "templates" / "admin_manage.html").read_text(encoding="utf-8")
        self.assertIn("admin_manage_shared.js", template)
        self.assertIn("admin_manage.css", template)
        self.assertIn('id="adminLogsHelp"', template)
        self.assertIn('aria-describedby="adminLogsHelp"', template)
        self.assertIn('id="admin-log-container"', template)
        self.assertNotIn("admin-incremental-checkbox", template)
        self.assertIn("admin-delete-empty-chats-btn", template)
        self.assertIn("/admin/channels", template)
        self.assertIn("/admin/clone", template)
        self.assertIn("/admin/clone/runs/manage", template)
        self.assertIn("/admin/recovery", template)
        self.assertIn("克隆工作台", template)
        self.assertIn("克隆记录列表", template)

    def test_shared_admin_log_helpers_build_focusable_list_entries(self) -> None:
        source = (ROOT / "static" / "admin_manage_shared.js").read_text(
            encoding="utf-8"
        )
        styles = (ROOT / "static" / "admin_manage.css").read_text(encoding="utf-8")
        self.assertIn("document.createElement('ol')", source)
        self.assertIn("document.createElement('li')", source)
        self.assertIn("list.className = 'admin-log-list'", source)
        self.assertIn("placeholder.setAttribute('tabindex', '0')", source)
        self.assertIn("line.setAttribute('tabindex', '0')", source)
        self.assertNotIn("buildAccessibleLogText", source)
        self.assertNotIn("list.setAttribute('aria-live'", source)
        self.assertNotIn("list.setAttribute('aria-relevant'", source)
        self.assertNotIn("list.setAttribute('aria-atomic'", source)
        self.assertIn(".admin-log-list {", styles)
        self.assertIn(".admin-log-placeholder {", styles)

    def test_admin_log_templates_do_not_describe_live_broadcast(self) -> None:
        for template_name in (
            "admin_manage.html",
            "admin_channels.html",
            "admin_recovery.html",
            "admin_clone.html",
            "admin_clone_create.html",
            "admin_clone_migrate.html",
            "admin_clone_runs.html",
            "admin_clone_run_detail.html",
        ):
            template = (ROOT / "templates" / template_name).read_text(
                encoding="utf-8"
            )
            self.assertNotIn("自动播报", template, template_name)

    def test_admin_channels_template_loads_shared_helpers(self) -> None:
        template = (ROOT / "templates" / "admin_channels.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("admin_manage_shared.js", template)
        self.assertIn("admin_channels.js", template)
        self.assertIn("admin_channels.css", template)
        self.assertIn("admin-absent-list-toggle-btn", template)
        self.assertIn("admin-scan-absent-btn", template)
        self.assertIn("admin-restricted-list-toggle-btn", template)
        self.assertIn("admin-scan-restricted-btn", template)
        self.assertIn("admin-restricted-filter-select", template)
        self.assertNotIn("搜索框", template)

    def test_admin_recovery_template_loads_shared_helpers(self) -> None:
        template = (ROOT / "templates" / "admin_recovery.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("admin_manage_shared.js", template)
        self.assertIn("admin_recovery.js", template)
        self.assertIn("admin_recovery.css", template)
        self.assertIn("admin-recovery-scan-btn", template)
        self.assertIn("admin-recovery-restore-all-btn", template)
        self.assertIn("admin-recovery-list", template)

    def test_admin_clone_hub_template_is_navigation_only(self) -> None:
        template = (ROOT / "templates" / "admin_clone.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("admin_clone.css", template)
        self.assertIn("克隆工作台", template)
        self.assertIn("eyebrow", template)
        self.assertIn("/admin/clone/create", template)
        self.assertIn("/admin/clone/migrate", template)
        self.assertIn("/admin/clone/runs/manage", template)
        self.assertIn("clone-hub-card", template)
        self.assertNotIn("admin_manage_shared.js", template)
        self.assertNotIn("admin_clone.js", template)
        self.assertNotIn("admin-clone-source-select", template)

    def test_admin_clone_create_template_loads_shared_helpers(self) -> None:
        template = (ROOT / "templates" / "admin_clone_create.html").read_text(
            encoding="utf-8"
        )
        source = (ROOT / "static" / "admin_clone.js").read_text(encoding="utf-8")
        self.assertIn("admin_manage_shared.js", template)
        self.assertIn("admin_clone.js", template)
        self.assertIn("admin_clone.css", template)
        self.assertIn('data-clone-mode="create"', template)
        self.assertIn("admin-clone-source-select", template)
        self.assertIn("admin-clone-preflight-btn", template)
        self.assertIn("admin-clone-start-btn", template)
        self.assertIn("admin-clone-runs-list", template)
        self.assertIn("/admin/clone/migrate", template)
        self.assertNotIn("admin-clone-plan-summary", template)
        self.assertNotIn("admin-clone-message-limit-input", template)
        self.assertNotIn("下一步：迁移历史消息", template)
        self.assertIn("function isCreatePage(elements)", source)
        self.assertIn("function getSourcePlaceholderText(elements)", source)
        self.assertIn("空副本创建完成后，这里会出现“继续到迁移页”。", source)
        self.assertIn("查看记录详情", source)
        self.assertIn("var CLONE_MODE_CREATE = 'create';", source)

    def test_admin_clone_migrate_template_loads_shared_helpers(self) -> None:
        template = (ROOT / "templates" / "admin_clone_migrate.html").read_text(
            encoding="utf-8"
        )
        source = (ROOT / "static" / "admin_clone.js").read_text(encoding="utf-8")
        self.assertIn("admin_manage_shared.js", template)
        self.assertIn("admin_clone.js", template)
        self.assertIn("admin_clone.css", template)
        self.assertIn('data-clone-mode="migrate"', template)
        self.assertIn("admin-clone-runs-list", template)
        self.assertIn("admin-clone-plan-summary", template)
        self.assertIn("admin-clone-timeline-summary", template)
        self.assertIn("admin-clone-message-limit-input", template)
        self.assertIn("admin-clone-send-delay-input", template)
        self.assertNotIn("admin-clone-start-btn", template)
        self.assertIn("function isMigratePage(elements)", source)
        self.assertIn("function buildPlanStatusText(plan)", source)
        self.assertIn("function syncUrlRunId(elements, runId)", source)
        self.assertIn("function buildTimelineMigrationOptions", source)
        self.assertNotIn("消息数从小到大", template)
        self.assertEqual(1, source.count("message_limit:"))
        self.assertIn("send_delay_ms:", source)
        self.assertIn("source_copy_without_attribution", source)
        self.assertIn("隐藏来源复制转发", source)
        self.assertNotIn("/resolve-media", source)
        self.assertNotIn("/migrate-media", source)
        self.assertNotIn("/migrate-text", source)
        self.assertIn("/migrate-timeline", source)
        self.assertNotIn("clone_media_resolve_preflight", source)
        self.assertNotIn("clone_media_migration", source)
        self.assertNotIn("clone_text_migration", source)
        self.assertIn("clone_timeline_migration", source)
        self.assertIn("function renderTimelineMigration", source)
        self.assertIn("function isTimelineMigrationAllowed", source)
        self.assertIn("preview.can_migrate_timeline !== true", source)
        self.assertIn("cloneState.timelineMigration = payload && payload.timeline_migration", source)
        self.assertIn("function appendTimelinePreviewSummary", source)
        self.assertIn("'剩余时间线'", source)
        self.assertIn("'文本剩余'", source)
        self.assertNotIn("'数据库风险组'", source)

    def test_admin_clone_runs_template_loads_shared_helpers(self) -> None:
        template = (ROOT / "templates" / "admin_clone_runs.html").read_text(
            encoding="utf-8"
        )
        source = (ROOT / "static" / "admin_clone_runs.js").read_text(
            encoding="utf-8"
        )
        self.assertIn("admin_manage_shared.js", template)
        self.assertIn("admin_clone_runs.js", template)
        self.assertIn("admin_clone.css", template)
        self.assertIn("admin-clone-runs-manage-list", template)
        self.assertNotIn("admin-clone-runs-limit-select", template)
        self.assertIn("/api/admin/clone/runs", source)
        self.assertIn("function buildRunMigrationHref(run)", source)
        self.assertIn("function buildRunDetailHref(runId)", source)
        self.assertIn("查看记录详情", source)
        self.assertNotIn("run_id", template)
        self.assertNotIn("源消息数", source)
        self.assertNotIn("localStorage", source)
        self.assertNotIn("innerHTML", source)

    def test_admin_clone_run_detail_template_loads_shared_helpers(self) -> None:
        template = (ROOT / "templates" / "admin_clone_run_detail.html").read_text(
            encoding="utf-8"
        )
        source = (ROOT / "static" / "admin_clone_run_detail.js").read_text(
            encoding="utf-8"
        )
        self.assertIn("admin_manage_shared.js", template)
        self.assertIn("admin_clone_run_detail.js", template)
        self.assertIn("admin_clone.css", template)
        self.assertIn("admin-clone-runs-detail-summary", template)
        self.assertIn("admin-clone-runs-next-step", template)
        self.assertIn("admin-clone-runs-delete-help", template)
        self.assertIn("admin-clone-runs-failure-summary-text", template)
        self.assertIn("admin-clone-runs-mapping-summary-text", template)
        self.assertIn("admin-clone-run-delete-dialog", template)
        self.assertNotIn("admin-clone-runs-mapping-mode-filter", template)
        self.assertIn("/detail", source)
        self.assertIn("method: 'DELETE'", source)
        self.assertIn("function buildNextStepText(payload)", source)
        self.assertIn("function renderDetailActions(elements, run, payload)", source)
        self.assertIn("当前将删除 “", source)
        self.assertIn("window.location.assign('/admin/clone/runs/manage');", source)
        self.assertIn("admin-clone-run-detail-migrate-nav-link", source)
        self.assertNotIn("Run Detail", template)
        self.assertNotIn("localStorage", source)
        self.assertNotIn("innerHTML", source)

    def test_admin_login_template_uses_standalone_login_script(self) -> None:
        template = (ROOT / "templates" / "admin_login.html").read_text(
            encoding="utf-8"
        )
        source = (ROOT / "static" / "admin_login.js").read_text(encoding="utf-8")
        self.assertIn('id="admin-login-page"', template)
        self.assertIn("admin_login.js", template)
        self.assertIn("data-next-path", template)
        self.assertIn("ALLOWED_NEXT_PATHS", source)
        self.assertIn("/admin/clone", source)
        self.assertIn("/admin/clone/create", source)
        self.assertIn("/admin/clone/migrate", source)
        self.assertIn("/admin/clone/runs/manage", source)
        self.assertIn("/admin/clone/runs/detail", source)
        self.assertIn("/admin/recovery", source)
        self.assertIn("window.location.assign(getNextPath(elements));", source)
        self.assertIn("payload.error.trim()", source)

    def test_page_templates_load_page_specific_stylesheets(self) -> None:
        context_template = (ROOT / "templates" / "context.html").read_text(encoding="utf-8")
        open_telegram_template = (ROOT / "templates" / "open_telegram.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("context.css", context_template)
        self.assertIn("open_telegram.css", open_telegram_template)

    def test_open_telegram_fallback_copy_matches_rendered_web_link(self) -> None:
        template = (ROOT / "templates" / "open_telegram.html").read_text(
            encoding="utf-8"
        )
        source = (ROOT / "static" / "open_telegram.js").read_text(encoding="utf-8")
        self.assertIn('id="openTelegramWebLink"', template)
        self.assertIn('document.getElementById("openTelegramWebLink")', source)
        self.assertIn("if (webLink && webLink.href)", source)

    def test_search_results_do_not_inject_user_content_via_inner_html(self) -> None:
        source = (ROOT / "static" / "app.js").read_text(encoding="utf-8")
        self.assertNotIn("innerHTML = badgesHtml + (item.title", source)
        self.assertNotIn("function formatFileSize", source)
        self.assertNotIn("function formatDuration", source)
        self.assertNotIn("function typeToLabel", source)

    def test_search_count_cache_is_keyed_by_backend_data_version(self) -> None:
        source = (ROOT / "static" / "app.js").read_text(encoding="utf-8")
        self.assertIn("data_version: String(dataVersion || \"\")", source)
        self.assertIn("_getCachedCount(data.data_version)", source)

    def test_background_count_version_change_does_not_force_refresh_prompt(self) -> None:
        source = (ROOT / "static" / "app.js").read_text(encoding="utf-8")
        self.assertNotIn("数据库已更新，请重新搜索以刷新结果。", source)
        self.assertIn(
            "data.data_version = countData.data_version || data.data_version;",
            source,
        )

    def test_background_count_updates_summary_without_rerendering_results(self) -> None:
        source = (ROOT / "static" / "app.js").read_text(encoding="utf-8")
        self.assertIn("function applyCountToRenderedResults(payload)", source)
        self.assertGreaterEqual(source.count("applyCountToRenderedResults(data);"), 2)

    def test_search_form_changes_cancel_pending_background_count(self) -> None:
        source = (ROOT / "static" / "app.js").read_text(encoding="utf-8")
        self.assertIn("function _markSearchCriteriaDirty()", source)
        self.assertIn("currentSearchId += 1;", source)
        self.assertIn('els.queryInput.addEventListener("input"', source)
        self.assertIn('els.scopeSelect.addEventListener("change"', source)
        self.assertIn('els.startDateInput.addEventListener("input"', source)
        self.assertIn('els.endDateInput.addEventListener("input"', source)

    def test_search_template_exposes_accessible_text_date_filters(self) -> None:
        template = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
        self.assertIn('id="startDateInput"', template)
        self.assertIn('id="endDateInput"', template)
        self.assertIn('id="clearStartDateBtn"', template)
        self.assertIn('id="clearEndDateBtn"', template)
        self.assertIn('type="text"', template)
        self.assertIn('inputmode="numeric"', template)
        self.assertIn('aria-describedby="dateRangeHelp"', template)
        self.assertIn('aria-label="清空开始日期"', template)
        self.assertIn('aria-label="清空结束日期"', template)
        self.assertIn("留空表示不限时间", template)

    def test_date_filter_clear_buttons_only_show_when_inputs_have_values(self) -> None:
        source = (ROOT / "static" / "app.js").read_text(encoding="utf-8")
        self.assertIn("function updateDateClearButtons()", source)
        self.assertIn("els.clearStartDateBtn.hidden", source)
        self.assertIn("els.clearEndDateBtn.hidden", source)
        self.assertIn("clearDateInput(els.startDateInput, els.clearStartDateBtn)", source)
        self.assertIn("clearDateInput(els.endDateInput, els.clearEndDateBtn)", source)

    def test_date_filter_clear_buttons_disable_while_searching(self) -> None:
        source = (ROOT / "static" / "app.js").read_text(encoding="utf-8")
        self.assertIn("els.clearStartDateBtn.disabled = isSearching;", source)
        self.assertIn("els.clearEndDateBtn.disabled = isSearching;", source)

    def test_search_page_has_group_facets_region(self) -> None:
        template = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
        source = (ROOT / "static" / "app.js").read_text(encoding="utf-8")
        self.assertIn('id="groupFacets"', template)
        self.assertIn('aria-label="按群组聚合的命中统计"', template)
        self.assertIn("function renderGroupFacets(facets)", source)
        self.assertIn("button.className = \"group-facet-btn\"", source)
        self.assertIn("data.chat_facets = countData.chat_facets || []", source)

    def test_context_page_does_not_inject_user_content_via_inner_html(self) -> None:
        source = (ROOT / "static" / "context.js").read_text(encoding="utf-8")
        self.assertNotIn("innerHTML = badgesHtml + (item.content", source)
        self.assertNotIn("function formatFileSize", source)
        self.assertNotIn("function formatDuration", source)
        self.assertNotIn("function typeToLabel", source)

    def test_frontend_scripts_avoid_inner_html(self) -> None:
        for script_path in (ROOT / "static").glob("*.js"):
            source = script_path.read_text(encoding="utf-8")
            self.assertNotIn("innerHTML", source, str(script_path))

    def test_context_page_uses_backend_title_fallback_for_empty_media_text(self) -> None:
        source = (ROOT / "static" / "context.js").read_text(encoding="utf-8")
        self.assertIn("item.content || item.title", source)

    def test_context_pagination_buttons_track_loading_and_exhaustion(self) -> None:
        source = (ROOT / "static" / "context.js").read_text(encoding="utf-8")
        self.assertIn("hasMoreBefore", source)
        self.assertIn("hasMoreAfter", source)
        self.assertIn("function updateLoadButtonState()", source)
        self.assertIn("function markDirectionExhausted(direction)", source)
        self.assertIn("items.length < 100", source)

    def test_context_template_has_no_inline_script(self) -> None:
        template = (ROOT / "templates" / "context.html").read_text(encoding="utf-8")
        self.assertNotIn("window.PAGE_CONFIG", template)

    def test_admin_manage_uses_shared_helpers(self) -> None:
        source = (ROOT / "static" / "admin_manage.js").read_text(encoding="utf-8")
        shared_source = (ROOT / "static" / "admin_manage_shared.js").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("function normalizeChats", source)
        self.assertNotIn("function appendLog", source)
        self.assertNotIn("function pickFirstText", source)
        self.assertNotIn("function trapDialogFocus", source)
        self.assertNotIn("function getDialogFocusableElements", source)
        self.assertNotIn("incrementalCheckbox", source)
        self.assertIn("function createJobAndStartPolling", source)
        self.assertIn("function confirmAction", source)
        self.assertIn("function handleDeleteEmptyChatsClick", source)
        self.assertIn("/api/admin/jobs/delete-empty-chats", source)
        self.assertIn("function getTargetScopeLabel", source)
        self.assertIn("function buildSnapshotProgressMessage", source)
        self.assertIn("shared.buildSnapshotProgressMessage", source)
        self.assertIn("shared.getCreatedJobId", source)
        self.assertIn("shared.getTargetScopeLabel", source)
        self.assertIn("shared.getSelectedOptionLabel", source)
        self.assertIn("shared.setDialogOpenState", source)
        self.assertIn("shared.trapFocusWithin", source)
        self.assertIn(
            "onUnauthorized: sessionController.handleUnauthorizedResponse", source
        )
        self.assertNotIn("onUnauthorized: handleUnauthorizedResponse", source)
        self.assertIn("elements && elements.loginDialog", source)
        self.assertNotIn("任务创建成功但缺少 job_id", source)
        self.assertEqual(1, shared_source.count("任务创建成功但缺少 job_id"))
        self.assertIn("line.setAttribute('role', 'listitem');", shared_source)
        self.assertIn("placeholder.setAttribute('role', 'listitem');", shared_source)
        self.assertIn("line.setAttribute('tabindex', '0');", shared_source)

    def test_admin_channels_traps_login_dialog_focus(self) -> None:
        source = (ROOT / "static" / "admin_channels.js").read_text(encoding="utf-8")
        self.assertIn("shared.trapFocusWithin", source)
        self.assertIn("document.addEventListener('keydown'", source)
        self.assertIn("trapFocusWithin(elements.loginDialog, event);", source)

    def test_admin_progress_copy_avoids_failed_fraction_wording(self) -> None:
        source = (ROOT / "static" / "admin_manage.js").read_text(encoding="utf-8")
        shared_source = (ROOT / "static" / "admin_manage_shared.js").read_text(
            encoding="utf-8"
        )
        self.assertIn("shared.buildSnapshotProgressMessage", source)
        self.assertIn("任务失败", shared_source)
        self.assertIn("正在整理结果", shared_source)
        self.assertNotIn("message: '[进度] ' + stage + ' ' + current + '/' + total", source)

    def test_admin_login_feedback_uses_inline_status_instead_of_alerts(self) -> None:
        manage_template = (ROOT / "templates" / "admin_manage.html").read_text(
            encoding="utf-8"
        )
        channels_template = (ROOT / "templates" / "admin_channels.html").read_text(
            encoding="utf-8"
        )
        manage_source = (ROOT / "static" / "admin_manage.js").read_text(
            encoding="utf-8"
        )
        channels_source = (ROOT / "static" / "admin_channels.js").read_text(
            encoding="utf-8"
        )
        self.assertIn('id="admin-login-status"', manage_template)
        self.assertIn('id="admin-login-status"', channels_template)
        self.assertIn("function setLoginStatus(elements, message)", manage_source)
        self.assertIn("function setLoginStatus(elements, message)", channels_source)
        self.assertNotIn("alert(", manage_source)
        self.assertNotIn("alert(", channels_source)
        self.assertNotIn("window.location.reload", manage_source)
        self.assertNotIn("window.location.reload", channels_source)

    def test_admin_recovery_traps_login_dialog_focus(self) -> None:
        source = (ROOT / "static" / "admin_recovery.js").read_text(encoding="utf-8")
        self.assertIn("shared.trapFocusWithin", source)
        self.assertIn("document.addEventListener('keydown'", source)
        self.assertIn("trapFocusWithin(elements.loginDialog, event);", source)
        self.assertNotIn("alert(", source)
        self.assertNotIn("window.location.reload", source)

    def test_admin_clone_traps_login_dialog_focus(self) -> None:
        source = (ROOT / "static" / "admin_clone.js").read_text(encoding="utf-8")
        self.assertIn("shared.trapFocusWithin", source)
        self.assertIn("document.addEventListener('keydown'", source)
        self.assertIn("trapFocusWithin(elements.loginDialog, event);", source)
        self.assertIn("/api/admin/clone/runs", source)
        self.assertIn("/deep-preflight", source)
        self.assertNotIn("/migrate-text", source)
        self.assertIn("/migrate-timeline", source)
        self.assertIn("clone_timeline_migration", source)
        self.assertNotIn("clone_text_migration", source)
        self.assertIn("admin-clone-plan-summary", source)
        self.assertNotIn("alert(", source)
        self.assertNotIn("window.location.reload", source)

    def test_admin_recovery_ready_filter_requires_empty_database_chat(self) -> None:
        source = (ROOT / "static" / "admin_recovery.js").read_text(encoding="utf-8")
        self.assertIn(
            "return !isCandidatePending(item) && Number((item && item.message_count) || 0) <= 0;",
            source,
        )
        self.assertIn("return items.filter(isCandidateReady);", source)

    def test_admin_recovery_busy_state_keeps_readonly_controls_enabled(self) -> None:
        source = (ROOT / "static" / "admin_recovery.js").read_text(encoding="utf-8")
        self.assertIn("data-recovery-job-action", source)
        self.assertIn("var isJobAction = button.getAttribute('data-recovery-job-action') === 'true';", source)
        self.assertNotIn("setElementDisabled(elements.refreshBtn, disabled);", source)
        self.assertNotIn("setElementDisabled(elements.filterSelect, disabled);", source)
        self.assertNotIn("setElementDisabled(elements.listToggleBtn, disabled);", source)
        self.assertNotIn("copyBtn.disabled = recoveryState.busy;", source)

    def test_card_action_buttons_have_item_specific_accessible_names(self) -> None:
        channels_source = (ROOT / "static" / "admin_channels.js").read_text(
            encoding="utf-8"
        )
        recovery_source = (ROOT / "static" / "admin_recovery.js").read_text(
            encoding="utf-8"
        )

        self.assertIn("function getChannelActionLabel(item)", channels_source)
        self.assertIn(
            "copyBtn.setAttribute('aria-label', '复制 ' + channelLabel + ' 的群组或频道信息');",
            channels_source,
        )
        self.assertIn(
            "deleteBtn.setAttribute('aria-label', '从数据库删除 ' + channelLabel + ' 的全部数据');",
            channels_source,
        )
        restricted_start = channels_source.index("function renderRestrictedChannels")
        restricted_end = channels_source.index("async function loadRestrictedChannels")
        restricted_render_source = channels_source[restricted_start:restricted_end]
        self.assertIn(
            "actions: createChannelActions(item, elements, { allowDelete: true }),",
            restricted_render_source,
        )
        delete_start = channels_source.index("async function handleDeleteChannelData")
        delete_end = channels_source.index("function startJobPolling")
        delete_source = channels_source[delete_start:delete_end]
        self.assertIn("await loadRestrictedChannels(elements);", delete_source)
        self.assertIn("function getCandidateActionLabel(item)", recovery_source)
        self.assertIn(
            "copyBtn.setAttribute('aria-label', '复制 ' + candidateLabel + ' 的恢复候选信息');",
            recovery_source,
        )
        self.assertIn(
            "restoreBtn.setAttribute('aria-label', '恢复 ' + candidateLabel + ' 的群组或频道摘要到数据库');",
            recovery_source,
        )

    def test_admin_shared_fetch_attaches_csrf_header_to_write_requests(self) -> None:
        source = (ROOT / "static" / "admin_manage_shared.js").read_text(
            encoding="utf-8"
        )
        self.assertIn("var adminCsrfToken = '';", source)
        self.assertIn("function setAdminCsrfToken(token)", source)
        self.assertIn("function buildFetchHeaders(url, requestOptions)", source)
        self.assertIn("headers['X-CSRF-Token'] = adminCsrfToken;", source)
        self.assertIn("setAdminCsrfToken(payload.csrf_token);", source)
        self.assertIn("return normalizedUrl !== '/api/admin/auth/login';", source)

    def test_admin_shared_fetch_prefers_server_error_message(self) -> None:
        source = (ROOT / "static" / "admin_manage_shared.js").read_text(
            encoding="utf-8"
        )
        self.assertIn("function buildResponseErrorMessage(response, payload)", source)
        self.assertIn("return serverMessage;", source)
        self.assertNotIn("数据库忙或系统异常", source)

    def test_admin_login_overlay_hides_background_from_focus_and_assistive_tech(self) -> None:
        source = (ROOT / "static" / "admin_manage_shared.js").read_text(
            encoding="utf-8"
        )
        self.assertIn("function setPageInteractionState(pageElement, interactive)", source)
        self.assertIn("pageElement.setAttribute('aria-hidden', 'true');", source)
        self.assertIn("pageElement.setAttribute('inert', '');", source)
        self.assertIn("pageElement.inert = true;", source)
        self.assertIn("pageElement.removeAttribute('aria-hidden');", source)
        self.assertIn("pageElement.removeAttribute('inert');", source)
        self.assertIn("pageElement.inert = false;", source)

    def test_admin_page_scripts_consistently_route_unauthorized_callbacks(self) -> None:
        for script_name in (
            "admin_channels.js",
            "admin_clone.js",
            "admin_clone_run_detail.js",
            "admin_clone_runs.js",
            "admin_manage.js",
            "admin_recovery.js",
        ):
            source = (ROOT / "static" / script_name).read_text(encoding="utf-8")
            matches = re.findall(
                r"onUnauthorized:\s*([A-Za-z0-9_.]+)", source
            )
            self.assertTrue(matches, script_name)
            self.assertEqual(
                {"sessionController.handleUnauthorizedResponse"},
                set(matches),
                script_name,
            )
            self.assertNotIn(
                "onUnauthorized: handleUnauthorizedResponse",
                source,
                script_name,
            )


if __name__ == "__main__":
    unittest.main()
