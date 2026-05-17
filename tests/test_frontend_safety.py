import pathlib
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
        self.assertNotIn("admin-incremental-checkbox", template)
        self.assertIn("/admin/channels", template)

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

    def test_page_templates_load_page_specific_stylesheets(self) -> None:
        context_template = (ROOT / "templates" / "context.html").read_text(encoding="utf-8")
        open_telegram_template = (ROOT / "templates" / "open_telegram.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("context.css", context_template)
        self.assertIn("open_telegram.css", open_telegram_template)

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
        self.assertIn("function getTargetScopeLabel", source)
        self.assertIn("function buildSnapshotProgressMessage", source)
        self.assertIn("shared.buildSnapshotProgressMessage", source)
        self.assertIn("shared.getCreatedJobId", source)
        self.assertIn("shared.getTargetScopeLabel", source)
        self.assertIn("shared.getSelectedOptionLabel", source)
        self.assertIn("shared.setDialogOpenState", source)
        self.assertIn("shared.trapFocusWithin", source)
        self.assertNotIn("任务创建成功但缺少 job_id", source)
        self.assertEqual(1, shared_source.count("任务创建成功但缺少 job_id"))

    def test_admin_progress_copy_avoids_failed_fraction_wording(self) -> None:
        source = (ROOT / "static" / "admin_manage.js").read_text(encoding="utf-8")
        shared_source = (ROOT / "static" / "admin_manage_shared.js").read_text(
            encoding="utf-8"
        )
        self.assertIn("shared.buildSnapshotProgressMessage", source)
        self.assertIn("任务失败", shared_source)
        self.assertIn("正在整理结果", shared_source)
        self.assertNotIn("message: '[进度] ' + stage + ' ' + current + '/' + total", source)


if __name__ == "__main__":
    unittest.main()
