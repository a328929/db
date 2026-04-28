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
        self.assertNotIn("admin-incremental-checkbox", template)

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

    def test_context_page_does_not_inject_user_content_via_inner_html(self) -> None:
        source = (ROOT / "static" / "context.js").read_text(encoding="utf-8")
        self.assertNotIn("innerHTML = badgesHtml + (item.content", source)
        self.assertNotIn("function formatFileSize", source)
        self.assertNotIn("function formatDuration", source)
        self.assertNotIn("function typeToLabel", source)

    def test_context_page_uses_backend_title_fallback_for_empty_media_text(self) -> None:
        source = (ROOT / "static" / "context.js").read_text(encoding="utf-8")
        self.assertIn("item.content || item.title", source)

    def test_context_template_has_no_inline_script(self) -> None:
        template = (ROOT / "templates" / "context.html").read_text(encoding="utf-8")
        self.assertNotIn("window.PAGE_CONFIG", template)

    def test_admin_manage_uses_shared_helpers(self) -> None:
        source = (ROOT / "static" / "admin_manage.js").read_text(encoding="utf-8")
        self.assertNotIn("function normalizeChats", source)
        self.assertNotIn("function appendLog", source)
        self.assertNotIn("function pickFirstText", source)
        self.assertNotIn("function trapDialogFocus", source)
        self.assertNotIn("function getDialogFocusableElements", source)
        self.assertNotIn("incrementalCheckbox", source)
        self.assertIn("function createJobAndStartPolling", source)
        self.assertIn("function confirmAction", source)
        self.assertIn("function getTargetScopeLabel", source)
        self.assertIn("shared.getSelectedOptionLabel", source)
        self.assertIn("shared.setDialogOpenState", source)
        self.assertIn("shared.trapFocusWithin", source)
        self.assertEqual(1, source.count("任务创建成功但缺少 job_id"))

    def test_admin_progress_copy_avoids_failed_fraction_wording(self) -> None:
        source = (ROOT / "static" / "admin_manage.js").read_text(encoding="utf-8")
        self.assertIn("任务失败", source)
        self.assertIn("正在整理结果", source)
        self.assertNotIn("message: '[进度] ' + stage + ' ' + current + '/' + total", source)


if __name__ == "__main__":
    unittest.main()
