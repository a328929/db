"""测试 date_utils.js 的集成和正确性"""

import re


def test_date_utils_file_exists():
    """验证 date_utils.js 文件存在"""
    from pathlib import Path

    date_utils_path = Path(__file__).parent.parent / 'static' / 'date_utils.js'
    assert date_utils_path.exists(), "date_utils.js should exist"


def test_date_utils_exports_to_window():
    """验证 date_utils.js 导出到全局对象"""
    from pathlib import Path

    date_utils_path = Path(__file__).parent.parent / 'static' / 'date_utils.js'
    content = date_utils_path.read_text()

    assert 'window.TgHarvestDateUtils' in content, "Should export TgHarvestDateUtils"
    assert 'parseUtcDateTime:' in content, "Should export parseUtcDateTime"
    assert 'formatShanghaiDateTime:' in content, "Should export formatShanghaiDateTime"
    assert 'formatDateTime:' in content, "Should export formatDateTime"


def test_display_helpers_removed_duplication():
    """验证 display_helpers.js 移除了重复的日期格式化代码"""
    from pathlib import Path

    helpers_path = Path(__file__).parent.parent / 'static' / 'display_helpers.js'
    content = helpers_path.read_text()

    # 应该引用 date_utils
    assert 'TgHarvestDateUtils' in content, "Should reference TgHarvestDateUtils"

    # 不应该有重复的实现
    assert 'SHANGHAI_DATE_TIME_FORMATTER = new Intl.DateTimeFormat' not in content, \
        "Should not contain duplicate formatter"
    assert 'function parseUtcDateTime(' not in content, \
        "Should not contain duplicate parseUtcDateTime"
    assert 'function formatShanghaiDateTime(' not in content, \
        "Should not contain duplicate formatShanghaiDateTime"


def test_admin_manage_shared_removed_duplication():
    """验证 admin_manage_shared.js 移除了重复的日期格式化代码"""
    from pathlib import Path

    shared_path = Path(__file__).parent.parent / 'static' / 'admin_manage_shared.js'
    content = shared_path.read_text()

    # 应该引用 date_utils
    assert 'TgHarvestDateUtils' in content, "Should reference TgHarvestDateUtils"

    # 不应该有重复的实现
    assert content.count('SHANGHAI_DATE_TIME_FORMATTER = new Intl.DateTimeFormat') == 0, \
        "Should not contain duplicate formatter"
    assert 'function parseUtcDateTime(' not in content, \
        "Should not contain duplicate parseUtcDateTime"
    assert 'function formatShanghaiDateTime(' not in content, \
        "Should not contain duplicate formatShanghaiDateTime"


def test_all_admin_templates_load_date_utils_first():
    """验证所有管理页面模板先加载 date_utils.js"""
    from pathlib import Path

    templates_dir = Path(__file__).parent.parent / 'templates'

    admin_templates = [
        'admin_channels.html',
        'admin_clone.html',
        'admin_clone_create.html',
        'admin_clone_message_delete.html',
        'admin_clone_run_detail.html',
        'admin_clone_runs.html',
        'admin_manage.html',
        'admin_recovery.html',
        'admin_sync.html',
    ]

    for template_name in admin_templates:
        template_path = templates_dir / template_name
        content = template_path.read_text()

        # 查找 date_utils.js 和 admin_manage_shared.js 的位置
        date_utils_match = re.search(r"date_utils\.js", content)
        shared_match = re.search(r"admin_manage_shared\.js", content)

        assert date_utils_match, f"{template_name} should include date_utils.js"
        assert shared_match, f"{template_name} should include admin_manage_shared.js"

        # date_utils.js 应该在 admin_manage_shared.js 之前
        assert date_utils_match.start() < shared_match.start(), \
            f"{template_name}: date_utils.js must be loaded before admin_manage_shared.js"


def test_frontend_templates_load_date_utils_first():
    """验证前端搜索页面模板先加载 date_utils.js"""
    from pathlib import Path

    templates_dir = Path(__file__).parent.parent / 'templates'

    frontend_templates = [
        ('index.html', 'display_helpers.js'),
        ('context.html', 'display_helpers.js'),
    ]

    for template_name, dependent_file in frontend_templates:
        template_path = templates_dir / template_name
        content = template_path.read_text()

        # 查找 date_utils.js 和 display_helpers.js 的位置
        date_utils_match = re.search(r"date_utils\.js", content)
        helpers_match = re.search(rf"{dependent_file}", content)

        assert date_utils_match, f"{template_name} should include date_utils.js"
        assert helpers_match, f"{template_name} should include {dependent_file}"

        # date_utils.js 应该在 display_helpers.js 之前
        assert date_utils_match.start() < helpers_match.start(), \
            f"{template_name}: date_utils.js must be loaded before {dependent_file}"


def test_code_reduction():
    """验证代码行数显著减少"""
    from pathlib import Path

    display_helpers_path = Path(__file__).parent.parent / 'static' / 'display_helpers.js'
    admin_shared_path = Path(__file__).parent.parent / 'static' / 'admin_manage_shared.js'

    display_lines = len(display_helpers_path.read_text().splitlines())
    admin_lines = len(admin_shared_path.read_text().splitlines())

    # display_helpers.js 应该少于 100 行（原来 144 行）
    assert display_lines < 100, f"display_helpers.js should be < 100 lines, got {display_lines}"

    # admin_manage_shared.js 应该少于 1200 行（原来 1247 行）
    assert admin_lines < 1200, f"admin_manage_shared.js should be < 1200 lines, got {admin_lines}"
