"""测试 dom_utils.js 的集成和正确性"""

import re
from pathlib import Path


def test_dom_utils_file_exists():
    """验证 dom_utils.js 文件存在"""
    dom_utils_path = Path(__file__).parent.parent / 'static' / 'dom_utils.js'
    assert dom_utils_path.exists(), "dom_utils.js should exist"


def test_dom_utils_exports_to_window():
    """验证 dom_utils.js 导出到全局对象"""
    dom_utils_path = Path(__file__).parent.parent / 'static' / 'dom_utils.js'
    content = dom_utils_path.read_text()

    assert 'window.TgHarvestDomUtils' in content, "Should export TgHarvestDomUtils"
    assert 'createTextElement:' in content, "Should export createTextElement"
    assert 'createInfoPill:' in content, "Should export createInfoPill"
    assert 'createChannelInfoPill:' in content, "Should export createChannelInfoPill"
    assert 'createRecoveryInfoPill:' in content, "Should export createRecoveryInfoPill"


def test_admin_channels_removed_duplication():
    """验证 admin_channels.js 移除了重复的DOM函数"""
    channels_path = Path(__file__).parent.parent / 'static' / 'admin_channels.js'
    content = channels_path.read_text()

    # 应该引用 dom_utils
    assert 'TgHarvestDomUtils' in content, "Should reference TgHarvestDomUtils"

    # 不应该有重复的实现
    assert content.count('function createTextElement(') == 0, \
        "Should not contain duplicate createTextElement"
    assert content.count('function createInfoPill(') == 0, \
        "Should not contain duplicate createInfoPill"


def test_admin_sync_removed_duplication():
    """验证 admin_sync.js 移除了重复的DOM函数"""
    sync_path = Path(__file__).parent.parent / 'static' / 'admin_sync.js'
    content = sync_path.read_text()

    # 应该引用 dom_utils
    assert 'TgHarvestDomUtils' in content, "Should reference TgHarvestDomUtils"

    # 不应该有重复的实现
    assert content.count('function createTextElement(') == 0, \
        "Should not contain duplicate createTextElement"


def test_admin_recovery_removed_duplication():
    """验证 admin_recovery.js 移除了重复的DOM函数"""
    recovery_path = Path(__file__).parent.parent / 'static' / 'admin_recovery.js'
    content = recovery_path.read_text()

    # 应该引用 dom_utils
    assert 'TgHarvestDomUtils' in content, "Should reference TgHarvestDomUtils"

    # 不应该有重复的实现
    assert content.count('function createTextElement(') == 0, \
        "Should not contain duplicate createTextElement"
    assert content.count('function createInfoPill(') == 0, \
        "Should not contain duplicate createInfoPill"


def test_templates_load_dom_utils_first():
    """验证相关模板先加载 dom_utils.js"""
    templates_dir = Path(__file__).parent.parent / 'templates'

    templates_to_check = [
        ('admin_channels.html', 'admin_channels.js'),
        ('admin_sync.html', 'admin_sync.js'),
        ('admin_recovery.html', 'admin_recovery.js'),
    ]

    for template_name, dependent_file in templates_to_check:
        template_path = templates_dir / template_name
        content = template_path.read_text()

        # 查找 dom_utils.js 和依赖文件的位置
        dom_utils_match = re.search(r"dom_utils\.js", content)
        dependent_match = re.search(rf"{dependent_file}", content)

        assert dom_utils_match, f"{template_name} should include dom_utils.js"
        assert dependent_match, f"{template_name} should include {dependent_file}"

        # dom_utils.js 应该在依赖文件之前
        assert dom_utils_match.start() < dependent_match.start(), \
            f"{template_name}: dom_utils.js must be loaded before {dependent_file}"


def test_code_reduction():
    """验证代码行数减少"""
    static_dir = Path(__file__).parent.parent / 'static'

    channels_lines = len((static_dir / 'admin_channels.js').read_text().splitlines())
    sync_lines = len((static_dir / 'admin_sync.js').read_text().splitlines())
    recovery_lines = len((static_dir / 'admin_recovery.js').read_text().splitlines())

    # admin_channels.js 应该少于 1196 行（原来）
    assert channels_lines < 1196, f"admin_channels.js should be < 1196 lines, got {channels_lines}"

    # admin_sync.js 应该少于 1473 行（原来）
    assert sync_lines < 1473, f"admin_sync.js should be < 1473 lines, got {sync_lines}"

    # admin_recovery.js 应该少于 823 行（原来）
    assert recovery_lines < 823, f"admin_recovery.js should be < 823 lines, got {recovery_lines}"
