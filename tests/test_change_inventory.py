import unittest

from tools.change_inventory import (
    StatusEntry,
    classify_path,
    find_forbidden_entries,
    format_inventory,
    parse_status_porcelain_z,
)


class ChangeInventoryTests(unittest.TestCase):
    def test_classifies_key_paths_by_theme(self) -> None:
        cases = {
            ".github/workflows/ci.yml": "CI / Dev Tooling",
            "tg_harvest/web/auth.py": "Security / Admin Auth",
            "tg_harvest/web/routes/search.py": "Security / Admin Auth",
            "tg_harvest/admin_jobs/core.py": "Admin Jobs / Runtime Recovery",
            "static/admin_channels.js": "Frontend Admin UX / JS Safety",
            "tg_harvest/search/service.py": "Search / Storage / Ingest Correctness",
            "tg_harvest/runtime/paths.py": "Search / Storage / Ingest Correctness",
            "tools/recover_missing_media.py": "Operator Tools / Telegram Scripts",
            "docs/change_management.md": "Documentation",
            "tests/test_search_service.py": "Tests",
        }

        for path, expected_theme in cases.items():
            with self.subTest(path=path):
                self.assertEqual(expected_theme, classify_path(path))

    def test_parse_status_porcelain_z_handles_untracked_and_renamed_paths(self) -> None:
        raw_status = b" M README.md\0?? docs/change_management.md\0R  new.py\0old.py\0"

        entries = parse_status_porcelain_z(raw_status)

        self.assertEqual(
            [
                StatusEntry(status=" M", path="README.md"),
                StatusEntry(status="??", path="docs/change_management.md"),
                StatusEntry(status="R ", path="new.py", old_path="old.py"),
            ],
            entries,
        )

    def test_find_forbidden_entries_detects_runtime_and_secret_paths(self) -> None:
        entries = [
            StatusEntry(status="??", path=".runtime/db/tg_data.db"),
            StatusEntry(status="??", path=".env"),
            StatusEntry(status=" M", path="tg_harvest/web/auth.py"),
        ]

        forbidden_entries = find_forbidden_entries(entries)

        self.assertEqual(entries[:2], forbidden_entries)

    def test_format_inventory_groups_entries_with_counts(self) -> None:
        inventory = format_inventory(
            [
                StatusEntry(status=" M", path="tg_harvest/web/auth.py"),
                StatusEntry(status="??", path="tools/check_project_quality.py"),
            ]
        )

        self.assertIn("Total changed paths: 2", inventory)
        self.assertIn("## CI / Dev Tooling (1)", inventory)
        self.assertIn("- ?? tools/check_project_quality.py", inventory)
        self.assertIn("## Security / Admin Auth (1)", inventory)
        self.assertIn("- M tg_harvest/web/auth.py", inventory)


if __name__ == "__main__":
    unittest.main()
