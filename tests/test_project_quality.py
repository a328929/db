import sys
import unittest

from tools.check_project_quality import build_steps


class ProjectQualityTests(unittest.TestCase):
    def test_dependency_audit_checks_runtime_requirements(self) -> None:
        steps = build_steps(include_tests=False)

        audit_steps = [step for step in steps if step.name == "Audit Python dependencies"]

        self.assertEqual(len(audit_steps), 1)
        self.assertEqual(
            audit_steps[0].command,
            (
                sys.executable,
                "-m",
                "pip_audit",
                "-r",
                "requirements.txt",
            ),
        )


if __name__ == "__main__":
    unittest.main()
