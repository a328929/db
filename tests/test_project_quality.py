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

    def test_test_run_exports_coverage_for_sonarqube(self) -> None:
        steps = build_steps(include_tests=True)
        commands_by_name = {step.name: step.command for step in steps}

        self.assertEqual(
            commands_by_name["Run tests with coverage"],
            (sys.executable, "-m", "coverage", "run", "-m", "pytest"),
        )
        self.assertEqual(
            commands_by_name["Export coverage report"],
            (sys.executable, "-m", "coverage", "xml", "-o", "coverage.xml"),
        )
        self.assertEqual(
            commands_by_name["Run static JavaScript tests with coverage"],
            (
                "node",
                "--test",
                "--experimental-test-coverage",
                "--test-reporter=lcov",
                "--test-reporter-destination=static-coverage.lcov",
                "tests/static_coverage.test.js",
            ),
        )


if __name__ == "__main__":
    unittest.main()
