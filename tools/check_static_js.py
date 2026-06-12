import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATIC_DIR = ROOT / "static"


def main() -> int:
    node_bin = shutil.which("node")
    if not node_bin:
        print("node executable not found; install Node.js to check static JavaScript", file=sys.stderr)
        return 1

    js_files = sorted(STATIC_DIR.glob("*.js"))
    if not js_files:
        print("No static JavaScript files found", file=sys.stderr)
        return 1

    failed = False
    for path in js_files:
        rel_path = path.relative_to(ROOT)
        result = subprocess.run(
            [node_bin, "--check", str(path)],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0:
            print(f"[OK] {rel_path}")
            continue

        failed = True
        print(f"[FAIL] {rel_path}", file=sys.stderr)
        if result.stdout:
            print(result.stdout, file=sys.stderr, end="" if result.stdout.endswith("\n") else "\n")
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="" if result.stderr.endswith("\n") else "\n")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
