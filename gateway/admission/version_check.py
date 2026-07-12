#!/usr/bin/env python3
"""Admission version bump helper — ensures consistency across all version references."""

import re
import sys
from pathlib import Path

ADMISSION_DIR = Path(__file__).parent


def get_current_version() -> str:
    """Read current version from __init__.py."""
    init_file = ADMISSION_DIR / "__init__.py"
    content = init_file.read_text()
    match = re.search(r'__version__\s*=\s*"([^"]+)"', content)
    if not match:
        raise ValueError("Cannot find __version__ in __init__.py")
    return match.group(1)


def validate_version_format(version: str) -> bool:
    """Check if version follows semver MAJOR.MINOR.PATCH."""
    return bool(re.match(r'^\d+\.\d+\.\d+$', version))


def check_changelog_has_version(version: str) -> bool:
    """Check if CHANGELOG.md has an entry for this version."""
    changelog = ADMISSION_DIR / "CHANGELOG.md"
    content = changelog.read_text()
    pattern = rf'\[{re.escape(version)}\]'
    return bool(re.search(pattern, content))


def check_readme_version(version: str) -> bool:
    """Check if README.md references the current version."""
    readme = ADMISSION_DIR / "README.md"
    content = readme.read_text()
    return version in content


def run_tests() -> bool:
    """Run admission test suite."""
    import subprocess
    import glob

    # Use venv python if available
    venv_python = ADMISSION_DIR.parent.parent / "venv" / "bin" / "python3"
    python_cmd = str(venv_python) if venv_python.exists() else "python3"

    # Expand glob pattern
    test_dir = ADMISSION_DIR.parent.parent / "tests" / "gateway"
    test_files = glob.glob(str(test_dir / "test_admission_*.py"))

    if not test_files:
        print(f"\n✗ No test files found in {test_dir}")
        return False

    result = subprocess.run(
        [python_cmd, "-m", "pytest"] + test_files + ["-q"],
        cwd=ADMISSION_DIR.parent.parent,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"\nTest output:\n{result.stdout}\n{result.stderr}")
    return result.returncode == 0


def main():
    print("🔍 Admission Version Consistency Check\n")

    # 1. Get current version
    try:
        version = get_current_version()
        print(f"✓ Current version: {version}")
    except Exception as e:
        print(f"✗ Failed to read version: {e}")
        return 1

    # 2. Validate format
    if not validate_version_format(version):
        print(f"✗ Invalid version format: {version} (expected: MAJOR.MINOR.PATCH)")
        return 1
    print(f"✓ Version format valid")

    # 3. Check CHANGELOG
    if not check_changelog_has_version(version):
        print(f"✗ CHANGELOG.md missing entry for [{version}]")
        return 1
    print(f"✓ CHANGELOG.md has [{version}] entry")

    # 4. Check README
    if not check_readme_version(version):
        print(f"⚠ README.md doesn't reference {version} (optional)")
    else:
        print(f"✓ README.md references {version}")

    # 5. Run tests
    print("\n🧪 Running test suite...")
    if not run_tests():
        print("✗ Tests failed")
        return 1
    print("✓ All tests passed")

    print(f"\n✅ Version {version} is consistent and ready to commit")
    return 0


if __name__ == "__main__":
    sys.exit(main())
