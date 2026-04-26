#!/usr/bin/env python3
"""Global version check — validates all versioned modules."""

from __future__ import annotations
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent

# Modules with version management
VERSIONED_MODULES = [
    ("gateway/admission", "Admission Control"),
    ("gateway/tasks", "Task Product Layer"),
    ("gateway/observability", "Observability"),
    ("agent/session_health.py", "Session Health"),
]


def check_module_version(module_path: str, module_name: str) -> tuple[bool, str]:
    """Check if a module has __version__ defined."""
    full_path = REPO_ROOT / module_path
    
    if not full_path.exists():
        return False, f"✗ {module_name}: path not found"
    
    # For .py files, read directly
    if module_path.endswith(".py"):
        try:
            content = full_path.read_text()
            if '__version__' in content:
                # Extract version
                import re
                match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
                if match:
                    version = match.group(1)
                    return True, f"✓ {module_name}: v{version}"
                else:
                    return False, f"✗ {module_name}: __version__ found but cannot parse"
            else:
                return False, f"✗ {module_name}: no __version__"
        except Exception as e:
            return False, f"✗ {module_name}: {e}"
    
    # For directories, check __init__.py
    init_file = full_path / "__init__.py"
    if not init_file.exists():
        return False, f"✗ {module_name}: no __init__.py"
    
    try:
        content = init_file.read_text()
        if '__version__' in content:
            import re
            match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
            if match:
                version = match.group(1)
                return True, f"✓ {module_name}: v{version}"
            else:
                return False, f"✗ {module_name}: __version__ found but cannot parse"
        else:
            return False, f"✗ {module_name}: no __version__"
    except Exception as e:
        return False, f"✗ {module_name}: {e}"


def main():
    print("🔍 Global Version Check\n")
    
    all_ok = True
    for module_path, module_name in VERSIONED_MODULES:
        ok, msg = check_module_version(module_path, module_name)
        print(msg)
        if not ok:
            all_ok = False
    
    print()
    
    if all_ok:
        print("✅ All modules have version management")
        return 0
    else:
        print("❌ Some modules missing version management")
        return 1


if __name__ == "__main__":
    sys.exit(main())
