from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "pnc_release_feishu_target_common.py"
spec = importlib.util.spec_from_file_location("pnc_release_feishu_target_common", SCRIPT_PATH)
assert spec is not None
assert spec.loader is not None
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_common_accepts_expected_release_target() -> None:
    mod.ensure_release_target(mod.EXPECTED_WIKI_NODE, mod.EXPECTED_SPACE_ID)


def test_common_rejects_old_release_wiki_node() -> None:
    try:
        mod.ensure_release_target(mod.OLD_WIKI_NODE, mod.EXPECTED_SPACE_ID)
    except ValueError as exc:
        assert mod.OLD_WIKI_NODE in str(exc)
        assert mod.EXPECTED_WIKI_NODE in str(exc)
    else:
        raise AssertionError("old wiki node should be rejected")


def test_common_rejects_wrong_space_id() -> None:
    try:
        mod.ensure_release_target(mod.EXPECTED_WIKI_NODE, "bad-space")
    except ValueError as exc:
        assert "bad-space" in str(exc)
        assert mod.EXPECTED_SPACE_ID in str(exc)
    else:
        raise AssertionError("wrong space id should be rejected")
