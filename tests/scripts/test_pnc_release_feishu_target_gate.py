from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "pnc_release_feishu_target_gate.py"
spec = importlib.util.spec_from_file_location("pnc_release_feishu_target_gate", SCRIPT_PATH)
assert spec is not None
assert spec.loader is not None
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_gate_defaults_match_expected_constants() -> None:
    result = mod.run(mod.EXPECTED_WIKI_NODE, mod.EXPECTED_SPACE_ID, mod.DEFAULT_FEISHU_WIKI_URL)
    checks = {c["name"]: c for c in result["checks"]}
    assert result["actual"]["wiki_node"] == "DWcXwxUwIiJoIAkgSbFclfcfnLd"
    assert result["actual"]["space_id"] == "7558826224870490114"
    assert checks["target_node_matches_expected"]["ok"] is True
    assert checks["space_id_matches_expected"]["ok"] is True
    assert checks["wiki_url_matches_expected"]["ok"] is True


def test_gate_rejects_old_release_folder() -> None:
    result = mod.run("Wp5awZTinieUjTkyNaYcxAWenpe", mod.EXPECTED_SPACE_ID, mod.DEFAULT_FEISHU_WIKI_URL)
    checks = {c["name"]: c for c in result["checks"]}
    assert result["ok"] is False
    assert checks["target_node_matches_expected"]["ok"] is False


def test_gate_rejects_wrong_space_id() -> None:
    result = mod.run(mod.EXPECTED_WIKI_NODE, "wrong-space", mod.DEFAULT_FEISHU_WIKI_URL)
    checks = {c["name"]: c for c in result["checks"]}
    assert result["ok"] is False
    assert checks["space_id_matches_expected"]["ok"] is False
