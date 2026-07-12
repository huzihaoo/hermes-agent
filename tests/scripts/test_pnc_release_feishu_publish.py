from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "pnc_release_feishu_publish.py"
spec = importlib.util.spec_from_file_location("pnc_release_feishu_publish", SCRIPT_PATH)
assert spec is not None
assert spec.loader is not None
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_build_payload_uses_guarded_release_target() -> None:
    payload = mod.build_payload(version="0.14.2", title="", content="# hi")
    assert payload["parentNodeToken"] == "DWcXwxUwIiJoIAkgSbFclfcfnLd"
    assert payload["targetId"] == "7558826224870490114"
    assert payload["targetType"] == "wiki"
    assert payload["title"] == "PNC-Agent Release 0.14.2"


def test_build_metadata_reports_new_wiki_target() -> None:
    meta = mod.build_metadata(version="0.14.2", title="")
    assert meta == {
        "version": "0.14.2",
        "title": "PNC-Agent Release 0.14.2",
        "wiki_node": "DWcXwxUwIiJoIAkgSbFclfcfnLd",
        "space_id": "7558826224870490114",
        "wiki_url": "https://minieye.feishu.cn/wiki/DWcXwxUwIiJoIAkgSbFclfcfnLd",
    }


def test_main_emits_payload_json(tmp_path: Path, capsys) -> None:
    content_file = tmp_path / "release.md"
    content_file.write_text("# release\n", encoding="utf-8")
    rc = mod.main(["--version", "0.14.2", "--content-file", str(content_file), "--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    assert rc == 0
    assert data["ok"] is True
    assert data["payload"]["parentNodeToken"] == "DWcXwxUwIiJoIAkgSbFclfcfnLd"
    assert data["payload"]["content"] == "# release\n"


def test_main_rejects_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.md"
    try:
        mod.main(["--version", "0.14.2", "--content-file", str(missing), "--json"])
    except SystemExit as exc:
        assert "content file not found" in str(exc)
    else:
        raise AssertionError("missing content file should fail")
