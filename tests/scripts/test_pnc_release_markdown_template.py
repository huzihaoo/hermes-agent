from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "pnc_release_markdown_template.py"
spec = importlib.util.spec_from_file_location("pnc_release_markdown_template", SCRIPT_PATH)
assert spec is not None
assert spec.loader is not None
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_build_release_markdown_has_html_marker_and_standard_sections() -> None:
    text = mod.build_release_markdown(
        version="0.14.3",
        highlights=["Release 文档发布链路标准化"],
        validations=["focused pytest passed"],
        rollback=["保留 dry-run，不自动外发"],
    )
    assert text.startswith("# PNC-Agent Release 0.14.3")
    assert "<!-- PNC_RELEASE_HTML_URL -->" in text
    assert "## 用户会直接感受到的变化" in text
    assert "- Release 文档发布链路标准化" in text
    assert "- focused pytest passed" in text
    assert "- 保留 dry-run，不自动外发" in text


def test_main_writes_output_and_json_receipt(tmp_path: Path, capsys) -> None:
    out = tmp_path / "release.md"
    rc = mod.main([
        "--version",
        "0.14.3",
        "--highlight",
        "a;b",
        "--validation",
        "gate passed",
        "--output",
        str(out),
        "--json",
    ])
    assert rc == 0
    receipt = capsys.readouterr().out
    assert '"ok": true' in receipt
    text = out.read_text(encoding="utf-8")
    assert "- a" in text
    assert "- b" in text
    assert "<!-- PNC_RELEASE_HTML_URL -->" in text


def test_build_result_reports_marker() -> None:
    result = mod.build_result(version="0.14.3", output=None, content="x <!-- PNC_RELEASE_HTML_URL -->")
    assert result["ok"] is True
    assert result["has_html_url_marker"] is True
    assert result["output"] == ""
