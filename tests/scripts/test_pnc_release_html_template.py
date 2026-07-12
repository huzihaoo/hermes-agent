from __future__ import annotations

from pathlib import Path

from scripts.pnc_release_browser_gate import validate as browser_validate
from scripts.pnc_release_html_template import render_release_pages, write_release_pages


def test_render_release_pages_uses_0139_product_shape() -> None:
    version = "0.14.9"
    pages = render_release_pages(version)
    main = pages[f"pnc-agent-runtime-patch-release-{version}.html"]
    assert "PNC-Agent 0.14.9：一个入口，看懂这版对你有什么用" in main
    assert "class=\"hero\"" in main
    assert "class=\"status\"" in main
    assert "用户会直接感受到的变化" in main
    assert "前后对比" in main
    assert "补充内容从这里进入" in main
    assert "这版不混写什么" in main
    assert "内部验收状态（用户版）" in main
    assert "分享方式" in main
    assert main.count("data-modal=\"m") >= 4
    assert main.count("class=\"tab") >= 3
    assert "release-lanes-index-0.14.9.html" in main


def test_write_release_pages_passes_browser_gate(tmp_path: Path) -> None:
    version = "0.14.9"
    result = write_release_pages(version, output_root=tmp_path, publish_root=None, http_base=tmp_path.as_uri())
    assert result["ok"] is True
    assert (tmp_path / "pnc-agent-release" / f"pnc-agent-release-{version}" / "release" / f"pnc-agent-runtime-patch-release-{version}.html").is_file()
    gate = browser_validate(version, http_base=tmp_path.as_uri())
    assert gate["ok"] is True
    assert all(check["ok"] for check in gate["checks"])
