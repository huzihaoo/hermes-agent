from __future__ import annotations

from pathlib import Path

from scripts.pnc_release_browser_gate import validate


def _write_html(base: Path, version: str) -> None:
    rel = base / "pnc-agent-release" / f"pnc-agent-release-{version}" / "release"
    rel.mkdir(parents=True)
    main = rel / f"pnc-agent-runtime-patch-release-{version}.html"
    main.write_text(
        f'''<!doctype html><html><head><title>PNC-Agent {version}：一个入口，看懂这版对你有什么用</title></head>
<body>
<h1>PNC-Agent {version}：一个入口，看懂这版对你有什么用</h1>
<h2>用户会直接感受到的变化</h2>
<h2>前后对比</h2>
<h2>补充内容从这里进入</h2>
<h2>这版不混写什么</h2>
<h2>内部验收状态（用户版）</h2>
<h2>分享方式</h2>
<button class="readmore" data-modal="m1">怎么使用</button>
<button class="readmore" data-modal="m2">怎么使用</button>
<button class="readmore" data-modal="m3">怎么使用</button>
<button class="readmore" data-modal="m4">怎么使用</button>
<div class="modal" id="m1">使用方式：<ul><li>a</li></ul></div>
<div class="modal" id="m2">使用方式：<ul><li>b</li></ul></div>
<div class="modal" id="m3">使用方式：<ul><li>c</li></ul></div>
<div class="modal" id="m4">使用方式：<ul><li>d</li></ul></div>
<button class="tab" data-tab="p1"></button><button class="tab" data-tab="p2"></button><button class="tab" data-tab="p3"></button>
<div class="panel" id="p1"></div><div class="panel" id="p2"></div><div class="panel" id="p3"></div>
<a class="card" href="skills-lane-release-note-{version}.html">skills</a>
<a class="card" href="workspace-knowledge-lane-release-note-{version}.html">workspace</a>
<a class="card" href="tools-tests-lane-release-note-{version}.html">tools</a>
<a class="card" href="release-lanes-index-{version}.html">index</a>
<script>
 document.querySelectorAll('[data-modal]'); document.querySelectorAll('.modal');
 document.querySelectorAll('.tab'); document.querySelectorAll('.panel');
</script>
</body></html>''',
        encoding="utf-8",
    )
    for name, title in [
        (f"skills-lane-release-note-{version}.html", "Agent 做事方法改进：用户会感受到什么"),
        (f"workspace-knowledge-lane-release-note-{version}.html", "可复用知识与交付资产：用户会感受到什么"),
        (f"tools-tests-lane-release-note-{version}.html", "交付工具与验收保障：用户会感受到什么"),
        (f"release-lanes-index-{version}.html", f"PNC-Agent {version}：release lanes 索引"),
    ]:
        (rel / name).write_text(f"<!doctype html><title>{title}</title>", encoding="utf-8")


def test_browser_gate_passes_for_complete_release_page(tmp_path):
    version = "0.13.10"
    _write_html(tmp_path, version)
    result = validate(version, http_base=tmp_path.as_uri())
    assert result["ok"] is True


def test_browser_gate_fails_when_modal_contract_missing(tmp_path):
    version = "0.13.10"
    _write_html(tmp_path, version)
    main = tmp_path / "pnc-agent-release" / f"pnc-agent-release-{version}" / "release" / f"pnc-agent-runtime-patch-release-{version}.html"
    text = main.read_text(encoding="utf-8").replace('data-modal="m4"', 'data-modal="missing"')
    main.write_text(text, encoding="utf-8")
    result = validate(version, http_base=tmp_path.as_uri())
    assert result["ok"] is False
    failed = {c["name"] for c in result["checks"] if not c["ok"]}
    assert "modal:m4" in failed
