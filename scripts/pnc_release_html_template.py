#!/usr/bin/env python3
"""Generate product-style PNC-Agent release HTML pages.

The generated page intentionally follows the accepted 0.13.9 release-page shape:
hero, status cards, user-impact cards, compare tabs, lane cards, boundary note,
user-facing verification table, sharing section, modal details, and release-specific
lane pages.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_HTTP_BASE = "http://192.168.26.174:8088"
DEFAULT_CIFS_BASE = "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp"
DEFAULT_VM_ROOT = "/mnt/tmp"
DEFAULT_PUBLISH_ROOT = "/home/mini/workspace/minieye_ci_eval/ci_report_publish"

STYLE = r'''
:root{--bg:#f6f7fb;--card:#fff;--ink:#172033;--muted:#667085;--line:#e6e9f0;--blue:#3157d5;--green:#12805c;--amber:#8a6100;--soft:#eef3ff;--soft2:#eefaf4}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.68 -apple-system,BlinkMacSystemFont,"Segoe UI",PingFang SC,"Microsoft YaHei",sans-serif}.wrap{max-width:1040px;margin:0 auto;padding:28px 20px 56px}.hero{background:linear-gradient(135deg,#fff,#eef4ff);border:1px solid var(--line);border-radius:24px;padding:28px;box-shadow:0 18px 45px rgba(20,30,60,.08)}.kicker{color:var(--blue);font-weight:850}h1{margin:8px 0 10px;font-size:36px;line-height:1.16}.lead{font-size:18px;color:#344054;max-width:850px}.chips,.links{display:flex;gap:8px;flex-wrap:wrap;margin-top:16px}.chip,.links a,.back{display:inline-flex;align-items:center;border:1px solid #c8d5ff;background:#fff;color:var(--blue);border-radius:999px;padding:8px 12px;text-decoration:none;font-weight:760}.chip{color:#344054;border-color:var(--line);font-weight:500}.status{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-top:18px}.stat{background:#fff;border:1px solid var(--line);border-radius:16px;padding:14px}.label{color:var(--muted);font-size:12px}.value{font-size:19px;font-weight:850;margin-top:4px;color:var(--green)}.section{background:var(--card);border:1px solid var(--line);border-radius:20px;padding:20px;margin-top:16px;box-shadow:0 8px 24px rgba(20,30,60,.04)}.section h2{margin:0 0 12px;font-size:22px}.cards{display:grid;grid-template-columns:repeat(2,1fr);gap:12px}.card{border:1px solid var(--line);border-radius:18px;padding:17px;background:#fff}.card .tag{display:inline-block;background:var(--soft);color:var(--blue);border-radius:999px;padding:4px 9px;font-size:12px;font-weight:800}.card h3{margin:10px 0 8px;font-size:18px}.card p{margin:6px 0;color:#344054}.card ul{margin:8px 0 0;padding-left:18px;color:#344054}.tabs{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0}.tab{border:1px solid var(--line);background:#fff;border-radius:999px;padding:8px 12px;cursor:pointer}.tab.active{background:var(--blue);color:#fff;border-color:var(--blue)}.panel{display:none}.panel.active{display:block}.compare{display:grid;grid-template-columns:1fr auto 1fr;gap:10px}.box{border:1px solid var(--line);border-radius:14px;padding:13px}.before{background:#fff7ed}.after{background:var(--soft2)}.arrow{align-self:center;color:var(--blue);font-weight:900}.note{background:#fff7e6;border:1px solid #f0d08a;border-radius:16px;padding:14px}.small,.footer{color:var(--muted);font-size:13px}.footer{margin-top:18px}.readmore{margin-top:8px;border:0;background:var(--blue);color:#fff;border-radius:999px;padding:8px 12px;font-weight:750;cursor:pointer}.modal{position:fixed;inset:0;background:rgba(15,23,42,.42);display:none;align-items:center;justify-content:center;padding:18px;z-index:10}.modal.open{display:flex}.dialog{max-width:680px;background:#fff;border-radius:20px;padding:22px;box-shadow:0 30px 80px rgba(0,0,0,.25)}.close{float:right;border:1px solid var(--line);background:#fff;border-radius:999px;padding:6px 10px;cursor:pointer}.scenario{background:var(--soft);border-radius:14px;padding:12px;margin:10px 0}table{width:100%;border-collapse:collapse}th,td{border-bottom:1px solid var(--line);padding:10px;text-align:left;vertical-align:top}th{background:#fafbff;color:#475467}@media(max-width:760px){.status,.cards{grid-template-columns:1fr}.compare{grid-template-columns:1fr}.arrow{display:none}h1{font-size:29px}}
'''

SCRIPT = r'''document.querySelectorAll('.tab').forEach(btn=>btn.addEventListener('click',()=>{document.querySelectorAll('.tab').forEach(b=>b.classList.remove('active'));document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));btn.classList.add('active');document.getElementById(btn.dataset.tab).classList.add('active')}));document.querySelectorAll('[data-modal]').forEach(btn=>btn.addEventListener('click',()=>document.getElementById(btn.dataset.modal).classList.add('open')));document.querySelectorAll('[data-close]').forEach(btn=>btn.addEventListener('click',()=>btn.closest('.modal').classList.remove('open')));document.querySelectorAll('.modal').forEach(m=>m.addEventListener('click',e=>{if(e.target===m)m.classList.remove('open')}));document.addEventListener('keydown',e=>{if(e.key==='Escape')document.querySelectorAll('.modal').forEach(m=>m.classList.remove('open'))});'''


@dataclass(frozen=True)
class ImpactCard:
    tag: str
    title: str
    body: str
    modal_title: str
    scenario: str
    bullets: list[str]


@dataclass(frozen=True)
class CompareTab:
    tab: str
    before: str
    after: str


@dataclass(frozen=True)
class LanePage:
    filename: str
    tag: str
    title: str
    card_body: str
    summary: str
    bullets: list[str]


def _version_token(version: str) -> str:
    version = version.strip()
    if not re.fullmatch(r"\d+(?:\.\d+){1,3}(?:[-_A-Za-z0-9.]+)?", version):
        raise SystemExit(f"unsafe version: {version!r}")
    return version


def _esc(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def default_release_spec(version: str) -> dict[str, Any]:
    version = _version_token(version)
    return {
        "version": version,
        "lead": "这是一份用户侧发布页。它只讲本次发布结果、用户能感受到的变化，以及从哪里查看补充说明。",
        "chips": [f"已上线：{version}", "一个主入口", "用户视角说明", "补充内容页内跳转"],
        "status": [
            ["运行版本", version],
            ["服务状态", "已验证"],
            ["发布文档", "已验证"],
            ["主入口", "1 个"],
        ],
        "impact_cards": [
            {
                "tag": "发布流程",
                "title": "发布页、Feishu 文档、HTML 链接不再靠手工拼",
                "body": "后续 release 默认 dry-run，目录、HTML URL、closeout 会先自动校验；只有显式 execute 才真正外发。",
                "modal_title": "发布流程更自动",
                "scenario": "先跑 dry-run 校验目录、HTML、Feishu target；确认都绿后再 execute。",
                "bullets": ["减少手工漏项。", "默认不外发。", "文档入口和 HTML 页面保持一致。"],
            },
            {
                "tag": "Memory 安全",
                "title": "租户/话题内记忆不会再悄悄落到全局",
                "body": "gateway、session reset、flush 这些路径都会携带 scope，没安全作用域时直接 fail-closed。",
                "modal_title": "Memory 作用域更安全",
                "scenario": "租户/话题内的 durable write 必须有 scoped store；没有就拒绝。",
                "bullets": ["避免全局记忆被污染。", "flush/reset 路径也遵守同样规则。", "作用域问题更容易定位。"],
            },
            {
                "tag": "Feishu 追问",
                "title": "短追问更不容易串到别的任务",
                "body": "像“报告为什么没有贴出来？”这种 follow-up，会优先看当前 topic 的 task context，而不是去全局乱搜历史。",
                "modal_title": "Feishu 短追问更靠谱",
                "scenario": "用户只补一句追问时，先看当前 topic 的 task context，再决定是否需要历史搜索。",
                "bullets": ["减少串任务回答。", "fresh session 也更稳。", "当前话题状态更容易被继承。"],
            },
            {
                "tag": "Dashboard 证据",
                "title": "任务页面能直接看到证据矩阵",
                "body": "列表和详情页会显示 VM bridge、artifact、verification、missing layer，排查时不再只看一个状态字。",
                "modal_title": "Dashboard 证据可直接读",
                "scenario": "先看 evidence matrix、VM bridge、artifacts、verification，再决定是否追日志。",
                "bullets": ["减少只看 raw status 的误判。", "sidecar/shared-state 缺哪层一眼可见。", "更适合新人接手。"],
            },
        ],
        "compare_tabs": [
            {"tab": "发布闭环", "before": "目录、HTML、Feishu 文档和 closeout 容易分散，手工步骤多。", "after": "同一条 release 流程能校验目录、HTML 入口、closeout 和 Feishu 目标。"},
            {"tab": "Memory 作用域", "before": "租户/话题内写入有机会静默落进全局记忆。", "after": "没有安全 scope 就直接拒绝，避免把局部事实污染成全局记忆。"},
            {"tab": "Feishu 追问", "before": "Feishu 短追问可能误命中无关 CLI/session 历史。", "after": "优先注入当前 topic task context，并对该类 turn 阻断 cross-session search。"},
        ],
        "lanes": [
            {"filename": f"skills-lane-release-note-{version}.html", "tag": "Agent 工作方式", "title": "让发布和记忆治理更少漏项", "card_body": "说明 release flow、memory scope、topic context 这些改动为什么能减少反复手工确认。", "summary": "这一页解释为什么这次发布把 release flow、memory scope 和 topic context 串成了更稳的闭环。", "bullets": ["发布流程默认 dry-run，防止误发。", "memory 写入按作用域 fail-closed，不再悄悄落进全局。", "Feishu topic follow-up 优先使用当前任务上下文。"]},
            {"filename": f"workspace-knowledge-lane-release-note-{version}.html", "tag": "知识与边界", "title": "让 live runtime 和文档边界更清楚", "card_body": "说明 workspace / release 文档 / HTML 入口为什么各自有明确边界，不再混成一团。", "summary": "这一页只讲用户能感受到的边界，不混写实现过程。", "bullets": ["主发布页是唯一对外入口。", "Feishu verified 文档是可检索记录，不取代 HTML 主入口。", "workspace/runbook/skills 的作用是帮助复盘和复用，而不是堆给最终用户。"]},
            {"filename": f"tools-tests-lane-release-note-{version}.html", "tag": "工具与测试", "title": "交付结果可以直接验证", "card_body": "说明 closeout、html gate、browser gate、focused tests、artifact 校验怎么一起构成这次交付证据。", "summary": "这一页说明这次交付为什么不是“说完成了”，而是有可验证证据。", "bullets": ["HTML gate 确认 VM HTTP 页面存在且可读。", "browser gate 确认结构、modal、tab、lane 页面可用。", "focused tests、artifact sha256、runtime health 都有 readback。"]},
            {"filename": f"release-lanes-index-{version}.html", "tag": "完整索引", "title": "查看完整 release scope", "card_body": "如果你要审计更完整的变更范围，从索引页进入；普通阅读只看主页面即可。", "summary": "给需要审计完整范围的人看。", "bullets": [f"PNC-Agent {version} 对应 release 已进入统一 VM HTTP HTML 主入口。", "外部发布闭环包括 VM HTML、Feishu verified doc、host/VM health readback。", "普通用户只需要主页面，审计者再看本索引。"]},
        ],
        "boundary_note": "这页只展示最终可用的发布结果。内部测试命令、实现细节、历史脏树和非 canonical 文档不放进用户主阅读路径。",
        "verification_rows": [
            ["运行版本", f"PNC-Agent {version} 已完成 release 切分。"],
            ["服务状态", "Host gateway 和 VM 核心服务已做运行态 readback。"],
            ["交付页面", "VM HTTP 主链接可直接打开 HTML，并通过 HTML/browser gate。"],
            ["发布文档", "Feishu verified 文档作为可检索发布记录。"],
        ],
        "sharing": "对外只发当前主发布页链接；需要时再从页内进入补充说明。Feishu verified 文档作为可检索记录保留，不替代主发布页。",
    }


def _load_spec(version: str, spec_file: str = "") -> dict[str, Any]:
    spec = default_release_spec(version)
    if spec_file:
        override = json.loads(Path(spec_file).read_text(encoding="utf-8"))
        spec.update(override)
        spec["version"] = _version_token(str(spec.get("version") or version))
    return spec


def _html_shell(title: str, body: str) -> str:
    return f'<!doctype html>\n<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{_esc(title)}</title>\n<style>{STYLE}</style></head><body><main class="wrap">{body}<script>{SCRIPT}</script>\n</main></body></html>\n'


def render_lane_page(version: str, lane: dict[str, Any]) -> str:
    bullets = "".join(f"<li>{_esc(item)}</li>" for item in lane.get("bullets", []))
    body = (
        f'<a class="back" href="pnc-agent-runtime-patch-release-{_esc(version)}.html">返回主发布页</a>'
        f'<section class="hero"><div class="kicker">PNC-Agent Release {_esc(version)}</div><h1>{_esc(lane["title"])}</h1><p class="lead">{_esc(lane.get("summary", ""))}</p></section>'
        f'<section class="section"><h2>用户会感受到什么</h2><ul>{bullets}</ul></section>'
    )
    return _html_shell(f'{lane["title"]} - PNC-Agent Release {version}', body)


def render_main_page(spec: dict[str, Any]) -> str:
    version = _version_token(str(spec["version"]))
    chips = "".join(f'<span class="chip">{_esc(chip)}</span>' for chip in spec.get("chips", []))
    status = "".join(f'<div class="stat"><div class="label">{_esc(k)}</div><div class="value">{_esc(v)}</div></div>' for k, v in spec.get("status", []))
    cards = []
    modals = []
    for idx, card in enumerate(spec.get("impact_cards", []), start=1):
        modal_id = f"m{idx}"
        cards.append(
            f'<article class="card"><span class="tag">{_esc(card["tag"])}</span><h3>{_esc(card["title"])}</h3><p>{_esc(card["body"])}</p><button class="readmore" data-modal="{modal_id}">怎么使用</button></article>'
        )
        bullets = "".join(f"<li>{_esc(item)}</li>" for item in card.get("bullets", []))
        modals.append(
            f'<div class="modal" id="{modal_id}"><div class="dialog"><button class="close" data-close>关闭</button><h3>{_esc(card.get("modal_title", card["title"]))}</h3><div class="scenario"><b>使用方式：</b>{_esc(card.get("scenario", "打开主页面后按需进入对应补充说明。"))}</div><ul>{bullets}</ul></div></div>'
        )
    tab_buttons = []
    panels = []
    for idx, tab in enumerate(spec.get("compare_tabs", []), start=1):
        panel_id = f"p{idx}"
        active = " active" if idx == 1 else ""
        tab_buttons.append(f'<button class="tab{active}" data-tab="{panel_id}">{_esc(tab["tab"])}</button>')
        panels.append(f'<div id="{panel_id}" class="panel{active}"><div class="compare"><div class="box before"><b>之前</b><br>{_esc(tab["before"])}</div><div class="arrow">→</div><div class="box after"><b>现在</b><br>{_esc(tab["after"])}</div></div></div>')
    lane_cards = []
    for lane in spec.get("lanes", []):
        lane_cards.append(
            f'<article class="card"><span class="tag">{_esc(lane["tag"])}</span><h3>{_esc(lane["title"])}</h3><p>{_esc(lane["card_body"])}</p><a class="back" href="{_esc(lane["filename"])}">打开说明</a></article>'
        )
    rows = "".join(f"<tr><td>{_esc(k)}</td><td>{_esc(v)}</td></tr>" for k, v in spec.get("verification_rows", []))
    body = f'''
<section class="hero"><div class="kicker">PNC-Agent Release {_esc(version)}</div><h1>PNC-Agent {_esc(version)}：一个入口，看懂这版对你有什么用</h1><p class="lead">{_esc(spec.get("lead", ""))}</p></section>
<section class="section"><div class="chips">{chips}</div><div class="status">{status}</div></section>
<section class="section"><h2>用户会直接感受到的变化</h2><div class="cards">{''.join(cards)}</div></section>
<section class="section"><h2>前后对比</h2><div class="tabs">{''.join(tab_buttons)}</div>{''.join(panels)}</section>
<section class="section"><h2>补充内容从这里进入</h2><p>对外只分享当前主发布页。技能、知识、工具测试这些补充内容，按用户价值拆到下面几个子页里。</p><div class="cards">{''.join(lane_cards)}</div></section>
<section class="section"><h2>这版不混写什么</h2><div class="note">{_esc(spec.get("boundary_note", ""))}</div></section>
<section class="section"><h2>内部验收状态（用户版）</h2><table><thead><tr><th>检查项</th><th>结论</th></tr></thead><tbody>{rows}</tbody></table></section>
<section class="section"><h2>分享方式</h2><p>{_esc(spec.get("sharing", ""))}</p></section>
<div class="footer">自包含 HTML，无远程脚本。主页面是唯一对外入口。</div>
{''.join(modals)}
'''
    return _html_shell(f"PNC-Agent {version}：一个入口，看懂这版对你有什么用", body)


def render_release_pages(version: str, spec_file: str = "") -> dict[str, str]:
    spec = _load_spec(version, spec_file=spec_file)
    version = _version_token(str(spec["version"]))
    html_name = f"pnc-agent-runtime-patch-release-{version}.html"
    pages = {html_name: render_main_page(spec)}
    for lane in spec.get("lanes", []):
        pages[str(lane["filename"])] = render_lane_page(version, lane)
    return pages


def write_release_pages(
    version: str,
    *,
    output_root: Path,
    spec_file: str = "",
    publish_root: Path | None = None,
    http_base: str = DEFAULT_HTTP_BASE,
    cifs_base: str = DEFAULT_CIFS_BASE,
) -> dict[str, Any]:
    version = _version_token(version)
    rel_dir = Path("pnc-agent-release") / f"pnc-agent-release-{version}" / "release"
    target_dir = output_root / rel_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    pages = render_release_pages(version, spec_file=spec_file)
    for name, content in pages.items():
        (target_dir / name).write_text(content, encoding="utf-8")
    if publish_root is not None:
        publish_dir = publish_root / rel_dir
        publish_dir.parent.mkdir(parents=True, exist_ok=True)
        if publish_dir.exists() or publish_dir.is_symlink():
            if publish_dir.is_symlink() or publish_dir.is_file():
                publish_dir.unlink()
        if not publish_dir.exists():
            publish_dir.symlink_to(target_dir)
    files = {}
    for p in sorted(target_dir.glob("*")):
        if p.is_file():
            data = p.read_bytes()
            files[p.name] = {"size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    receipt = {
        "ok": True,
        "version": version,
        "target_dir": str(target_dir),
        "http_url": f"{http_base.rstrip('/')}/{rel_dir.as_posix()}/pnc-agent-runtime-patch-release-{version}.html",
        "cifs_dir": f"{cifs_base.rstrip('/')}/{rel_dir.as_posix()}/",
        "files": files,
        "generated_at": time.time(),
    }
    (target_dir / "vm-publish-verification.json").write_text(json.dumps(receipt, ensure_ascii=False, indent=2), encoding="utf-8")
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output-root", default=DEFAULT_VM_ROOT)
    parser.add_argument("--publish-root", default=DEFAULT_PUBLISH_ROOT)
    parser.add_argument("--spec-file", default="")
    parser.add_argument("--http-base", default=DEFAULT_HTTP_BASE)
    parser.add_argument("--cifs-base", default=DEFAULT_CIFS_BASE)
    parser.add_argument("--no-publish-symlink", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    result = write_release_pages(
        args.version,
        output_root=Path(args.output_root),
        spec_file=args.spec_file,
        publish_root=None if args.no_publish_symlink else Path(args.publish_root),
        http_base=args.http_base,
        cifs_base=args.cifs_base,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"PNC_RELEASE_HTML_TEMPLATE PASS version={result['version']}")
        print(result["http_url"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
