import pytest

import json

from gateway.feishu_task_card import (
    INTERNAL_HTML_HIDDEN_TEXT,
    assert_no_forbidden_fragments,
    render_status_line,
    render_task_card,
)
from scripts.pnc_foxglove_delivery import canonical_viz_mcap_path, foxglove_url


def _dump(card):
    import json
    return json.dumps(card, ensure_ascii=False)


def test_render_three_user_states():
    running = render_task_card({"user_state": "running"})
    awaiting = render_task_card({"user_state": "awaiting_user", "pending_confirms": [{"id": "c1", "question": "是否继续？", "options": ["继续", "中止"], "resolved": None}]})
    done = render_task_card({"user_state": "done", "delivery": {"conclusion": "已完成", "artifact_path": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/t/"}})

    assert "VM 已经接手开始跑了" in _dump(running)
    assert "是否继续" in _dump(awaiting)
    assert "继续" in _dump(awaiting)
    assert "结论：已完成" in _dump(done)
    assert "//hfs1.minieye.tech/" in _dump(done)


def test_empty_delivery_section_is_omitted():
    card = render_task_card({"user_state": "running", "delivery": {}})
    assert "交付区" not in _dump(card)


def test_forbidden_fragments_raise():
    with pytest.raises(ValueError):
        render_task_card({"status_line": "当前判定为 已完成"})
    with pytest.raises(ValueError):
        assert_no_forbidden_fragments({"x": "状态同步如下"})


def test_template_fills_completed_without_status_override():
    assert render_status_line({"user_state": "completed", "delivery": {"conclusion": "OK"}}) == "这边已经跑完了，结论给你收好了：OK"


def test_all_confirm_option_presets_render_buttons():
    from gateway.feishu_task_card import CONFIRM_OPTION_PRESETS

    for preset, options in CONFIRM_OPTION_PRESETS.items():
        card = render_task_card({
            "task_id": f"task-{preset}",
            "user_state": "awaiting_user",
            "pending_confirms": [{"id": "c1", "question": "请选择", "preset": preset, "resolved": None}],
        })
        action_blocks = [el for el in card["elements"] if el.get("tag") == "action"]
        labels = [action["text"]["content"] for action in action_blocks[0]["actions"]]
        assert labels == options
        assert all(action["value"]["task_id"] == f"task-{preset}" for action in action_blocks[0]["actions"])


def test_g1q3_delivery_section_renders_business_fields():
    card = render_task_card({
        "user_state": "done",
        "status_line": "RCA 报告已生成，候选归因待人工确认。",
        "delivery": {
            "conclusion": "RCA 报告已生成",
            "attribution_status": "hypothesis_ready",
            "report_status": "html_delivery_ready",
            "candidate_cause": "触发请求出现但 TTC/gate 风险上下文不足",
            "responsibility_candidate": "刘培瑞",
            "artifact_label": "打开 HTML 报告",
            "artifact_path": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/t/index.html",
            "artifact_root": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/t/",
            "boundaries": ["需要人工确认候选原因、责任域与证据边界"],
        },
        "diagnostics": {
            "shared_state": "completed",
            "attribution_status": "hypothesis_ready",
            "report_status": "html_delivery_ready",
            "key_decision": "reused_existing_report",
            "blocker": "无",
        },
    })
    text = _dump(card)
    assert "归因状态：已有候选归因，待人工确认" in text
    assert "报告状态：内部审计产物已生成" in text
    assert "候选原因：触发请求出现但 TTC/gate 风险上下文不足" in text
    assert "责任候选：刘培瑞" in text
    assert "内部审计产物已生成" in text
    assert "打开 HTML 报告" not in text
    assert "index.html" not in text
    assert "**诊断**" in text
    assert "命中既有报告，已复用" in text


def test_delivery_section_does_not_render_html_source():
    card = render_task_card({
        "user_state": "done",
        "delivery": {
            "conclusion": "<style>.header h1{font-size:15px}</style>",
            "artifact_path": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/t/index.html",
        },
    })
    text = _dump(card)
    assert ".header h1" not in text
    assert "<style>" not in text
    assert "已隐藏疑似 HTML/CSS 源码" in text


def test_diagnostics_section_is_bottom_and_compact():
    card = render_task_card({
        "user_state": "done",
        "delivery": {"conclusion": "RCA 报告已生成"},
        "diagnostics": {
            "shared_state": "completed",
            "attribution_status": "hypothesis_ready",
            "report_status": "html_delivery_ready",
            "key_decision": "reused_existing_report",
            "blocker": "无",
            "ignored": "not-rendered",
        },
    })
    diag = card["elements"][-1]["content"]
    assert diag.startswith("**诊断**")
    assert diag.count("\n- ") == 5
    assert "ignored" not in diag


def test_render_task_card_backfills_milestones_from_vm_bridge_progress():
    rendered = render_task_card({
        "task_id": "task-progress",
        "user_state": "running",
        "vm_bridge": {"progress": {"phase": "read_mcap", "message": "读取mcap", "ts": "2026-06-18T12:00:00+00:00"}},
        "recent_events": [{"phase": "sync_repo", "summary": "同步仓库", "ts": "2026-06-18T11:59:00+00:00"}],
    })
    text = json.dumps(rendered, ensure_ascii=False)
    assert "执行阶段：读取mcap" in text
    assert "执行阶段：同步仓库" in text


def test_milestone_timestamps_render_as_business_datetime_without_iso_offset():
    rendered = render_task_card({
        "task_id": "task-timefmt",
        "user_state": "running",
        "milestones": [
            {"ts": "2026-06-23T10:39:42+08:00", "label": "已接单，开始读取飞书问题"},
            {"ts": "2026-06-23T02:40:21.221264+00:00", "label": "任务状态更新：in_progress"},
        ],
    })
    text = json.dumps(rendered, ensure_ascii=False)
    assert "2026-06-23 10:39:42 · 已接单" in text
    assert "2026-06-23 10:40:21 · 任务状态更新：in_progress" in text
    assert "T10:39:42+08:00" not in text
    assert ".221264" not in text


def test_v10_delivery_three_part_and_cifs_success_rendered():
    card = render_task_card({
        "user_state": "done",
        "delivery": {
            "conclusion": "已完成",
            "input_original": "/bad/original.mcap",
            "input_resolved": "/mnt/minieye/mdrive4/case/original.mcap",
            "artifact_vm": "/mnt/tmp/task-v10/output/",
            "artifact_cifs": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-v10/",
            "cifs_status": "success",
        },
    })
    text = _dump(card)
    assert "📥 输入：/bad/original.mcap → 实际读取：/mnt/minieye/mdrive4/case/original.mcap" in text
    assert "📤 产物(VM)：/mnt/tmp/task-v10/output/" in text
    assert "📦 取件(CIFS)：//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-v10/" in text
    assert "CIFS 状态：成功，可从取件路径获取" in text


def test_v10_delivery_cifs_failure_has_remedy_and_missing_fields_explicit():
    card = render_task_card({
        "user_state": "done",
        "delivery": {
            "conclusion": "已完成",
            "artifact_vm": "/mnt/tmp/task-v10-fail/",
            "cifs_status": "failed",
        },
    })
    text = _dump(card)
    assert "📥 输入：未落地/不适用" not in text
    assert "📤 产物(VM)：/mnt/tmp/task-v10-fail/" in text
    assert "📦 取件(CIFS)：未落地/不适用" in text
    assert "本次未落 CIFS" in text
    assert "拉取到CIFS" in text


def test_g1q3_delivery_section_uses_user_readable_status_and_clean_paths():
    card = render_task_card({
        "user_state": "done",
        "delivery": {
            "conclusion": "RCA 报告已生成",
            "attribution_status": "hypothesis_ready",
            "report_status": "html_delivery_ready",
            "candidate_cause": "候选因果判断：实际减速度相对 OOI 加速度偏重，建议由 控制 继续核查。；",
            "input_original": "飞书问题 7026726390 + mdi clip -u abc -s ./",
            "input_resolved": "mdi refresh2 -u abc -s ./",
            "artifact_vm": "/mnt/tmp/g1q3/",
            "artifact_cifs": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3/",
            "cifs_status": "success",
        },
        "diagnostics": {"shared_state": "blocked", "attribution_status": "hypothesis_ready", "report_status": "html_delivery_ready"},
    })
    text = _dump(card)
    assert "归因状态：已有候选归因，待人工确认" in text
    assert "报告状态：内部审计产物已生成" in text
    assert "候选原因：实际减速度相对 OOI 加速度偏重，建议由控制继续核查" in text
    assert "候选原因：候选因果判断" not in text
    assert "。；" not in text
    assert "📥 输入：远程读取引用（不执行 MDI 下载）" in text
    assert "mdi clip" not in text
    assert "mdi refresh2" not in text
    assert "HTML 报告路径" not in text
    assert "shared-state" not in text
    assert "html_delivery_ready" not in text
    assert "hypothesis_ready" not in text


def test_legacy_download_statuses_never_promise_automatic_download():
    card = render_task_card({
        "user_state": "running",
        "milestones": [{"ts": "", "label": "gate=ready_to_download"}],
        "delivery": {
            "conclusion": "等待旧状态迁移",
            "report_status": "need_download",
        },
    })
    text = _dump(card)
    assert "报告状态：待补充数据/证据" in text
    assert "ready_to_download" not in text
    assert "自动下载" not in text
    assert "等待下载" not in text


def test_legacy_mdi_command_is_neutralized_in_non_input_card_fields():
    card = render_task_card({
        "user_state": "awaiting_user",
        "delivery": {
            "conclusion": "请运行 mdi event -u opaque -s ./ 后继续",
            "report_status": "need_user_data",
        },
    })

    text = _dump(card)
    assert "历史数据地址已转换为远程读取引用" in text
    assert "mdi event" not in text.lower()
    assert "opaque" not in text


def test_http_html_artifact_is_not_user_visible_without_foxglove():
    card = render_task_card({
        "user_state": "done",
        "delivery": {
            "conclusion": "RCA 报告已生成",
            "artifact_label": "打开 HTML 报告",
            "artifact_path": "https://example.invalid/report/index.html",
        },
    })
    text = _dump(card)
    assert "https://example.invalid/report/index.html" not in text
    assert "打开 HTML 报告" not in text
    assert "当前尚无可交付 Foxglove 可视化" in text


def test_internal_http_report_is_hidden_while_cifs_directory_stays_pickup():
    card = render_task_card({
        "user_state": "done",
        "delivery": {
            "conclusion": "RCA 报告已生成",
            "artifact_label": "打开 HTML 报告",
            "artifact_path": "http://192.168.26.174:18081/G1Q3_RCA/cases/7026726390_acc/index.html",
            "artifact_root": "http://192.168.26.174:18081/G1Q3_RCA/cases/7026726390_acc",
            "artifact_cifs": "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/7026726390_acc",
            "cifs_status": "success",
        },
    })
    text = _dump(card)
    assert "http://192.168.26.174:18081/G1Q3_RCA/cases/7026726390_acc/index.html" not in text
    assert "📦 取件(CIFS)：//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/7026726390_acc" in text
    assert "打开 HTML 报告" not in text


def test_html_paths_in_display_metadata_are_hidden_without_foxglove():
    card = render_task_card({
        "user_state": "done",
        "delivery": {
            "report_status": "html_delivery_ready",
            "artifact_root": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/demo/index.html",
            "artifact_vm": "/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/demo/index.html",
            "artifact_cifs": "//hfs.minieye.tech/department-perception_test_team/G1Q3_RCA/cases/demo/index.html",
        },
    })

    text = _dump(card)
    assert "index.html" not in text


def test_g1q3_card_prefers_foxglove_and_hides_independent_html_link():
    viz_mcap_vm = canonical_viz_mcap_path("6986500860_fcw")
    public_foxglove_url = foxglove_url(viz_mcap_vm)
    card = render_task_card({
        "user_state": "done",
        "delivery": {
            "conclusion": "RCA 报告已生成",
            "report_status": "html_delivery_ready",
            "artifact_label": "打开 HTML 报告",
            "artifact_path": "http://192.168.26.174:18081/G1Q3_RCA/cases/6986500860_fcw/index.html",
            "foxglove_url": public_foxglove_url,
            "viz_mcap_vm": viz_mcap_vm,
            "attribution_causal_text": "目标测速异常 -> ACC 纵向请求波动 -> 减速过重",
        },
    })

    text = _dump(card)
    assert f"[打开 foxglove 可视化]({public_foxglove_url})" in text
    assert "[打开 HTML 报告]" not in text
    assert "http://192.168.26.174:18081/G1Q3_RCA/cases/6986500860_fcw/index.html" not in text
    assert "归因因果：目标测速异常 -> ACC 纵向请求波动 -> 减速过重" in text
    assert "@" not in text


@pytest.mark.parametrize(
    "internal_pointer",
    [
        "http://internal/G1Q3_RCA/demo/index%2Ehtml",
        "http://internal/G1Q3_RCA/demo/index%252Ehtml",
        "http://internal/G1Q3_RCA/demo/report.xhtml",
        "http://192.168.26.174:18081?case=demo",
        "https://internal/report?file=index&#46;html",
    ],
)
def test_g1q3_card_hides_encoded_internal_html_references(internal_pointer):
    card = render_task_card({
        "task_id": "g1q3-rca-encoded-html",
        "user_state": "done",
        "delivery": {
            "rca_status": "report_ready",
            "artifact_path": internal_pointer,
        },
    })

    assert internal_pointer not in _dump(card)


def test_non_rca_report_status_keeps_public_html_artifact():
    public_report = "https://docs.example/release/report.html"
    card = render_task_card({
        "task_id": "ordinary-task",
        "user_state": "done",
        "delivery": {
            "report_status": "published",
            "artifact_path": public_report,
        },
    })

    assert public_report in _dump(card)


def test_g1q3_card_preserves_exact_validated_foxglove_with_dot_name():
    viz_mcap_vm = canonical_viz_mcap_path("case.html")
    public_foxglove_url = foxglove_url(viz_mcap_vm)
    card = render_task_card({
        "task_id": "g1q3-rca-case-html",
        "user_state": "done",
        "delivery": {
            "rca_status": "report_ready",
            "artifact_path": "http://192.168.26.174:18081/index.html",
            "foxglove_url": public_foxglove_url,
            "viz_mcap_vm": viz_mcap_vm,
        },
    })

    assert public_foxglove_url in _dump(card)


def test_g1q3_card_sanitizes_internal_html_from_all_visible_text_only():
    card = render_task_card({
        "task_id": "case.html",
        "business_line": "g1q3-rca",
        "user_state": "awaiting_user",
        "status_line": "状态 http://internal/status/index%2Ehtml",
        "pending_confirms": [
            {
                "id": "review",
                "question": "复核 http://internal/question/report.xhtml",
                "options": ["确认"],
                "resolved": None,
            }
        ],
        "delivery": {
            "rca_status": "report_ready",
            "conclusion": "结论见 http://internal/conclusion/index%252Ehtml",
            "boundaries": ["http://192.168.26.174:18081?case=boundary"],
        },
        "diagnostics": {
            "blocker": "http://internal/diagnostic/index&amp;#46;html",
        },
    })

    dumped = _dump(card)
    assert "internal/" not in dumped
    assert ":18081" not in dumped
    assert INTERNAL_HTML_HIDDEN_TEXT in dumped
    action = next(item for item in card["elements"] if item.get("tag") == "action")
    assert action["actions"][0]["value"]["task_id"] == "case.html"



def test_render_task_card_declined_out_of_scope_delivery():
    card = render_task_card({
        "task_id": "g1q3-rca-out-of-scope",
        "user_state": "done",
        "status_line": "不予自动受理/转人工：问题所属项目未命中 G1Q3 受理范围",
        "delivery": {
            "conclusion": "不予自动受理/转人工：问题所属项目未命中 G1Q3 受理范围",
            "report_status": "out_of_scope",
            "attribution_status": "not_applicable",
            "boundaries": ["实际项目=「某非G1项目」"],
            "human_action_kind": "need_triage",
        },
    })

    dumped = json.dumps(card, ensure_ascii=False)
    assert "不予受理/转人工" in dumped
    assert "实际项目=「某非G1项目」" in dumped
    assert "待补齐数据" not in dumped
