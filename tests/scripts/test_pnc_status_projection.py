import json
from pathlib import Path

from scripts.pnc_status_projection import derive_presentation, no_deliverable_forbidden_hits, sanitize_milestones

FIXTURE_PATH = Path(__file__).with_name("fixtures") / "pnc_status_projection_cases.json"
FIXTURES = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))["cases"]


def _fixture(task_id: str):
    fixture = FIXTURES[task_id]
    assert fixture["source_case_id"] == task_id
    return fixture["meta"], fixture["evidence_text"]


def _project(task_id: str, truth=None, contract=None):
    meta, text = _fixture(task_id)
    contract = {'created_at': meta.get('created_at'), 'updated_at': meta.get('updated_at'), **(contract or {})}
    if 'status-check' in task_id:
        contract['mode'] = 'status_check'
    return derive_presentation(meta.get('state'), contract, truth or {}, text, {})


def test_a1_blocked_intake_6954459231_need_input_not_optimistic():
    p = _project('20260629-190539-g1q3-rca-issue-intake-6954459231')
    assert p['lane'] == 'need_evidence'
    assert p['action_category'] == 'hard'
    assert '正在自动下载' not in p['status_line']
    assert '完成后出' not in p['status_line']
    assert 'PDCL' in p['missing_reason'] or '数据' in p['missing_reason']
    assert p['cifs_status'] != 'success'


def test_a2_report_ready_7031356230_overrides_running_and_keeps_soft_confirm():
    p = _project('20260629-192549-g1q3-rca-issue-intake-7031356230-56230_047667', truth={
        'real_report': True,
        'has_deliverable_report': True,
        'report_status': 'html_delivery_ready',
        'honest_conclusion': 'RCA 报告已生成；请复核边界。',
        'index_html_vm': '/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/legacy/legacy/index.html',
    })
    assert p['lane'] == 'report_ready'
    assert p['diagnostic_state'] == '报告已生成'
    assert p['human_action_kind'] == 'confirm_review'
    assert p['cifs_status'] == 'success'
    assert p['report_status'] == 'html_delivery_ready'
    assert p['artifact_label'] == '打开 HTML 报告'
    cleaned = sanitize_milestones([{'ts': '2026-06-29 19:37:00', 'label': '执行阶段：running'}] + p['milestones'], p)
    assert all('running' not in m['label'] for m in cleaned)


def test_report_ready_prefers_foxglove_when_both_surfaces_exist():
    p = derive_presentation(
        'completed',
        {
            'report_index_html_vm': '/mnt/minieye/pdcl/department/perception_test_team/G1Q3_RCA/cases/case_a/index.html',
            'foxglove_url': 'https://192.168.21.217/?ds=foxglove-http&ds.mcapPath=/mnt/case_a.viz.mcap',
        },
        {'real_report': True, 'has_deliverable_report': True},
    )

    assert p['lane'] == 'report_ready'
    assert p['report_status'] == 'report_ready'
    assert p['artifact_label'] == '打开 foxglove 可视化'


def test_report_ready_accepts_html_url_fragment_surface():
    p = derive_presentation(
        "completed",
        contract={"artifact_path": "https://reports.example/case/index.html#evidence"},
        report_truth={"real_report": True},
    )
    assert p["has_deliverable_report"] is True
    assert p["report_status"] == "html_delivery_ready"


def test_malformed_html_url_surface_fails_closed():
    p = derive_presentation(
        "completed",
        contract={"artifact_path": "http://[broken"},
        report_truth={"real_report": True},
    )
    assert p["has_deliverable_report"] is False
    assert p["user_state"] != "done"


def test_report_ready_fails_closed_when_both_surfaces_are_missing():
    p = derive_presentation(
        'completed',
        {'report': {'status': 'report_ready', 'is_deliverable': True}},
        {'real_report': True, 'has_deliverable_report': True},
    )

    assert p['lane'] != 'report_ready'
    assert p['has_deliverable_report'] is False


def test_a5_completed_need_evidence_7015689036_not_done():
    p = _project('20260611-144408-g1q3-rca-issue-intake-7015689036')
    assert p['lane'] == 'need_evidence'
    assert p['user_state'] == 'in_progress'
    assert '报告已生成' not in p['status_line']
    assert p['report_status'] == 'need_user_data'


def test_a6_not_admissible_7029768863_no_originator_action():
    p = _project('20260627-142804-g1q3-rca-issue-intake-7029768863-68863_4a42ba')
    assert p['lane'] == 'not_admissible'
    assert p['human_action_kind'] == 'none'
    assert '受理未通过' in p['status_line']


def test_a7_timeout_honest_failed_no_report():
    p = _project('20260615-191917-g1q3-rca-7017699515-fresh-rerun-v2-without-prior')
    assert p['lane'] == 'failed_timeout'
    assert p['report_status'] == 'failed'
    assert p['cifs_status'] != 'success'


def test_a8_pending_queue_honest():
    p = _project('20260615-113107-g1q3-rca-issue-intake-6966935625')
    assert p['lane'] == 'pending'
    assert '排队' in p['status_line']
    assert p['report_status'] == 'pending'


def test_a9_status_check_done_no_fake_report():
    p = _project('20260603-105813-g1q3-rca-status-check-g1q3-042')
    assert p['lane'] in {'status_check_done', 'need_evidence'}
    assert ('无新增' in p['status_line']) or ('未生成 RCA 报告' in p['conclusion'])
    assert p['report_status'] in {'status_check_done', 'need_user_data'}


def test_a10_invalid_frame_id_structured_need_input():
    p = _project('20260615-193823-g1q3-rca-issue-intake-7000062233')
    assert p['lane'] == 'blocked_frame'
    assert p['human_action_kind'] == 'need_frame'
    assert 'frame_id' in p['missing_reason']


def test_a3_conditional_forbidden_fragments_only_without_report():
    bad = {'elements': [{'tag': 'markdown', 'content': '已受理，正在自动下载/解析数据，完成后出 RCA 结论。'}]}
    assert no_deliverable_forbidden_hits(bad, has_deliverable_report=False)
    assert '正在自动下载/解析' in no_deliverable_forbidden_hits(bad, has_deliverable_report=True)


def test_a11_sanitize_drops_stale_report_positive_milestone_when_no_report():
    # Regression: a case re-classified report_ready -> need_download keeps a stale
    # positive milestone.  sanitize_milestones must drop it when the projection has
    # no deliverable report, so the render-time fail-closed guard never crashes.
    stale = [
        {'ts': '2026-06-23 22:01:55', 'label': '已接单，开始读取飞书问题'},
        {'ts': '2026-06-24 14:07:06', 'label': 'RCA 报告已生成，等待人工确认候选归因'},
        {'ts': '2026-06-24 14:11:17', 'label': '本轮暂停，待补充信息后继续 RCA'},
    ]
    no_report = {'lane': 'need_evidence', 'has_deliverable_report': False}
    cleaned = sanitize_milestones(stale, no_report)
    assert all('报告已生成' not in m['label'] for m in cleaned)
    # And the sanitizer output must satisfy the very guard that renders the card.
    for m in cleaned:
        assert not no_deliverable_forbidden_hits(m['label'], has_deliverable_report=False)
    # With a genuine report the positive milestone is legitimately retained.
    with_report = {'lane': 'report_ready', 'has_deliverable_report': True}
    kept = sanitize_milestones(stale, with_report)
    assert any('报告已生成' in m['label'] for m in kept)


def test_nested_pipeline_blocker_routes_infra_failure_to_pipeline_fix():
    pipeline_result = {
        "status": "blocked",
        "stage": "s3b_translate",
        "blocker": {
            "kind": "translate_workdir_permission",
            "fault_class": "infra_self_healable",
            "retryable": True,
            "message": "translate work directory is not writable",
        },
    }

    projection = derive_presentation(
        "blocked",
        {
            "business_state": "blocked_need_evidence",
            "pipeline_result": pipeline_result,
        },
        {"pipeline_result": pipeline_result},
        "",
        {},
    )

    assert projection["lane"] == "pipeline_fix"
    assert projection["report_status"] == "need_pipeline_fix"
    assert projection["human_action_kind"] == "none"
    assert projection["requires_user_input"] is False
    assert projection["action_category"] == "none"


def test_awaiting_remote_read_stays_in_progress_without_stage_metadata():
    projection = derive_presentation(
        "running",
        {
            "business_state": "awaiting_download",
            "report": {"status": "need_download", "is_deliverable": False},
            "user_action": {"requires_user_input": False},
        },
        {"report_status": "need_download"},
        "downloading",
        {},
    )

    assert projection["lane"] == "in_progress"
    assert projection["report_status"] == "in_progress"
    assert projection["requires_user_input"] is False


def test_historical_need_download_input_projects_remote_reference_guidance():
    projection = derive_presentation(
        'completed',
        {
            'business_state': 'missing_user_input',
            'user_action': {
                'next_action_text': '请补充问题数据地址_PDCL 或有效 PDCL 下载命令',
            },
        },
        {'report_status': 'need_download'},
        'gate=ready_to_download',
        {},
    )

    rendered = json.dumps(projection, ensure_ascii=False)
    assert projection['lane'] == 'need_evidence'
    assert 'event/clip 引用' in projection['missing_reason']
    assert '远程读取' in projection['missing_reason']
    assert '不执行 MDI 下载' in projection['missing_reason']
    assert 'mdi refresh' not in rendered.lower()
    assert 'mdi download' not in rendered.lower()
    assert '继续下载/解析' not in rendered
    assert '数据下载执行中' not in rendered


def test_historical_s2_download_running_stage_is_displayed_as_remote_read():
    projection = derive_presentation(
        'running',
        {
            'pipeline_result': {
                'status': 'running',
                'stage': 's2_download',
            },
        },
        {},
        '',
        {},
    )

    assert projection['lane'] == 'in_progress'
    assert projection['report_status'] == 'in_progress'
    assert '远程读取问题数据中' in projection['status_line']
    assert '数据下载执行中' not in projection['status_line']
    assert 'pipeline running' not in projection['status_line']


def test_historical_download_stage_message_cannot_override_remote_read_label():
    projection = derive_presentation(
        'running',
        {},
        {},
        '',
        {
            'pipeline_result': {
                'status': 'running',
                'stage': 's2_download',
                'message': 'downloading source data',
            },
        },
    )

    assert '远程读取问题数据中' in projection['status_line']
    assert 'downloading' not in projection['status_line']


def test_forbidden_guard_catches_old_group_retrigger_and_download_claims():
    rendered = '数据已下载；重发问题链接，我会自动重跑 RCA。'
    hits = no_deliverable_forbidden_hits(rendered, has_deliverable_report=False)

    assert '数据已下载' in hits
    assert '重发问题链接，我会自动重跑' in hits
