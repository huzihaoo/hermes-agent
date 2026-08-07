from __future__ import annotations

import hashlib
import json

import pytest

from gateway import pnc_rca_delivery_contract as delivery_contract_module
from gateway import pnc_rca_quality_oracle as quality_oracle_module
from gateway.pnc_rca_admission import build_rca_admission
from gateway.pnc_rca_abstention_projection import (
    build_gate_a_identifier_binding,
    project_gate_a_report,
)
from gateway.pnc_rca_delivery_contract import (
    DELIVERY_THREAD_EFFECT_KIND,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    TERMINAL_FALLBACK_CONTRACT_SCHEMA_VERSION,
    TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
    TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    DeliveryContractError,
    MAX_DELIVERY_ARTIFACT_BYTES,
    MAX_DELIVERY_ARTIFACTS,
    ADOPTION_PROMPT_LINE,
    build_public_rca_result,
    build_report_cifs_path,
    build_report_artifact_url,
    build_report_url,
    build_report_vm_path,
    build_terminal_delivery,
    build_thread_reply_effect,
    canonical_issue_url,
    compute_artifact_set_id,
    MAX_FEISHU_COMMENT_BYTES,
    render_public_rca_result,
    rerun_prompt_line,
    verify_delivery_bundle,
)
from gateway.pnc_rca_issue_focus import (
    ANALYSIS_CAPABILITY_UNSUPPORTED,
    ANALYSIS_COMPLETE,
    ANALYSIS_INSUFFICIENT_STATEMENT,
    ISSUE_FOCUS_EVIDENCE_SCHEMA_VERSION,
    issue_title_sha256,
    resolve_issue_intent,
)
from scripts.pnc_foxglove_delivery import (
    canonical_viz_mcap_cifs_path,
    canonical_viz_mcap_path,
    foxglove_url,
)
from gateway.pnc_rca_quality_oracle import MEDIUM_TIER_DISCLAIMER


FORMAL_SUBMISSION_KEY = "g1q3-rca-s1-" + "a" * 64
FORMAL_ARTIFACT_SET_ID = "g1q3-rca-artifact-v1-" + "b" * 64
FORMAL_REPORT_PATH = (
    f"/G1Q3_RCA/cases/{FORMAL_SUBMISSION_KEY}/{FORMAL_ARTIFACT_SET_ID}/index.html"
)


@pytest.fixture(autouse=True)
def _configured_viewer_origin(monkeypatch):
    monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", "https://viewer.internal")


def _admission(*, offset: int = 10):
    return build_rca_admission(
        project_key="t03o4q",
        project_simple_name="g1q3",
        work_item_type_key="issue",
        work_item_id="7041712812",
        rule_version="issue-created-v1",
        topic="feishu-project-workflow-event",
        partition=2,
        offset=offset,
    )


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _bundle(
    *,
    admission=None,
    explicit_kind: str | None = "foxglove_viz",
    include_web_assets: bool = False,
):
    admission = admission or _admission()
    root = f"/mnt/tmp/{admission.submission_key}/"
    index_html = (
        b'<!doctype html><title>RCA</title><video src="assets/media/video.mp4"></video>'
    )
    if include_web_assets:
        index_html = (
            b"<!doctype html><title>RCA</title>"
            b'<link rel="stylesheet" href="assets/app.css">'
            b'<script src="assets/app.js"></script>'
            b'<video src="assets/media/video.mp4"></video>'
        )
    contents = {
        "index.html": index_html,
        "report_data.json": b'{"schema_version":"g1q3_rca_report_v2"}',
        "assets/media/video.mp4": b"fake-video-bytes",
    }
    if include_web_assets:
        contents.update({
            "assets/app.css": b"body{color:#111}",
            "assets/app.js": b"globalThis.RCA_READY=true;",
        })
    manifest = {
        "schema_version": "delivery_manifest_v2",
        "sealed": True,
        "submission_key": admission.submission_key,
        "business_key": admission.business_key,
        "generation": admission.generation,
        "project_key": admission.source_refs.project_key,
        "work_item_type_key": admission.source_refs.work_item_type_key,
        "work_item_id": admission.source_refs.work_item_id,
        "artifact_revision": 1,
        "sealed_at": "2026-07-10T08:00:00+00:00",
        # HTML remains in the sealed artifact inventory for internal consumers;
        # the public delivery kind is the published Foxglove surface.
        "deliverable_kind": "foxglove_viz",
        "dependencies_complete": True,
        "artifact_root": root,
        "html_validation": {
            "state": "html_delivery_ready",
            "report_data_sha256": _sha(contents["report_data.json"]),
            "blockers": [],
            "fidelity_ok": True,
        },
        "artifacts": [
            {
                "role": "index_html",
                "path": "index.html",
                "size": len(contents["index.html"]),
                "sha256": _sha(contents["index.html"]),
                "media_type": "text/html; charset=utf-8",
                "required": True,
            },
            {
                "role": "report_data",
                "path": "report_data.json",
                "size": len(contents["report_data.json"]),
                "sha256": _sha(contents["report_data.json"]),
                "media_type": "application/json",
                "required": True,
            },
            {
                "role": "video",
                "path": "assets/media/video.mp4",
                "size": len(contents["assets/media/video.mp4"]),
                "sha256": _sha(contents["assets/media/video.mp4"]),
                "media_type": "video/mp4",
                "required": False,
            },
        ],
    }
    if include_web_assets:
        manifest["artifacts"].extend([
            {
                "role": "stylesheet",
                "path": "assets/app.css",
                "size": len(contents["assets/app.css"]),
                "sha256": _sha(contents["assets/app.css"]),
                "media_type": "text/css; charset=utf-8",
                "required": True,
            },
            {
                "role": "javascript",
                "path": "assets/app.js",
                "size": len(contents["assets/app.js"]),
                "sha256": _sha(contents["assets/app.js"]),
                "media_type": "text/javascript; charset=utf-8",
                "required": True,
            },
        ])
    manifest["artifact_set_id"] = compute_artifact_set_id(manifest)
    manifest["report_url"] = build_report_url(
        admission.submission_key, manifest["artifact_set_id"]
    )
    manifest["report_vm_path"] = build_report_vm_path(
        admission.submission_key, manifest["artifact_set_id"]
    )
    manifest["report_cifs_path"] = build_report_cifs_path(
        admission.submission_key, manifest["artifact_set_id"]
    )
    report = {
        "status": "report_ready",
        "is_deliverable": True,
        "requires_human_review": True,
    }
    if explicit_kind is not None:
        report["deliverable_kind"] = explicit_kind
    viz_path = canonical_viz_mcap_path(admission.submission_key)
    viz_bytes = b"verified-viz-mcap"
    viz_manifest_base = {
        "schema_version": "g1q3_rca_viz_publication_v1",
        "status": "published",
        "submission_key": admission.submission_key,
        "path": viz_path,
        "size": len(viz_bytes),
        "sha256": _sha(viz_bytes),
        "source_path": root + "cases/7041712812_acc/7041712812_acc.viz.mcap",
        "source_sha256": _sha(viz_bytes),
        "published_at": "2026-07-10T08:00:01+00:00",
    }
    viz_manifest_bytes = (
        json.dumps(
            viz_manifest_base,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    viz_manifest_path = viz_path.removesuffix(".viz.mcap") + ".viz.manifest.json"
    viz_publication = {
        **viz_manifest_base,
        "manifest_path": viz_manifest_path,
        "manifest_size": len(viz_manifest_bytes),
        "manifest_sha256": _sha(viz_manifest_bytes),
    }
    contract = {
        "schema_version": "g1q3_delivery_contract_v1",
        "task_id": admission.submission_key,
        "run_id": admission.submission_key,
        "work_item_id": admission.source_refs.work_item_id,
        "business_state": "report_completed",
        "report": report,
        "summary": {
            "short_conclusion": "自动RCA未归因：现有证据未形成可确认的因果链。"
        },
        "artifacts": {
            "delivery_manifest_vm": root + "delivery_manifest.json",
            "artifact_set_id": manifest["artifact_set_id"],
            "index_html_vm": root + "index.html",
            "report_data_vm": root + "report_data.json",
            "viz_mcap_vm": viz_path,
            "viz_publication": viz_publication,
        },
        "consumer_capability": _consumer_capability(),
    }
    observed = [
        {
            "path": root + path,
            "size": len(data),
            "sha256": _sha(data),
            "is_file": True,
            "is_symlink": False,
            "parents_symlink_free": True,
        }
        for path, data in contents.items()
    ]
    observed.extend([
        {
            "path": viz_path,
            "size": len(viz_bytes),
            "sha256": _sha(viz_bytes),
            "is_file": True,
            "is_symlink": False,
            "parents_symlink_free": True,
            "sha256_attested_by_manifest": True,
        },
        {
            "path": viz_manifest_path,
            "size": len(viz_manifest_bytes),
            "sha256": _sha(viz_manifest_bytes),
            "is_file": True,
            "is_symlink": False,
            "parents_symlink_free": True,
        },
    ])
    dependencies = [root + "assets/media/video.mp4"]
    if include_web_assets:
        dependencies.extend([root + "assets/app.css", root + "assets/app.js"])
    return admission, contract, manifest, observed, dependencies


def _consumer_capability(*, applicability="applied"):
    return {
        "schema_version": "rca_consumer_capability_publication_v1",
        "capability_profile": "g1q3_863_consumer",
        "capability_version": "g1q3_863_consumer_v1",
        "evaluator_scope": "g1q3_rca_evaluator_scope_v4",
        "applicability": applicability,
        "not_applied_reason": (
            ""
            if applicability == "applied"
            else "no_decoded_signal_or_evaluator_evidence"
        ),
        "actual_signals": ["AEBReq"] if applicability == "applied" else [],
        "actual_fields": ["OOI_ID"] if applicability == "applied" else [],
        "actual_evaluators": (
            [{"evaluator_id": "aeb_trigger", "status": "supported"}]
            if applicability == "applied"
            else []
        ),
        "unused_capabilities": [
            {
                "evaluator_id": "fcw_trigger",
                "status": "not_applicable",
                "reason": "not applicable",
            }
        ],
        "evidence": {
            "issue_frame_id": 123,
            "focus_window": {"start_ts": 0.0, "end_ts": 1.0},
            "field_lineage": {
                "schema_version": "g1q3_field_lineage_v2",
                "fidelity_ok": True,
                "status": "pass",
            },
            "viz_lineage": {
                "schema_version": "g1q3_viz_lineage_v1",
                "ok": True,
                "status": "pass",
            },
        },
    }


def _add_structural_candidate(contract, conclusion="减速度请求偏重。"):
    contract["upstream_dispatch"] = {
        "hit_evaluator_keys": ["aeb_trigger"],
        "hit_window_envelope": None,
        "hit_windows": [],
        "owner_bucket": "acc_longitudinal_control",
        "owner_bucket_label": "纵向控制",
        "reason": "single_owner_bucket_hit",
        "schema_version": "g1q3_upstream_dispatch_v2",
        "terminal_classification": "valid_dispatch",
    }
    contract["report"]["candidate_owner_domain"] = "ACC"
    contract["report"]["is_candidate"] = True
    contract["summary"]["short_conclusion"] = conclusion
    contract["public_result"] = {
        "summary": {"short_conclusion": conclusion},
        "candidate": "ACC",
        "responsibility": {"status": "candidate"},
        "evidence_summary": {"refs": []},
        "causal_chain": {
            "narrative": [
                {"role": "现象", "text": "车辆减速度偏重。"},
                {"role": "证据", "text": "减速度请求与目标状态不匹配。"},
                {"role": "因果判断", "text": conclusion},
            ]
        },
        "user_action": {},
    }
    return contract


def test_delivery_projects_consumer_capability_into_field_and_comment():
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["consumer_capability"] = _consumer_capability()
    _add_structural_candidate(contract)

    verified = verify_delivery_bundle(
        admission=admission,
        delivery_contract=contract,
        delivery_manifest=manifest,
        observed_files=observed,
        html_dependencies=dependencies,
    )

    assert "归因判断：减速度请求偏重。" in verified.conclusion
    assert MEDIUM_TIER_DISCLAIMER in verified.conclusion
    assert "g1q3_863_consumer" not in verified.conclusion
    assert "signals/fields/evaluators" not in verified.effect_payload["comment_content"]
    assert "Foxglove 证据：" in verified.effect_payload["comment_content"]
    assert verified.effect_payload["comment_content"].splitlines()[-1] == (
        ADOPTION_PROMPT_LINE
    )
    assert rerun_prompt_line(canonical_issue_url("g1q3", "7041712812")) in (
        verified.effect_payload["comment_content"].splitlines()
    )
    assert (
        verified.effect_payload["field_updates"][0]["field_value"]
        == verified.effect_payload["result_field_value"]
    )
    assert verified.effect_payload["schema_version"] == (
        delivery_contract_module.DELIVERY_EFFECT_SCHEMA_VERSION
    )
    assert verified.effect_payload["result_field_value"].splitlines() == [
        (
            "归因结论：减速度请求偏重；减速度请求与目标状态不匹配；"
            f"{MEDIUM_TIER_DISCLAIMER}。"
        ),
        "责任模块：ACC 功能链",
    ]
    assert verified.effect_payload["result_field_value"] != verified.conclusion
    assert verified.effect_payload["terminal_class"] == "candidate_hypothesis"
    assert verified.effect_payload["requires_human_review"] is True


def test_gate_a_observation_projection_suppresses_candidate_without_golden():
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["consumer_capability"] = _consumer_capability()
    _add_structural_candidate(contract)
    contract["gate_a_projection"] = project_gate_a_report({
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": "aeb_trigger",
                "domain": "AEB",
                "pattern": "trigger",
                "status": "supported",
                "evidence_refs": [
                    {
                        "signal": "AEBReq",
                        "window": [0.0, 1.0],
                        "evidence": "窗口内观测到 AEB 请求。",
                    }
                ],
            }
        ],
    }, identifier_binding=build_gate_a_identifier_binding(
        contract["consumer_capability"]
    ))

    verified = verify_delivery_bundle(
        admission=admission,
        delivery_contract=contract,
        delivery_manifest=manifest,
        observed_files=observed,
        html_dependencies=dependencies,
    )

    assert verified.effect_payload["terminal_class"] == "honest_non_attribution"
    assert "建议责任方：" not in verified.conclusion
    assert "候选" not in verified.conclusion
    assert MEDIUM_TIER_DISCLAIMER not in verified.conclusion
    assert "已观测到评测项 aeb_trigger 的支持证据" in verified.conclusion
    assert "信号 AEBReq" in verified.conclusion
    assert "窗口 0~1s" in verified.conclusion
    assert "窗口内观测到 AEB 请求" not in verified.conclusion
    assert verified.effect_payload["requires_human_review"] is False


def test_gate_a_render_keeps_observation_when_legacy_dispatch_has_no_hit():
    _admission_obj, contract, _manifest, _observed, _dependencies = _bundle()
    contract["consumer_capability"]["actual_evaluators"][0]["status"] = "refuted"
    contract["gate_a_projection"] = project_gate_a_report({
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": "aeb_trigger",
                "domain": "AEB",
                "status": "refuted",
                "evidence_refs": [
                    {
                        "signal": "AEBReq",
                        "window": [-1.0, 1.0],
                        "evidence": "窗口内未观测到 AEB 请求。",
                    }
                ],
            }
        ],
    }, identifier_binding=build_gate_a_identifier_binding(
        contract["consumer_capability"]
    ))

    rendered = render_public_rca_result(contract)

    assert "信号 AEBReq" in rendered
    assert "窗口 -1~1s" in rendered
    assert "窗口内未观测到 AEB 请求" not in rendered
    assert "现有证据不支持评测项 aeb_trigger" in rendered
    assert "未发现已知异常模式" not in rendered
    assert MEDIUM_TIER_DISCLAIMER not in rendered


def test_gate_a_l0_projection_does_not_claim_evaluator_observation():
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["consumer_capability"] = _consumer_capability()
    _add_structural_candidate(contract)
    contract["gate_a_projection"] = project_gate_a_report({
        "input_materialized": False,
        "failure_class": "remote_event_not_found",
    })

    verified = verify_delivery_bundle(
        admission=admission,
        delivery_contract=contract,
        delivery_manifest=manifest,
        observed_files=observed,
        html_dependencies=dependencies,
    )

    assert verified.effect_payload["terminal_class"] == "honest_non_attribution"
    assert "建议责任方：" not in verified.conclusion
    assert "已读取评测器观测事实" not in verified.conclusion
    assert "未找到对应事件" in verified.conclusion
    assert verified.conclusion.splitlines() == [
        "本单未能定向",
        "观测事实：当前数据源未找到对应事件；本次未取得可用于归因的分析数据。",
        "本次仅发布弃权事实，未输出责任归因。",
    ]
    assert MEDIUM_TIER_DISCLAIMER not in verified.conclusion


def test_gate_a_projection_rejects_forged_candidate_fields():
    projection = project_gate_a_report({
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": "aeb_trigger",
                "status": "supported",
                "evidence_refs": [{"signal": "AEBReq"}],
            }
        ],
    }, identifier_binding=build_gate_a_identifier_binding(_consumer_capability()))
    projection["evaluator_projection"]["evaluators"][0]["candidate"] = "ACC"
    with pytest.raises(DeliveryContractError, match="gate_a_projection_invalid"):
        render_public_rca_result({"gate_a_projection": projection})


def test_supported_tier_requires_golden_and_does_not_request_human_review(
    monkeypatch,
):
    monkeypatch.setattr(
        quality_oracle_module,
        "release_golden_registry_status",
        lambda: {
            "present": True,
            "valid": True,
            "low_tier_golden_ready": True,
            "active_inventory_binding_valid": True,
            "evaluators": {
                "aeb_trigger": {
                    "evaluator_id": "aeb_trigger",
                    "status": "passed",
                    "source_kind": "owner_confirmed_case",
                    "evaluator_source_sha256": "c" * 64,
                    "positive_golden_sha256": "a" * 64,
                    "negative_golden_sha256": "b" * 64,
                    "test_receipt_sha256": "d" * 64,
                }
            },
            "fully_validated_evaluators": {
                "aeb_trigger": {
                    "evaluator_id": "aeb_trigger",
                    "fully_validated": True,
                }
            },
        },
    )
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["consumer_capability"] = _consumer_capability()
    conclusion = "AEB 触发请求持续成立并经控制链传导，导致本次制动。"
    contract["report"].update(
        candidate_owner_domain="AEB",
        is_candidate=False,
    )
    contract["summary"]["short_conclusion"] = conclusion
    contract["quality_classification"] = "supported_attribution"
    contract["upstream_dispatch"] = {
        "hit_evaluator_keys": ["aeb_trigger"],
        "hit_window_envelope": None,
        "hit_windows": [],
        "owner_bucket": "acc_longitudinal_control",
        "owner_bucket_label": "纵向控制",
        "reason": "single_owner_bucket_hit",
        "schema_version": "g1q3_upstream_dispatch_v2",
        "terminal_classification": "valid_dispatch",
    }
    contract["public_result"] = {
        "summary": {"short_conclusion": conclusion},
        "candidate": "AEB",
        "responsibility": {"status": "supported"},
        "evidence_summary": {"refs": [{"evidence_ref": "frame:123/aeb_trigger"}]},
        "causal_chain": {
            "narrative": [
                {"role": "现象", "text": "车辆发生制动。"},
                {"role": "证据", "text": "AEB 触发请求持续成立。"},
                {"role": "因果判断", "text": conclusion},
            ]
        },
        "user_action": {},
    }

    verified = verify_delivery_bundle(
        admission=admission,
        delivery_contract=contract,
        delivery_manifest=manifest,
        observed_files=observed,
        html_dependencies=dependencies,
    )

    assert verified.effect_payload["terminal_class"] == "supported_attribution"
    assert verified.effect_payload["confidence_tier"] == "high"
    assert verified.effect_payload["requires_human_review"] is False


def test_public_projection_keeps_evidence_conflict_without_debug_terms():
    contract = {
        "summary": {
            "short_conclusion": (
                "问题评论声称 ID 75 / age 226 / 速度 0.0，但绑定 PDCL 事件的 "
                "28 个 OOI 槽位仅观测到目标 ID [1, 67]（活动槽位 [5, 9]），"
                "未找到该目标组合；问题描述证据与生产数据源不一致，本次禁止输出责任归因。"
            )
        },
        "evidence_boundary": ["问题描述证据与生产数据源不一致，未找到该目标组合。"],
        "artifacts": {"attribution_causal_text": "问题描述证据与生产数据源不一致。"},
    }

    result = build_public_rca_result(contract)
    rendered = render_public_rca_result(contract)

    assert result["attribution_ready"] is False
    assert "问题单描述的目标与生产数据不一致" in result["conclusion"]
    assert "关键目标不匹配" in result["causal_chain"]
    assert "核对绑定的 PDCL 事件" in result["next_action"]
    assert "OOI" not in rendered
    assert "槽位" not in rendered


def test_public_projection_attributes_data_binding_conflict_without_blame_shift():
    contract = {
        "summary": {
            "short_conclusion": (
                "问题数据/回灌链路不一致：绑定数据未出现问题描述中的目标，"
                "当前不能将误触发归责于 AEB。"
            )
        },
        "report": {
            "candidate_owner": "问题数据/回灌链路",
            "candidate_owner_domain": "问题数据/回灌链路",
        },
        "artifacts": {
            "attribution_causal_text": (
                "问题描述目标与绑定数据不一致，责任指向问题数据/回灌链路。"
            )
        },
        "public_result": {
            "summary": {
                "status": "blocked",
                "short_conclusion": (
                    "问题数据/回灌链路不一致：绑定数据未出现问题描述中的目标，"
                    "当前不能将误触发归责于 AEB。"
                ),
            },
            "responsibility": {
                "status": "candidate_data_integrity_conflict",
            },
            "causal_chain": {
                "narrative": [
                    {
                        "role": "因果判断",
                        "text": "描述目标未在绑定数据中出现，责任指向问题数据/回灌链路。",
                    }
                ]
            },
            "evidence_boundary": ["绑定数据未观测到问题描述中的目标。"],
        },
    }

    result = build_public_rca_result(contract)

    assert result["attribution_ready"] is True
    assert result["responsibility"] == "问题数据/回灌链路"
    assert "问题数据/回灌链路" in result["causal_chain"]
    assert "不能将误触发归责于 AEB" in result["conclusion"]


def test_public_projection_humanizes_evaluator_and_responsibility_domain():
    contract = {
        "upstream_dispatch": {
            "hit_evaluator_keys": ["acc_heavy_decel_spec"],
            "hit_window_envelope": None,
            "hit_windows": [],
            "owner_bucket": "acc_longitudinal_control",
            "owner_bucket_label": "纵向控制",
            "reason": "single_owner_bucket_hit",
            "schema_version": "g1q3_upstream_dispatch_v2",
            "terminal_classification": "valid_dispatch",
        },
        "summary": {
            "short_conclusion": (
                "decoded evaluator 已支持候选归因方向：ACC decoded 证据显示实际减速度偏重。"
            )
        },
        "report": {
            "candidate_owner": "ACC decoded 证据",
            "candidate_owner_domain": "ACC",
        },
        "evidence_boundary": ["原始 mcap 已解码出函数级证据。"],
        "artifacts": {
            "attribution_causal_text": "目标状态与减速度请求不匹配，导致车辆减速度偏重。"
        },
    }

    rendered = render_public_rca_result(contract)

    assert rendered.splitlines()[0] == "建议责任方：纵向控制"
    assert rendered.splitlines()[1] == MEDIUM_TIER_DISCLAIMER
    assert "原始 mcap 已解码出函数级证据" in rendered
    assert "decoded" not in rendered
    assert "evaluator" not in rendered


def test_public_projection_rejects_stale_pipeline_next_action():
    contract = {
        "summary": {
            "short_conclusion": "生产数据已读取，但问题单未提供可核验的现象描述，不能自动归因。"
        },
        "user_action": {
            "next_action_text": "已受理；数据待受控远程读取（自动管线），无需发起人补数据。"
        },
    }

    rendered = render_public_rca_result(contract)

    assert "待受控远程读取" not in rendered
    assert "下一步：" not in rendered
    assert "里程碑：" not in rendered


def test_public_projection_humanizes_legacy_success_wrapped_terminal_result():
    contract = {
        "summary": {
            "short_conclusion": (
                "自动RCA未归因：当前问题域不在已验证的自动分析域内，已生成诊断报告并转人工分流。"
            )
        },
        "report": {"status": "report_ready"},
    }

    rendered = render_public_rca_result(contract)

    assert rendered.splitlines() == [
        "本单未能定向",
        MEDIUM_TIER_DISCLAIMER,
        "未发现已知异常模式",
        "本次没有判据命中，无法提供异常时间窗。",
    ]
    assert "问题域" not in rendered
    assert "自动RCA未归因" not in rendered
    assert "已生成诊断报告" not in rendered


def test_honest_abstention_projects_existing_window_signals_and_foxglove_handoff():
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["consumer_capability"] = _consumer_capability()
    contract["consumer_capability"]["evidence"]["focus_window"].update(
        {"start_frame": None, "end_frame": None}
    )

    verified = verify_delivery_bundle(
        admission=admission,
        delivery_contract=contract,
        delivery_manifest=manifest,
        observed_files=observed,
        html_dependencies=dependencies,
    )

    conclusion = verified.effect_payload["conclusion"]
    comment = verified.effect_payload["comment_content"]
    assert verified.effect_payload["terminal_class"] == "honest_non_attribution"
    assert "自动分析未形成可派单结论" in conclusion
    assert "相对问题时刻 0.000s-1.000s" in conclusion
    assert "`AEBReq`" in conclusion
    assert "`OOI_ID`" in conclusion
    assert verified.foxglove_url in comment
    assert "自行查看该区间" in comment
    assert "自动跳转或预置 layout" in comment
    assert ADOPTION_PROMPT_LINE not in comment


def test_honest_abstention_requires_existing_window_and_signal_capability():
    admission, contract, manifest, observed, dependencies = _bundle()
    contract.pop("consumer_capability")

    with pytest.raises(DeliveryContractError) as raised:
        verify_delivery_bundle(
            admission=admission,
            delivery_contract=contract,
            delivery_manifest=manifest,
            observed_files=observed,
            html_dependencies=dependencies,
        )

    assert raised.value.code == "consumer_capability_required_for_abstain"


@pytest.mark.parametrize(
    "internal_pointer",
    [
        "http://internal/G1Q3_RCA/demo/index%2Ehtml",
        "http://internal/G1Q3_RCA/demo/index&amp;#46;html",
        "http://192.168.26.174:18081?case=demo",
    ],
)
def test_public_delivery_rejects_internal_html_hidden_in_conclusion(
    internal_pointer,
):
    admission, contract, manifest, observed, dependencies = _bundle()
    _add_structural_candidate(
        contract,
        conclusion=f"证据见 {internal_pointer}",
    )

    with pytest.raises(DeliveryContractError) as raised:
        verify_delivery_bundle(
            admission=admission,
            delivery_contract=contract,
            delivery_manifest=manifest,
            observed_files=observed,
            html_dependencies=dependencies,
        )

    assert raised.value.code == "delivery_public_html_reference_forbidden"


def test_public_projection_keeps_out_of_scope_separate_from_abstention():
    rendered = render_public_rca_result(
        {
            "upstream_dispatch": {
                "hit_evaluator_keys": [],
                "hit_window_envelope": None,
                "hit_windows": [],
                "owner_bucket": None,
                "owner_bucket_label": None,
                "reason": "out_of_scope",
                "schema_version": "g1q3_upstream_dispatch_v2",
                "terminal_classification": "out_of_scope",
            },
            "summary": {"short_conclusion": "内部 scope gate 已拒绝。"},
        }
    )

    assert rendered.splitlines() == [
        "本单不在自动分析范围",
        MEDIUM_TIER_DISCLAIMER,
    ]
    assert "未发现已知异常模式" not in rendered


def test_public_projection_prefers_specific_causal_evidence_and_four_lines():
    contract = {
        "upstream_dispatch": {
            "hit_evaluator_keys": ["acc_heavy_decel_spec"],
            "hit_window_envelope": None,
            "hit_windows": [],
            "owner_bucket": "acc_longitudinal_control",
            "owner_bucket_label": "纵向控制",
            "reason": "single_owner_bucket_hit",
            "schema_version": "g1q3_upstream_dispatch_v2",
            "terminal_classification": "valid_dispatch",
        },
        "summary": {
            "short_conclusion": "ACC 异常退出判据命中，建议核查 ACC 状态机/抑制标志。"
        },
        "report": {"candidate_owner_domain": "ACC"},
        "public_result": {
            "causal_chain": {
                "hypotheses": [
                    {
                        "claim": "ACC 异常退出判据命中，建议核查 ACC 状态机/抑制标志。",
                        "supporting_evidence": [
                            {
                                "name": "异常退出",
                                "evidence": "STM_ACC_Mode 4/7/8 -> 2/9/10",
                            }
                        ],
                    }
                ]
            }
        },
    }

    rendered = render_public_rca_result(contract)

    assert rendered.splitlines() == [
        "建议责任方：纵向控制",
        MEDIUM_TIER_DISCLAIMER,
        "依据：异常退出：STM_ACC_Mode 4/7/8 -> 2/9/10。",
        "归因判断：ACC 异常退出判据命中，建议核查 ACC 状态机/抑制标志。",
    ]


def test_result_field_projection_is_two_lines_and_deduplicates_causal_evidence():
    contract = {
        "summary": {"short_conclusion": "自车加速源于纵向请求持续抬升。"},
        "report": {"candidate_owner_domain": "CONTROL_LONGITUDINAL"},
        "public_result": {
            "summary": {"short_conclusion": "自车加速源于纵向请求持续抬升。"},
            "responsibility": {"candidate": "CONTROL_LONGITUDINAL"},
            "causal_chain": {
                "narrative": [
                    {
                        "role": "因果判断",
                        "text": (
                            "纵向请求持续抬升，因此自车加速源于纵向请求持续抬升。"
                        ),
                    },
                    {"role": "证据", "text": "纵向请求持续抬升。"},
                ]
            },
            "evidence_summary": {
                "refs": [{"summary": "纵向请求持续抬升。"}]
            },
        },
    }

    rendered = delivery_contract_module.render_public_rca_result_field(
        contract,
        terminal_class="candidate_hypothesis",
    )

    assert rendered.splitlines() == [
        (
            "归因结论：纵向请求持续抬升，因此自车加速源于纵向请求持续抬升；"
            f"{MEDIUM_TIER_DISCLAIMER}。"
        ),
        "责任模块：纵向控制",
    ]
    assert rendered.count("纵向请求持续抬升，因此") == 1


def test_result_field_projection_abstention_is_exactly_two_lines():
    contract = {
        "summary": {
            "short_conclusion": (
                "生产数据已读取，但问题单未提供可核验的现象描述，不能自动归因。"
            )
        }
    }

    rendered = delivery_contract_module.render_public_rca_result_field(
        contract,
        terminal_class="honest_non_attribution",
    )

    assert len(rendered.splitlines()) == 2
    assert rendered.splitlines()[0].startswith("归因结论：")
    assert rendered.splitlines()[1] == "责任模块：暂无法判断"
    assert "因果关系：" not in rendered
    assert "关键证据：" not in rendered


def test_delivery_rejects_false_applied_consumer_capability():
    admission, contract, manifest, observed, dependencies = _bundle()
    capability = _consumer_capability()
    capability["actual_signals"] = []
    capability["actual_fields"] = []
    capability["actual_evaluators"] = []
    contract["consumer_capability"] = capability

    with pytest.raises(DeliveryContractError) as raised:
        verify_delivery_bundle(
            admission=admission,
            delivery_contract=contract,
            delivery_manifest=manifest,
            observed_files=observed,
            html_dependencies=dependencies,
        )

    assert raised.value.code == "consumer_capability_false_applied"


def test_delivery_blocks_supported_claim_without_emitted_supported_key():
    admission, contract, manifest, observed, dependencies = _bundle()
    capability = _consumer_capability()
    capability["actual_evaluators"][0]["status"] = "refuted"
    contract["consumer_capability"] = capability
    contract["quality_classification"] = "supported_attribution"

    with pytest.raises(DeliveryContractError) as raised:
        verify_delivery_bundle(
            admission=admission,
            delivery_contract=contract,
            delivery_manifest=manifest,
            observed_files=observed,
            html_dependencies=dependencies,
        )

    assert raised.value.code == "classification_conflict"
    assert "supported_attribution_evaluator_count_zero" in raised.value.detail


def test_delivery_blocks_banned_public_phrase():
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["summary"]["short_conclusion"] = "自动RCA未归因：请核对问题数据地址。"

    with pytest.raises(DeliveryContractError) as raised:
        verify_delivery_bundle(
            admission=admission,
            delivery_contract=contract,
            delivery_manifest=manifest,
            observed_files=observed,
            html_dependencies=dependencies,
        )

    assert raised.value.code == "classification_conflict"
    assert "banned_public_phrase" in raised.value.detail


def test_delivery_rejects_low_tier_blame_or_user_action_in_sealed_contract():
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["summary"]["short_conclusion"] = (
        "问题单缺少问题数据地址，不能自动归因，请补齐后重新发起。"
    )

    delivery = verify_delivery_bundle(
        admission=admission,
        delivery_contract=contract,
        delivery_manifest=manifest,
        observed_files=observed,
        html_dependencies=dependencies,
    )

    assert delivery.conclusion.splitlines()[0] == "本单未能定向"
    assert "请补齐" not in delivery.conclusion
    assert "问题单缺少" not in delivery.effect_payload["comment_content"]


def test_mdrive4_readiness_terminal_is_explicit_and_business_neutral():
    admission = _admission()
    terminal = build_terminal_delivery(
        business_key=admission.business_key,
        submission_key=admission.submission_key,
        generation=admission.generation,
        project_key=admission.source_refs.project_key,
        work_item_type_key=admission.source_refs.work_item_type_key,
        work_item_id="7044346306",
        outcome="quarantined",
        terminal_state="submission_quarantined",
        error_code="business_profile_adapter_not_ready",
        source_error_code="business_profile_adapter_not_ready",
        diagnostic_detail=(
            "已按官方字段路由到 mdrive4（数据 resolver=mdrive4_recorder_mcap_reference_v1，"
            "评测器=ct_evaluator_217_20260722，命名空间=rca/mdrive4），输入适配状态为 "
            "input_adapter_pending；本次不生成归因结论，不会进入 G1Q3，也不会回退到其他项目评测器"
        ),
    )

    assert terminal.diagnostic_code == "business_adapter_not_ready"
    diagnostic_lines = terminal.diagnostic_result.splitlines()
    assert diagnostic_lines[:2] == ["本单未能定向", MEDIUM_TIER_DISCLAIMER]
    assert any(line.startswith("检查路径：") for line in diagnostic_lines)
    assert any(line.startswith("数据来源：") for line in diagnostic_lines)
    assert any(line.startswith("检查结果：") for line in diagnostic_lines)
    assert "不能跨项目借用其他归因能力" in terminal.diagnostic_result
    assert "mdrive4" not in terminal.diagnostic_result
    assert "mdrive4_recorder_mcap_reference_v1" not in terminal.diagnostic_result
    assert "ct_evaluator_217_20260722" not in terminal.diagnostic_result
    assert "rca/mdrive4" not in terminal.diagnostic_result
    assert terminal.contract["diagnostic_detail"].startswith(
        "已按官方字段路由到 mdrive4"
    )
    assert terminal.effect_payload["comment_content"].splitlines()[0] == (
        "本单未能定向"
    )


def test_out_of_scope_terminal_uses_distinct_public_copy_and_rerun_prompt():
    admission = _admission()
    terminal = build_terminal_delivery(
        business_key=admission.business_key,
        submission_key=admission.submission_key,
        generation=admission.generation,
        project_key=admission.source_refs.project_key,
        work_item_type_key=admission.source_refs.work_item_type_key,
        work_item_id=admission.source_refs.work_item_id,
        outcome="quarantined",
        terminal_state="out_of_scope",
        error_code="out_of_scope",
        source_error_code="out_of_scope",
    )

    assert terminal.diagnostic_code == "out_of_scope"
    diagnostic_lines = terminal.diagnostic_result.splitlines()
    assert diagnostic_lines[:2] == [
        "本单不在自动分析范围",
        MEDIUM_TIER_DISCLAIMER,
    ]
    assert any(line.startswith("检查路径：") for line in diagnostic_lines)
    assert any(line.startswith("数据来源：") for line in diagnostic_lines)
    assert any(line.startswith("检查结果：") for line in diagnostic_lines)
    assert terminal.effect_payload["comment_content"].splitlines()[-1] == (
        rerun_prompt_line(canonical_issue_url("t03o4q", "7041712812"))
    )
    assert "G1Q3 RCA 机器人终态" not in terminal.effect_payload["comment_content"]


def _reseal(contract, manifest):
    manifest["artifact_set_id"] = compute_artifact_set_id(manifest)
    manifest["report_url"] = build_report_url(
        manifest["submission_key"], manifest["artifact_set_id"]
    )
    manifest["report_vm_path"] = build_report_vm_path(
        manifest["submission_key"], manifest["artifact_set_id"]
    )
    manifest["report_cifs_path"] = build_report_cifs_path(
        manifest["submission_key"], manifest["artifact_set_id"]
    )
    contract["artifacts"]["artifact_set_id"] = manifest["artifact_set_id"]


def _verify(bundle):
    admission, contract, manifest, observed, dependencies = bundle
    return verify_delivery_bundle(
        admission=admission,
        delivery_contract=contract,
        delivery_manifest=manifest,
        observed_files=observed,
        html_dependencies=dependencies,
    )


def _focus_refs():
    return ["report_data.json#/issue_focus"]


def _focus_payload(title: str, *, status: str = ANALYSIS_COMPLETE):
    intent = resolve_issue_intent(title)
    if status == ANALYSIS_COMPLETE:
        capabilities = [
            {
                "key": key,
                "status": "available",
                "provider": "g1q3_rca_worker",
                "version": "focus-test-v1",
                "evidence_refs": _focus_refs(),
            }
            for key in intent.required_capabilities
        ]
        segments = [
            {
                "role": key,
                "start_ts": 1.0,
                "end_ts": 2.0,
                "evidence_refs": _focus_refs(),
            }
            for key in intent.required_segments
        ]
        entities = [
            {
                "role": key,
                "target_id": str(index + 1),
                "object_class": "vru" if key == "vru_target" else "vehicle",
                "speed_summary": "1.0s-2.0s: 8.0 -> 2.0 m/s",
                "distance_summary": "1.0s-2.0s: 18.0 -> 5.0 m",
                "evidence_refs": _focus_refs(),
            }
            for index, key in enumerate(intent.required_entities)
        ]
        measurements = [
            {
                "key": key,
                "unit": "m/s",
                "summary": "已复算。",
                "evidence_refs": _focus_refs(),
            }
            for key in intent.required_measurements
        ]
        checks = [
            {
                "key": key,
                "status": "supported",
                "summary": "已闭环。",
                "evidence_refs": _focus_refs(),
            }
            for key in intent.required_checks
        ]
        calculations = [
            {
                "key": key,
                "formula": "a_lat = v^2 * kappa",
                "unit": "m/s^2",
                "summary": "已复算。",
                "evidence_refs": _focus_refs(),
            }
            for key in intent.required_calculations
        ]
        missing = []
        unsupported = []
        stop_reason = ""
    elif status == ANALYSIS_CAPABILITY_UNSUPPORTED:
        capabilities = segments = entities = measurements = checks = calculations = []
        unsupported = [intent.required_capabilities[0]]
        missing = [f"capability:{unsupported[0]}"]
        stop_reason = "该焦点能力未接入。"
    else:
        capabilities = segments = entities = measurements = checks = calculations = []
        missing = ["statement:problem_statement"]
        unsupported = []
        stop_reason = "问题陈述不足。"
    return {
        "schema_version": ISSUE_FOCUS_EVIDENCE_SCHEMA_VERSION,
        "issue_intent": intent.to_dict(),
        "title_sha256": issue_title_sha256(title),
        "analysis_status": status,
        "capabilities": capabilities,
        "segments": segments,
        "entities": entities,
        "measurements": measurements,
        "checks": checks,
        "calculations": calculations,
        "missing_requirements": missing,
        "unsupported_capabilities": unsupported,
        "stop_reason": stop_reason,
    }


def test_focus_bound_v2_delivery_carries_title_and_validation_into_effect():
    title = "ACC-前车切入，跟停前前车，自车加速后刹停"
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["schema_version"] = delivery_contract_module.DELIVERY_CONTRACT_SCHEMA_VERSION
    contract["issue_focus"] = _focus_payload(title)
    _add_structural_candidate(contract)

    delivery = verify_delivery_bundle(
        admission=admission,
        delivery_contract=contract,
        delivery_manifest=manifest,
        observed_files=observed,
        html_dependencies=dependencies,
        issue_title=title,
        report_issue_focus=contract["issue_focus"],
    )

    assert delivery.effect_payload["issue_title"] == title
    assert delivery.effect_payload["issue_focus_validation"]["analysis_status"] == (
        ANALYSIS_COMPLETE
    )
    assert delivery.effect_payload["issue_focus_sha256"]


def test_focus_bound_v2_missing_focus_is_rejected_before_publication():
    title = "ACC-前车切入，跟停前前车，自车加速后刹停"
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["schema_version"] = delivery_contract_module.DELIVERY_CONTRACT_SCHEMA_VERSION

    with pytest.raises(DeliveryContractError) as raised:
        verify_delivery_bundle(
            admission=admission,
            delivery_contract=contract,
            delivery_manifest=manifest,
            observed_files=observed,
            html_dependencies=dependencies,
            issue_title=title,
        )

    assert raised.value.code == "issue_focus_evidence_missing"


def test_bound_v1_without_focus_cannot_publish_generic_candidate():
    title = "ACC-前车切入，跟停前前车，自车加速后刹停"
    admission, contract, manifest, observed, dependencies = _bundle()
    _add_structural_candidate(contract)

    with pytest.raises(DeliveryContractError) as raised:
        verify_delivery_bundle(
            admission=admission,
            delivery_contract=contract,
            delivery_manifest=manifest,
            observed_files=observed,
            html_dependencies=dependencies,
            issue_title=title,
        )

    assert raised.value.code == "issue_focus_evidence_missing"


def test_focus_bound_v2_requires_manifest_read_report_binding():
    title = "ACC-前方仪表无目标，制动"
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["schema_version"] = delivery_contract_module.DELIVERY_CONTRACT_SCHEMA_VERSION
    contract["issue_focus"] = _focus_payload(title)

    with pytest.raises(DeliveryContractError) as missing:
        verify_delivery_bundle(
            admission=admission,
            delivery_contract=contract,
            delivery_manifest=manifest,
            observed_files=observed,
            html_dependencies=dependencies,
            issue_title=title,
        )
    assert missing.value.code == "issue_focus_report_binding_missing"

    report_focus = json.loads(json.dumps(contract["issue_focus"], ensure_ascii=False))
    report_focus["stop_reason"] = "tampered"
    with pytest.raises(DeliveryContractError) as mismatch:
        verify_delivery_bundle(
            admission=admission,
            delivery_contract=contract,
            delivery_manifest=manifest,
            observed_files=observed,
            html_dependencies=dependencies,
            issue_title=title,
            report_issue_focus=report_focus,
        )
    assert mismatch.value.code == "issue_focus_report_binding_mismatch"


def test_focus_capability_stop_cannot_be_promoted_by_generic_candidate():
    title = "AEB-AEB触发仪表无双闪"
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["schema_version"] = delivery_contract_module.DELIVERY_CONTRACT_SCHEMA_VERSION
    contract["issue_focus"] = _focus_payload(
        title,
        status=ANALYSIS_CAPABILITY_UNSUPPORTED,
    )
    _add_structural_candidate(contract)

    delivery = verify_delivery_bundle(
        admission=admission,
        delivery_contract=contract,
        delivery_manifest=manifest,
        observed_files=observed,
        html_dependencies=dependencies,
        issue_title=title,
        report_issue_focus=contract["issue_focus"],
    )

    assert delivery.effect_payload["terminal_class"] == "honest_non_attribution"
    assert delivery.conclusion.splitlines() == [
        "本单未能定向",
        "问题焦点所需能力未接入，未输出责任归因。",
    ]
    assert delivery.effect_payload["result_field_value"].splitlines()[1] == (
        "责任模块：暂无法判断"
    )


def test_focus_insufficient_statement_is_explicit_stop():
    title = "HMI-S弯"
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["schema_version"] = delivery_contract_module.DELIVERY_CONTRACT_SCHEMA_VERSION
    contract["issue_focus"] = _focus_payload(
        title,
        status=ANALYSIS_INSUFFICIENT_STATEMENT,
    )
    _add_structural_candidate(contract)

    delivery = verify_delivery_bundle(
        admission=admission,
        delivery_contract=contract,
        delivery_manifest=manifest,
        observed_files=observed,
        html_dependencies=dependencies,
        issue_title=title,
        report_issue_focus=contract["issue_focus"],
    )

    assert delivery.conclusion == "本单未能定向\n问题陈述不足，未输出责任归因。"


def test_valid_sealed_evidence_and_published_viz_build_issue_effect():
    delivery = _verify(_bundle())

    assert delivery.submission_key == _admission().submission_key
    assert delivery.effect_payload["effect_kind"] == "feishu_issue_comment"
    assert delivery.effect_payload["work_item_id"] == "7041712812"
    assert delivery.effect_payload["marker"] == delivery.marker
    assert delivery.marker in delivery.effect_payload["comment_content"]
    assert delivery.effect_key in delivery.marker
    result_field_value = delivery.effect_payload["result_field_value"]
    assert result_field_value.splitlines()[0].startswith("归因结论：")
    assert result_field_value.splitlines()[1] == "责任模块：暂无法判断"
    assert delivery.effect_payload["field_updates"] == [
        {
            "field_key": "field_9193cb",
            "field_value": result_field_value,
        },
        {
            "field_key": "field_8c912e",
            "field_value": delivery.report_url,
        },
    ]
    assert delivery.effect_payload["report_link_kind"] == "foxglove_viz"
    assert delivery.effect_payload["project_key"] == "t03o4q"
    assert delivery.effect_payload["project_simple_name"] == "g1q3"
    assert delivery.target_key == "feishu_project:t03o4q:issue:7041712812"
    assert delivery.report_url == delivery.foxglove_url
    assert delivery.report_url != delivery.manifest["report_url"]
    assert delivery.effect_payload["report_cifs_path"] == canonical_viz_mcap_cifs_path(
        delivery.submission_key
    )
    assert (
        delivery.effect_payload["report_cifs_path"]
        not in delivery.effect_payload["comment_content"]
    )
    assert delivery.viz_mcap_vm == canonical_viz_mcap_path(delivery.submission_key)
    assert delivery.foxglove_url == foxglove_url(delivery.viz_mcap_vm)
    assert delivery.foxglove_url in delivery.effect_payload["comment_content"]
    assert delivery.manifest["report_url"] not in delivery.effect_payload["comment_content"]
    assert delivery.issue_url == (
        "https://project.feishu.cn/g1q3/issue/detail/7041712812"
    )


def test_manifest_html_kind_is_rejected_even_when_internal_html_is_valid():
    admission, contract, manifest, observed, dependencies = _bundle()
    manifest["deliverable_kind"] = "html"

    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))

    assert exc.value.code == "delivery_kind_unsupported"


@pytest.mark.parametrize(
    "invalid_url",
    [
        "https://viewer.internal/G1Q3_RCA/cases/demo/index.html",
        "https://192.168.21.217/?ds=foxglove-http&ds.mcapPath=/mnt/tmp/demo/demo.viz.mcap",
        (
            "https://192.168.21.217/?ds=foxglove-http&ds.mcapPath="
            f"{canonical_viz_mcap_path(FORMAL_SUBMISSION_KEY)}&extra=1"
        ),
    ],
)
def test_public_comment_rejects_noncanonical_foxglove_url(invalid_url):
    with pytest.raises(DeliveryContractError) as exc:
        delivery_contract_module.build_issue_comment_content(
            marker="[RCA_DELIVERY:test:0123456789ab]",
            work_item_id="7041712812",
            report_status="report_ready",
            conclusion="本单未能定向\n仅供参考，待确认",
            report_url=invalid_url,
            foxglove_url=invalid_url,
            report_cifs_path="",
            issue_url=canonical_issue_url("t03o4q", "7041712812"),
        )

    assert exc.value.code == "foxglove_url_invalid"


@pytest.mark.parametrize("project_simple_name", ["", "../wrong"])
def test_success_delivery_requires_a_canonical_project_slug(project_simple_name):
    admission = build_rca_admission(
        project_key="t03o4q",
        project_simple_name=project_simple_name,
        work_item_type_key="issue",
        work_item_id="7041712812",
        rule_version="issue-created-v1",
        topic="feishu-project-workflow-event",
        partition=2,
        offset=10,
    )

    with pytest.raises(DeliveryContractError) as exc:
        _verify(_bundle(admission=admission))

    expected = (
        "delivery_project_simple_name_missing"
        if not project_simple_name
        else "delivery_project_simple_name_invalid"
    )
    assert exc.value.code == expected


def test_html_bundle_without_published_viz_is_not_publicly_deliverable():
    admission, contract, manifest, observed, dependencies = _bundle()
    publication = contract["artifacts"].pop("viz_publication")
    contract["artifacts"].pop("viz_mcap_vm")
    contract["report"]["deliverable_kind"] = "html"
    contract["report"]["status"] = "html_delivery_ready"
    observed = [
        item
        for item in observed
        if item["path"] not in {publication["path"], publication["manifest_path"]}
    ]

    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))

    assert exc.value.code == "delivery_kind_unsupported"


def test_foxglove_public_delivery_requires_published_viz_artifact():
    admission, contract, manifest, observed, dependencies = _bundle()
    publication = contract["artifacts"].pop("viz_publication")
    contract["artifacts"].pop("viz_mcap_vm")
    observed = [
        item
        for item in observed
        if item["path"] not in {publication["path"], publication["manifest_path"]}
    ]

    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))

    assert exc.value.code == "viz_publication_missing"


def test_case_local_viz_cannot_replace_formal_published_viz():
    admission, contract, manifest, observed, dependencies = _bundle()
    publication = contract["artifacts"]["viz_publication"]
    case_local = (
        f"/mnt/tmp/{admission.submission_key}/cases/7041712812_acc/"
        "7041712812_acc.viz.mcap"
    )
    contract["artifacts"]["viz_mcap_vm"] = case_local
    publication["path"] = case_local

    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))

    assert exc.value.code == "viz_publication_path_invalid"


def test_viz_observation_requires_publication_manifest_attestation():
    admission, contract, manifest, observed, dependencies = _bundle()
    viz_path = contract["artifacts"]["viz_mcap_vm"]
    next(item for item in observed if item["path"] == viz_path).pop(
        "sha256_attested_by_manifest"
    )

    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))

    assert exc.value.code == "viz_publication_observation_mismatch"


def test_delivery_rejects_empty_result_field_conclusion():
    bundle = _bundle()
    bundle[1]["summary"] = {"short_conclusion": "  ", "l0": ""}

    with pytest.raises(DeliveryContractError) as exc_info:
        _verify(bundle)

    assert exc_info.value.code == "delivery_conclusion_missing"


def test_thread_reply_effect_is_bound_to_exact_topic_and_is_deterministic():
    delivery = _verify(_bundle())
    target = {
        "schema_version": "pnc_rca_delivery_target_v1",
        "platform": "feishu",
        "chat_id": "oc_123456",
        "thread_id": "topic:om_root123",
        "reply_anchor_message_id": "om_root123",
        "source_message_id": "om_trigger456",
        "requester_id": "ou_requester789",
        "reply_in_thread": True,
        "output_cap": "L1",
    }
    built = build_thread_reply_effect(
        issue_effect_payload=delivery.effect_payload,
        target_key="feishu_thread:oc_123456:om_root123",
        target=target,
    )
    repeated = build_thread_reply_effect(
        issue_effect_payload=delivery.effect_payload,
        target_key="feishu_thread:oc_123456:om_root123",
        target=target,
    )

    assert built == repeated
    effect_key, semantic_sha, payload = built
    assert payload["effect_kind"] == DELIVERY_THREAD_EFFECT_KIND
    assert payload["semantic_payload_sha256"] == semantic_sha
    assert payload["effect_key"] == effect_key
    assert payload["thread_id"] == "topic:om_root123"
    assert payload["idempotency_uuid"]
    assert payload["marker"] in payload["message_content"]
    assert delivery.foxglove_url in payload["message_content"]
    assert delivery.manifest["report_url"] not in payload["message_content"]
    assert delivery.report_url == delivery.foxglove_url
    assert delivery.issue_url in payload["message_content"]
    assert (
        '<at user_id="ou_requester789"></at>'
        in payload["message_content"]
    )


def test_focus_bound_thread_reply_preserves_issue_focus_binding():
    title = "ACC-前车切入，跟停前前车，自车加速后刹停"
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["schema_version"] = delivery_contract_module.DELIVERY_CONTRACT_SCHEMA_VERSION
    contract["issue_focus"] = _focus_payload(title)
    _add_structural_candidate(contract)
    delivery = verify_delivery_bundle(
        admission=admission,
        delivery_contract=contract,
        delivery_manifest=manifest,
        observed_files=observed,
        html_dependencies=dependencies,
        issue_title=title,
        report_issue_focus=contract["issue_focus"],
    )
    target = {
        "schema_version": "pnc_rca_delivery_target_v1",
        "platform": "feishu",
        "chat_id": "oc_123456",
        "thread_id": "topic:om_root123",
        "reply_anchor_message_id": "om_root123",
        "source_message_id": "om_trigger456",
        "requester_id": "ou_requester789",
        "reply_in_thread": True,
        "output_cap": "L1",
    }

    _effect_key, _semantic_sha, payload = build_thread_reply_effect(
        issue_effect_payload=delivery.effect_payload,
        target_key="feishu_thread:oc_123456:om_root123",
        target=target,
    )

    assert payload["issue_title"] == title
    assert payload["issue_focus_sha256"] == delivery.effect_payload[
        "issue_focus_sha256"
    ]
    assert payload["issue_focus_validation"] == delivery.effect_payload[
        "issue_focus_validation"
    ]


def test_thread_reply_effect_refuses_target_or_topic_fallback_drift():
    delivery = _verify(_bundle())
    target = {
        "schema_version": "pnc_rca_delivery_target_v1",
        "platform": "feishu",
        "chat_id": "oc_123456",
        "thread_id": "topic:om_root123",
        "reply_anchor_message_id": "om_root123",
        "source_message_id": "om_trigger456",
        "requester_id": "ou_requester789",
        "reply_in_thread": True,
        "output_cap": "L1",
    }

    with pytest.raises(DeliveryContractError) as exc:
        build_thread_reply_effect(
            issue_effect_payload=delivery.effect_payload,
            target_key="feishu_thread:oc_other:om_root123",
            target=target,
        )
    assert exc.value.code == "delivery_subscription_target_invalid"

    with pytest.raises(DeliveryContractError) as exc:
        build_thread_reply_effect(
            issue_effect_payload=delivery.effect_payload,
            target_key="feishu_thread:oc_123456:om_root123",
            target={**target, "thread_id": ""},
        )
    assert exc.value.code == "delivery_subscription_target_invalid"


def test_artifact_identity_is_independent_of_publication_location():
    _admission_value, _contract, manifest, _observed, _dependencies = _bundle()
    baseline = compute_artifact_set_id(manifest)
    relocated = dict(manifest)
    relocated["report_url"] = "http://invalid.example/another/location"

    assert compute_artifact_set_id(relocated) == baseline


def test_report_url_uses_public_https_origin_and_content_address():
    assert build_report_url(FORMAL_SUBMISSION_KEY, FORMAL_ARTIFACT_SET_ID) == (
        "https://viewer.internal/G1Q3_RCA/cases/"
        f"{FORMAL_SUBMISSION_KEY}/{FORMAL_ARTIFACT_SET_ID}/index.html"
    )


def test_report_files_use_governed_task_landing_and_share():
    assert build_report_vm_path(FORMAL_SUBMISSION_KEY, FORMAL_ARTIFACT_SET_ID) == (
        f"/mnt/tmp/{FORMAL_SUBMISSION_KEY}/{FORMAL_ARTIFACT_SET_ID}/index.html"
    )
    assert build_report_cifs_path(FORMAL_SUBMISSION_KEY, FORMAL_ARTIFACT_SET_ID) == (
        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
        f"{FORMAL_SUBMISSION_KEY}/{FORMAL_ARTIFACT_SET_ID}/index.html"
    )


def test_legacy_manifest_v1_cannot_bypass_required_publication_paths():
    admission, contract, manifest, observed, dependencies = _bundle()
    manifest["schema_version"] = "delivery_manifest_v1"

    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))

    assert exc.value.code == "delivery_manifest_schema_unsupported"


@pytest.mark.parametrize(
    ("target", "field", "code"),
    [
        ("manifest", "unbound_extra", "delivery_manifest_shape_invalid"),
        (
            "artifact",
            "unbound_extra",
            "delivery_manifest_artifact_shape_invalid",
        ),
        ("html_validation", "unbound_extra", "html_validation_shape_invalid"),
    ],
)
def test_manifest_v2_rejects_unbound_fields(target, field, code):
    admission, contract, manifest, observed, dependencies = _bundle()
    if target == "manifest":
        manifest[field] = {"not": "identity-bound"}
    elif target == "artifact":
        manifest["artifacts"][0][field] = "not-identity-bound"
    else:
        manifest["html_validation"][field] = "not-identity-bound"

    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))

    assert exc.value.code == code


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (
            "report_vm_path",
            "/mnt/minieye/pdcl/department/perception_test_team/"
            "G1Q3_RCA/cases/report/index.html",
        ),
        (
            "report_cifs_path",
            "//hfs.minieye.tech/department-perception_test_team/"
            "G1Q3_RCA/cases/report/index.html",
        ),
    ],
)
def test_report_identity_paths_reject_non_task_landing(field, value):
    admission, contract, manifest, observed, dependencies = _bundle()
    manifest[field] = value

    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))

    assert exc.value.code == "report_path_identity_mismatch"


def test_changed_artifact_content_gets_a_different_publication_url():
    admission, _contract, manifest, _observed, _dependencies = _bundle()
    changed = {
        **manifest,
        "artifacts": [dict(item) for item in manifest["artifacts"]],
    }
    changed["artifacts"][-1]["sha256"] = "f" * 64
    changed_id = compute_artifact_set_id(changed)

    assert changed_id != manifest["artifact_set_id"]
    assert (
        build_report_url(admission.submission_key, changed_id)
        != (manifest["report_url"])
    )


@pytest.mark.parametrize("identity_kind", ["submission", "artifact_set"])
def test_report_url_identity_must_match_submission_and_artifact_set(identity_kind):
    admission, contract, manifest, observed, dependencies = _bundle()
    if identity_kind == "artifact_set":
        report_url = build_report_url(
            admission.submission_key, "g1q3-rca-artifact-v1-" + "f" * 64
        )
    else:
        report_url = build_report_url(
            "g1q3-rca-s1-" + "f" * 64, manifest["artifact_set_id"]
        )
    manifest["report_url"] = report_url

    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))

    assert exc.value.code == "report_url_identity_mismatch"


def test_report_asset_url_is_confined_to_the_content_addressed_directory():
    admission, _contract, manifest, _observed, _dependencies = _bundle()
    report_url = build_report_url(admission.submission_key, manifest["artifact_set_id"])

    assert build_report_artifact_url(report_url, "assets/app.css") == (
        report_url.rsplit("/", 1)[0] + "/assets/app.css"
    )
    with pytest.raises(DeliveryContractError) as exc:
        build_report_artifact_url(report_url, "../other/index.html")
    assert exc.value.code == "artifact_path_invalid"


def test_manifest_enforces_artifact_count_file_and_bundle_limits():
    _admission_value, _contract, manifest, _observed, _dependencies = _bundle()
    manifest["artifacts"][0]["size"] = MAX_DELIVERY_ARTIFACT_BYTES + 1
    with pytest.raises(DeliveryContractError) as exc:
        compute_artifact_set_id(manifest)
    assert exc.value.code == "delivery_artifact_file_too_large"

    _admission_value, _contract, manifest, _observed, _dependencies = _bundle()
    for item in manifest["artifacts"]:
        item["size"] = MAX_DELIVERY_ARTIFACT_BYTES
    with pytest.raises(DeliveryContractError) as exc:
        compute_artifact_set_id(manifest)
    assert exc.value.code == "delivery_artifact_bundle_too_large"

    _admission_value, _contract, manifest, _observed, _dependencies = _bundle()
    manifest["artifacts"].extend(
        dict(manifest["artifacts"][-1])
        for _index in range(MAX_DELIVERY_ARTIFACTS - len(manifest["artifacts"]) + 1)
    )
    with pytest.raises(DeliveryContractError) as exc:
        compute_artifact_set_id(manifest)
    assert exc.value.code == "delivery_manifest_artifacts_invalid"


def test_large_conclusion_is_utf8_bounded_while_report_links_are_preserved():
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["consumer_capability"] = _consumer_capability()
    _add_structural_candidate(contract, "候选结论" * 10_000)

    delivery = _verify((admission, contract, manifest, observed, dependencies))

    content = delivery.effect_payload["comment_content"]
    assert len(content.encode("utf-8")) <= MAX_FEISHU_COMMENT_BYTES
    assert delivery.foxglove_url in content
    assert manifest["report_url"] not in content
    assert delivery.conclusion.splitlines()[0] == "建议责任方：纵向控制"
    assert delivery.conclusion.splitlines()[1] == MEDIUM_TIER_DISCLAIMER
    assert any(line.endswith("...") for line in delivery.conclusion.splitlines())
    result_field_value = delivery.effect_payload["result_field_value"]
    assert len(result_field_value.encode("utf-8")) <= (
        delivery_contract_module.MAX_CONCLUSION_BYTES
    )
    assert len(result_field_value.splitlines()) == 2
    assert result_field_value.splitlines()[0].startswith("归因结论：")
    assert result_field_value.splitlines()[1] == "责任模块：ACC 功能链"
    assert MEDIUM_TIER_DISCLAIMER in result_field_value.splitlines()[0]
    assert {item.role for item in delivery.artifacts} == {
        "index_html",
        "report_data",
        "video",
    }


def test_contract_without_explicit_foxglove_kind_is_rejected():
    with pytest.raises(DeliveryContractError) as exc:
        _verify(_bundle(explicit_kind=None))
    assert exc.value.code == "delivery_kind_unsupported"


def test_viz_only_or_non_html_explicit_kind_is_rejected():
    bundle = list(_bundle(explicit_kind="viz"))
    with pytest.raises(DeliveryContractError) as exc:
        _verify(tuple(bundle))
    assert exc.value.code == "delivery_kind_unsupported"


def test_missing_manifest_fails_closed():
    admission, contract, _manifest, observed, dependencies = _bundle()
    with pytest.raises(DeliveryContractError) as exc:
        verify_delivery_bundle(
            admission=admission,
            delivery_contract=contract,
            delivery_manifest={},
            observed_files=observed,
            html_dependencies=dependencies,
        )
    assert exc.value.code == "delivery_manifest_missing"


def test_html_and_json_are_both_required_even_when_contract_says_deliverable():
    admission, contract, manifest, observed, dependencies = _bundle()
    manifest["artifacts"] = [
        item for item in manifest["artifacts"] if item["role"] != "report_data"
    ]
    _reseal(contract, manifest)
    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))
    assert exc.value.code == "required_html_artifact_missing"


def test_dependency_hash_mismatch_is_permanent_contract_error():
    admission, contract, manifest, observed, dependencies = _bundle()
    next(item for item in observed if item["path"].endswith("video.mp4"))["sha256"] = (
        "0" * 64
    )
    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))
    assert exc.value.code == "artifact_hash_mismatch"


def test_symlink_observation_is_rejected_even_with_matching_hash():
    admission, contract, manifest, observed, dependencies = _bundle()
    observed[0]["is_file"] = False
    observed[0]["is_symlink"] = True
    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))
    assert exc.value.code == "artifact_not_regular_file"


def test_symlinked_parent_directory_is_rejected():
    admission, contract, manifest, observed, dependencies = _bundle()
    next(item for item in observed if item["path"].endswith("video.mp4"))[
        "parents_symlink_free"
    ] = False
    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))
    assert exc.value.code == "artifact_not_regular_file"


def test_path_traversal_is_rejected_before_observation_lookup():
    admission, contract, manifest, observed, dependencies = _bundle()
    manifest["artifacts"][0]["path"] = "../index.html"
    _reseal(contract, manifest)
    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))
    assert exc.value.code == "artifact_path_invalid"


def test_manifest_identity_must_match_admission():
    admission, contract, manifest, observed, dependencies = _bundle()
    manifest["work_item_id"] = "7041712813"
    _reseal(contract, manifest)
    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))
    assert exc.value.code == "delivery_identity_mismatch"


def test_kafka_offset_is_audit_only_for_delivery_and_effect_keys():
    first = _verify(_bundle(admission=_admission(offset=10)))
    replay = _verify(_bundle(admission=_admission(offset=999)))

    assert first.submission_key == replay.submission_key
    assert first.artifact_set_id == replay.artifact_set_id
    assert first.delivery_id == replay.delivery_id
    assert first.effect_key == replay.effect_key


def test_generation_two_has_an_independent_delivery_and_effect_key():
    first = _verify(_bundle())
    second_admission = build_rca_admission(
        project_key="t03o4q",
        project_simple_name="g1q3",
        work_item_type_key="issue",
        work_item_id="7041712812",
        rule_version="issue-created-v1",
        trigger_kind="manual_retrigger",
        generation=2,
    )
    second = _verify(_bundle(admission=second_admission))

    assert first.business_key == second.business_key
    assert first.submission_key != second.submission_key
    assert first.artifact_set_id != second.artifact_set_id
    assert first.delivery_id != second.delivery_id
    assert first.effect_key != second.effect_key


def test_every_html_dependency_must_be_manifested_and_hash_verified():
    admission, contract, manifest, observed, dependencies = _bundle()
    manifest["artifacts"] = [
        item for item in manifest["artifacts"] if item["role"] != "video"
    ]
    _reseal(contract, manifest)

    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))
    assert exc.value.code == "html_dependency_not_manifested"


@pytest.mark.parametrize(
    ("mutator", "code"),
    [
        (
            lambda c, m: c.update(business_state="final_closed"),
            "delivery_business_state_not_ready",
        ),
        (
            lambda c, m: c["report"].update(is_deliverable=False),
            "delivery_report_not_deliverable",
        ),
        (
            lambda c, m: c["report"].update(requires_human_review="yes"),
            "delivery_review_boundary_invalid",
        ),
        (lambda c, m: m.update(sealed=False), "delivery_manifest_not_sealed"),
        (lambda c, m: m.update(artifact_set_id="0" * 64), "artifact_set_id_mismatch"),
    ],
)
def test_report_truth_and_seal_fail_closed(mutator, code):
    admission, contract, manifest, observed, dependencies = _bundle()
    mutator(contract, manifest)
    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))
    assert exc.value.code == code


@pytest.mark.parametrize(
    "url",
    [
        f"http://192.168.26.175:18081{FORMAL_REPORT_PATH}",
        f"http://192.168.26.174:18082{FORMAL_REPORT_PATH}",
        f"http://user@192.168.26.174:18081{FORMAL_REPORT_PATH}",
        f"https://viewer.internal{FORMAL_REPORT_PATH}?q=1",
        f"https://viewer.internal{FORMAL_REPORT_PATH}#x",
        (
            "https://viewer.internal/G1Q3_RCA/cases/%2e%2e/"
            f"{FORMAL_ARTIFACT_SET_ID}/index.html"
        ),
        (
            f"https://viewer.internal/G1Q3_RCA/cases/{FORMAL_SUBMISSION_KEY}/"
            "%252e%252e/index.html"
        ),
        (
            f"https://viewer.internal/G1Q3_RCA/cases/{FORMAL_SUBMISSION_KEY}/"
            f"{FORMAL_ARTIFACT_SET_ID}/nested/index.html"
        ),
    ],
)
def test_report_url_is_exactly_the_formal_public_html_route(url):
    admission, contract, manifest, observed, dependencies = _bundle()
    manifest["report_url"] = url

    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))
    assert exc.value.code == "report_url_invalid"


def test_report_url_accepts_the_exact_verified_internal_service(monkeypatch):
    monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", "http://192.168.26.174:18081")
    bundle = list(_bundle())
    bundle[2]["report_url"] = (
        f"http://192.168.26.174:18081/G1Q3_RCA/cases/"
        f"{bundle[2]['submission_key']}/{bundle[2]['artifact_set_id']}/index.html"
    )
    delivery = _verify(tuple(bundle))
    assert delivery.report_url == delivery.foxglove_url
    assert delivery.report_url != bundle[2]["report_url"]


@pytest.mark.parametrize(
    ("field", "value", "code"),
    [
        ("artifact_revision", None, "delivery_field_invalid"),
        ("artifact_revision", "1", "delivery_field_invalid"),
        ("sealed_at", "2026-07-10T08:00:00", "delivery_manifest_sealed_at_invalid"),
    ],
)
def test_manifest_revision_and_timezone_aware_seal_are_mandatory(field, value, code):
    admission, contract, manifest, observed, dependencies = _bundle()
    manifest[field] = value
    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))
    assert exc.value.code == code


def test_html_validation_object_is_mandatory():
    admission, contract, manifest, observed, dependencies = _bundle()
    manifest.pop("html_validation")
    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))
    assert exc.value.code == "html_validation_missing"


@pytest.mark.parametrize(
    ("field", "value", "code"),
    [
        ("state", "html_review_ready", "html_validation_state_invalid"),
        ("blockers", ["video_unaligned"], "html_validation_blocked"),
        ("fidelity_ok", False, "html_validation_fidelity_failed"),
    ],
)
def test_html_validation_must_be_delivery_ready_unblocked_and_faithful(
    field, value, code
):
    admission, contract, manifest, observed, dependencies = _bundle()
    manifest["html_validation"][field] = value
    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))
    assert exc.value.code == code


def test_html_validation_is_bound_to_sealed_report_data_hash():
    admission, contract, manifest, observed, dependencies = _bundle()
    manifest["html_validation"]["report_data_sha256"] = "f" * 64
    _reseal(contract, manifest)
    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))
    assert exc.value.code == "html_validation_report_data_hash_mismatch"


def test_revision_seal_and_html_validation_are_part_of_artifact_identity():
    _admission, _contract, manifest, _observed, _dependencies = _bundle()
    baseline = compute_artifact_set_id(manifest)
    for field, value in (
        ("artifact_revision", 2),
        ("sealed_at", "2026-07-10T08:00:01+00:00"),
    ):
        changed = dict(manifest)
        changed[field] = value
        assert compute_artifact_set_id(changed) != baseline


def test_formal_cases_artifact_cannot_replace_task_root_sealed_bundle():
    admission, contract, manifest, observed, dependencies = _bundle()
    formal = (
        "/mnt/minieye/pdcl/department/perception_test_team/"
        "G1Q3_RCA/cases/7041712812_acc/index.html"
    )
    manifest["artifacts"][0]["path"] = formal
    _reseal(contract, manifest)
    contract["artifacts"]["index_html_vm"] = formal
    with pytest.raises(DeliveryContractError) as exc:
        _verify((admission, contract, manifest, observed, dependencies))
    assert exc.value.code == "artifact_path_outside_root"


def test_mcap_is_never_an_html_delivery_dependency():
    _admission, _contract, manifest, _observed, _dependencies = _bundle()
    manifest["artifacts"].append({
        "role": "viz_mcap",
        "path": "viz.mcap",
        "size": 100,
        "sha256": "a" * 64,
        "media_type": "application/octet-stream",
        "required": False,
    })
    with pytest.raises(DeliveryContractError) as exc:
        compute_artifact_set_id(manifest)
    assert exc.value.code == "html_delivery_mcap_forbidden"


def _terminal_delivery(**updates):
    values = {
        "business_key": "rca-business-test",
        "submission_key": FORMAL_SUBMISSION_KEY,
        "generation": 1,
        "project_key": "t03o4q",
        "work_item_type_key": "issue",
        "work_item_id": "7051585084",
        "outcome": "terminal_failed",
        "terminal_state": "failed",
        "error_code": "vm_terminal_failed_unclassified",
    }
    values.update(updates)
    return build_terminal_delivery(**values)


def test_terminal_v2_writes_only_honest_result_without_fake_report_url():
    delivery = _terminal_delivery()

    assert delivery.effect_payload["schema_version"] == (
        TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION
    )
    assert delivery.diagnostic_code == "analysis_failed"
    diagnostic_lines = delivery.diagnostic_result.splitlines()
    assert diagnostic_lines[:2] == ["本单未能定向", MEDIUM_TIER_DISCLAIMER]
    assert any(line.startswith("检查路径：") for line in diagnostic_lines)
    assert any(line.startswith("数据来源：") for line in diagnostic_lines)
    assert any(line.startswith("检查结果：") for line in diagnostic_lines)
    assert "第 1 代" not in delivery.diagnostic_result
    assert "可能保留自其他代次" not in delivery.diagnostic_result
    assert delivery.effect_payload["field_updates"] == [
        {
            "field_key": "field_9193cb",
            "field_value": delivery.diagnostic_result,
        }
    ]
    assert delivery.job_payload()["report_url"] == ""
    assert delivery.contract == {
        "schema_version": "pnc_rca_terminal_diagnostic_v1",
        "generation": 1,
        "diagnostic_code": "analysis_failed",
        "diagnostic_result": delivery.diagnostic_result,
        "diagnostic_report_status": "not_generated",
        "report_field_write_policy": "preserve_existing",
        "preserved_report_semantics": "other_generation_not_current",
    }
    assert "field_8c912e" not in json.dumps(delivery.effect_payload, ensure_ascii=False)
    assert delivery.effect_payload["comment_content"].splitlines()[-1] == (
        rerun_prompt_line(canonical_issue_url("t03o4q", "7051585084"))
    )
    assert "本终态不改写" not in delivery.effect_payload["comment_content"]
    assert "不代表第 1 代结论" not in delivery.effect_payload["comment_content"]


def test_terminal_v2_result_and_preserve_policy_are_bound_to_generation():
    first = _terminal_delivery(generation=1)
    second = _terminal_delivery(generation=2)

    assert first.diagnostic_result == second.diagnostic_result
    assert "第 2 代" not in second.diagnostic_result
    assert "不代表第 2 代结论" not in second.effect_payload["comment_content"]
    assert second.effect_payload["generation"] == 2
    assert second.effect_payload["field_updates"] == [
        {
            "field_key": "field_9193cb",
            "field_value": second.diagnostic_result,
        }
    ]
    assert second.contract["generation"] == 2
    assert second.contract["report_field_write_policy"] == "preserve_existing"


def test_comment_only_terminal_v4_preserves_historical_v2_field_semantics():
    historical = _terminal_delivery()
    current = _terminal_delivery(
        schema_version=TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY
    )

    assert historical.effect_payload["schema_version"] == (
        TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION
    )
    assert historical.effect_payload["field_updates"]
    assert current.effect_payload["schema_version"] == (
        TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY
    )
    assert current.effect_payload["field_updates"] == []
    assert current.effect_payload["comment_content"]


def test_comment_only_terminal_v5_preserves_historical_v3_field_semantics():
    historical = _terminal_delivery(
        error_code="service_pipeline_runner_failed",
        terminal_fallback=_terminal_fallback(),
        schema_version=TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
    )
    current = _terminal_delivery(
        error_code="service_pipeline_runner_failed",
        terminal_fallback=_terminal_fallback(),
        schema_version=TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    )

    assert historical.effect_payload["field_updates"]
    assert current.effect_payload["field_updates"] == []
    assert current.effect_payload["schema_version"] == (
        TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY
    )


def _terminal_fallback(*, elapsed_seconds=1800):
    return {
        "schema_version": TERMINAL_FALLBACK_CONTRACT_SCHEMA_VERSION,
        "work_started_at": "2026-07-10T08:00:00+00:00",
        "deadline_at": "2026-07-10T08:30:00+00:00",
        "elapsed_seconds": elapsed_seconds,
        "confidence_tier": "low",
        "terminal_class": "honest_non_attribution",
        "route_key": "rca-failure-route-" + "a" * 64,
        "route_kind": "internal_alert",
        "route_owner": "rca-engineering",
    }


def test_terminal_fallback_v3_is_oracle_low_not_legacy_diagnostic():
    delivery = _terminal_delivery(
        error_code="service_pipeline_runner_failed",
        terminal_fallback=_terminal_fallback(),
        schema_version=TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
    )

    payload = delivery.effect_payload
    assert payload["schema_version"] == TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION
    assert payload["terminal_class"] == "honest_non_attribution"
    assert payload["confidence_tier"] == "low"
    assert payload["quality_oracle"]["schema_version"] == (
        "pnc_rca_structural_tier_oracle_v2"
    )
    assert (
        payload["quality_oracle_sha256"]
        == hashlib.sha256(
            json.dumps(
                payload["quality_oracle"],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
    )
    assert delivery.diagnostic_code == ""
    assert delivery.diagnostic_result == ""
    assert "diagnostic_code" not in delivery.contract
    assert "service_pipeline_runner_failed" not in payload["comment_content"]
    assert (
        "evidence_summary" not in delivery.contract["public_contract"]["public_result"]
    )
    assert all(
        phrase not in payload["comment_content"]
        for phrase in ("已取得的证据", "关键证据", "证据包")
    )


def test_terminal_fallback_v3_rejects_predeadline_receipt():
    with pytest.raises(DeliveryContractError) as exc:
        _terminal_delivery(
            terminal_fallback=_terminal_fallback(elapsed_seconds=1799),
            schema_version=TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
        )

    assert exc.value.code == "terminal_fallback_contract_invalid"


def test_terminal_fallback_v3_rejects_unbacked_evidence_wording(monkeypatch):
    original = delivery_contract_module._terminal_fallback_public_contract()
    tampered = json.loads(json.dumps(original, ensure_ascii=False))
    tampered["public_result"]["summary"]["short_conclusion"] = (
        "自动RCA未归因：已取得的证据尚不足以归因。"
    )
    monkeypatch.setattr(
        delivery_contract_module,
        "_terminal_fallback_public_contract",
        lambda: tampered,
    )

    with pytest.raises(DeliveryContractError) as exc:
        _terminal_delivery(
            terminal_fallback=_terminal_fallback(),
            schema_version=TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
        )

    assert exc.value.code == "terminal_fallback_unbacked_evidence_claim"


@pytest.mark.parametrize(
    ("source_error_code", "expected_diagnostic_code"),
    [
        ("issue_field_missing_remote_data_reference", "input_remote_data_required"),
        ("issue_field_invalid_remote_data_reference", "input_remote_data_invalid"),
        ("issue_field_invalid_frame_reference", "input_frame_required"),
        ("host_meegle_preread_timeout", "issue_source_unavailable"),
    ],
)
def test_pre_submit_terminal_projects_specific_safe_diagnostic(
    source_error_code, expected_diagnostic_code
):
    delivery = _terminal_delivery(
        outcome="quarantined",
        terminal_state="submission_quarantined",
        error_code="outbox_submission_quarantined",
        source_error_code=source_error_code,
    )

    assert delivery.diagnostic_code == expected_diagnostic_code
    serialized = json.dumps(delivery.effect_payload, ensure_ascii=False)
    assert source_error_code not in serialized
    assert delivery.effect_payload["error_code"] == "outbox_submission_quarantined"


def test_terminal_v1_remains_comment_only_for_historical_payloads():
    delivery = _terminal_delivery(
        schema_version=TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1
    )

    assert delivery.effect_payload["schema_version"] == (
        TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1
    )
    assert "field_updates" not in delivery.effect_payload
    assert delivery.contract == {}
    assert delivery.diagnostic_result == ""
    assert delivery.job_payload()["report_url"] == ""
