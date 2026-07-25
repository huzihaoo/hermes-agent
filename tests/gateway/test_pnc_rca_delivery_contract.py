from __future__ import annotations

import hashlib
import json

import pytest

from gateway.pnc_rca_admission import build_rca_admission
from gateway.pnc_rca_delivery_contract import (
    DELIVERY_THREAD_EFFECT_KIND,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1,
    DeliveryContractError,
    MAX_DELIVERY_ARTIFACT_BYTES,
    MAX_DELIVERY_ARTIFACTS,
    build_report_cifs_path,
    build_report_artifact_url,
    build_report_url,
    build_report_vm_path,
    build_terminal_delivery,
    build_thread_reply_effect,
    compute_artifact_set_id,
    MAX_FEISHU_COMMENT_BYTES,
    verify_delivery_bundle,
)
from scripts.pnc_foxglove_delivery import (
    canonical_viz_mcap_cifs_path,
    canonical_viz_mcap_path,
    foxglove_url,
)


FORMAL_SUBMISSION_KEY = "g1q3-rca-s1-" + "a" * 64
FORMAL_ARTIFACT_SET_ID = "g1q3-rca-artifact-v1-" + "b" * 64
FORMAL_REPORT_PATH = (
    f"/G1Q3_RCA/cases/{FORMAL_SUBMISSION_KEY}/"
    f"{FORMAL_ARTIFACT_SET_ID}/index.html"
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
        b'<!doctype html><title>RCA</title>'
        b'<video src="assets/media/video.mp4"></video>'
    )
    if include_web_assets:
        index_html = (
            b'<!doctype html><title>RCA</title>'
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
        contents.update(
            {
                "assets/app.css": b"body{color:#111}",
                "assets/app.js": b"globalThis.RCA_READY=true;",
            }
        )
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
        "deliverable_kind": "html",
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
        manifest["artifacts"].extend(
            [
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
            ]
        )
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
        "summary": {"short_conclusion": "候选因果判断：减速度请求偏重。"},
        "artifacts": {
            "delivery_manifest_vm": root + "delivery_manifest.json",
            "artifact_set_id": manifest["artifact_set_id"],
            "index_html_vm": root + "index.html",
            "report_data_vm": root + "report_data.json",
            "viz_mcap_vm": viz_path,
            "viz_publication": viz_publication,
        },
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
    observed.extend(
        [
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
        ]
    )
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
            "field_lineage": {
                "schema_version": "g1q3_field_lineage_v2",
                "fidelity_ok": True,
            },
            "viz_lineage": {"ok": True, "status": "pass"},
        },
    }


def test_delivery_projects_consumer_capability_into_field_and_comment():
    admission, contract, manifest, observed, dependencies = _bundle()
    contract["consumer_capability"] = _consumer_capability()

    verified = verify_delivery_bundle(
        admission=admission,
        delivery_contract=contract,
        delivery_manifest=manifest,
        observed_files=observed,
        html_dependencies=dependencies,
    )

    assert "归因结论：减速度请求偏重。" in verified.conclusion
    assert "g1q3_863_consumer" not in verified.conclusion
    assert "signals/fields/evaluators" not in verified.effect_payload["comment_content"]
    assert "报告页用于查看证据" in verified.effect_payload["comment_content"]
    assert (
        verified.effect_payload["field_updates"][0]["field_value"]
        == verified.conclusion
    )


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
    assert "已按官方字段路由到 mdrive4" in terminal.diagnostic_result
    assert "mdrive4_recorder_mcap_reference_v1" not in terminal.diagnostic_result
    assert "ct_evaluator_217_20260722" not in terminal.diagnostic_result
    assert "rca/mdrive4" not in terminal.diagnostic_result
    assert "不能跨项目借用其他归因能力" in terminal.diagnostic_result
    assert terminal.contract["diagnostic_detail"].startswith(
        "已按官方字段路由到 mdrive4"
    )
    assert "【RCA 结果】" in terminal.effect_payload["comment_content"]
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


def test_valid_sealed_evidence_and_published_viz_build_issue_effect():
    delivery = _verify(_bundle())

    assert delivery.submission_key == _admission().submission_key
    assert delivery.effect_payload["effect_kind"] == "feishu_issue_comment"
    assert delivery.effect_payload["work_item_id"] == "7041712812"
    assert delivery.effect_payload["marker"] == delivery.marker
    assert delivery.marker in delivery.effect_payload["comment_content"]
    assert delivery.effect_key in delivery.marker
    assert delivery.effect_payload["field_updates"] == [
        {
            "field_key": "field_9193cb",
            "field_value": delivery.conclusion,
        },
        {
            "field_key": "field_8c912e",
            "field_value": delivery.report_url,
        },
    ]
    assert delivery.effect_payload["report_link_kind"] == "html_report"
    assert delivery.effect_payload["project_key"] == "t03o4q"
    assert delivery.effect_payload["project_simple_name"] == "g1q3"
    assert delivery.target_key == "feishu_project:t03o4q:issue:7041712812"
    assert delivery.report_url == delivery.manifest["report_url"]
    assert delivery.report_url != delivery.foxglove_url
    assert delivery.effect_payload["report_cifs_path"] == canonical_viz_mcap_cifs_path(
        delivery.submission_key
    )
    assert delivery.effect_payload["report_cifs_path"] not in delivery.effect_payload["comment_content"]
    assert delivery.viz_mcap_vm == canonical_viz_mcap_path(delivery.submission_key)
    assert delivery.foxglove_url == foxglove_url(delivery.viz_mcap_vm)
    assert delivery.report_url in delivery.effect_payload["comment_content"]
    assert delivery.foxglove_url not in delivery.effect_payload["comment_content"]
    assert delivery.issue_url == (
        "https://project.feishu.cn/g1q3/issue/detail/7041712812"
    )


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


def test_html_bundle_without_published_viz_is_not_deliverable():
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
    assert delivery.report_url in payload["message_content"]
    assert delivery.foxglove_url not in payload["message_content"]
    assert delivery.report_url != delivery.foxglove_url
    assert delivery.issue_url in payload["message_content"]


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
    assert build_report_url(admission.submission_key, changed_id) != (
        manifest["report_url"]
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
    report_url = build_report_url(
        admission.submission_key, manifest["artifact_set_id"]
    )

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
    contract["summary"]["short_conclusion"] = "候选结论" * 10_000

    delivery = _verify((admission, contract, manifest, observed, dependencies))

    content = delivery.effect_payload["comment_content"]
    assert len(content.encode("utf-8")) <= MAX_FEISHU_COMMENT_BYTES
    assert delivery.foxglove_url not in content
    assert manifest["report_url"] in content
    assert delivery.conclusion.splitlines()[0].endswith("...")
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
    next(item for item in observed if item["path"].endswith("video.mp4"))[
        "sha256"
    ] = "0" * 64
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
        (lambda c, m: c.update(business_state="final_closed"), "delivery_business_state_not_ready"),
        (lambda c, m: c["report"].update(is_deliverable=False), "delivery_report_not_deliverable"),
        (lambda c, m: c["report"].update(requires_human_review=False), "delivery_review_boundary_missing"),
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
    assert _verify(tuple(bundle)).report_url == bundle[2]["report_url"]


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
    manifest["artifacts"].append(
        {
            "role": "viz_mcap",
            "path": "viz.mcap",
            "size": 100,
            "sha256": "a" * 64,
            "media_type": "application/octet-stream",
            "required": False,
        }
    )
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
    assert "本次未生成可确认的自动归因" in delivery.diagnostic_result
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
    assert "field_8c912e" not in json.dumps(
        delivery.effect_payload, ensure_ascii=False
    )
    assert "本次未生成可确认的自动归因" in delivery.effect_payload["comment_content"]
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
