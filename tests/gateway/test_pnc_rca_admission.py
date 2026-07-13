import re

import pytest

from gateway.pnc_rca_admission import (
    RCA_ADMISSION_SCHEMA_VERSION,
    RcaAdmissionError,
    build_rca_admission,
    build_rca_issue_scope_key,
    build_rca_trigger_context,
    validate_rca_admission,
    validate_rca_trigger_context,
)


BASE = {
    "project_key": "t03o4q",
    "project_simple_name": "g1q3",
    "work_item_type_key": "issue",
    "work_item_id": "7041712812",
    "rule_version": "issue-created-v1",
}


def test_issue_created_replay_uses_generation_one_create_once_key():
    first = build_rca_admission(**BASE, topic="workflow", partition=1, offset=10)
    replay = build_rca_admission(**BASE, topic="workflow", partition=2, offset=99)

    assert first.generation == 1
    assert first.create_once is True
    assert first.dedupe_scope == "submission_key"
    assert first.business_key == replay.business_key
    assert first.submission_key == replay.submission_key
    assert first.source_refs.offset == 10
    assert replay.source_refs.offset == 99
    assert re.fullmatch(r"g1q3-rca-[bs]1-[0-9a-f]{64}", first.submission_key)
    assert len(first.submission_key) <= 128


def test_business_identity_includes_rule_version_but_not_transport_coordinates():
    original = build_rca_admission(**BASE, topic="workflow", partition=0, offset=1)
    next_rule = build_rca_admission(**{**BASE, "rule_version": "issue-created-v2"})
    next_issue = build_rca_admission(**{**BASE, "work_item_id": "7041712813"})

    assert original.business_key != next_rule.business_key
    assert original.business_key != next_issue.business_key


def test_issue_scope_identity_excludes_rule_version_but_keeps_exact_issue_refs():
    scope = build_rca_issue_scope_key(
        project_key=BASE["project_key"],
        work_item_type_key=BASE["work_item_type_key"],
        work_item_id=BASE["work_item_id"],
    )
    replay = build_rca_issue_scope_key(
        project_key=BASE["project_key"],
        work_item_type_key=BASE["work_item_type_key"],
        work_item_id=BASE["work_item_id"],
    )
    other_issue = build_rca_issue_scope_key(
        project_key=BASE["project_key"],
        work_item_type_key=BASE["work_item_type_key"],
        work_item_id="7041712813",
    )

    assert scope == replay
    assert scope != other_issue
    assert re.fullmatch(r"g1q3-rca-issue-v1-[0-9a-f]{64}", scope)


def test_v2_adds_project_simple_name_without_changing_stable_business_key():
    with_slug = build_rca_admission(**BASE)
    other_slug = build_rca_admission(
        **{**BASE, "project_simple_name": "renamed-project-slug"}
    )

    assert with_slug.schema_version == RCA_ADMISSION_SCHEMA_VERSION
    assert with_slug.source_refs.project_simple_name == "g1q3"
    assert with_slug.business_key == other_slug.business_key
    assert with_slug.submission_key == other_slug.submission_key


def test_manual_initial_generation_and_source_neutral_trigger_context():
    admission = build_rca_admission(
        **BASE, trigger_kind="manual_issue_request"
    )
    context = build_rca_trigger_context(
        source_kind="feishu_group_manual",
        project_key=BASE["project_key"],
        project_simple_name=BASE["project_simple_name"],
        work_item_type_key=BASE["work_item_type_key"],
        work_item_id=BASE["work_item_id"],
        rule_version=BASE["rule_version"],
        issue_url=(
            "https://project.feishu.cn/g1q3/issue/detail/7041712812"
        ),
    )

    assert admission.generation == 1
    assert admission.source_refs.topic == ""
    assert admission.source_refs.partition is None
    assert admission.source_refs.offset is None
    assert validate_rca_trigger_context(context.to_dict()) == context


def test_issue_created_generation_and_kafka_coordinates_fail_closed():
    with pytest.raises(RcaAdmissionError, match="generation 1"):
        build_rca_admission(**BASE, generation=2)
    with pytest.raises(RcaAdmissionError, match="generation 1"):
        build_rca_admission(**BASE, generation=True)
    with pytest.raises(RcaAdmissionError, match="generation 1"):
        build_rca_admission(**BASE, generation=1.0)  # type: ignore[arg-type]
    with pytest.raises(RcaAdmissionError, match="provided together"):
        build_rca_admission(**BASE, topic="workflow", partition=0)
    with pytest.raises(RcaAdmissionError, match="non-negative"):
        build_rca_admission(**BASE, topic="workflow", partition=-1, offset=0)


def test_manual_retrigger_requires_explicit_new_generation():
    with pytest.raises(RcaAdmissionError, match="explicit generation"):
        build_rca_admission(**BASE, trigger_kind="manual_retrigger")
    with pytest.raises(RcaAdmissionError, match="explicit generation"):
        build_rca_admission(**BASE, trigger_kind="manual_retrigger", generation=1)

    generation_two = build_rca_admission(**BASE, trigger_kind="manual_retrigger", generation=2)
    generation_two_replay = build_rca_admission(
        **BASE,
        trigger_kind="manual_retrigger",
        generation=2,
        topic="workflow",
        partition=0,
        offset=7,
    )
    generation_three = build_rca_admission(**BASE, trigger_kind="manual_retrigger", generation=3)

    assert generation_two.business_key == generation_three.business_key
    assert generation_two.submission_key == generation_two_replay.submission_key
    assert generation_two.submission_key != generation_three.submission_key


def test_validator_rederives_keys_and_rejects_forgery():
    admission = build_rca_admission(**BASE, topic="workflow", partition=0, offset=7)
    assert validate_rca_admission(admission) == admission
    assert validate_rca_admission(admission.to_dict()) == admission

    forged = admission.to_dict()
    forged["submission_key"] = "g1q3-rca-s1-" + "0" * 64
    with pytest.raises(RcaAdmissionError, match="does not match"):
        validate_rca_admission(forged)


def test_required_business_refs_are_not_case_folded_or_guessed():
    with pytest.raises(RcaAdmissionError, match="project_key is required"):
        build_rca_admission(**{**BASE, "project_key": " "})

    lower = build_rca_admission(**BASE)
    upper = build_rca_admission(**{**BASE, "project_key": "T03O4Q"})
    assert lower.business_key != upper.business_key
