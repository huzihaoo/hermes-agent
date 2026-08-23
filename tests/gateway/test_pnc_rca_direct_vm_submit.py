from __future__ import annotations

from copy import deepcopy

import pytest

from gateway.pnc_rca_direct_vm_submit import (
    DIRECT_VM_SUBMIT_SCHEMA_VERSION,
    DirectVmSubmitError,
    build_direct_vm_request,
    status_first_submit,
    validate_direct_vm_request,
)


TASK_ID = "g1q3-rca-s1-" + "a" * 64
ARTIFACT_ROOT = f"/mnt/tmp/{TASK_ID}/"
ARTIFACT_CIFS_ROOT = f"//hfs1.example.test/rca/tmp/{TASK_ID}/"


def _source_refs(**updates):
    value = {
        "origin_source_id": "g1q3-rca-source-v1-" + "b" * 64,
        "source_event_id": "feishu-project-workflow-event:0:10",
        "generation": 1,
        "business_key": "g1q3-rca-b1-" + "c" * 64,
        "submission_key": TASK_ID,
    }
    value.update(updates)
    return value


def _execution_request(*, source_refs=None, **updates):
    refs = _source_refs() if source_refs is None else source_refs
    value = {
        "schema_version": "g1q3_rca_execution_request_v2",
        "request_kind": "issue_intake",
        "work_item": {
            "project_key": "project-key",
            "work_item_type": "problem-type",
            "work_item_id": "7041712812",
        },
        "data": {
            "data_access": {
                "schema_version": "g1q3_rca_remote_data_access_v1",
                "mode": "remote_read",
                "transport": "pdcl_pyclip",
                "references": [
                    {
                        "kind": "event",
                        "event_uuid": "event-7041712812",
                        "reader_class": "RemoteEventReader",
                    }
                ],
                "source": {
                    "field": "问题数据地址_PDCL",
                    "value_sha256": "e" * 64,
                },
                "reader_contract": {
                    "distribution": "pdcl_pyclip",
                    "required_version": "0.1.6+rca.2",
                    "mdi_download_allowed": False,
                    "fallback": "forbidden",
                    "completeness": "full_requested_scope",
                },
            },
            "artifact_root": ARTIFACT_ROOT,
            "artifact_cifs_root": ARTIFACT_CIFS_ROOT,
        },
        "execution_policy": {
            "data_access_mode": "remote_read",
            "allow_download": False,
            "input_materialization": "forbidden",
            "artifact_root": ARTIFACT_ROOT,
        },
        "source_refs": refs,
    }
    value.update(updates)
    return value


def _request(**updates):
    source_refs = updates.pop("source_refs", _source_refs())
    execution_request = updates.pop(
        "execution_request", _execution_request(source_refs=source_refs)
    )
    values = {
        "task_id": TASK_ID,
        "submission_key": TASK_ID,
        "auth": {
            "principal": "pnc-rca-direct-outbox",
            "capability": "g1q3_rca_direct_vm_submit",
        },
        "source_refs": source_refs,
        "execution_request": execution_request,
        "artifact_root": ARTIFACT_ROOT,
        "artifact_cifs_root": ARTIFACT_CIFS_ROOT,
    }
    values.update(updates)
    return build_direct_vm_request(**values)


def _missing(task_id=TASK_ID):
    return {
        "state": "missing",
        "task_id": task_id,
        "submission_key": "",
        "identity_sha256": "",
    }


def _observed(request, *, state="completed", identity_sha256=None, submission_key=None):
    return {
        "state": state,
        "task_id": request.task_id,
        "submission_key": request.submission_key
        if submission_key is None
        else submission_key,
        "identity_sha256": request.identity_sha256
        if identity_sha256 is None
        else identity_sha256,
    }


def test_builder_seals_strict_envelope_contract_and_identity():
    request = _request()

    assert request.schema_version == DIRECT_VM_SUBMIT_SCHEMA_VERSION
    assert request.task_id == request.submission_key == TASK_ID
    assert request.create_once is True
    assert request.allow_download is False
    assert request.artifact_root == ARTIFACT_ROOT
    assert request.artifact_cifs_root == ARTIFACT_CIFS_ROOT
    assert len(request.contract_sha256) == len(request.identity_sha256) == 64
    assert validate_direct_vm_request(request.to_dict()) == request
    assert _request().identity_sha256 == request.identity_sha256


@pytest.mark.parametrize(
    ("field", "error"),
    [
        ("contract_sha256", "direct_vm_contract_hash_mismatch"),
        ("identity_sha256", "direct_vm_identity_hash_mismatch"),
    ],
)
def test_validator_rederives_both_hashes(field, error):
    payload = _request().to_dict()
    payload[field] = "d" * 64

    with pytest.raises(DirectVmSubmitError, match=error):
        validate_direct_vm_request(payload)


def test_validator_rejects_unknown_top_level_fields_before_transport():
    payload = _request().to_dict()
    payload["legacy_adapter"] = True

    with pytest.raises(DirectVmSubmitError, match="envelope_unknown_field"):
        validate_direct_vm_request(payload)


def test_builder_rejects_arbitrary_execution_request_before_transport():
    with pytest.raises(
        DirectVmSubmitError,
        match="direct_vm_execution_request_(schema|fields)_invalid",
    ):
        _request(execution_request={"foo": "bar"})


def test_business_product_fields_and_narrative_are_not_legacy_metadata():
    execution_request = _execution_request()
    execution_request["work_item"]["product_id"] = "prod-42"
    execution_request["evidence"] = {
        "summary": "Production release has risk and needs review."
    }

    request = _request(execution_request=execution_request)

    assert request.execution_request["work_item"]["product_id"] == "prod-42"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value["execution_policy"].pop("data_access_mode"),
        lambda value: value["execution_policy"].pop("allow_download"),
        lambda value: value["execution_policy"].pop("input_materialization"),
        lambda value: value["data"].update({"data_access": {}}),
    ],
)
def test_execution_request_requires_remote_read_contract(mutation):
    execution_request = _execution_request()
    mutation(execution_request)

    with pytest.raises(
        DirectVmSubmitError,
        match="data_access|download|materialization|remote_data_access",
    ):
        _request(execution_request=execution_request)


@pytest.mark.parametrize(
    "auth",
    [
        {"principal": "", "capability": "g1q3_rca_direct_vm_submit"},
        {
            "principal": "pnc-rca-direct-outbox",
            "capability": "g1q3_rca_direct_vm_submit",
            "token": "must-not-cross-boundary",
        },
    ],
)
def test_auth_is_identity_only_and_cannot_carry_secrets(auth):
    with pytest.raises(DirectVmSubmitError, match="direct_vm_auth"):
        _request(auth=auth)


@pytest.mark.parametrize(
    "source_refs",
    [
        {**_source_refs(), "topic": "must-not-be-guessed"},
        {**_source_refs(), "generation": True},
        {**_source_refs(), "submission_key": "other-task"},
    ],
)
def test_source_refs_are_exact_and_bound_to_submission(source_refs):
    with pytest.raises(DirectVmSubmitError, match="direct_vm_source"):
        _request(
            source_refs=source_refs,
            execution_request=_execution_request(source_refs=source_refs),
        )


def test_task_id_must_equal_submission_key():
    with pytest.raises(DirectVmSubmitError, match="task_identity_mismatch"):
        _request(task_id="other-task")


@pytest.mark.parametrize(
    ("artifact_root", "artifact_cifs_root"),
    [
        (f"mnt/tmp/{TASK_ID}/", ARTIFACT_CIFS_ROOT),
        (f"/mnt/tmp/../{TASK_ID}/", ARTIFACT_CIFS_ROOT),
        ("/mnt/tmp/other-task/", ARTIFACT_CIFS_ROOT),
        (ARTIFACT_ROOT, f"hfs1.example.test/rca/tmp/{TASK_ID}/"),
        (ARTIFACT_ROOT, "//hfs1.example.test/rca/tmp/other-task/"),
    ],
)
def test_artifact_paths_are_canonical_absolute_and_identity_bound(
    artifact_root, artifact_cifs_root
):
    with pytest.raises(DirectVmSubmitError, match="artifact_.*root_invalid"):
        _request(
            artifact_root=artifact_root,
            artifact_cifs_root=artifact_cifs_root,
        )


def test_nested_artifact_paths_cannot_disagree_with_envelope():
    execution_request = _execution_request()
    execution_request["data"]["artifact_root"] = "/mnt/tmp/other-task/"

    with pytest.raises(DirectVmSubmitError, match="artifact_root_mismatch"):
        _request(execution_request=execution_request)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update({"allow_download": True}),
        lambda value: value.update({"pdcl_download_cmd": "disabled"}),
        lambda value: value.update({"note": "mdi download event -u unsafe"}),
        lambda value: value.update({"input_materialization": "allowed"}),
        lambda value: value.update({"data_access_mode": "minimal_download"}),
    ],
)
def test_no_download_contract_is_recursive(mutation):
    execution_request = _execution_request()
    mutation(execution_request)

    with pytest.raises(
        DirectVmSubmitError,
        match="direct_vm_.*download|materialization|data_access_mode",
    ):
        _request(execution_request=execution_request)


@pytest.mark.parametrize(
    "forbidden",
    [
        {"release_id": "candidate"},
        {"epoch_id": "e1"},
        {"w3_snapshot": "snapshot"},
        {"capacity_mode": "steady"},
        {"workspace_runtime": "runtime"},
        {"prod_receipt": "receipt"},
        {"write_fence": "issued"},
        {"resource_gate_bypass": False},
        {"risk_class": "high"},
        {"queue_if_blocked": False},
        {"lane": "heavy"},
        {"resource_class": "standard"},
        {"adapter": "rca_prod"},
    ],
)
def test_release_and_legacy_production_metadata_is_recursively_forbidden(forbidden):
    execution_request = _execution_request(extra=forbidden)

    with pytest.raises(DirectVmSubmitError, match="direct_vm_forbidden"):
        _request(execution_request=execution_request)


def test_matching_pre_status_is_deduplicated_without_create():
    request = _request()
    calls = []

    result = status_first_submit(
        request,
        lambda task_id: (
            calls.append(("status", task_id)) or _observed(request, state="failed")
        ),
        lambda _payload: pytest.fail("matching identity must suppress create"),
    )

    assert result.outcome == "deduplicated"
    assert result.observed_state == "failed"
    assert result.retryable is False
    assert result.create_attempted is False
    assert calls == [("status", TASK_ID)]


def test_mismatched_pre_status_is_permanent_conflict_without_create():
    request = _request()

    result = status_first_submit(
        request,
        lambda _task_id: _observed(request, identity_sha256="d" * 64),
        lambda _payload: pytest.fail("identity conflict must suppress create"),
    )

    assert result.outcome == "permanent_conflict"
    assert result.reason == "pre_status_identity_mismatch"
    assert result.retryable is False
    assert result.create_attempted is False


@pytest.mark.parametrize(
    "status",
    [
        {
            "state": "unknown",
            "task_id": TASK_ID,
            "submission_key": "",
            "identity_sha256": "",
        },
        {
            "state": "missing",
            "task_id": TASK_ID,
            "submission_key": TASK_ID,
            "identity_sha256": "d" * 64,
        },
    ],
)
def test_unknown_or_unproven_missing_status_retries_without_create(status):
    request = _request()

    result = status_first_submit(
        request,
        lambda _task_id: status,
        lambda _payload: pytest.fail("unproven absence must suppress create"),
    )

    assert result.outcome == "retry"
    assert result.retryable is True
    assert result.create_attempted is False


def test_status_transport_unavailable_retries_without_create():
    request = _request()

    def unavailable(_task_id):
        raise TimeoutError("status transport unavailable")

    result = status_first_submit(
        request,
        unavailable,
        lambda _payload: pytest.fail("unavailable status must suppress create"),
    )

    assert result.outcome == "retry"
    assert result.reason == "pre_status_unavailable"
    assert result.create_attempted is False


def test_proven_missing_creates_once_then_reconciles_from_fresh_status():
    request = _request()
    statuses = iter([_missing(), _observed(request)])
    calls = []

    def status(task_id):
        calls.append(("status", task_id))
        return next(statuses)

    def create(payload):
        calls.append(("create", payload))
        assert validate_direct_vm_request(payload) == request
        return {"accepted": True}

    result = status_first_submit(request, status, create)

    assert result.outcome == "reconciled"
    assert result.reason == "post_status_identity_match"
    assert result.retryable is False
    assert result.create_attempted is True
    assert [call[0] for call in calls] == ["status", "create", "status"]


def test_ambiguous_create_error_still_reconciles_from_post_status():
    request = _request()
    statuses = iter([_missing(), _observed(request, state="failed")])

    def ambiguous_create(_payload):
        raise TimeoutError("response lost after create")

    result = status_first_submit(
        request, lambda _task_id: next(statuses), ambiguous_create
    )

    assert result.outcome == "reconciled"
    assert result.observed_state == "failed"
    assert result.create_attempted is True


@pytest.mark.parametrize(
    ("post_status", "outcome", "retryable"),
    [
        (_missing(), "retry", True),
        (
            {
                "state": "unknown",
                "task_id": TASK_ID,
                "submission_key": "",
                "identity_sha256": "",
            },
            "retry",
            True,
        ),
        (
            {
                "state": "completed",
                "task_id": TASK_ID,
                "submission_key": "other-task",
                "identity_sha256": "d" * 64,
            },
            "permanent_conflict",
            False,
        ),
    ],
)
def test_post_create_status_is_authoritative(post_status, outcome, retryable):
    request = _request()
    statuses = iter([_missing(), deepcopy(post_status)])

    result = status_first_submit(
        request,
        lambda _task_id: next(statuses),
        lambda _payload: {"accepted": True},
    )

    assert result.outcome == outcome
    assert result.retryable is retryable
    assert result.create_attempted is True
