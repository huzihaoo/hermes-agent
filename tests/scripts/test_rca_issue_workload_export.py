from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from gateway.pnc_rca_data_access import build_remote_data_access
from scripts import pnc_rca_release_gate as release_gate
from scripts import rca_issue_workload_export as exporter


OBSERVED_AT = "2026-07-14T04:00:00.000000Z"


def _taxonomy_body() -> dict:
    return {
        "list": [
            {
                "field_key": exporter.FUNCTION_CATEGORY_FIELD,
                "field_name": "问题所属功能类别",
                "field_type": "tree-select",
                "option": [
                    {
                        "option_id": "driving",
                        "option_name": "行车辅助",
                        "children": [
                            {"option_id": "acc", "option_name": "ACC"},
                            {"option_id": "lcc", "option_name": "LCC"},
                            {"option_id": "fcw", "option_name": "FCW"},
                            {"option_id": "dnp", "option_name": "DNP-owner-approved"},
                        ],
                    }
                ],
            }
        ]
    }


def _moql_field(key: str, value: object) -> dict:
    if key == exporter.WORK_ITEM_ID_FIELD:
        wrapped = {"long_value": int(value)}
        value_type = "long_value"
    elif key == exporter.FUNCTION_CATEGORY_FIELD:
        parent, leaf = value
        wrapped = {
            "cascade_key_label_value": {
                "key": parent[0],
                "label": parent[1],
                "children": [{"key": leaf[0], "label": leaf[1]}],
            }
        }
        value_type = "cascade_key_label_value"
    else:
        wrapped = {"string_value": str(value)}
        value_type = "string_value"
    return {"key": key, "name": key, "value": wrapped, "value_type": value_type}


def _query_body(rows: list[dict], *, count: int, session_id: str = "session-secret") -> dict:
    return {
        "data": {
            "1": [
                {
                    "moql_field_list": [
                        _moql_field(exporter.WORK_ITEM_ID_FIELD, row["work_item_id"]),
                        _moql_field(exporter.FUNCTION_CATEGORY_FIELD, row["category"]),
                        _moql_field(exporter.PDCL_DATA_FIELD, row["pdcl"]),
                    ]
                }
                for row in rows
            ]
        },
        "list": [{"count": count, "group_infos": [{"group_id": "1"}]}],
        "session_id": session_id,
        "extra_info": None,
        "search_status_info": None,
    }


class _FakeClient:
    def __init__(self, rows: list[dict]):
        self.rows = rows
        self.mql_calls: list[str] = []

    def auth_status(self) -> dict:
        return {
            "authenticated": True,
            "host": exporter.MEEGLE_HOST,
            "expires_in_minutes": 60,
        }

    def field_metadata(self, **_kwargs: object) -> dict:
        return _taxonomy_body()

    def query(self, *, project_key: str, mql: str) -> dict:
        assert project_key == exporter.PROJECT_KEY
        self.mql_calls.append(mql)
        limit = mql.rsplit(" LIMIT ", 1)[1]
        offset, size = (int(value) for value in limit.split(","))
        return _query_body(
            self.rows[offset : offset + size],
            count=len(self.rows),
            session_id=f"session-{offset}",
        )


def _taxonomy_material() -> tuple[list[exporter.TaxonomyOption], str]:
    material, options, digest = exporter._load_taxonomy(
        _taxonomy_body(), expected_field_key=exporter.FUNCTION_CATEGORY_FIELD
    )
    assert material["field_key"] == exporter.FUNCTION_CATEGORY_FIELD
    return options, digest


def _canonical_write(path: Path, value: dict) -> None:
    path.write_bytes(exporter._canonical_json_bytes(value) + b"\n")
    path.chmod(0o600)


def _mapping_artifacts(taxonomy_sha256: str) -> tuple[dict, dict]:
    rules = [
        {
            "function_domain": domain,
            "option_ids": ["driving", leaf_id],
            "option_path": ["行车辅助", leaf_label],
        }
        for domain, leaf_id, leaf_label in (
            ("ACC", "acc", "ACC"),
            ("LCC", "lcc", "LCC"),
            ("FCW", "fcw", "FCW"),
            ("DNP", "dnp", "DNP-owner-approved"),
        )
    ]
    rules_material = {
        "schema_version": exporter.MAPPING_SCHEMA_VERSION,
        "project_key": exporter.PROJECT_KEY,
        "work_item_type": exporter.WORK_ITEM_TYPE,
        "field_key": exporter.FUNCTION_CATEGORY_FIELD,
        "taxonomy_sha256": taxonomy_sha256,
        "rules": rules,
    }
    approval_receipt = {
        "schema_version": exporter.MAPPING_APPROVAL_SCHEMA_VERSION,
        "authority": "PDCL/data owner",
        "approved_by": "owner-key-1",
        "approved_at": OBSERVED_AT,
        "mapping_rules_sha256": exporter._sha256_json(rules_material),
    }
    approval_receipt_raw = exporter._canonical_json_bytes(approval_receipt) + b"\n"
    mapping = {
        **rules_material,
        "approval": {
            "authority": "PDCL/data owner",
            "approved_by": "owner-key-1",
            "approved_at": OBSERVED_AT,
            "receipt_sha256": hashlib.sha256(approval_receipt_raw).hexdigest(),
        },
    }
    return mapping, approval_receipt


def test_scan_is_read_only_redacted_and_tracks_rejections(tmp_path: Path) -> None:
    secret_event = "event-secret-must-not-be-persisted"
    raw_command = f"mdi download event -u {secret_event} -s ./"
    rows = [
        {
            "work_item_id": "7000000001",
            "category": (("driving", "行车辅助"), ("acc", "ACC")),
            "pdcl": raw_command,
        },
        {
            "work_item_id": "7000000002",
            "category": (("driving", "行车辅助"), ("lcc", "LCC")),
            "pdcl": "not a supported address",
        },
        {
            "work_item_id": "7000000003",
            "category": (("driving", "行车辅助"), ("fcw", "FCW")),
            "pdcl": "mdi download clip -u clip-safe -s ./",
        },
    ]
    client = _FakeClient(rows)

    scan = exporter.scan_workloads(
        client,
        repo_root=Path(__file__).resolve().parents[2],
        page_size=2,
        observed_at=OBSERVED_AT,
    )

    assert len(client.mql_calls) == 2
    assert all(call.startswith("SELECT ") for call in client.mql_calls)
    assert all("ORDER BY `work_item_id` ASC" in call for call in client.mql_calls)
    assert scan.census["feishu"]["mutation_performed"] is False
    assert scan.census["statistics"]["snapshot_stable"] is True
    assert scan.census["statistics"]["source_scan_complete"] is True
    assert scan.census["statistics"]["records_seen"] == 3
    assert scan.census["statistics"]["valid_work_item_count"] == 2
    categories = {
        tuple(item["option_path"]): item
        for item in scan.census["statistics"]["categories"]
    }
    assert categories[("行车辅助", "ACC")]["unique_reference_count"] == 1
    assert categories[("行车辅助", "ACC")]["reader_class_counts"] == {
        "RemoteEventReader": 1
    }
    assert scan.census["statistics"]["rejection_reasons"] == {
        "remote_data_reference_invalid": 1
    }
    serialized = json.dumps(scan.census, ensure_ascii=False)
    assert secret_event not in serialized
    assert raw_command not in serialized
    assert "session-0" not in serialized
    assert scan.census["security"] == {
        "raw_issue_payload_persisted": False,
        "raw_pdcl_field_persisted": False,
        "description_or_attachment_persisted": False,
        "credential_or_token_persisted": False,
        "input_materialized": False,
        "input_materialized_bytes": 0,
    }
    mapping_request = exporter.build_mapping_request(
        scan,
        repo_root=Path(__file__).resolve().parents[2],
        census_file_sha256="8" * 64,
    )
    assert mapping_request["required_authority"] == "PDCL/data owner"
    assert mapping_request["feishu_binding"]["taxonomy_sha256"] == scan.taxonomy_sha256
    assert mapping_request["security"]["issue_identifiers_included"] is False
    assert {
        item["path"] for item in mapping_request["required_artifacts"]["schemas"]
    } == {
        "docs/pnc/schemas/rca_issue_workload_export_census_v1.schema.json",
        "docs/pnc/schemas/rca_issue_domain_mapping_v1.schema.json",
        "docs/pnc/schemas/rca_issue_domain_mapping_approval_v1.schema.json",
        "docs/pnc/schemas/rca_issue_workload_export_receipt_v1.schema.json",
    }
    request_text = json.dumps(mapping_request, ensure_ascii=False)
    assert secret_event not in request_text
    assert raw_command not in request_text


def test_meegle_failure_redacts_stderr() -> None:
    secret = "token=do-not-report"

    def runner(_args: object, _timeout: float) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([], 7, "", secret)

    client = exporter.MeegleClient(runner=runner)
    with pytest.raises(exporter.ExportError) as exc_info:
        client.auth_status()

    assert exc_info.value.code == "meegle_read_failed"
    assert secret not in exc_info.value.detail
    assert hashlib.sha256(secret.encode()).hexdigest() in exc_info.value.detail


def test_domain_mapping_requires_owner_only_taxonomy_bound_receipt(
    tmp_path: Path,
) -> None:
    options, taxonomy_sha256 = _taxonomy_material()
    mapping_path = tmp_path / "mapping.json"
    approval_path = tmp_path / "approval.json"
    mapping_body, approval_receipt = _mapping_artifacts(taxonomy_sha256)
    _canonical_write(mapping_path, mapping_body)
    _canonical_write(approval_path, approval_receipt)

    mapping = exporter.load_domain_mapping(
        mapping_path,
        approval_receipt_path=approval_path,
        project_key=exporter.PROJECT_KEY,
        work_item_type=exporter.WORK_ITEM_TYPE,
        taxonomy_options=options,
        taxonomy_sha256=taxonomy_sha256,
    )

    assert mapping.rules[(('driving', 'dnp'), ('行车辅助', 'DNP-owner-approved'))] == "DNP"
    assert mapping.approval["authority"] == "PDCL/data owner"

    mapping_path.chmod(0o644)
    with pytest.raises(exporter.ExportError, match="owner-only") as exc_info:
        exporter.load_domain_mapping(
            mapping_path,
            approval_receipt_path=approval_path,
            project_key=exporter.PROJECT_KEY,
            work_item_type=exporter.WORK_ITEM_TYPE,
            taxonomy_options=options,
            taxonomy_sha256=taxonomy_sha256,
        )
    assert exc_info.value.code == "domain_mapping_unsafe"


def _candidate(
    *, index: int, domain: str, option_id: str, option_label: str
) -> exporter.WorkloadCandidate:
    kind = "clip" if index % 4 == 0 else "event"
    if kind == "clip":
        source = f"mdi download clip -u clip-{index:04d} -s ./"
    else:
        source = f"mdi download event -u event-{index:04d} -s ./"
    access = build_remote_data_access(source)
    assert len(access["references"]) == 1
    return exporter.WorkloadCandidate(
        work_item_id=f"70{index:08d}",
        option_ids=("driving", option_id),
        option_path=("行车辅助", option_label),
        data_access=access,
    )


def _sealed_scan() -> tuple[exporter.ScanResult, exporter.DomainMapping]:
    options, taxonomy_sha256 = _taxonomy_material()
    domain_specs = (
        ("ACC", "acc", "ACC"),
        ("LCC", "lcc", "LCC"),
        ("FCW", "fcw", "FCW"),
        ("DNP", "dnp", "DNP-owner-approved"),
    )
    candidates = []
    rules = {}
    for domain_index, (domain, option_id, option_label) in enumerate(domain_specs):
        rules[(("driving", option_id), ("行车辅助", option_label))] = domain
        for within_domain in range(50):
            index = domain_index * 50 + within_domain
            candidates.append(
                _candidate(
                    index=index,
                    domain=domain,
                    option_id=option_id,
                    option_label=option_label,
                )
            )
    census = {
        "schema_version": exporter.CENSUS_SCHEMA_VERSION,
        "source": {
            "component": exporter.COMPONENT,
            "component_commit": "a" * 40,
            "module": exporter.MODULE_PATH,
            "module_sha256": "b" * 64,
            "committed_match": True,
            "module_clean": True,
        },
        "statistics": {"snapshot_stable": True, "source_scan_complete": True},
        "security": {
            "raw_issue_payload_persisted": False,
            "raw_pdcl_field_persisted": False,
            "description_or_attachment_persisted": False,
            "credential_or_token_persisted": False,
            "input_materialized": False,
            "input_materialized_bytes": 0,
        },
    }
    scan = exporter.ScanResult(census, candidates, options, taxonomy_sha256)
    mapping = exporter.DomainMapping(
        rules=rules,
        artifact_sha256="c" * 64,
        rules_material_sha256="e" * 64,
        approval={
            "authority": "PDCL/data owner",
            "approved_by": "owner-key-1",
            "approved_at": OBSERVED_AT,
            "receipt_sha256": "d" * 64,
        },
    )
    return scan, mapping


def test_manifest_is_deterministic_and_accepted_by_release_gate_consumer() -> None:
    scan, mapping = _sealed_scan()

    manifest, receipt = exporter.build_manifest(
        scan, mapping, generated_at=OBSERVED_AT, census_file_sha256="f" * 64
    )
    repeated, _ = exporter.build_manifest(
        scan, mapping, generated_at=OBSERVED_AT, census_file_sha256="f" * 64
    )

    assert exporter._canonical_json_bytes(manifest) == exporter._canonical_json_bytes(
        repeated
    )
    assert len(manifest["cases"]) == 200
    assert [case["case_id"] for case in manifest["cases"]] == sorted(
        case["case_id"] for case in manifest["cases"]
    )
    assert receipt["selection"]["domain_counts"] == exporter.DOMAIN_QUOTAS
    assert receipt["selection"]["unique_work_items"] == 200
    assert receipt["selection"]["unique_references"] == 200
    assert receipt["census"]["file_sha256"] == "f" * 64
    assert receipt["mapping"]["rules_material_sha256"] == "e" * 64
    assert receipt["manifest"]["body_sha256"] == exporter._sha256_json(manifest)
    assert receipt["manifest"]["file_sha256"] == hashlib.sha256(
        exporter._canonical_json_bytes(manifest) + b"\n"
    ).hexdigest()
    serialized = exporter._canonical_json_bytes(manifest).decode()
    assert "mdi download" not in serialized
    assert "-s ./" not in serialized

    soak_records = []
    for case in manifest["cases"]:
        reference = case["data_access"]["references"][0]
        kind = reference["kind"]
        locator_field = "clip_uuid" if kind == "clip" else "event_uuid"
        reader_class = reference["reader_class"]
        locator_sha256 = hashlib.sha256(reference[locator_field].encode()).hexdigest()
        reference_material = {
            "kind": kind,
            "reader_class": reader_class,
            "locator_field": locator_field,
            "locator_sha256": locator_sha256,
        }
        soak_records.append(
            {
                "case_id": case["case_id"],
                "work_item_id": case["work_item_id"],
                "function_domain": case["function_domain"],
                "quota_domain": exporter._quota_domain(case["function_domain"]),
                "workload_manifest_record_sha256": release_gate._remote_soak_sha256(
                    case
                ),
                "reference": {
                    **reference_material,
                    "reference_binding_sha256": release_gate._remote_soak_sha256(
                        reference_material
                    ),
                },
            }
        )
    started_at = datetime.strptime(OBSERVED_AT, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
        tzinfo=timezone.utc
    ) + timedelta(seconds=1)
    detail = release_gate._check_remote_reader_soak_manifest(
        manifest,
        summary={
            "source": manifest["source"],
            "sha256": release_gate._remote_soak_sha256(manifest),
        },
        records=soak_records,
        attempted=200,
        soak_started_at=started_at,
    )
    assert detail["case_count"] == 200
    assert detail["source_artifact_sha256"] == "b" * 64


def test_secure_atomic_write_is_canonical_owner_only_and_rejects_symlink(
    tmp_path: Path,
) -> None:
    output = tmp_path / "artifact.json"
    digest = exporter._secure_atomic_write(output, {"z": 1, "a": 2})

    assert output.read_bytes() == b'{"a":2,"z":1}\n'
    assert digest == hashlib.sha256(output.read_bytes()).hexdigest()
    assert os.stat(output).st_mode & 0o777 == 0o600

    target = tmp_path / "target.json"
    target.write_text("{}\n", encoding="utf-8")
    link = tmp_path / "link.json"
    link.symlink_to(target)
    with pytest.raises(exporter.ExportError) as exc_info:
        exporter._secure_atomic_write(link, {"safe": True})
    assert exc_info.value.code == "output_path_invalid"
