from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.error import URLError

import pytest

from scripts import pnc_rca_feishu_ingress_hold as hold
from scripts import pnc_rca_cutover_guard as cutover_guard


CHAT_A = "oc_aaaaaaaaaaaaaaaa"
CHAT_B = "oc_bbbbbbbbbbbbbbbb"
NOW = datetime(2026, 7, 13, 4, 0, tzinfo=timezone.utc)
MACHINE = {"source": "test_machine", "sha256": "1" * 64}


def _write_json(path: Path, value: object) -> None:
    path.write_bytes(hold._canonical_json(value))
    path.chmod(0o600)


def _sha256_json_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _precutover_service_state(old_runtime: dict) -> dict:
    jobs = {}
    for label in cutover_guard.SERVICE_LABELS:
        loaded = label == cutover_guard.GATEWAY_LABEL
        jobs[label] = {
            "launchd": {
                "label": label,
                "loaded": loaded,
                "state": "running" if loaded else "absent",
                "pid": old_runtime["process"]["pid"] if loaded else None,
                "last_exit_status": None,
            },
            "plist": {
                "path": str(
                    cutover_guard.CANONICAL_LAUNCH_AGENTS_ROOT / f"{label}.plist"
                ),
                "state": "regular",
                "sha256": hashlib.sha256(label.encode()).hexdigest(),
                "size_bytes": len(label),
                "mode": "0644",
                "uid": os.geteuid(),
                "nlink": 1,
            },
        }
    return {
        "schema_version": cutover_guard.LIVE_SERVICE_STATE_SCHEMA_VERSION,
        "target_runtime_root": str(cutover_guard.CANONICAL_LIVE_ROOT),
        "labels": list(cutover_guard.SERVICE_LABELS),
        "jobs": jobs,
    }


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _host_repo(path: Path) -> Path:
    adapter = path / hold.ADAPTER_RELATIVE_PATH
    adapter.parent.mkdir(parents=True)
    adapter.write_text(
        '_API_POLL_SIDECAR_SCHEMA = "feishu_api_poll_state_v1"\n',
        encoding="utf-8",
    )
    alias = path / "gateway/platforms/feishu.py"
    alias.parent.mkdir(parents=True)
    alias.write_text(
        "from plugins.platforms.feishu import adapter as _adapter\n",
        encoding="utf-8",
    )
    _git(path, "init", "-q")
    _git(path, "config", "user.email", "test@example.invalid")
    _git(path, "config", "user.name", "Ingress Hold Test")
    _git(path, "add", hold.ADAPTER_RELATIVE_PATH, "gateway/platforms/feishu.py")
    _git(path, "commit", "-qm", "fixture")
    return path


def _item(chat_id: str, message_id: str, create_time_ms: int) -> dict:
    return {
        "message_id": message_id,
        "msg_type": "text",
        "chat_id": chat_id,
        "create_time": str(create_time_ms),
        "update_time": str(create_time_ms),
        "root_id": "",
        "parent_id": "",
        "body": {"content": json.dumps({"text": f"@bot {message_id}"})},
        "sender": {
            "id": "ou_test_sender",
            "id_type": "open_id",
            "sender_type": "user",
        },
        "mentions": [],
    }


def _snapshot(chat_id: str, floor_ms: int, items: list[dict]) -> dict:
    return {
        "schema_version": hold.CHAT_SNAPSHOT_SCHEMA_VERSION,
        "chat_id": chat_id,
        "floor_ms": floor_ms,
        "complete": True,
        "started_at_ms": 2_000_000_000_000,
        "completed_at_ms": 2_000_000_000_001,
        "pages": [
            {
                "page_index": 0,
                "request_cursor_sha256": hashlib.sha256(b"").hexdigest(),
                "response_cursor_sha256": hashlib.sha256(b"").hexdigest(),
                "item_count": len(items),
                "accepted_count": len(items),
                "has_more": False,
                "stopped_at_floor": False,
            }
        ],
        "items": items,
    }


class SnapshotReader:
    def __init__(self, items_by_chat: dict[str, list[dict]], *, fail_chat: str = ""):
        self.items_by_chat = items_by_chat
        self.fail_chat = fail_chat
        self.calls: list[tuple[str, int, int, int]] = []

    def snapshot_chat(
        self,
        chat_id: str,
        *,
        floor_ms: int,
        page_size: int,
        max_pages: int,
    ) -> dict:
        self.calls.append((chat_id, floor_ms, page_size, max_pages))
        if chat_id == self.fail_chat:
            raise URLError("partial failure")
        return _snapshot(chat_id, floor_ms, self.items_by_chat.get(chat_id, []))


@pytest.fixture
def setup(tmp_path: Path) -> SimpleNamespace:
    repo = _host_repo(tmp_path / "candidate")
    env_file = tmp_path / "feishu.env"
    env_file.write_text(
        "FEISHU_APP_ID=cli_test_app\n"
        "FEISHU_APP_SECRET=never-persist-this-secret\n"
        "FEISHU_DOMAIN=feishu\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    live_sidecar = tmp_path / "runtime" / "feishu_api_poll_state_v1.json"
    live_sidecar.parent.mkdir()
    approval = tmp_path / "approval.json"
    cutover = tmp_path / "cutover.json"
    writer_stop = tmp_path / "writer-stop.json"
    inputs = hold.HoldInputs(
        env_file=env_file,
        host_candidate=repo,
        live_sidecar=live_sidecar,
        chat_ids=(CHAT_B, CHAT_A, CHAT_A),
        hold_id="hold-test-20260713",
        run_root=tmp_path / "hold-run",
        canonical_gateway_root=cutover_guard.CANONICAL_LIVE_ROOT,
        approval_receipt=approval,
        cutover_binding=cutover,
        page_size=25,
        max_pages=10,
    )
    return SimpleNamespace(
        inputs=inputs,
        repo=repo,
        env_file=env_file,
        live_sidecar=live_sidecar,
        approval=approval,
        cutover=cutover,
        writer_stop=writer_stop,
        writer_stop_observation=None,
    )


def _plan(
    setup: SimpleNamespace,
    reader: SnapshotReader | None = None,
    *,
    hold_start_ms: int = 2_000_000_000_000,
) -> hold.HoldResult:
    active = reader or SnapshotReader({CHAT_A: [], CHAT_B: []})
    return hold.run_ingress_hold(
        setup.inputs,
        phase="plan",
        reader=active,
        now=NOW,
        clock_ms=lambda: hold_start_ms,
        machine_identity_observer=lambda: MACHINE,
    )


def _authorize(setup: SimpleNamespace, plan: dict) -> None:
    plan_path = setup.inputs.run_root / hold.PLAN_FILENAME
    plan_sha = _sha256_json_file(plan_path)
    host = plan["host_adapter_identity"]
    identity = hold._approval_identity(MACHINE)
    _write_json(
        setup.approval,
        {
            "schema_version": hold.APPROVAL_SCHEMA_VERSION,
            "hold_id": plan["hold_id"],
            "decision": "authorize_feishu_ingress_hold_staging",
            "created_at": (NOW - timedelta(minutes=1)).isoformat(),
            "expires_at": (NOW + timedelta(hours=1)).isoformat(),
            "nonce": "nonce-abcdefghijklmnop",
            "plan_sha256": plan_sha,
            "chat_set_sha256": plan["chat_set_sha256"],
            "host_commit": host["host_commit"],
            "adapter_sha256": host["adapter_sha256"],
            "adapter_sidecar_schema": host["adapter_sidecar_schema"],
            "live_sidecar_identity_sha256": hold._sha256_json(
                plan["live_sidecar_identity"]
            ),
            "app_scope": plan["app_scope"],
            "action_set": list(hold.APPLY_ACTION_SET),
            "action_set_sha256": hold._sha256_json(list(hold.APPLY_ACTION_SET)),
            "identity": identity,
        },
    )
    live_runtime = {
        "schema_version": cutover_guard.RUNTIME_FILES_IDENTITY_SCHEMA_VERSION,
        "canonical_root": str(cutover_guard.CANONICAL_LIVE_ROOT),
        "root_identity": {"inode": 1},
        "runtime_files_sha256": "8" * 64,
    }
    old_runtime = {
        "schema_version": cutover_guard.GATEWAY_RUNNING_OBSERVATION_SCHEMA_VERSION,
        "canonical_root": str(cutover_guard.CANONICAL_LIVE_ROOT),
        "launchd": {
            "label": cutover_guard.GATEWAY_LABEL,
            "loaded": True,
            "pid": 41001,
            "state": "running",
        },
        "process": {
            "pid": 41001,
            "process_create_time": NOW.timestamp() - 60,
            "executable": str(
                cutover_guard.CANONICAL_LIVE_ROOT / ".venv/bin/python"
            ),
            "cwd": str(cutover_guard.CANONICAL_LIVE_ROOT),
            "cmdline_sha256": "9" * 64,
            "loaded_runtime_closure_sha256": cutover_guard._sha256_json(
                live_runtime
            ),
        },
        "live_runtime_identity": live_runtime,
    }
    writer_stop_observation = {
        "schema_version": (
            cutover_guard.GATEWAY_WRITER_STOP_OBSERVATION_SCHEMA_VERSION
        ),
        "canonical_root": str(cutover_guard.CANONICAL_LIVE_ROOT),
        "launchd": {
            "label": cutover_guard.GATEWAY_LABEL,
            "loaded": True,
            "pid": None,
            "state": "not_running",
        },
        "process_census": {
            "probe": "psutil_gateway_canonical_runtime_census_v1",
            "canonical_root": str(cutover_guard.CANONICAL_LIVE_ROOT),
            "matching_processes": [],
        },
        "live_runtime_identity": live_runtime,
        "live_sidecar_identity": plan["live_sidecar_identity"],
    }
    lease_fingerprint = "a" * 64
    release_prepare_sha256 = "b" * 64
    release_approval_sha256 = "c" * 64
    old_runtime_sha256 = cutover_guard._sha256_json(old_runtime)
    precutover_services = _precutover_service_state(old_runtime)
    writer_stop_body = {
        "schema_version": cutover_guard.WRITER_STOP_RECEIPT_SCHEMA_VERSION,
        "release_id": "release-20260713-a",
        "hold_id": plan["hold_id"],
        "plan_sha256": plan_sha,
        "observed_at": (NOW - timedelta(seconds=10)).isoformat(),
        "production_effects_executed": False,
        "lease_fingerprint": lease_fingerprint,
        "release_prepare_manifest_sha256": release_prepare_sha256,
        "approval_receipt_sha256": release_approval_sha256,
        "old_gateway_process": old_runtime["process"],
        "old_gateway_runtime_identity": old_runtime,
        "old_gateway_runtime_identity_sha256": old_runtime_sha256,
        "precutover_service_state": precutover_services,
        "precutover_service_state_sha256": cutover_guard._sha256_json(
            precutover_services
        ),
        "writer_stop_observation": writer_stop_observation,
        "writer_stop_observation_sha256": cutover_guard._sha256_json(
            writer_stop_observation
        ),
        "live_sidecar_identity": plan["live_sidecar_identity"],
        "live_sidecar_identity_sha256": hold._sha256_json(
            plan["live_sidecar_identity"]
        ),
    }
    _write_json(setup.writer_stop, writer_stop_body)
    setup.writer_stop_observation = writer_stop_observation
    lease = SimpleNamespace(
        fingerprint=lease_fingerprint,
        body={
            "release_id": "release-20260713-a",
            "expires_at": (NOW + timedelta(minutes=30)).isoformat(),
            "release_prepare_manifest": {"sha256": release_prepare_sha256},
            "approval_receipt": {"sha256": release_approval_sha256},
        },
        assert_active=lambda: None,
    )
    hold.build_cutover_binding(
        setup.inputs,
        release_id="release-20260713-a",
        writer_stop_receipt=setup.writer_stop,
        lease=lease,
        output_path=setup.cutover,
        now=NOW,
    )


def _install_gate_validator(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import pnc_rca_release_gate

    def validator(**kwargs: object) -> dict:
        return {
            "schema_version": hold.GATE_VALIDATION_SCHEMA_VERSION,
            "ok": True,
            "plan_sha256": kwargs["plan_sha256"],
            "approval_receipt_sha256": kwargs["approval_receipt_sha256"],
            "cutover_binding_sha256": kwargs["cutover_binding_sha256"],
            "writer_stop_receipt_sha256": kwargs[
                "writer_stop_receipt_sha256"
            ],
            "cutover_lease_fingerprint": kwargs["writer_stop_receipt"][
                "lease_fingerprint"
            ],
            "old_gateway_runtime_identity_sha256": kwargs[
                "writer_stop_receipt"
            ]["old_gateway_runtime_identity_sha256"],
            "gateway_writer_state": "stopped",
        }

    monkeypatch.setattr(
        pnc_rca_release_gate,
        "validate_feishu_ingress_hold_cutover_binding",
        validator,
        raising=False,
    )


def _apply(
    setup: SimpleNamespace,
    reader: SnapshotReader,
    monkeypatch: pytest.MonkeyPatch,
) -> hold.HoldResult:
    _install_gate_validator(monkeypatch)
    return hold.run_ingress_hold(
        setup.inputs,
        phase="apply",
        reader=reader,
        now=NOW,
        clock_ms=lambda: 2_000_000_000_100,
        machine_identity_observer=lambda: MACHINE,
        writer_stop_observer=lambda: setup.writer_stop_observation,
    )


def _rewrite_writer_receipt(setup: SimpleNamespace, body: dict) -> None:
    _write_json(setup.writer_stop, body)
    cutover = json.loads(setup.cutover.read_text())
    cutover["writer_stop_receipt_sha256"] = _sha256_json_file(setup.writer_stop)
    _write_json(setup.cutover, cutover)


def _sidecar(app_scope: str, *, state: dict, revision: int = 7) -> dict:
    pending_count = sum(len(items) for items in state["pending"].values())
    continuation_count = len(state["scan_state"])
    return {
        "schema_version": "feishu_api_poll_state_v1",
        "app_scope": app_scope,
        "revision": revision,
        "updated_at": 1_999_999_999.0,
        "rollback_readiness": {
            "ready": pending_count == 0 and continuation_count == 0,
            "pending_count": pending_count,
            "scan_continuation_count": continuation_count,
        },
        "state": state,
    }


def test_plan_is_read_only_owner_only_and_binds_exact_inputs(setup: SimpleNamespace) -> None:
    reader = SnapshotReader(
        {
            CHAT_A: [_item(CHAT_A, "om_plan_a", 2_000_000_000_010)],
            CHAT_B: [],
        }
    )

    result = _plan(setup, reader)

    assert result.phase == "plan"
    assert result.body["chat_ids"] == [CHAT_A, CHAT_B]
    assert result.body["live_sidecar_identity"]["state"] == "absent"
    assert result.body["host_adapter_identity"]["host_commit"] == _git(
        setup.repo, "rev-parse", "HEAD"
    )
    assert result.body["api_snapshot"]["chats"][CHAT_A]["pages"][0][
        "request_cursor_sha256"
    ] == hashlib.sha256(b"").hexdigest()
    assert result.body["side_effect_contract"] == {
        "feishu_message_writes": False,
        "live_sidecar_writes": False,
        "gateway_process_changes": False,
        "launchctl_invoked": False,
        "auth_token_exchange": True,
        "message_api": "GET_only",
        "output_scope": "unique_owner_only_run_root",
    }
    assert not setup.live_sidecar.exists()
    assert (setup.inputs.run_root.stat().st_mode & 0o777) == 0o700
    assert (result.artifact_path.stat().st_mode & 0o777) == 0o600
    serialized = result.artifact_path.read_text(encoding="utf-8")
    assert "never-persist-this-secret" not in serialized
    assert "same-filesystem sibling temp" in result.body["future_install"]["procedure"]
    assert reader.calls == [
        (CHAT_A, 2_000_000_000_000, 25, 10),
        (CHAT_B, 2_000_000_000_000, 25, 10),
    ]


def test_host_identity_binds_plugin_implementation_behind_gateway_alias(
    setup: SimpleNamespace,
) -> None:
    identity = hold._host_adapter_identity(setup.repo)

    assert identity["adapter_relative_path"] == (
        "plugins/platforms/feishu/adapter.py"
    )
    assert identity["adapter_sidecar_schema"] == hold._API_POLL_SIDECAR_SCHEMA
    assert identity["adapter_sha256"] == hashlib.sha256(
        (setup.repo / hold.ADAPTER_RELATIVE_PATH).read_bytes()
    ).hexdigest()


def test_repeated_plan_reuses_exact_snapshot_without_refetch(setup: SimpleNamespace) -> None:
    first = _plan(setup)
    unexpected = SnapshotReader({}, fail_chat=CHAT_A)

    second = _plan(setup, unexpected)

    assert second.resumed is True
    assert second.body == first.body
    assert unexpected.calls == []


def test_partial_chat_failure_publishes_no_plan(setup: SimpleNamespace) -> None:
    reader = SnapshotReader({CHAT_A: []}, fail_chat=CHAT_B)

    with pytest.raises(hold.IngressHoldError) as error:
        _plan(setup, reader)

    assert error.value.code == "feishu_ingress_hold_read_api_failed"
    assert not (setup.inputs.run_root / hold.PLAN_FILENAME).exists()
    assert not setup.live_sidecar.exists()


def test_plan_rejects_dirty_or_uncommitted_host_adapter(setup: SimpleNamespace) -> None:
    adapter = setup.repo / hold.ADAPTER_RELATIVE_PATH
    adapter.write_text(adapter.read_text() + "# dirty\n", encoding="utf-8")

    with pytest.raises(hold.IngressHoldError) as error:
        _plan(setup)

    assert error.value.code == "feishu_ingress_hold_host_tree_dirty"


def test_old_sidecar_schema_fails_closed_before_api(setup: SimpleNamespace) -> None:
    _write_json(
        setup.live_sidecar,
        {
            "schema_version": "feishu_api_poll_state_v0",
            "app_scope": "not-relevant",
            "revision": 1,
            "updated_at": 1,
            "rollback_readiness": {
                "ready": True,
                "pending_count": 0,
                "scan_continuation_count": 0,
            },
            "state": hold._empty_state(),
        },
    )
    reader = SnapshotReader({})

    with pytest.raises(hold.IngressHoldError) as error:
        _plan(setup, reader)

    assert error.value.code == "feishu_ingress_hold_sidecar_schema_unsupported"
    assert reader.calls == []


def test_sidecar_change_during_snapshot_invalidates_plan(setup: SimpleNamespace) -> None:
    app_scope = hold._app_scope(
        {"FEISHU_APP_ID": "cli_test_app", "FEISHU_DOMAIN": "feishu"}
    )

    class RacingReader(SnapshotReader):
        def snapshot_chat(self, chat_id: str, **kwargs: int) -> dict:
            result = super().snapshot_chat(chat_id, **kwargs)
            if chat_id == CHAT_A:
                _write_json(
                    setup.live_sidecar,
                    _sidecar(app_scope, state=hold._empty_state()),
                )
            return result

    with pytest.raises(hold.IngressHoldError) as error:
        _plan(setup, RacingReader({CHAT_A: [], CHAT_B: []}))

    assert error.value.code == "feishu_ingress_hold_live_sidecar_changed"
    assert not (setup.inputs.run_root / hold.PLAN_FILENAME).exists()


def test_apply_requires_release_gate_validator_before_refetch(
    setup: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(setup).body
    _authorize(setup, plan)
    from scripts import pnc_rca_release_gate

    monkeypatch.delattr(
        pnc_rca_release_gate,
        "validate_feishu_ingress_hold_cutover_binding",
        raising=False,
    )
    reader = SnapshotReader({})

    with pytest.raises(hold.IngressHoldError) as error:
        hold.run_ingress_hold(
            setup.inputs,
            phase="apply",
            reader=reader,
            now=NOW,
            machine_identity_observer=lambda: MACHINE,
            writer_stop_observer=lambda: setup.writer_stop_observation,
        )

    assert error.value.code == "feishu_ingress_hold_gate_validator_unsupported"
    assert reader.calls == []
    assert not (setup.inputs.run_root / hold.STAGED_SIDECAR_FILENAME).exists()


def test_old_gate_signature_is_unsupported_before_refetch(
    setup: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(setup).body
    _authorize(setup, plan)
    from scripts import pnc_rca_release_gate

    def old_validator(
        *,
        plan,
        plan_sha256,
        approval_receipt,
        approval_receipt_sha256,
        cutover_binding,
        cutover_binding_sha256,
    ):
        del (
            plan,
            plan_sha256,
            approval_receipt,
            approval_receipt_sha256,
            cutover_binding,
            cutover_binding_sha256,
        )
        return {}

    monkeypatch.setattr(
        pnc_rca_release_gate,
        "validate_feishu_ingress_hold_cutover_binding",
        old_validator,
        raising=False,
    )
    reader = SnapshotReader({})

    with pytest.raises(hold.IngressHoldError) as error:
        hold.run_ingress_hold(
            setup.inputs,
            phase="apply",
            reader=reader,
            now=NOW,
            machine_identity_observer=lambda: MACHINE,
            writer_stop_observer=lambda: setup.writer_stop_observation,
        )

    assert error.value.code == "feishu_ingress_hold_gate_validator_unsupported"
    assert reader.calls == []


def test_gate_receives_exact_writer_stop_body_and_raw_sha(
    setup: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(setup).body
    _authorize(setup, plan)
    from scripts import pnc_rca_release_gate

    observed = {}

    def validator(**kwargs):
        observed.update(kwargs)
        receipt = kwargs["writer_stop_receipt"]
        return {
            "schema_version": hold.GATE_VALIDATION_SCHEMA_VERSION,
            "ok": True,
            "plan_sha256": kwargs["plan_sha256"],
            "approval_receipt_sha256": kwargs["approval_receipt_sha256"],
            "cutover_binding_sha256": kwargs["cutover_binding_sha256"],
            "writer_stop_receipt_sha256": kwargs[
                "writer_stop_receipt_sha256"
            ],
            "cutover_lease_fingerprint": receipt["lease_fingerprint"],
            "old_gateway_runtime_identity_sha256": receipt[
                "old_gateway_runtime_identity_sha256"
            ],
            "gateway_writer_state": "stopped",
        }

    monkeypatch.setattr(
        pnc_rca_release_gate,
        "validate_feishu_ingress_hold_cutover_binding",
        validator,
        raising=False,
    )
    hold.run_ingress_hold(
        setup.inputs,
        phase="apply",
        reader=SnapshotReader({CHAT_A: [], CHAT_B: []}),
        now=NOW,
        machine_identity_observer=lambda: MACHINE,
        writer_stop_observer=lambda: setup.writer_stop_observation,
    )

    assert observed["writer_stop_receipt"] == json.loads(
        setup.writer_stop.read_text()
    )
    assert observed["writer_stop_receipt_sha256"] == _sha256_json_file(
        setup.writer_stop
    )


@pytest.mark.parametrize(
    "field",
    [
        "release_id",
        "hold_id",
        "plan_sha256",
        "lease_fingerprint",
        "old_gateway_runtime_identity_sha256",
        "live_sidecar_identity",
    ],
)
def test_swapped_writer_stop_receipt_fails_before_refetch(
    setup: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    plan = _plan(setup).body
    _authorize(setup, plan)
    body = json.loads(setup.writer_stop.read_text())
    if field == "release_id":
        body[field] = "release-swapped-20260713"
    elif field == "hold_id":
        body[field] = "hold-swapped-20260713"
    elif field == "plan_sha256":
        body[field] = "f" * 64
    elif field == "lease_fingerprint":
        body[field] = "f" * 64
    elif field == "old_gateway_runtime_identity_sha256":
        body["old_gateway_runtime_identity"]["process"][
            "process_create_time"
        ] += 1
        body["old_gateway_process"] = body["old_gateway_runtime_identity"][
            "process"
        ]
        body[field] = cutover_guard._sha256_json(
            body["old_gateway_runtime_identity"]
        )
    else:
        body[field]["revision"] = 99
        body["writer_stop_observation"][field]["revision"] = 99
        body["live_sidecar_identity_sha256"] = hold._sha256_json(body[field])
        body["writer_stop_observation_sha256"] = cutover_guard._sha256_json(
            body["writer_stop_observation"]
        )
    _rewrite_writer_receipt(setup, body)
    _install_gate_validator(monkeypatch)
    reader = SnapshotReader({})

    with pytest.raises(hold.IngressHoldError):
        hold.run_ingress_hold(
            setup.inputs,
            phase="apply",
            reader=reader,
            now=NOW,
            machine_identity_observer=lambda: MACHINE,
            writer_stop_observer=lambda: setup.writer_stop_observation,
        )

    assert reader.calls == []
    assert not (setup.inputs.run_root / hold.STAGED_SIDECAR_FILENAME).exists()


def test_stale_writer_stop_receipt_fails_before_refetch(
    setup: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(setup).body
    _authorize(setup, plan)
    body = json.loads(setup.writer_stop.read_text())
    body["observed_at"] = (
        NOW - timedelta(seconds=cutover_guard.MAX_WRITER_STOP_AGE_SECONDS + 1)
    ).isoformat()
    _rewrite_writer_receipt(setup, body)
    _install_gate_validator(monkeypatch)
    reader = SnapshotReader({})

    with pytest.raises(hold.IngressHoldError) as error:
        hold.run_ingress_hold(
            setup.inputs,
            phase="apply",
            reader=reader,
            now=NOW,
            machine_identity_observer=lambda: MACHINE,
            writer_stop_observer=lambda: setup.writer_stop_observation,
        )

    assert error.value.code == "writer_stop_receipt_stale"
    assert reader.calls == []


@pytest.mark.parametrize("attack", ["symlink", "hardlink", "permissions", "oversize", "duplicate"])
def test_writer_stop_receipt_file_attacks_fail_before_refetch(
    setup: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    attack: str,
) -> None:
    plan = _plan(setup).body
    _authorize(setup, plan)
    if attack == "symlink":
        target = tmp_path / "writer-target.json"
        target.write_bytes(setup.writer_stop.read_bytes())
        target.chmod(0o600)
        setup.writer_stop.unlink()
        setup.writer_stop.symlink_to(target)
    elif attack == "hardlink":
        os.link(setup.writer_stop, tmp_path / "writer-link.json")
    elif attack == "permissions":
        setup.writer_stop.chmod(0o644)
    elif attack == "oversize":
        setup.writer_stop.write_bytes(b"x" * (cutover_guard.MAX_JSON_BYTES + 1))
        setup.writer_stop.chmod(0o600)
    else:
        raw = setup.writer_stop.read_text()
        setup.writer_stop.write_text(raw[:-2] + ',"release_id":"duplicate"}\n')
        setup.writer_stop.chmod(0o600)
    _install_gate_validator(monkeypatch)
    reader = SnapshotReader({})

    with pytest.raises(hold.IngressHoldError):
        hold.run_ingress_hold(
            setup.inputs,
            phase="apply",
            reader=reader,
            now=NOW,
            machine_identity_observer=lambda: MACHINE,
            writer_stop_observer=lambda: setup.writer_stop_observation,
        )

    assert reader.calls == []


@pytest.mark.parametrize("drift", ["sidecar", "hidden_gateway", "old_runtime"])
def test_live_writer_stop_drift_fails_before_refetch(
    setup: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    plan = _plan(setup).body
    _authorize(setup, plan)
    live = json.loads(json.dumps(setup.writer_stop_observation))
    if drift == "sidecar":
        live["live_sidecar_identity"]["revision"] = 99
    elif drift == "hidden_gateway":
        live["process_census"]["matching_processes"] = [{
            "pid": 51001,
            "process_create_time": NOW.timestamp(),
            "cmdline_sha256": "f" * 64,
        }]
    else:
        live["live_runtime_identity"]["runtime_files_sha256"] = "f" * 64
    _install_gate_validator(monkeypatch)
    reader = SnapshotReader({})

    with pytest.raises(hold.IngressHoldError):
        hold.run_ingress_hold(
            setup.inputs,
            phase="apply",
            reader=reader,
            now=NOW,
            machine_identity_observer=lambda: MACHINE,
            writer_stop_observer=lambda: live,
        )

    assert reader.calls == []


def test_relative_writer_stop_path_fails_before_receipt_or_refetch(
    setup: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(setup).body
    _authorize(setup, plan)
    cutover = json.loads(setup.cutover.read_text())
    cutover["writer_stop_receipt_path"] = "writer-stop.json"
    _write_json(setup.cutover, cutover)
    _install_gate_validator(monkeypatch)
    reader = SnapshotReader({})

    with pytest.raises(hold.IngressHoldError) as error:
        hold.run_ingress_hold(
            setup.inputs,
            phase="apply",
            reader=reader,
            now=NOW,
            machine_identity_observer=lambda: MACHINE,
            writer_stop_observer=lambda: setup.writer_stop_observation,
        )

    assert error.value.code == "feishu_ingress_hold_writer_stop_receipt_path_invalid"
    assert reader.calls == []


def test_receipt_swap_during_gate_validation_fails_before_refetch(
    setup: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(setup).body
    _authorize(setup, plan)
    from scripts import pnc_rca_release_gate

    def validator(**kwargs):
        receipt = kwargs["writer_stop_receipt"]
        swapped = dict(receipt)
        swapped["plan_sha256"] = "f" * 64
        _write_json(setup.writer_stop, swapped)
        return {
            "schema_version": hold.GATE_VALIDATION_SCHEMA_VERSION,
            "ok": True,
            "plan_sha256": kwargs["plan_sha256"],
            "approval_receipt_sha256": kwargs["approval_receipt_sha256"],
            "cutover_binding_sha256": kwargs["cutover_binding_sha256"],
            "writer_stop_receipt_sha256": kwargs[
                "writer_stop_receipt_sha256"
            ],
            "cutover_lease_fingerprint": receipt["lease_fingerprint"],
            "old_gateway_runtime_identity_sha256": receipt[
                "old_gateway_runtime_identity_sha256"
            ],
            "gateway_writer_state": "stopped",
        }

    monkeypatch.setattr(
        pnc_rca_release_gate,
        "validate_feishu_ingress_hold_cutover_binding",
        validator,
        raising=False,
    )
    reader = SnapshotReader({})

    with pytest.raises(hold.IngressHoldError) as error:
        hold.run_ingress_hold(
            setup.inputs,
            phase="apply",
            reader=reader,
            now=NOW,
            machine_identity_observer=lambda: MACHINE,
            writer_stop_observer=lambda: setup.writer_stop_observation,
        )

    assert error.value.code == "feishu_ingress_hold_writer_stop_receipt_changed"
    assert reader.calls == []


def test_noncanonical_gateway_root_is_rejected(setup: SimpleNamespace, tmp_path) -> None:
    inputs = hold.HoldInputs(**{
        **setup.inputs.__dict__,
        "canonical_gateway_root": tmp_path / "other-live",
    })

    with pytest.raises(hold.IngressHoldError) as error:
        hold.run_ingress_hold(
            inputs,
            phase="plan",
            reader=SnapshotReader({}),
            now=NOW,
            machine_identity_observer=lambda: MACHINE,
        )

    assert error.value.code == "feishu_ingress_hold_canonical_root_invalid"


def test_apply_captures_downtime_messages_deduplicates_and_preserves_watermarks(
    setup: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_scope = hold._app_scope(
        {"FEISHU_APP_ID": "cli_test_app", "FEISHU_DOMAIN": "feishu"}
    )
    existing = _item(CHAT_A, "om_existing_pending", 1_999_999_999_900)
    seen = _item(CHAT_A, "om_already_seen", 1_999_999_999_950)
    state = hold._empty_state()
    state["pending"] = {CHAT_A: [existing]}
    state["baselined_chat_ids"] = [CHAT_A]
    state["last_seen_create_time_ms"] = {CHAT_A: 1_999_999_999_800}
    state["cursor_message_ids"] = {CHAT_A: ["om_cursor"]}
    state["discovery_floor_ms"] = {CHAT_A: 1_999_999_999_700}
    state["seen_message_ids"] = [seen["message_id"]]
    _write_json(setup.live_sidecar, _sidecar(app_scope, state=state))
    plan_message = _item(CHAT_A, "om_plan", 2_000_000_000_010)
    cursor_message = _item(CHAT_A, "om_cursor", 1_999_999_999_800)
    plan = _plan(
        setup,
        SnapshotReader(
            {
                CHAT_A: [plan_message, seen, cursor_message],
                CHAT_B: [],
            }
        ),
    ).body
    _authorize(setup, plan)
    downtime = _item(CHAT_A, "om_during_writer_stop", 2_000_000_000_090)
    duplicate_plan = dict(plan_message)

    result = _apply(
        setup,
        SnapshotReader(
            {
                CHAT_A: [
                    duplicate_plan,
                    downtime,
                    downtime,
                    seen,
                    cursor_message,
                ],
                CHAT_B: [],
            }
        ),
        monkeypatch,
    )

    staged_path = setup.inputs.run_root / hold.STAGED_SIDECAR_FILENAME
    staged = json.loads(staged_path.read_text(encoding="utf-8"))
    ids = [item["message_id"] for item in staged["state"]["pending"][CHAT_A]]
    assert ids == ["om_existing_pending", "om_plan", "om_during_writer_stop"]
    assert staged["revision"] == 8
    assert staged["state"]["last_seen_create_time_ms"] == {
        CHAT_A: 1_999_999_999_800
    }
    assert staged["state"]["cursor_message_ids"] == {CHAT_A: ["om_cursor"]}
    assert staged["state"]["discovery_floor_ms"] == {
        CHAT_A: 1_999_999_999_700,
        CHAT_B: 2_000_000_000_000,
    }
    assert result.body["production_effects_executed"] is False
    assert setup.live_sidecar.read_bytes() == hold._canonical_json(
        _sidecar(app_scope, state=state)
    )
    receipt = json.loads(
        (setup.inputs.run_root / hold.APPLY_RECEIPT_FILENAME).read_text()
    )
    assert receipt["watermark_proof"]["non_regressing"] is True
    assert receipt["watermark_proof"]["added_message_ids_by_chat"][CHAT_A] == [
        "om_during_writer_stop",
        "om_plan",
    ]
    assert receipt["live_sidecar_written"] is False
    assert receipt["future_install"]["staged_source"] == str(staged_path)
    assert "atomically rename" in receipt["future_install"]["procedure"]


def test_repeated_apply_is_no_clobber_idempotent_with_same_cutover_snapshot(
    setup: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(setup).body
    _authorize(setup, plan)
    reader = SnapshotReader({CHAT_A: [], CHAT_B: []})
    first = _apply(setup, reader, monkeypatch)

    second = _apply(setup, SnapshotReader({CHAT_A: [], CHAT_B: []}), monkeypatch)

    assert first.body == second.body
    assert second.resumed is True


def test_cutover_binding_requires_stopped_writer(
    setup: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(setup).body
    _authorize(setup, plan)
    body = json.loads(setup.cutover.read_text())
    body["gateway_writer_state"] = "running"
    _write_json(setup.cutover, body)
    reader = SnapshotReader({})

    with pytest.raises(hold.IngressHoldError) as error:
        _apply(setup, reader, monkeypatch)

    assert error.value.code == "feishu_ingress_hold_cutover_gateway_writer_state_mismatch"
    assert reader.calls == []


def test_owner_lock_rejects_concurrent_operation(setup: SimpleNamespace) -> None:
    root, _created = hold._ensure_run_root(setup.inputs.run_root)

    with hold._run_lock(root):
        with pytest.raises(hold.IngressHoldError) as error:
            with hold._run_lock(root):
                pytest.fail("second lock unexpectedly acquired")

    assert error.value.code == "feishu_ingress_hold_in_progress"


def test_no_clobber_publication_accepts_exact_duplicate_and_rejects_conflict(
    tmp_path: Path,
) -> None:
    root = tmp_path / "owned"
    root.mkdir(mode=0o700)
    path = root / "artifact.json"

    assert hold._publish_no_clobber(path, {"value": 1}) is False
    assert hold._publish_no_clobber(path, {"value": 1}) is True
    with pytest.raises(hold.IngressHoldError) as error:
        hold._publish_no_clobber(path, {"value": 2})

    assert error.value.code == "feishu_ingress_hold_artifact_conflict"


class _Response:
    def __init__(self, payload: dict):
        self.raw = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, limit: int) -> bytes:
        return self.raw[:limit]


def test_readonly_api_paginates_with_get_and_records_cursor_hashes() -> None:
    requests = []
    responses = iter(
        [
            {"code": 0, "tenant_access_token": "tenant-token"},
            {
                "code": 0,
                "data": {
                    "items": [_item(CHAT_A, "om_new", 2_000_000_000_010)],
                    "has_more": True,
                    "page_token": "cursor-2",
                },
            },
            {
                "code": 0,
                "data": {
                    "items": [_item(CHAT_A, "om_old", 2_000_000_000_005)],
                    "has_more": False,
                    "page_token": "",
                },
            },
        ]
    )

    def opener(request: object, *, timeout: int) -> _Response:
        requests.append((request, timeout))
        return _Response(next(responses))

    api = hold.FeishuReadOnlyMessageApi(
        {
            "FEISHU_APP_ID": "cli_test",
            "FEISHU_APP_SECRET": "secret",
            "FEISHU_DOMAIN": "feishu",
        },
        opener=opener,
        clock_ms=iter([1, 2]).__next__,
    )

    snapshot = api.snapshot_chat(
        CHAT_A,
        floor_ms=2_000_000_000_000,
        page_size=50,
        max_pages=3,
    )

    token_request, first_get, second_get = [request for request, _ in requests]
    assert token_request.get_method() == "POST"
    assert "/auth/v3/tenant_access_token/internal" in token_request.full_url
    assert first_get.get_method() == second_get.get_method() == "GET"
    assert "/open-apis/im/v1/messages?" in first_get.full_url
    assert "page_token=cursor-2" in second_get.full_url
    assert all("/messages" not in request.full_url or request.get_method() == "GET" for request, _ in requests)
    assert [item["message_id"] for item in snapshot["items"]] == [
        "om_old",
        "om_new",
    ]
    assert snapshot["pages"][1]["request_cursor_sha256"] == hashlib.sha256(
        b"cursor-2"
    ).hexdigest()


@pytest.mark.parametrize(
    ("responses", "max_pages", "expected_code"),
    [
        (
            [
                {"code": 0, "tenant_access_token": "token"},
                {
                    "code": 0,
                    "data": {"items": [], "has_more": True, "page_token": "same"},
                },
                {
                    "code": 0,
                    "data": {"items": [], "has_more": True, "page_token": "same"},
                },
            ],
            3,
            "feishu_ingress_hold_pagination_cycle",
        ),
        (
            [
                {"code": 0, "tenant_access_token": "token"},
                {
                    "code": 0,
                    "data": {"items": [], "has_more": True, "page_token": "next"},
                },
            ],
            1,
            "feishu_ingress_hold_pagination_incomplete",
        ),
    ],
)
def test_readonly_api_rejects_unclosed_pagination(
    responses: list[dict],
    max_pages: int,
    expected_code: str,
) -> None:
    payloads = iter(responses)
    api = hold.FeishuReadOnlyMessageApi(
        {"FEISHU_APP_ID": "app", "FEISHU_APP_SECRET": "secret"},
        opener=lambda _request, timeout: _Response(next(payloads)),
        clock_ms=lambda: 1,
    )

    with pytest.raises(hold.IngressHoldError) as error:
        api.snapshot_chat(CHAT_A, floor_ms=0, page_size=50, max_pages=max_pages)

    assert error.value.code == expected_code
