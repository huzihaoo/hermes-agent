from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from gateway.pnc_rca_direct_vm_transport import (
    DEFAULT_REMOTE_CREATOR_PATH,
    DEFAULT_REMOTE_SHARED_STATE_MODULE_PATH,
    DEFAULT_REMOTE_VALIDATOR_MODULE_PATH,
    DEFAULT_REMOTE_CREATOR_SHA256,
    DEFAULT_REMOTE_VALIDATOR_SHA256,
    DirectVmTransport,
    DirectVmTransportConfig,
    DirectVmTransportError,
    REVIEWED_SSH_MINI_AGENT,
    build_direct_vm_transport,
)
from tests.gateway.test_pnc_rca_direct_vm_submit import _missing, _request


TASK_ID = _request().task_id


@dataclass
class _Runner:
    response: dict[str, object] | None = None
    responses: list[dict[str, object]] | None = None
    returncode: int = 0
    stderr: str = ""

    def __post_init__(self) -> None:
        self.calls: list[tuple[list[str], dict[str, object]]] = []

    def __call__(
        self, command: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append((command, kwargs))
        response = self.response
        if self.responses:
            response = self.responses.pop(0)
        stdout = json.dumps(response or {}, sort_keys=True)
        return subprocess.CompletedProcess(
            command,
            self.returncode,
            stdout=stdout,
            stderr=self.stderr,
        )


def _transport(runner: _Runner, **updates: object) -> DirectVmTransport:
    config = {
        "ssh_mini_agent": "/Users/songying/.local/bin/ssh-mini-agent",
        "remote_creator_path": DEFAULT_REMOTE_CREATOR_PATH,
        "remote_shared_state_module_path": DEFAULT_REMOTE_SHARED_STATE_MODULE_PATH,
        "create_enabled": True,
    }
    config.update(updates)
    return DirectVmTransport(config, command_runner=runner)


def test_defaults_pin_worker_state_paths_and_creation_is_opt_in() -> None:
    config = DirectVmTransportConfig().normalized()

    assert config.create_enabled is False
    assert config.shared_state_root == "/home/mini/.hermes/shared-state"
    assert config.remote_creator_path == DEFAULT_REMOTE_CREATOR_PATH
    assert (
        config.remote_shared_state_module_path
        == DEFAULT_REMOTE_SHARED_STATE_MODULE_PATH
    )
    assert config.remote_validator_module_path == DEFAULT_REMOTE_VALIDATOR_MODULE_PATH
    assert config.remote_creator_sha256 != "__CREATOR_SHA256__"
    assert config.remote_validator_sha256 != "__VALIDATOR_SHA256__"
    assert "release" not in json.dumps(config.public_dict(), sort_keys=True).lower()


def test_production_builder_requires_reviewed_agent_path() -> None:
    assert (
        build_direct_vm_transport({"create_enabled": False}).config.ssh_mini_agent
        == REVIEWED_SSH_MINI_AGENT
    )
    with pytest.raises(ValueError, match="reviewed_path"):
        build_direct_vm_transport({
            "ssh_mini_agent": "/tmp/fake-agent",
            "create_enabled": False,
        })
    test_transport = build_direct_vm_transport(
        {"ssh_mini_agent": "/tmp/fake-agent", "create_enabled": False},
        test_only=True,
    )
    assert test_transport.config.ssh_mini_agent == "/tmp/fake-agent"


@pytest.mark.parametrize(
    "field",
    [
        "shared_state_root",
        "remote_creator_path",
        "remote_shared_state_module_path",
        "remote_validator_module_path",
    ],
)
def test_production_builder_requires_reviewed_vm_paths(field: str) -> None:
    updates = {field: "/home/mini/.hermes/worker-state/other.py"}
    if field == "shared_state_root":
        updates[field] = "/mnt/tmp/test-direct-root"
    with pytest.raises(ValueError, match=f"{field}_must_use_reviewed_path"):
        build_direct_vm_transport({**updates, "create_enabled": False})


@pytest.mark.parametrize(
    "updates",
    [
        {"shared_state_root": "relative/root"},
        {"shared_state_root": "/home/mini/.hermes/../escape"},
        {"remote_creator_path": "/home/mini/.hermes/worker-state/../x.py"},
        {"remote_shared_state_module_path": "/tmp/helper.py"},
    ],
)
def test_config_rejects_unsafe_or_unpinned_paths(updates: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        DirectVmTransportConfig(**updates).normalized()


def test_create_disabled_is_fail_closed_without_invoking_agent() -> None:
    runner = _Runner()
    transport = DirectVmTransport(
        {"ssh_mini_agent": "/bin/agent", "create_enabled": False},
        command_runner=runner,
    )

    with pytest.raises(DirectVmTransportError) as raised:
        transport.create(_request().to_dict())

    assert raised.value.code == "direct_vm_transport_unavailable"
    assert runner.calls == []


@pytest.mark.parametrize(
    ("response", "expected_state"),
    [
        (_missing(), "missing"),
        (
            {
                "state": "unknown",
                "task_id": TASK_ID,
                "submission_key": "",
                "identity_sha256": "",
            },
            "unknown",
        ),
        (
            {
                "state": "not-a-state",
                "task_id": TASK_ID,
                "submission_key": "",
                "identity_sha256": "",
            },
            "unknown",
        ),
        (
            {
                "state": "missing",
                "task_id": TASK_ID,
                "submission_key": TASK_ID,
                "identity_sha256": "d" * 64,
            },
            "unknown",
        ),
    ],
)
def test_status_normalizes_wire_response(
    response: dict[str, object], expected_state: str
) -> None:
    runner = _Runner(response=response)
    transport = _transport(runner)

    observed = transport.status(TASK_ID)

    assert observed["state"] == expected_state
    if expected_state == "unknown" and response.get("state") != "unknown":
        assert transport.last_error
    else:
        assert observed == response


def test_status_uses_one_bounded_agent_verb_and_keeps_paths_out_of_argv() -> None:
    runner = _Runner(response=_missing())
    transport = _transport(runner)

    assert transport.status(TASK_ID)["state"] == "missing"
    command, kwargs = runner.calls[0]
    assert command == ["/Users/songying/.local/bin/ssh-mini-agent", "run_py_json"]
    assert "ssh-mini-run" not in command
    assert kwargs.get("shell", False) is False
    script = str(kwargs["input"])
    assert DEFAULT_REMOTE_CREATOR_PATH in script
    assert DEFAULT_REMOTE_VALIDATOR_MODULE_PATH in script
    assert "/home/mini/.hermes/shared-state" in script
    assert TASK_ID in script
    assert "direct_vm_module_parent_invalid" in script
    assert "direct_vm_module_permissions_invalid" in script
    assert "info.st_uid not in {0, os.geteuid()}" in script
    assert "info.st_uid == 0 and stat.S_IMODE(info.st_mode) & 0o022" in script
    assert (
        "info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) & 0o002" in script
    )
    assert "info.st_nlink != 1" in script
    assert "SUBMIT_MODULE" not in script
    assert "direct_vm_module_hash_mismatch" in script


def test_config_rejects_unbound_module_hashes() -> None:
    with pytest.raises(ValueError, match="remote_validator_sha256_invalid"):
        DirectVmTransportConfig(remote_validator_sha256="not-a-sha").normalized()


def test_reviewed_source_hashes_bind_to_formal_files() -> None:
    repo = Path(__file__).parents[2]
    creator = hashlib.sha256(
        (repo / "scripts/pnc_rca_direct_vm_creator.py").read_bytes()
    ).hexdigest()
    validator = hashlib.sha256(
        (repo / "scripts/pnc_rca_direct_vm_validator.py").read_bytes()
    ).hexdigest()
    assert creator == DEFAULT_REMOTE_CREATOR_SHA256
    assert validator == DEFAULT_REMOTE_VALIDATOR_SHA256


def test_remote_helper_failure_is_unknown_not_missing() -> None:
    runner = _Runner(returncode=1, stderr="helper unavailable")
    transport = _transport(runner)

    observed = transport.status(TASK_ID)

    assert observed["state"] == "unknown"
    assert transport.last_error == "direct_vm_remote_helper_failed"
    assert "helper unavailable" in transport.last_error_detail


def test_status_first_submit_retries_when_transport_cannot_prove_absence() -> None:
    runner = _Runner(returncode=1, stderr="no pinned helper")
    transport = _transport(runner)

    result = transport.submit(_request())

    assert result.outcome == "retry"
    assert result.reason == "pre_status_unknown"
    assert result.create_attempted is False
    assert len(runner.calls) == 1


def test_status_first_submit_creates_only_after_missing_and_reconciles() -> None:
    request = _request()
    runner = _Runner(
        responses=[
            _missing(),
            {
                "protocol_version": "g1q3_rca_direct_vm_transport_v1",
                "accepted": True,
                "created": True,
                "task_id": request.task_id,
                "submission_key": request.submission_key,
                "identity_sha256": request.identity_sha256,
                "state": "pending",
            },
            {
                "state": "existing",
                "task_id": request.task_id,
                "submission_key": request.submission_key,
                "identity_sha256": request.identity_sha256,
            },
        ]
    )
    transport = _transport(runner)

    result = transport.submit(request)

    assert result.outcome == "reconciled"
    assert result.create_attempted is True
    assert len(runner.calls) == 3
    assert "ENVELOPE" in str(runner.calls[1][1]["input"])


def test_create_response_is_identity_checked() -> None:
    request = _request()
    runner = _Runner(
        response={
            "protocol_version": "g1q3_rca_direct_vm_transport_v1",
            "accepted": True,
            "created": True,
            "task_id": request.task_id,
            "submission_key": request.submission_key,
            "identity_sha256": "d" * 64,
        }
    )
    transport = _transport(runner)

    with pytest.raises(DirectVmTransportError) as raised:
        transport.create(request.to_dict())

    assert raised.value.code == "direct_vm_create_identity_mismatch"
