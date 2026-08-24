from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from gateway.pnc_rca_direct_vm_transport import (
    DEFAULT_REMOTE_CREATOR_PATH,
    DEFAULT_REMOTE_SHARED_STATE_MODULE_PATH,
    DEFAULT_REMOTE_VALIDATOR_MODULE_PATH,
    DEFAULT_REMOTE_HUMANIZER_MODULE_PATH,
    DEFAULT_REMOTE_CREATOR_SHA256,
    DEFAULT_REMOTE_VALIDATOR_SHA256,
    DEFAULT_REMOTE_HUMANIZER_SHA256,
    DEFAULT_REMOTE_HUMANIZER_MODE,
    DEFAULT_REMOTE_HUMANIZER_BASELINE_COMMIT,
    DEFAULT_REMOTE_HUMANIZER_BASELINE_TREE,
    IDENTITY_KIND_GIT_WORKTREE,
    IDENTITY_KIND_SEALED_MATERIALIZED,
    IDENTITY_KIND_UNKNOWN,
    DirectVmTransport,
    DirectVmTransportConfig,
    DirectVmTransportError,
    REVIEWED_SSH_MINI_AGENT,
    _remote_script,
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
    assert config.remote_humanizer_module_path == DEFAULT_REMOTE_HUMANIZER_MODULE_PATH
    assert config.remote_creator_sha256 != "__CREATOR_SHA256__"
    assert config.remote_validator_sha256 != "__VALIDATOR_SHA256__"
    assert config.remote_humanizer_sha256 == DEFAULT_REMOTE_HUMANIZER_SHA256
    assert config.remote_humanizer_mode == DEFAULT_REMOTE_HUMANIZER_MODE
    assert (
        config.remote_humanizer_baseline_commit
        == DEFAULT_REMOTE_HUMANIZER_BASELINE_COMMIT
    )
    assert (
        config.remote_humanizer_baseline_tree == DEFAULT_REMOTE_HUMANIZER_BASELINE_TREE
    )
    assert config.remote_humanizer_identity_kind == IDENTITY_KIND_GIT_WORKTREE
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
        "remote_humanizer_module_path",
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
    assert DEFAULT_REMOTE_HUMANIZER_MODULE_PATH in script
    assert "/home/mini/.hermes/shared-state" in script
    assert TASK_ID in script
    assert "direct_vm_module_parent_invalid" in script
    assert "direct_vm_module_permissions_invalid" in script
    assert "info.st_uid not in {0, os.geteuid()}" in script
    assert "info.st_uid == 0 and stat.S_IMODE(info.st_mode) & 0o022" in script
    assert (
        "info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) & 0o002" in script
    )
    assert "before.st_nlink != 1" in script
    assert (
        "shared_state_raw = _stable_module_bytes(SHARED_STATE_MODULE, SHARED_STATE_SHA256)"
        in script
    )
    assert "HUMANIZER_MODE = 384" in script
    assert "SUBMIT_MODULE" not in script
    assert "direct_vm_module_hash_mismatch" in script


def test_config_rejects_unbound_module_hashes() -> None:
    with pytest.raises(ValueError, match="remote_validator_sha256_invalid"):
        DirectVmTransportConfig(remote_validator_sha256="not-a-sha").normalized()

    with pytest.raises(ValueError, match="remote_humanizer_mode"):
        DirectVmTransportConfig(remote_humanizer_mode=0o644).normalized()
    with pytest.raises(ValueError, match="remote_humanizer_baseline_commit_invalid"):
        DirectVmTransportConfig(remote_humanizer_baseline_commit="short").normalized()
    with pytest.raises(ValueError, match="remote_humanizer_identity_kind_invalid"):
        DirectVmTransportConfig(remote_humanizer_identity_kind="guessed").normalized()


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


def test_generated_status_rejects_shared_state_hash_before_creator_call(
    tmp_path: Path,
) -> None:
    creator_path = tmp_path / "creator.py"
    creator_path.write_text(
        "DIRECT_VM_CREATOR_SCHEMA_VERSION = 'g1q3_rca_direct_vm_creator_v1'\n"
        "def read_direct_vm_status(root, task_id, validator):\n"
        "    return {'state': 'missing', 'task_id': task_id, 'submission_key': '', 'identity_sha256': ''}\n",
        encoding="utf-8",
    )
    validator_path = tmp_path / "validator.py"
    validator_path.write_text(
        "DIRECT_VM_VALIDATOR_SCHEMA_VERSION = 'g1q3_rca_direct_vm_validator_v1'\n"
        "def validate_direct_vm_request(value):\n"
        "    return value\n",
        encoding="utf-8",
    )
    shared_path = tmp_path / "shared_state_v2.py"
    shared_path.write_text("# pinned ABI bytes\n", encoding="utf-8")
    humanizer_path = tmp_path / "humanizer.py"
    humanizer_path.write_text(
        "def build_task_state_notification(previous_task, task):\n    return None\n",
        encoding="utf-8",
    )
    for path in (creator_path, validator_path, shared_path, humanizer_path):
        path.chmod(0o600)

    def digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    kwargs = {
        "helper_path": str(creator_path),
        "shared_state_root": str(tmp_path / "root"),
        "shared_state_module_path": str(shared_path),
        "validator_module_path": str(validator_path),
        "humanizer_module_path": str(humanizer_path),
        "creator_sha256": digest(creator_path),
        "validator_sha256": digest(validator_path),
        "humanizer_sha256": digest(humanizer_path),
        "humanizer_mode": 0o600,
        # Empty provenance is test-only for the isolated temporary module
        # tree; production normalization requires the reviewed VM binding.
        "humanizer_baseline_commit": "",
        "humanizer_baseline_tree": "",
        "shared_state_sha256": digest(shared_path),
        "operation": "status",
        "task_id": TASK_ID,
    }
    good = subprocess.run(
        [sys.executable, "-I", "-c", _remote_script(**kwargs)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert good.returncode == 0, good.stderr
    assert json.loads(good.stdout)["state"] == "missing"

    git_called = tmp_path / "git-called"
    fake_git = tmp_path / "fake-git"
    fake_git.write_text(
        f"#!{sys.executable}\n"
        "from pathlib import Path\n"
        f"Path({str(git_called)!r}).write_text('called')\n"
        "raise SystemExit(99)\n",
        encoding="utf-8",
    )
    fake_git.chmod(0o755)
    for identity_kind, expected_error in (
        (IDENTITY_KIND_UNKNOWN, "direct_vm_humanizer_identity_kind_unknown"),
        (
            IDENTITY_KIND_SEALED_MATERIALIZED,
            "direct_vm_humanizer_identity_kind_unsupported",
        ),
    ):
        script = _remote_script(**kwargs, identity_kind=identity_kind).replace(
            "GIT_PATH = '/usr/bin/git'", f"GIT_PATH = {str(fake_git)!r}"
        )
        identity_blocked = subprocess.run(
            [sys.executable, "-I", "-c", script],
            text=True,
            capture_output=True,
            check=False,
        )
        assert identity_blocked.returncode != 0
        assert expected_error in identity_blocked.stderr
        assert not git_called.exists()

    kwargs["humanizer_baseline_commit"] = "f" * 40
    kwargs["humanizer_baseline_tree"] = "e" * 40
    provenance_blocked = subprocess.run(
        [sys.executable, "-I", "-c", _remote_script(**kwargs)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert provenance_blocked.returncode != 0
    assert "direct_vm_humanizer_provenance_mismatch" in provenance_blocked.stderr

    kwargs["humanizer_baseline_commit"] = ""
    kwargs["humanizer_baseline_tree"] = ""
    kwargs["shared_state_sha256"] = "0" * 64
    blocked = subprocess.run(
        [sys.executable, "-I", "-c", _remote_script(**kwargs)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert blocked.returncode != 0
    assert "direct_vm_module_hash_mismatch" in blocked.stderr


@pytest.mark.parametrize("mutation", ["tracked", "untracked"])
def test_generated_git_worktree_identity_rejects_dirty_worker_state(
    tmp_path: Path, mutation: str
) -> None:
    worker_state = tmp_path / "worker-state"
    worker_state.mkdir()
    creator_path = worker_state / "vm_coding_worker_v2.py"
    creator_path.write_text(
        "DIRECT_VM_CREATOR_SCHEMA_VERSION = 'g1q3_rca_direct_vm_creator_v1'\n"
        "def read_direct_vm_status(root, task_id, validator):\n"
        "    return {'state': 'missing', 'task_id': task_id, "
        "'submission_key': '', 'identity_sha256': ''}\n",
        encoding="utf-8",
    )
    validator_path = worker_state / "pnc_rca_direct_vm_validator.py"
    validator_path.write_text(
        "DIRECT_VM_VALIDATOR_SCHEMA_VERSION = 'g1q3_rca_direct_vm_validator_v1'\n"
        "def validate_direct_vm_request(value):\n"
        "    return value\n",
        encoding="utf-8",
    )
    shared_path = worker_state / "shared_state_v2.py"
    shared_path.write_text("# pinned ABI bytes\n", encoding="utf-8")
    humanizer_path = worker_state / "vm_feishu_humanizer.py"
    humanizer_path.write_text(
        "def build_task_state_notification(previous_task, task):\n"
        "    return None\n",
        encoding="utf-8",
    )
    tracked_path = worker_state / "reviewed_runtime.txt"
    tracked_path.write_text("reviewed\n", encoding="utf-8")
    for path in (creator_path, validator_path, shared_path, humanizer_path):
        path.chmod(0o600)

    def git(*arguments: str) -> str:
        completed = subprocess.run(
            ["/usr/bin/git", "-C", str(worker_state), *arguments],
            text=True,
            capture_output=True,
            check=True,
        )
        return completed.stdout.strip()

    git("init", "-q")
    git("config", "user.email", "direct-transport@example.invalid")
    git("config", "user.name", "Direct Transport Test")
    git("add", ".")
    git("commit", "-qm", "reviewed worker state")

    def digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    kwargs = {
        "helper_path": str(creator_path),
        "shared_state_root": str(tmp_path / "shared-state"),
        "shared_state_module_path": str(shared_path),
        "validator_module_path": str(validator_path),
        "humanizer_module_path": str(humanizer_path),
        "creator_sha256": digest(creator_path),
        "validator_sha256": digest(validator_path),
        "humanizer_sha256": digest(humanizer_path),
        "humanizer_mode": 0o600,
        "humanizer_baseline_commit": git("rev-parse", "HEAD"),
        "humanizer_baseline_tree": git("rev-parse", "HEAD^{tree}"),
        "shared_state_sha256": digest(shared_path),
        "operation": "status",
        "task_id": TASK_ID,
    }
    clean = subprocess.run(
        [sys.executable, "-I", "-c", _remote_script(**kwargs)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert clean.returncode == 0, clean.stderr

    if mutation == "tracked":
        tracked_path.write_text("modified\n", encoding="utf-8")
    else:
        (worker_state / "untracked_runtime.py").write_text(
            "UNTRACKED = True\n", encoding="utf-8"
        )
    blocked = subprocess.run(
        [sys.executable, "-I", "-c", _remote_script(**kwargs)],
        text=True,
        capture_output=True,
        check=False,
    )

    assert blocked.returncode != 0
    assert "direct_vm_humanizer_provenance_mismatch" in blocked.stderr


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
