from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from gateway.pnc_rca_direct_vm_submit import build_direct_vm_request
from scripts import pnc_rca_direct_vm_creator as creator
from tests.gateway.test_pnc_rca_direct_vm_submit import _request


def _write_old_abi_module(path: Path, *, with_create: bool = True) -> Path:
    create_function = (
        """
def create_task(*, root, title, goal_text, task_id, owner, requester_session_key, coding_session_key):
    root = Path(root)
    task_dir = root / 'tasks' / task_id
    task_dir.mkdir(parents=False, exist_ok=False)
    meta = {
        'task_id': task_id,
        'title': title,
        'owner': owner,
        'state': 'pending',
    }
    (task_dir / 'goal.md').write_text(goal_text + '\\n', encoding='utf-8')
    (task_dir / 'status.md').write_text('- state: pending\\n', encoding='utf-8')
    (task_dir / 'meta.json').write_text(json.dumps(meta), encoding='utf-8')
    dispatch = root / 'dispatch' / 'pending' / f'{task_id}.json'
    dispatch.write_text(json.dumps({'task_id': task_id, 'state': 'pending'}), encoding='utf-8')
    return {'task_id': task_id, 'task_dir': str(task_dir), 'dispatch_path': str(dispatch)}
"""
        if with_create
        else ""
    )
    path.write_text(
        "from pathlib import Path\nimport json\nimport vm_feishu_humanizer\n"
        "def ensure_canonical_root(root=None):\n"
        "    root = Path(root)\n"
        "    for name in ('tasks', 'dispatch/pending', 'dispatch/claimed', 'dispatch/done', 'dispatch/failed'):\n"
        "        (root / name).mkdir(parents=True, exist_ok=True)\n"
        "    return root\n" + create_function,
        encoding="utf-8",
    )
    return path


def _write_humanizer_module(path: Path) -> Path:
    path.write_text(
        "def build_task_state_notification(previous_task, task):\n    return None\n",
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def _bound_dependency_kwargs(
    tmp_path: Path, *, validator_path: Path | None = None
) -> dict[str, str]:
    validator = (
        validator_path or Path("scripts/pnc_rca_direct_vm_validator.py").resolve()
    )
    humanizer = _write_humanizer_module(tmp_path / "vm_feishu_humanizer.py")
    return {
        "validator_module_path": str(validator),
        "validator_sha256": (
            hashlib.sha256(validator.read_bytes()).hexdigest()
            if validator.is_file()
            else ""
        ),
        "humanizer_module_path": str(humanizer),
        "humanizer_sha256": hashlib.sha256(humanizer.read_bytes()).hexdigest(),
    }


def _load_request() -> dict[str, object]:
    return _request().to_dict()


def test_status_proves_missing_only_after_layout_probe(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "shared-state"
    monkeypatch.setattr(creator, "_safe_root", lambda _value: root)

    assert (
        creator.read_direct_vm_status(str(root), "task-missing")["state"] == "unknown"
    )
    assert not root.exists()

    for name in (
        "tasks",
        "dispatch/pending",
        "dispatch/claimed",
        "dispatch/done",
        "dispatch/failed",
    ):
        (root / name).mkdir(parents=True, exist_ok=True)
    observed = creator.read_direct_vm_status(str(root), "task-missing")
    assert observed == {
        "state": "missing",
        "task_id": "task-missing",
        "submission_key": "",
        "identity_sha256": "",
    }


def test_old_vm_shared_state_abi_creates_and_deduplicates(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "shared-state"
    module_path = _write_old_abi_module(tmp_path / "shared_state_v2.py")
    dependencies = _bound_dependency_kwargs(tmp_path)
    monkeypatch.setattr(creator, "_safe_root", lambda _value: root)

    request = _load_request()
    first = creator.create_direct_vm_task(
        str(root),
        request,
        shared_state_module_path=str(module_path),
        **dependencies,
    )
    second = creator.create_direct_vm_task(
        str(root),
        request,
        shared_state_module_path=str(module_path),
        **dependencies,
    )

    assert first["created"] is True
    assert second["deduplicated"] is True
    status = creator.read_direct_vm_status(str(root), request["task_id"])
    assert status["state"] == "existing"
    assert status["identity_sha256"] == request["identity_sha256"]
    metadata = json.loads(
        (root / "tasks" / str(request["task_id"]) / "meta.json").read_text()
    )["metadata"]
    assert metadata["direct_vm_allow_download"] is False
    assert "release_id" not in json.dumps(metadata)


def test_old_vm_shared_state_abi_conflicts_on_identity_change(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "shared-state"
    module_path = _write_old_abi_module(tmp_path / "shared_state_v2.py")
    dependencies = _bound_dependency_kwargs(tmp_path)
    monkeypatch.setattr(creator, "_safe_root", lambda _value: root)
    request = _load_request()
    creator.create_direct_vm_task(
        str(root),
        request,
        shared_state_module_path=str(module_path),
        **dependencies,
    )
    execution = dict(request["execution_request"])
    execution["request_kind"] = "issue_intake"
    execution["work_item"] = dict(execution["work_item"])
    execution["work_item"]["work_item_id"] = "different-item"
    changed = build_direct_vm_request(
        task_id=str(request["task_id"]),
        submission_key=str(request["submission_key"]),
        auth=request["auth"],
        source_refs=request["source_refs"],
        execution_request=execution,
        artifact_root=str(request["artifact_root"]),
        artifact_cifs_root=str(request["artifact_cifs_root"]),
    ).to_dict()

    result = creator.create_direct_vm_task(
        str(root),
        changed,
        shared_state_module_path=str(module_path),
        **dependencies,
    )
    assert result["accepted"] is False
    assert result["conflict"] is True


def test_creator_rejects_missing_submit_contract_before_write(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "shared-state"
    module_path = _write_old_abi_module(tmp_path / "shared_state_v2.py")
    monkeypatch.setattr(creator, "_safe_root", lambda _value: root)

    with pytest.raises(creator.DirectVmCreatorError) as raised:
        creator.create_direct_vm_task(
            str(root),
            _load_request(),
            shared_state_module_path=str(module_path),
            validator_module_path=str(tmp_path / "missing_validator.py"),
        )

    assert raised.value.code == "direct_vm_submit_contract_unavailable"
    assert not root.exists()


def test_creator_validator_error_precedes_unbound_humanizer(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "shared-state"
    module_path = _write_old_abi_module(tmp_path / "shared_state_v2.py")
    humanizer = _write_humanizer_module(tmp_path / "vm_feishu_humanizer.py")
    humanizer.chmod(0o644)
    monkeypatch.setattr(creator, "_safe_root", lambda _value: root)

    with pytest.raises(creator.DirectVmCreatorError) as raised:
        creator.create_direct_vm_task(
            str(root),
            _load_request(),
            shared_state_module_path=str(module_path),
            validator_module_path=str(tmp_path / "missing_validator.py"),
            humanizer_module_path=str(humanizer),
            humanizer_sha256=hashlib.sha256(humanizer.read_bytes()).hexdigest(),
        )

    assert raised.value.code == "direct_vm_submit_contract_unavailable"
    assert not root.exists()


def test_creator_requires_dedicated_humanizer_mode_and_hash(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "shared-state"
    module_path = _write_old_abi_module(tmp_path / "shared_state_v2.py")
    dependencies = _bound_dependency_kwargs(tmp_path)
    humanizer = Path(dependencies["humanizer_module_path"])
    monkeypatch.setattr(creator, "_safe_root", lambda _value: root)

    humanizer.chmod(0o644)
    with pytest.raises(creator.DirectVmCreatorError) as raised:
        creator.create_direct_vm_task(
            str(root),
            _load_request(),
            shared_state_module_path=str(module_path),
            **dependencies,
        )
    assert raised.value.code == "direct_vm_humanizer_unavailable"
    assert not root.exists()

    humanizer.chmod(0o600)
    dependencies["humanizer_sha256"] = "0" * 64
    with pytest.raises(creator.DirectVmCreatorError) as raised:
        creator.create_direct_vm_task(
            str(root),
            _load_request(),
            shared_state_module_path=str(module_path),
            **dependencies,
        )
    assert raised.value.code == "direct_vm_humanizer_unavailable"
    assert not root.exists()


def test_creator_rejects_group_or_world_writable_module(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "shared-state"
    module_path = _write_old_abi_module(tmp_path / "shared_state_v2.py")
    module_path.chmod(0o666)
    dependencies = _bound_dependency_kwargs(tmp_path)
    monkeypatch.setattr(creator, "_safe_root", lambda _value: root)

    with pytest.raises(creator.DirectVmCreatorError) as raised:
        creator.create_direct_vm_task(
            str(root),
            _load_request(),
            shared_state_module_path=str(module_path),
            **dependencies,
        )

    assert raised.value.code == "direct_vm_shared_state_creator_unavailable"
    assert not root.exists()


def test_creator_rejects_module_parent_symlink(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "shared-state"
    real_dir = tmp_path / "real-modules"
    real_dir.mkdir()
    module_path = _write_old_abi_module(real_dir / "shared_state_v2.py")
    linked_dir = tmp_path / "linked-modules"
    linked_dir.symlink_to(real_dir, target_is_directory=True)
    dependencies = _bound_dependency_kwargs(tmp_path)
    monkeypatch.setattr(creator, "_safe_root", lambda _value: root)

    with pytest.raises(creator.DirectVmCreatorError) as raised:
        creator.create_direct_vm_task(
            str(root),
            _load_request(),
            shared_state_module_path=str(linked_dir / module_path.name),
            **dependencies,
        )

    assert raised.value.code == "direct_vm_shared_state_creator_unavailable"
    assert not root.exists()


def test_creator_rejects_shared_state_abi_without_create_task(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "shared-state"
    module_path = _write_old_abi_module(
        tmp_path / "bad_shared_state.py", with_create=False
    )
    dependencies = _bound_dependency_kwargs(tmp_path)
    monkeypatch.setattr(creator, "_safe_root", lambda _value: root)

    with pytest.raises(creator.DirectVmCreatorError) as raised:
        creator.create_direct_vm_task(
            str(root),
            _load_request(),
            shared_state_module_path=str(module_path),
            **dependencies,
        )

    assert raised.value.code == "direct_vm_shared_state_creator_abi_invalid"


def test_creator_rejects_auth_impersonation_before_any_root_write(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "shared-state"
    request = _load_request()
    request["auth"] = {
        "principal": "other-principal",
        "capability": "g1q3_rca_direct_vm_submit",
    }
    # Rebuild the identity hash so the rejection specifically exercises the
    # creator's fixed auth contract rather than a malformed envelope hash.
    request = build_direct_vm_request(
        task_id=str(request["task_id"]),
        submission_key=str(request["submission_key"]),
        auth=request["auth"],
        source_refs=request["source_refs"],
        execution_request=request["execution_request"],
        artifact_root=str(request["artifact_root"]),
        artifact_cifs_root=str(request["artifact_cifs_root"]),
    ).to_dict()
    with pytest.raises(creator.DirectVmCreatorError) as raised:
        creator.create_direct_vm_task(
            str(root),
            request,
            shared_state_module_path=str(tmp_path / "missing_shared_state.py"),
            validator_module_path=str(tmp_path / "missing_validator.py"),
        )
    assert raised.value.code == "direct_vm_envelope_auth_mismatch"
    assert not root.exists()


def test_creator_rejects_overdeep_envelope_before_module_load(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "shared-state"
    request = _load_request()
    nested: object = "leaf"
    for _ in range(creator.MAX_JSON_DEPTH + 1):
        nested = [nested]
    request["execution_request"] = nested
    with pytest.raises(creator.DirectVmCreatorError) as raised:
        creator.create_direct_vm_task(
            str(root),
            request,
            shared_state_module_path=str(tmp_path / "missing_shared_state.py"),
            validator_module_path=str(tmp_path / "missing_validator.py"),
        )
    assert raised.value.code == "direct_vm_json_shape_exceeded"
    assert not root.exists()
