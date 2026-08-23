from __future__ import annotations

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
        "from pathlib import Path\nimport json\n"
        "def ensure_canonical_root(root=None):\n"
        "    root = Path(root)\n"
        "    for name in ('tasks', 'dispatch/pending', 'dispatch/claimed', 'dispatch/done', 'dispatch/failed'):\n"
        "        (root / name).mkdir(parents=True, exist_ok=True)\n"
        "    return root\n" + create_function,
        encoding="utf-8",
    )
    return path


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
    submit_path = Path("gateway/pnc_rca_direct_vm_submit.py").resolve()
    monkeypatch.setattr(creator, "_safe_root", lambda _value: root)

    request = _load_request()
    first = creator.create_direct_vm_task(
        str(root),
        request,
        shared_state_module_path=str(module_path),
        submit_module_path=str(submit_path),
    )
    second = creator.create_direct_vm_task(
        str(root),
        request,
        shared_state_module_path=str(module_path),
        submit_module_path=str(submit_path),
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
    submit_path = Path("gateway/pnc_rca_direct_vm_submit.py").resolve()
    monkeypatch.setattr(creator, "_safe_root", lambda _value: root)
    request = _load_request()
    creator.create_direct_vm_task(
        str(root),
        request,
        shared_state_module_path=str(module_path),
        submit_module_path=str(submit_path),
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
        submit_module_path=str(submit_path),
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
            submit_module_path=str(tmp_path / "missing_submit.py"),
        )

    assert raised.value.code == "direct_vm_submit_contract_unavailable"
    assert not root.exists()


def test_creator_rejects_shared_state_abi_without_create_task(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "shared-state"
    module_path = _write_old_abi_module(
        tmp_path / "bad_shared_state.py", with_create=False
    )
    submit_path = Path("gateway/pnc_rca_direct_vm_submit.py").resolve()
    monkeypatch.setattr(creator, "_safe_root", lambda _value: root)

    with pytest.raises(creator.DirectVmCreatorError) as raised:
        creator.create_direct_vm_task(
            str(root),
            _load_request(),
            shared_state_module_path=str(module_path),
            submit_module_path=str(submit_path),
        )

    assert raised.value.code == "direct_vm_shared_state_creator_abi_invalid"
