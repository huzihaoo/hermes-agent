from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from scripts import pnc_wrapper_transaction as wrappers


AUTHORITY_SHA = "a" * 64


def _write(path: Path, raw: bytes, mode: int = 0o755) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    path.chmod(mode)


@pytest.fixture
def wrapper_case(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    source = tmp_path / "source"
    target = tmp_path / "bin"
    evidence = tmp_path / "evidence"
    source.mkdir(mode=0o700)
    target.mkdir(mode=0o700)
    evidence.mkdir(mode=0o700)
    for index, name in enumerate(wrappers.SOURCE_NAMES):
        _write(
            source / "scripts" / "wrappers" / name,
            (
                "#!/bin/sh\n"
                f"# source-{index}\n"
                "exec /usr/bin/python3 /safe/pnc_live_exec.py service \"$@\"\n"
            ).encode(),
        )
        _write(target / name, f"#!/bin/sh\n# old-{index}\n".encode())
    for name in wrappers.RETIRED_NAMES:
        _write(target / name, b"#!/bin/sh\n# retired-old\n")
    monkeypatch.setattr(
        wrappers,
        "_source_provenance",
        lambda _root: {"commit": "b" * 40, "tree": "c" * 40},
    )
    return {
        "source": source.absolute(),
        "target": target.absolute(),
        "evidence": evidence.absolute(),
        "before": {
            name: (target / name).read_bytes()
            for name in (*wrappers.SOURCE_NAMES, *wrappers.RETIRED_NAMES)
        },
    }


def _plan(case: dict[str, Any], transaction_id: str) -> tuple[dict, Path]:
    return wrappers.build_plan(
        source_root=case["source"],
        target_bin=case["target"],
        evidence_root=case["evidence"],
        authority_sha256=AUTHORITY_SHA,
        transaction_id=transaction_id,
    )


def test_plan_is_owner_only_and_non_mutating(wrapper_case: dict[str, Any]) -> None:
    plan, path = _plan(wrapper_case, "plan-only")

    assert path.stat().st_mode & 0o777 == 0o600
    assert plan["mutation_performed"] is False
    assert [entry["name"] for entry in plan["entries"]] == [
        *wrappers.SOURCE_NAMES,
        *wrappers.RETIRED_NAMES,
    ]
    for name, raw in wrapper_case["before"].items():
        assert (wrapper_case["target"] / name).read_bytes() == raw


def test_apply_installs_dynamic_sources_retires_orphan_and_rolls_back(
    wrapper_case: dict[str, Any],
) -> None:
    plan, plan_path = _plan(wrapper_case, "apply-and-rollback")
    receipt = wrappers.apply_plan(plan, plan_path=plan_path)

    assert receipt["verification"] == "pass"
    assert receipt["installed"] == list(wrappers.SOURCE_NAMES)
    assert receipt["retired"] == list(wrappers.RETIRED_NAMES)
    for name in wrappers.SOURCE_NAMES:
        target = wrapper_case["target"] / name
        source = wrapper_case["source"] / "scripts" / "wrappers" / name
        assert target.read_bytes() == source.read_bytes()
        assert target.stat().st_mode & 0o777 == 0o755
        assert b"pnc_live_exec.py" in target.read_bytes()
    for name in wrappers.RETIRED_NAMES:
        assert not (wrapper_case["target"] / name).exists()

    rollback_path = wrapper_case["evidence"] / "rollback-receipt.json"
    result = wrappers.rollback_transaction(
        Path(receipt["receipt_path"]),
        output_path=rollback_path,
    )
    assert result["restored_to_pre_transaction"] is True
    assert rollback_path.stat().st_mode & 0o777 == 0o600
    for name, raw in wrapper_case["before"].items():
        assert (wrapper_case["target"] / name).read_bytes() == raw


def test_partial_apply_failure_restores_every_original_target(
    wrapper_case: dict[str, Any],
) -> None:
    plan, plan_path = _plan(wrapper_case, "partial-failure")
    calls = 0

    def fail_once(source: str | Path, target: str | Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 4:
            raise OSError("injected replace failure")
        os.replace(source, target)

    with pytest.raises(OSError, match="injected replace failure"):
        wrappers.apply_plan(
            plan,
            plan_path=plan_path,
            replace_func=fail_once,
        )

    for name, raw in wrapper_case["before"].items():
        assert (wrapper_case["target"] / name).read_bytes() == raw
    rollback = Path(plan["transaction_dir"]) / "automatic-rollback.json"
    assert json.loads(rollback.read_text(encoding="utf-8"))[
        "restored_to_pre_transaction"
    ] is True


def test_held_lock_is_not_removed(wrapper_case: dict[str, Any]) -> None:
    plan, plan_path = _plan(wrapper_case, "held-lock")
    lock = wrapper_case["target"] / ".pnc-wrapper-transaction.lock"
    lock.write_text("other-owner", encoding="utf-8")
    lock.chmod(0o600)

    with pytest.raises(
        wrappers.WrapperTransactionError,
        match="pnc_wrapper_transaction_lock_held",
    ):
        wrappers.apply_plan(plan, plan_path=plan_path)

    assert lock.read_text(encoding="utf-8") == "other-owner"
    for name, raw in wrapper_case["before"].items():
        assert (wrapper_case["target"] / name).read_bytes() == raw


def test_rollback_rejects_post_release_target_change(
    wrapper_case: dict[str, Any],
) -> None:
    plan, plan_path = _plan(wrapper_case, "rollback-drift")
    receipt = wrappers.apply_plan(plan, plan_path=plan_path)
    changed = wrapper_case["target"] / wrappers.SOURCE_NAMES[0]
    changed.write_bytes(b"#!/bin/sh\n# post-release owner change\n")
    changed.chmod(0o755)

    with pytest.raises(
        wrappers.WrapperTransactionError,
        match="pnc_wrapper_transaction_rollback_target_changed",
    ):
        wrappers.rollback_transaction(
            Path(receipt["receipt_path"]),
            output_path=wrapper_case["evidence"] / "blocked-rollback.json",
        )


@pytest.mark.parametrize("failure", ["missing", "hard_pin"])
def test_plan_rejects_missing_or_hard_pinned_source(
    wrapper_case: dict[str, Any], failure: str
) -> None:
    source = (
        wrapper_case["source"]
        / "scripts"
        / "wrappers"
        / wrappers.SOURCE_NAMES[0]
    )
    if failure == "missing":
        source.unlink()
        expected = "pnc_wrapper_transaction_source_missing"
    else:
        source.write_text(
            "#!/bin/sh\nexec /runtime/releases/hard-pinned/tool\n",
            encoding="utf-8",
        )
        source.chmod(0o755)
        expected = "pnc_wrapper_transaction_source_not_dynamic"

    with pytest.raises(wrappers.WrapperTransactionError, match=expected):
        _plan(wrapper_case, f"invalid-{failure}")
    assert not (wrapper_case["evidence"] / f"invalid-{failure}").exists()
