from __future__ import annotations

import hashlib
import json
import os
import plistlib
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from scripts import pnc_rca_release_freshness_gate as gate
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from scripts.pnc_live_exec import PNC_PYTHON_LAUNCHD_LABELS


REPO_ROOT = Path(__file__).resolve().parents[2]


def _write_plist(home: Path, label: str, *, stale: bool = False) -> Path:
    runtime = home / ".hermes" / "runtime"
    launcher = runtime / "governance-tools" / "pnc_live_exec.py"
    if label == gate.WATCHDOG_LABEL:
        arguments = [
            "/bin/zsh",
            str(runtime / "governance-tools" / "watcher-staleness-watchdog.sh"),
        ]
        environment = {"HERMES_HOME": str(home / ".hermes")}
        working_directory = str(runtime)
    elif stale:
        arguments = [
            str(home / ".hermes/runtime/venvs/hermes-old/bin/python"),
            str(home / ".hermes/runtime/releases/hermes-old/scripts/old.py"),
            label,
        ]
        environment = {"VIRTUAL_ENV": str(home / ".hermes/runtime/venvs/hermes-old")}
        working_directory = str(home / ".hermes/runtime/releases/hermes-old")
    else:
        arguments = ["/usr/bin/python3", str(launcher), label]
        environment = {"HERMES_HOME": str(home / ".hermes")}
        working_directory = str(runtime)
    body = {
        "Label": label,
        "ProgramArguments": arguments,
        "WorkingDirectory": working_directory,
        "EnvironmentVariables": environment,
    }
    path = home / "Library" / "LaunchAgents" / f"{label}.plist"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(plistlib.dumps(body))
    return path


def test_persisted_gate_rejects_a_pinned_definition(tmp_path: Path):
    for label in PNC_PYTHON_LAUNCHD_LABELS:
        _write_plist(tmp_path, label, stale=label == PNC_PYTHON_LAUNCHD_LABELS[0])
    _write_plist(tmp_path, gate.WATCHDOG_LABEL)

    _evidence, errors = gate.audit_persisted_definitions(
        home=tmp_path,
        hermes_home=tmp_path / ".hermes",
    )

    codes = {str(item["code"]) for item in errors}
    assert "pnc_release_plist_bypasses_active_manifest" in codes
    assert "pnc_release_plist_virtualenv_pinned" in codes
    assert "pnc_release_plist_runtime_pinned" in codes


def test_source_registry_has_no_pinned_plist_or_wrapper():
    plist_labels = set()
    plist_paths = [
        *REPO_ROOT.glob("local.pnc.*.plist"),
        REPO_ROOT / "ai.hermes.gateway.plist",
    ]
    for path in plist_paths:
        payload = plistlib.loads(path.read_bytes())
        plist_labels.add(payload["Label"])
        text = path.read_text(encoding="utf-8")
        assert not any(marker in text for marker in gate.FORBIDDEN_RUNTIME_MARKERS)
    assert plist_labels == set(gate.EXPECTED_LAUNCHD_LABELS)
    gateway = plistlib.loads((REPO_ROOT / "ai.hermes.gateway.plist").read_bytes())
    assert gateway["ProgramArguments"] == [
        "/usr/bin/python3",
        "/Users/songying/.hermes/runtime/governance-tools/pnc_live_exec.py",
        "ai.hermes.gateway",
        "gateway",
        "run",
        "--replace",
    ]
    assert "VIRTUAL_ENV" not in gateway["EnvironmentVariables"]

    wrapper_names = {
        "hermes-context-budget-check",
        "hermes-governance-check",
        "hermes-live-drift-guard",
        "hermes-provider-failure-audit",
        "hermes-release-fingerprint-check",
        "hermes-safe-worktree-remove",
        "hermes-worktree-hygiene",
        "hermes.current",
    }
    assert {
        path.name for path in (REPO_ROOT / "scripts" / "wrappers").iterdir()
    } == wrapper_names
    for name in wrapper_names:
        text = (REPO_ROOT / "scripts" / "wrappers" / name).read_text(encoding="utf-8")
        assert not any(marker in text for marker in gate.FORBIDDEN_RUNTIME_MARKERS)
    context_wrapper = (
        REPO_ROOT / "scripts" / "wrappers" / "hermes-context-budget-check"
    ).read_text(encoding="utf-8")
    assert "local.pnc.context-budget-check" in context_wrapper
    assert "--repo-root ." in context_wrapper
    assert "HERMES_CONTEXT_BUDGET_SCRIPT" not in context_wrapper
    assert "HERMES_CONTEXT_BUDGET_REPO_ROOT" not in context_wrapper
    assert gate.SERVICE_TARGETS["local.pnc.hermes-cli"] == (
        "runtime_script",
        "hermes_cli/main.py",
    )
    assert gate.SERVICE_TARGETS["local.pnc.release-freshness-gate"] == (
        "runtime_script",
        "scripts/pnc_rca_release_freshness_gate.py",
    )
    assert "local.pnc.g1q3-e2e-smoke" not in gate.SERVICE_TARGETS
    assert "local.pnc.live-promote" not in gate.SERVICE_TARGETS
    assert not (REPO_ROOT / "scripts/wrappers/hermes-g1q3-e2e-smoke").exists()
    assert not (REPO_ROOT / "scripts/wrappers/hermes-live").exists()


def test_retired_stable_entrypoints_must_be_absent(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name in gate.RETIRED_STABLE_ENTRYPOINTS:
        (bin_dir / name).write_text(
            "#!/bin/zsh\nexec pnc_live_exec retired-label\n",
            encoding="utf-8",
        )

    evidence, errors = gate.audit_retired_stable_entrypoints(tmp_path)

    assert {item["name"] for item in evidence if item["present"]} == set(
        gate.RETIRED_STABLE_ENTRYPOINTS
    )
    assert {
        (item["code"], item["name"])
        for item in errors
    } == {
        ("pnc_release_retired_entrypoint_present", name)
        for name in gate.RETIRED_STABLE_ENTRYPOINTS
    }


def test_absent_retired_stable_entrypoints_pass(tmp_path: Path):
    evidence, errors = gate.audit_retired_stable_entrypoints(tmp_path)

    assert errors == []
    assert all(item["present"] is False for item in evidence)


def test_loaded_gate_rejects_a_stale_launchd_snapshot(tmp_path: Path):
    launcher = tmp_path / ".hermes/runtime/governance-tools/pnc_live_exec.py"
    stale = "/Users/songying/.hermes/runtime/releases/hermes-old/scripts/old.py"

    def reader(label: str) -> subprocess.CompletedProcess[str]:
        raw = f"program = /usr/bin/python3\n{launcher} {label}\n"
        if label == PNC_PYTHON_LAUNCHD_LABELS[0]:
            raw = f"program = /usr/bin/python3\n{stale}\n"
        return subprocess.CompletedProcess(["launchctl"], 0, raw, "")

    _evidence, errors = gate.audit_loaded_definitions(
        hermes_home=tmp_path / ".hermes",
        launchctl_reader=reader,
    )

    assert any(
        item["code"] == "pnc_release_loaded_definition_bypasses_manifest"
        and item["label"] == PNC_PYTHON_LAUNCHD_LABELS[0]
        for item in errors
    )


def test_versioned_stable_entrypoints_reject_stale_launcher_and_wrapper(
    tmp_path: Path,
):
    home = tmp_path
    hermes_home = home / ".hermes"
    runtime_root = hermes_home / "runtime" / "releases" / "active"
    scripts = runtime_root / "scripts"
    wrappers = scripts / "wrappers"
    governance = hermes_home / "runtime" / "governance-tools"
    wrappers.mkdir(parents=True)
    governance.mkdir(parents=True)
    (home / "bin").mkdir()

    versioned = {
        scripts / "pnc_live_exec.py": b"active launcher\n",
        scripts / "pnc_rca_release_freshness_gate.py": b"active gate\n",
        scripts / "watcher_staleness_watchdog.sh": b"active watchdog\n",
        wrappers / "hermes.current": b"active wrapper\n",
    }
    for path, raw in versioned.items():
        path.write_bytes(raw)
    (governance / "pnc_live_exec.py").write_bytes(b"stale launcher\n")
    (governance / "pnc_rca_release_freshness_gate.py").write_bytes(
        versioned[scripts / "pnc_rca_release_freshness_gate.py"]
    )
    (governance / "watcher-staleness-watchdog.sh").write_bytes(
        versioned[scripts / "watcher_staleness_watchdog.sh"]
    )
    (home / "bin" / "hermes.current").write_bytes(b"stale wrapper\n")

    evidence, errors = gate.audit_versioned_stable_entrypoints(
        home=home,
        hermes_home=hermes_home,
        runtime_root=runtime_root,
    )

    stale_names = {
        item["name"]
        for item in errors
        if item["code"] == "pnc_release_stable_entrypoint_stale"
    }
    assert stale_names == {"pnc_live_exec.py", "wrapper:hermes.current"}
    assert {item["name"] for item in evidence if item["exact"]} == {
        "pnc_rca_release_freshness_gate.py",
        "watcher-staleness-watchdog.sh",
    }


def test_release_golden_registry_must_be_green_and_pipeline_bound(tmp_path: Path):
    hermes_home = tmp_path / ".hermes"
    runtime = hermes_home / "runtime"
    runtime.mkdir(parents=True)
    commit = "a" * 40
    tree = "b" * 40
    (runtime / "LIVE_MANIFEST.json").write_text(
        json.dumps({
            "face_git_bindings": {"g1q3_rca_pipeline": {"commit": commit, "tree": tree}}
        }),
        encoding="utf-8",
    )
    base = {
        "present": True,
        "valid": True,
        "low_tier_golden_ready": True,
        "pipeline_commit": commit,
        "pipeline_tree": tree,
        "evaluators": {
            "acc_decel_heavy": {
                "status": "passed",
                "evaluator_id": "acc_decel_heavy",
            }
        },
    }

    evidence, errors = gate.audit_release_golden_registry(
        hermes_home=hermes_home,
        registry=base,
    )
    assert errors == []
    assert evidence["active_pipeline_commit"] == commit

    empty = {**base, "evaluators": {}}
    _evidence, errors = gate.audit_release_golden_registry(
        hermes_home=hermes_home,
        registry=empty,
    )
    assert {item["code"] for item in errors} == {
        "pnc_release_golden_evaluator_set_empty"
    }

    failing = {**base, "low_tier_golden_ready": False}
    _evidence, errors = gate.audit_release_golden_registry(
        hermes_home=hermes_home,
        registry=failing,
    )
    assert {item["code"] for item in errors} == {
        "pnc_release_low_tier_golden_not_ready"
    }

    stale = {**base, "pipeline_commit": "c" * 40}
    _evidence, errors = gate.audit_release_golden_registry(
        hermes_home=hermes_home,
        registry=stale,
    )
    assert {item["code"] for item in errors} == {
        "pnc_release_golden_pipeline_binding_mismatch"
    }


def test_release_golden_registry_binds_explicit_active_inventory(tmp_path: Path):
    hermes_home = tmp_path / ".hermes"
    runtime = hermes_home / "runtime"
    runtime.mkdir(parents=True)
    commit = "a" * 40
    tree = "b" * 40
    (runtime / "LIVE_MANIFEST.json").write_text(
        json.dumps(
            {
                "face_git_bindings": {
                    "g1q3_rca_pipeline": {"commit": commit, "tree": tree}
                }
            }
        ),
        encoding="utf-8",
    )
    registry = {
        "present": True,
        "valid": True,
        "low_tier_golden_ready": True,
        "pipeline_commit": commit,
        "pipeline_tree": tree,
        "evaluators": {
            "lane_geometry_quality": {
                "status": "passed",
                "evaluator_id": "lane_geometry_quality",
            }
        },
    }

    evidence, errors = gate.audit_release_golden_registry(
        hermes_home=hermes_home,
        registry=registry,
        required_evaluator_ids=["lane_geometry_quality", "new_evaluator"],
    )

    assert evidence["missing_required_evaluator_ids"] == ["new_evaluator"]
    assert evidence["inventory_binding_valid"] is False
    assert {
        item["code"] for item in errors
    } == {"pnc_release_golden_required_evaluator_missing"}


def test_stable_target_audit_resolves_every_hash_bound_target(tmp_path: Path):
    expected = {
        label
        for label, (kind, _relative) in gate.SERVICE_TARGETS.items()
        if kind in {"governance_tool", "runtime_file"}
    }
    failed = sorted(expected)[0]

    def resolver(*, manifest_path, hermes_home, service_label):
        assert manifest_path == hermes_home / "runtime" / "LIVE_MANIFEST.json"
        if service_label == failed:
            raise gate.LiveExecError("active_runtime_stable_target_mismatch")
        return {
            "script": f"/stable/{service_label}.py",
            "script_sha256": "a" * 64,
            "runtime_commit": "b" * 40,
        }

    evidence, errors = gate.audit_stable_targets(
        hermes_home=tmp_path,
        runtime_resolver=resolver,
    )

    assert {item["label"] for item in evidence} == expected
    assert errors == [
        {
            "code": "active_runtime_stable_target_mismatch",
            "label": failed,
        }
    ]


def test_release_preflight_rejects_only_unresolved_incompatible_effects(
    tmp_path: Path,
):
    db_path = tmp_path / "control.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE rca_delivery_effects (
            effect_key TEXT PRIMARY KEY,
            outcome TEXT NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    rows = [
        (
            "legacy-v2",
            "success",
            "pending",
            {"schema_version": "pnc_rca_delivery_effect_v2"},
        ),
        (
            "current-v3",
            "success",
            "claimed",
            {"schema_version": gate.DELIVERY_EFFECT_SCHEMA_VERSION},
        ),
        (
            "adjudication",
            "success",
            "retry_wait",
            {"schema_version": gate.ADJUDICATION_EFFECT_SCHEMA_VERSION},
        ),
        (
            "terminal-v1",
            "terminal_failed",
            "uncertain",
            {"schema_version": gate.TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1},
        ),
        (
            "terminal-fallback-v3",
            "terminal_failed",
            "pending",
            {"schema_version": gate.TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION},
        ),
    ]
    conn.executemany(
        "INSERT INTO rca_delivery_effects VALUES (?, ?, ?, ?)",
        [
            (key, outcome, status, json.dumps(payload))
            for key, outcome, status, payload in rows
        ],
    )
    conn.commit()
    conn.close()

    evidence, errors = gate.audit_unresolved_effect_schema_compatibility(
        hermes_home=tmp_path,
        control_db_path=db_path,
    )

    assert evidence["unresolved_effect_count"] == 5
    assert evidence["incompatible_effect_count"] == 1
    assert evidence["incompatible_effect_keys"] == ["legacy-v2"]
    assert {item["code"] for item in errors} == {
        "pnc_release_unresolved_effect_schema_incompatible"
    }

    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE rca_delivery_effects SET status = 'succeeded' WHERE effect_key = 'legacy-v2'"
    )
    conn.commit()
    conn.close()
    evidence, errors = gate.audit_unresolved_effect_schema_compatibility(
        hermes_home=tmp_path,
        control_db_path=db_path,
    )
    assert evidence["unresolved_effect_count"] == 4
    assert evidence["incompatible_effect_count"] == 0
    assert errors == []


def _delivery_store_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_release_preflight_accepts_complete_v9_delivery_store_without_writes(
    tmp_path: Path,
):
    db_path = tmp_path / "control.sqlite3"
    RcaDeliveryStore(db_path)
    before_sha256 = _delivery_store_sha256(db_path)

    evidence, errors = gate.audit_delivery_store_schema(
        hermes_home=tmp_path,
        control_db_path=db_path,
    )

    assert errors == []
    assert evidence["schema_valid"] is True
    assert evidence["observed_schema_version"] == "pnc_rca_delivery_store_v9"
    assert evidence["quick_check"] == "ok"
    assert evidence["integrity_check"] == "ok"
    assert evidence["foreign_key_violation_count"] == 0
    assert evidence["canonical_object_count"] > 0
    assert _delivery_store_sha256(db_path) == before_sha256


def test_release_preflight_rejects_v8_delivery_store_marker_without_writes(
    tmp_path: Path,
):
    db_path = tmp_path / "control.sqlite3"
    RcaDeliveryStore(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE rca_delivery_meta SET value = 'pnc_rca_delivery_store_v8' "
            "WHERE key = 'schema_version'"
        )
    before_sha256 = _delivery_store_sha256(db_path)

    evidence, errors = gate.audit_delivery_store_schema(
        hermes_home=tmp_path,
        control_db_path=db_path,
    )

    assert evidence["schema_valid"] is False
    assert evidence["observed_schema_version"] == "pnc_rca_delivery_store_v8"
    assert errors == [
        {
            "code": "pnc_release_delivery_store_schema_not_current",
            "reason": "delivery_store_combined_migration_target_schema_invalid",
        }
    ]
    assert _delivery_store_sha256(db_path) == before_sha256


def test_release_preflight_rejects_missing_immutable_trigger_without_writes(
    tmp_path: Path,
):
    db_path = tmp_path / "control.sqlite3"
    RcaDeliveryStore(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP TRIGGER trg_rca_conclusion_adjudication_no_delete")
    before_sha256 = _delivery_store_sha256(db_path)

    evidence, errors = gate.audit_delivery_store_schema(
        hermes_home=tmp_path,
        control_db_path=db_path,
    )

    assert evidence["schema_valid"] is False
    assert evidence["observed_schema_version"] == "pnc_rca_delivery_store_v9"
    assert errors == [
        {
            "code": "pnc_release_delivery_store_schema_not_current",
            "reason": (
                "delivery_store_combined_migration_target_schema_contract_invalid"
            ),
        }
    ]
    assert _delivery_store_sha256(db_path) == before_sha256


def test_process_evidence_reads_real_process_identity(tmp_path: Path):
    script = tmp_path / "resident.py"
    script.write_text("import time; time.sleep(30)\n", encoding="utf-8")
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    env = dict(os.environ)
    env.update({
        "PNC_LIVE_RUNTIME_COMMIT": "a" * 40,
        "PNC_LIVE_SERVICE_LABEL": "local.pnc.fixture",
        "VIRTUAL_ENV": str(tmp_path / "venv"),
    })
    process = subprocess.Popen(
        [sys.executable, str(script)],
        cwd=runtime,
        env=env,
    )
    try:
        time.sleep(0.15)
        evidence, errors = gate._process_evidence(
            label="local.pnc.fixture",
            pid=process.pid,
            resolved={
                "runtime_root": str(runtime),
                "script": str(script),
                "runtime_commit": "a" * 40,
                "runtime_venv": str(tmp_path / "venv"),
            },
        )
    finally:
        process.terminate()
        process.wait(timeout=5)
    assert evidence["pid"] == process.pid
    assert errors == []


def test_cli_returns_nonzero_for_stale_persisted_definition(tmp_path: Path):
    home = tmp_path
    for label in PNC_PYTHON_LAUNCHD_LABELS:
        _write_plist(home, label, stale=label == PNC_PYTHON_LAUNCHD_LABELS[0])
    _write_plist(home, gate.WATCHDOG_LABEL)
    hermes_home = home / ".hermes"
    (hermes_home / "runtime" / "governance-tools").mkdir(parents=True)
    manifest = hermes_home / "runtime" / "LIVE_MANIFEST.json"
    manifest.write_text(json.dumps({}), encoding="utf-8")
    manifest.chmod(0o600)
    result = subprocess.run(
        [sys.executable, str(Path(gate.__file__)), "--json"],
        env={**os.environ, "HOME": str(home), "HERMES_HOME": str(hermes_home)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is False
    assert any(
        item["code"] == "pnc_release_plist_bypasses_active_manifest"
        for item in payload["errors"]
    )
