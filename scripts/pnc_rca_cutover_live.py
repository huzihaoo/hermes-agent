#!/usr/bin/env python3
"""Concrete, authority-injected live boundaries for the RCA cutover v2."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import stat
from typing import Any, Callable, Mapping, Protocol, Sequence

import psutil

from scripts import pnc_rca_cutover_adapter as adapter
from scripts import pnc_rca_cutover_guard as cutover_guard
from scripts import pnc_rca_production_cutover as cutover
from scripts.pnc_rca_store_migration_drill import (
    collect_writer_stop_evidence,
    write_receipt_atomic,
)


LIVE_SERVICE_STATE_SCHEMA_VERSION = cutover_guard.LIVE_SERVICE_STATE_SCHEMA_VERSION
LIVE_IDENTITY_SCHEMA_VERSION = "pnc_rca_projected_live_identity_v1"
WRITER_STOP_FILENAME = "adapter-writer-stop-evidence.json"
MAX_LAUNCHCTL_OUTPUT_BYTES = 4 * 1024 * 1024
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


class LiveBoundaryError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class ProcessFactory(Protocol):
    def __call__(self, pid: int) -> psutil.Process: ...


def _hash(value: Any) -> str:
    return cutover._sha256_json(value)


def _require_sha256(value: Any, code: str) -> str:
    text = str(value or "")
    if SHA256_RE.fullmatch(text) is None:
        raise LiveBoundaryError(code)
    return text


def _validate_labels(labels: Sequence[str]) -> tuple[str, ...]:
    if isinstance(labels, (str, bytes)):
        raise LiveBoundaryError("cutover_live_service_labels_invalid")
    normalized = tuple(labels)
    if not normalized or len(set(normalized)) != len(normalized) or any(
        label not in cutover.SERVICE_LABELS for label in normalized
    ):
        raise LiveBoundaryError("cutover_live_service_labels_invalid")
    return normalized


def _top_level_values(output: str, key: str, pattern: str) -> list[str]:
    matches = re.findall(
        rf"(?m)^([ \t]*){re.escape(key)}\s*=\s*({pattern})\s*$",
        output,
    )
    if not matches:
        return []
    depths = [len(indent.expandtabs(8)) for indent, _value in matches]
    minimum = min(depths)
    return [
        value.strip()
        for (_indent, value), depth in zip(matches, depths, strict=True)
        if depth == minimum
    ]


def _regular_state(path: Path) -> Mapping[str, Any]:
    selected = path.expanduser().absolute()
    if not selected.exists() and not selected.is_symlink():
        return {"path": str(selected), "state": "absent"}
    observed = adapter._read_stable_owner_file(selected)
    return {
        "path": str(selected),
        "state": "regular",
        "sha256": observed.sha256,
        "size_bytes": len(observed.raw),
        "mode": f"{observed.mode:04o}",
        "uid": observed.identity["st_uid"],
        "nlink": observed.identity["st_nlink"],
    }


class LaunchdServiceController:
    """Exact-argv launchd boundary used only by an authorized cutover adapter."""

    def __init__(
        self,
        *,
        evidence_root: Path,
        target_runtime_root: Path = cutover.CANONICAL_RUNTIME_ROOT,
        launch_agents_root: Path = cutover.CANONICAL_LAUNCH_AGENTS_ROOT,
        runner: adapter.CommandRunner | None = None,
        process_factory: ProcessFactory = psutil.Process,
        writer_stop_collector: Callable[[], Mapping[str, Any]] = (
            collect_writer_stop_evidence
        ),
        receipt_writer: Callable[[Path, Mapping[str, Any]], None] = (
            write_receipt_atomic
        ),
        precutover_service_state: Mapping[str, Any] | None = None,
    ) -> None:
        self._evidence_root = evidence_root.expanduser().absolute()
        self._target_runtime_root = target_runtime_root.expanduser().absolute()
        self._launch_agents_root = launch_agents_root.expanduser().absolute()
        self._runner = runner or adapter.SubprocessArgvRunner()
        self._process_factory = process_factory
        self._writer_stop_collector = writer_stop_collector
        self._receipt_writer = receipt_writer
        self._precutover_service_state = (
            json.loads(json.dumps(precutover_service_state))
            if precutover_service_state is not None
            else None
        )
        if (
            not self._evidence_root.is_absolute()
            or not self._target_runtime_root.is_absolute()
            or not self._launch_agents_root.is_absolute()
        ):
            raise LiveBoundaryError("cutover_live_path_invalid")

    @property
    def _domain(self) -> str:
        return f"gui/{os.geteuid()}"

    def _run(self, argv: Sequence[str]) -> adapter.CommandResult:
        expected = tuple(argv)
        result = self._runner.run(expected)
        if result.argv != expected:
            raise LiveBoundaryError("cutover_live_command_identity_mismatch")
        if len(result.stdout.encode("utf-8")) > MAX_LAUNCHCTL_OUTPUT_BYTES or len(
            result.stderr.encode("utf-8")
        ) > MAX_LAUNCHCTL_OUTPUT_BYTES:
            raise LiveBoundaryError("cutover_live_launchctl_output_too_large")
        return result

    def _job(self, label: str) -> Mapping[str, Any]:
        _validate_labels((label,))
        result = self._run(("/bin/launchctl", "print", f"{self._domain}/{label}"))
        if result.returncode == 113:
            return {
                "label": label,
                "loaded": False,
                "state": "absent",
                "pid": None,
                "last_exit_status": None,
            }
        if result.returncode != 0:
            raise LiveBoundaryError("cutover_live_launchctl_print_failed")
        pid_values = _top_level_values(result.stdout, "pid", r"[0-9]+")
        state_values = _top_level_values(result.stdout, "state", r"[^\n]+?")
        exit_values = _top_level_values(
            result.stdout, "last exit code", r"-?[0-9]+"
        )
        if len(pid_values) > 1 or len(state_values) > 1 or len(exit_values) > 1:
            raise LiveBoundaryError("cutover_live_launchctl_output_ambiguous")
        return {
            "label": label,
            "loaded": True,
            "state": state_values[0] if state_values else "unknown",
            "pid": int(pid_values[0]) if pid_values else None,
            "last_exit_status": int(exit_values[0]) if exit_values else None,
        }

    def _plist_state(self, label: str) -> Mapping[str, Any]:
        return _regular_state(self._launch_agents_root / f"{label}.plist")

    def capture_state(self, labels: Sequence[str]) -> Mapping[str, Any]:
        normalized = _validate_labels(labels)
        current = {
            "schema_version": LIVE_SERVICE_STATE_SCHEMA_VERSION,
            "target_runtime_root": str(self._target_runtime_root),
            "labels": list(normalized),
            "jobs": {
                label: {
                    "launchd": self._job(label),
                    "plist": self._plist_state(label),
                }
                for label in normalized
            },
        }
        if self._precutover_service_state is None:
            return current
        prior = self._precutover_service_state
        if (
            normalized != cutover.SERVICE_LABELS
            or not isinstance(prior, Mapping)
            or prior.get("schema_version") != LIVE_SERVICE_STATE_SCHEMA_VERSION
            or prior.get("target_runtime_root") != str(self._target_runtime_root)
            or prior.get("labels") != list(normalized)
            or not isinstance(prior.get("jobs"), Mapping)
            or set(prior["jobs"]) != set(normalized)
        ):
            raise LiveBoundaryError("cutover_live_precutover_service_state_invalid")
        for label in cutover.WRITER_LABELS:
            if current["jobs"][label]["launchd"]["loaded"] is not False:
                raise LiveBoundaryError("cutover_live_writer_not_stopped_for_snapshot")
        for label in normalized:
            prior_entry = prior["jobs"].get(label)
            if not isinstance(prior_entry, Mapping) or set(prior_entry) != {
                "launchd",
                "plist",
            }:
                raise LiveBoundaryError(
                    "cutover_live_precutover_service_state_invalid"
                )
            if prior_entry["plist"] != current["jobs"][label]["plist"]:
                raise LiveBoundaryError("cutover_live_precutover_plist_drift")
        gateway = prior["jobs"][cutover.SERVICE_LABELS[0]]["launchd"]
        if (
            not isinstance(gateway, Mapping)
            or gateway.get("loaded") is not True
            or not isinstance(gateway.get("pid"), int)
            or gateway["pid"] <= 0
        ):
            raise LiveBoundaryError("cutover_live_precutover_gateway_state_invalid")
        return json.loads(json.dumps(prior))

    def stop_writers(
        self,
        labels: Sequence[str],
        *,
        lease_fingerprint: str,
        lease_token: str,
    ) -> Mapping[str, Any]:
        normalized = _validate_labels(labels)
        if normalized != cutover.WRITER_LABELS:
            raise LiveBoundaryError("cutover_live_writer_labels_invalid")
        _require_sha256(
            lease_fingerprint, "cutover_live_lease_fingerprint_invalid"
        )
        if not isinstance(lease_token, str) or len(lease_token) < 16:
            raise LiveBoundaryError("cutover_live_lease_token_invalid")
        for label in normalized:
            if self._job(label)["loaded"]:
                result = self._run(
                    ("/bin/launchctl", "bootout", f"{self._domain}/{label}")
                )
                if result.returncode != 0:
                    raise LiveBoundaryError("cutover_live_writer_bootout_failed")
            if self._job(label)["loaded"]:
                raise LiveBoundaryError("cutover_live_writer_still_loaded")
        evidence = self._writer_stop_collector()
        self._evidence_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._evidence_root.chmod(0o700)
        receipt = self._evidence_root / WRITER_STOP_FILENAME
        self._receipt_writer(receipt, evidence)
        return {
            "schema_version": "pnc_rca_writer_stop_evidence_v1",
            "writer_labels": list(normalized),
            "receipt_sha256": hashlib.sha256(receipt.read_bytes()).hexdigest(),
            "receipt_path": str(receipt),
        }

    def _verify_plist(self, label: str) -> None:
        path = self._launch_agents_root / f"{label}.plist"
        observed = adapter._read_stable_owner_file(path)
        if observed.mode != 0o644:
            raise LiveBoundaryError("cutover_live_plist_mode_invalid")
        try:
            body = plistlib.loads(observed.raw)
        except Exception as exc:
            raise LiveBoundaryError("cutover_live_plist_invalid") from exc
        argv = body.get("ProgramArguments") if isinstance(body, Mapping) else None
        if (
            body.get("Label") != label
            or body.get("WorkingDirectory") != str(self._target_runtime_root)
            or not isinstance(argv, list)
            or not argv
            or not str(argv[0]).startswith(f"{self._target_runtime_root}/")
        ):
            raise LiveBoundaryError("cutover_live_plist_binding_invalid")

    def _running_process(self, pid: int) -> tuple[float, bool]:
        try:
            process = self._process_factory(pid)
            create_time = float(process.create_time())
            cwd = str(Path(process.cwd()).expanduser().absolute())
            environment = process.environ()
        except (OSError, psutil.Error, ValueError) as exc:
            raise LiveBoundaryError("cutover_live_process_observation_failed") from exc
        target_venv = str(self._target_runtime_root / ".venv")
        healthy = (
            create_time > 0
            and cwd == str(self._target_runtime_root)
            and environment.get("VIRTUAL_ENV") == target_venv
        )
        return create_time, healthy

    def verify(
        self, labels: Sequence[str], *, runtime_sha256: str
    ) -> Mapping[str, Any]:
        normalized = _validate_labels(labels)
        _require_sha256(runtime_sha256, "cutover_live_runtime_sha256_invalid")
        result: dict[str, Mapping[str, Any]] = {}
        for label in normalized:
            self._verify_plist(label)
            job = self._job(label)
            if not job["loaded"]:
                raise LiveBoundaryError("cutover_live_service_not_loaded")
            pid = job["pid"]
            if label in cutover.PERIODIC_SERVICE_LABELS:
                if pid is None:
                    create_time = None
                    healthy = job["last_exit_status"] == 0
                else:
                    create_time, healthy = self._running_process(pid)
                kind = "periodic"
            else:
                if not isinstance(pid, int) or pid <= 0:
                    raise LiveBoundaryError("cutover_live_resident_not_running")
                create_time, healthy = self._running_process(pid)
                kind = "resident"
            if not healthy:
                raise LiveBoundaryError("cutover_live_service_unhealthy")
            result[label] = {
                "kind": kind,
                "loaded": True,
                "pid": pid,
                "process_create_time": create_time,
                "runtime_sha256": runtime_sha256,
                "health_ok": True,
            }
        return result

    def start_residents(self, labels: Sequence[str]) -> list[str]:
        normalized = _validate_labels(labels)
        if normalized != cutover.RESIDENT_LABELS:
            raise LiveBoundaryError("cutover_live_resident_labels_invalid")
        started: list[str] = []
        for label in normalized:
            if self._job(label)["loaded"]:
                continue
            path = self._launch_agents_root / f"{label}.plist"
            self._verify_plist(label)
            result = self._run(
                ("/bin/launchctl", "bootstrap", self._domain, str(path))
            )
            if result.returncode != 0:
                raise LiveBoundaryError("cutover_live_resident_bootstrap_failed")
            if not self._job(label)["loaded"]:
                raise LiveBoundaryError("cutover_live_resident_not_loaded")
            started.append(label)
        return started

    def restore_state(self, state: Mapping[str, Any]) -> None:
        if (
            not isinstance(state, Mapping)
            or state.get("schema_version") != LIVE_SERVICE_STATE_SCHEMA_VERSION
            or state.get("target_runtime_root") != str(self._target_runtime_root)
            or not isinstance(state.get("labels"), list)
            or not isinstance(state.get("jobs"), Mapping)
        ):
            raise LiveBoundaryError("cutover_live_restore_state_invalid")
        labels = _validate_labels(state["labels"])
        jobs = state["jobs"]
        if set(jobs) != set(labels):
            raise LiveBoundaryError("cutover_live_restore_state_invalid")
        for label in labels:
            if self._job(label)["loaded"]:
                result = self._run(
                    ("/bin/launchctl", "bootout", f"{self._domain}/{label}")
                )
                if result.returncode != 0:
                    raise LiveBoundaryError("cutover_live_restore_bootout_failed")
        for label in labels:
            prior = jobs[label]
            launchd = prior.get("launchd") if isinstance(prior, Mapping) else None
            plist = prior.get("plist") if isinstance(prior, Mapping) else None
            if not isinstance(launchd, Mapping) or not isinstance(plist, Mapping):
                raise LiveBoundaryError("cutover_live_restore_state_invalid")
            if launchd.get("loaded") is not True:
                continue
            path = Path(str(plist.get("path") or ""))
            current = _regular_state(path)
            if (
                plist.get("state") != "regular"
                or current.get("sha256") != plist.get("sha256")
            ):
                raise LiveBoundaryError("cutover_live_restore_plist_drift")
            result = self._run(
                ("/bin/launchctl", "bootstrap", self._domain, str(path))
            )
            if result.returncode != 0:
                raise LiveBoundaryError("cutover_live_restore_bootstrap_failed")


def _expected_workspace_files(descriptor: Mapping[str, Any]) -> Mapping[str, Any]:
    identity = descriptor.get("identity")
    if not isinstance(identity, Mapping) or not isinstance(
        identity.get("file_sha256"), Mapping
    ):
        raise LiveBoundaryError("cutover_live_workspace_descriptor_invalid")
    root = Path(str(descriptor.get("path") or ""))
    manifest = Path(str(identity.get("manifest_path") or ""))
    try:
        relative = manifest.relative_to(root).as_posix()
    except ValueError as exc:
        raise LiveBoundaryError("cutover_live_workspace_descriptor_invalid") from exc
    result = {
        str(name): {"sha256": sha}
        for name, sha in identity["file_sha256"].items()
    }
    result[relative] = {"sha256": identity.get("manifest_sha256")}
    return result


def _tree_component(
    root: Path,
    *,
    expected: Mapping[str, Mapping[str, Any]],
    target_sha256: str,
) -> str:
    selected = root.expanduser().absolute()
    if not selected.exists() and not selected.is_symlink():
        return _hash({"path": str(selected), "state": "absent"})
    info = selected.lstat()
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) & 0o022
    ):
        raise LiveBoundaryError("cutover_live_tree_root_invalid")
    actual: dict[str, Mapping[str, Any]] = {}
    paths = sorted(selected.rglob("*"), key=lambda item: item.as_posix())
    if len(paths) > adapter.MAX_TREE_FILES:
        raise LiveBoundaryError("cutover_live_tree_too_large")
    for path in paths:
        child = path.lstat()
        if stat.S_ISLNK(child.st_mode):
            raise LiveBoundaryError("cutover_live_tree_symlink_forbidden")
        if stat.S_ISDIR(child.st_mode):
            if child.st_uid != os.geteuid() or stat.S_IMODE(child.st_mode) & 0o022:
                raise LiveBoundaryError("cutover_live_tree_directory_invalid")
            continue
        if not stat.S_ISREG(child.st_mode):
            raise LiveBoundaryError("cutover_live_tree_special_file_forbidden")
        observed = adapter._read_stable_owner_file(path)
        actual[path.relative_to(selected).as_posix()] = {
            "sha256": observed.sha256,
            "size_bytes": len(observed.raw),
            "mode": f"{observed.mode:04o}",
        }
    exact = set(actual) == set(expected)
    if exact:
        for relative, expected_entry in expected.items():
            item = actual[relative]
            if item["sha256"] != expected_entry.get("sha256"):
                exact = False
                break
            if "size_bytes" in expected_entry and item["size_bytes"] != expected_entry.get(
                "size_bytes"
            ):
                exact = False
                break
            if "mode" in expected_entry and item["mode"] != expected_entry.get("mode"):
                exact = False
                break
    return target_sha256 if exact else _hash({"path": str(selected), "files": actual})


def _file_component(path: Path, *, target_sha256: str) -> str:
    state = _regular_state(path)
    return (
        target_sha256
        if state.get("state") == "regular" and state.get("sha256") == target_sha256
        else _hash(state)
    )


class ProjectedLiveIdentityObserver:
    """Recompute the exact cutover CAS projection from live bytes."""

    def __init__(
        self,
        *,
        plan: Mapping[str, Any],
        payloads: Mapping[str, Mapping[str, Any]],
        path_mapper: Callable[[Path], Path] | None = None,
    ) -> None:
        if plan.get("schema_version") != cutover.PLAN_SCHEMA_VERSION:
            raise LiveBoundaryError("cutover_live_plan_invalid")
        if set(payloads) != {
            "candidate_environment",
            "active_release_binding",
            "feishu_sidecar",
            "runtime",
            "workspace",
        }:
            raise LiveBoundaryError("cutover_live_payload_set_invalid")
        self._plan = plan
        self._payloads = payloads
        self._map = path_mapper or (lambda path: path.expanduser().absolute())

    def _path(self, value: str | Path) -> Path:
        logical = Path(value).expanduser()
        if not logical.is_absolute() or ".." in logical.parts:
            raise LiveBoundaryError("cutover_live_projection_path_invalid")
        physical = self._map(logical)
        if not physical.is_absolute() or ".." in physical.parts:
            raise LiveBoundaryError("cutover_live_projection_path_invalid")
        return physical

    def __call__(self) -> Mapping[str, Any]:
        bindings = self._plan["bindings"]
        projection = self._plan["payload_bindings"]
        runtime = self._payloads["runtime"]
        workspace = self._payloads["workspace"]
        runtime_expected = runtime.get("files")
        if not isinstance(runtime_expected, Mapping):
            raise LiveBoundaryError("cutover_live_runtime_descriptor_invalid")
        runtime_sha = _tree_component(
            self._path(projection["runtime"]["canonical_path"]),
            expected=runtime_expected,
            target_sha256=bindings["runtime_content_sha256"],
        )
        workspace_sha = _tree_component(
            self._path(projection["workspace"]["canonical_path"]),
            expected=_expected_workspace_files(workspace),
            target_sha256=bindings["workspace_runtime_sha256"],
        )
        env_sha = _file_component(
            self._path(projection["candidate_environment"]["canonical_path"]),
            target_sha256=bindings["candidate_env_sha256"],
        )
        active_sha = _file_component(
            self._path(projection["active_release_binding"]["canonical_path"]),
            target_sha256=projection["active_release_binding"]["sha256"],
        )
        sidecar_sha = _file_component(
            self._path(projection["feishu_sidecar"]["canonical_path"]),
            target_sha256=bindings["feishu_sidecar_sha256"],
        )
        plist_observation: dict[str, str] = {}
        candidate_plists = projection["runtime"]["candidate_plist_sha256"]
        for candidate, target_sha in sorted(candidate_plists.items()):
            canonical = cutover.CANONICAL_LAUNCH_AGENTS_ROOT / candidate.replace(
                ".candidate.plist", ".plist"
            )
            state = _regular_state(self._path(canonical))
            plist_observation[candidate] = str(state.get("sha256") or _hash(state))
        plist_sha = (
            bindings["candidate_plist_set_sha256"]
            if plist_observation == dict(sorted(candidate_plists.items()))
            else _hash(plist_observation)
        )
        activation_sha = (
            bindings["activation_contract_sha256"]
            if env_sha == bindings["candidate_env_sha256"]
            and active_sha == projection["active_release_binding"]["sha256"]
            else _hash(
                {
                    "state": "candidate_activation_policy_not_installed",
                    "candidate_env_sha256": env_sha,
                    "active_release_binding_sha256": active_sha,
                }
            )
        )
        return {
            "runtime_content_sha256": runtime_sha,
            "workspace_runtime_sha256": workspace_sha,
            "candidate_env_sha256": env_sha,
            "active_release_binding_sha256": active_sha,
            "feishu_sidecar_sha256": sidecar_sha,
            "candidate_plist_set_sha256": plist_sha,
            "activation_contract_sha256": activation_sha,
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("schema",))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    _parser().parse_args(argv)
    print(
        json.dumps(
            {
                "schema_version": LIVE_IDENTITY_SCHEMA_VERSION,
                "production_projection_is_ambient": False,
                "mutation_requires_bound_cutover_adapter_authority": True,
                "cutover_phase": "install_gateway_aux_only",
                "rca_resident_start_included": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
