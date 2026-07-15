#!/usr/bin/env python3
"""Advance an installed RCA release to bounded canary readiness."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Any

from scripts import pnc_rca_activation as activation
from scripts import pnc_rca_cutover_adapter as adapter
from scripts import pnc_rca_cutover_live as live
from scripts import pnc_rca_production_cutover as cutover


MANIFEST_SCHEMA_VERSION = "pnc_rca_postinstall_activation_manifest_v1"
JOURNAL_SCHEMA_VERSION = "pnc_rca_postinstall_activation_step_v1"
RECEIPT_SCHEMA_VERSION = "pnc_rca_postinstall_activation_receipt_v1"
RESIDENT_INTENT_SCHEMA_VERSION = "pnc_rca_resident_start_intent_v1"
RESIDENT_DONE_SCHEMA_VERSION = "pnc_rca_resident_start_receipt_v1"
AUTHORIZATION_DECISION = "authorize_exact_rca_postinstall_bounded_canary_bootstrap"
MAX_COMMAND_OUTPUT_BYTES = 8 * 1024 * 1024
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


class PostinstallActivationError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class PostinstallInputs:
    candidate_python: Path
    control_db: Path
    evidence_dir: Path
    live_env: Path
    active_release_binding: Path
    release_id: str
    bootstrap_epoch_id: str
    expected_topic: str
    expected_rule_version: str
    preauthorization_receipt: Path
    preauthorization_capsule: Path
    preproduction_receipt: Path
    preproduction_capsule: Path
    kafka_event_uid: str
    manual_success_identity: Path
    manual_terminal_failure_identity: Path
    runtime_content_sha256: str
    journal_root: Path
    lock_path: Path
    receipt_path: Path


def _absolute(value: Any, *, field: str) -> Path:
    if not isinstance(value, str) or not value:
        raise PostinstallActivationError(f"postinstall_{field}_invalid")
    path = Path(value).expanduser()
    if not path.is_absolute() or ".." in path.parts:
        raise PostinstallActivationError(f"postinstall_{field}_invalid")
    return path.absolute()


def _owner_directory(path: Path, *, field: str) -> Path:
    selected = path.expanduser().absolute()
    try:
        info = selected.lstat()
    except OSError as exc:
        raise PostinstallActivationError(f"postinstall_{field}_invalid") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise PostinstallActivationError(f"postinstall_{field}_invalid")
    return selected


def _capsule_path(receipt: Path, phase: str) -> Path:
    if receipt.suffix != ".json":
        raise PostinstallActivationError("postinstall_gate_receipt_invalid")
    return receipt.with_name(f"{receipt.stem}.activation-{phase}.json")


def load_manifest(path: Path) -> PostinstallInputs:
    body = cutover._read_owned_json(
        path, artifact="postinstall_activation_manifest"
    ).body
    expected = {
        "schema_version",
        "candidate_python",
        "control_db",
        "evidence_dir",
        "live_env",
        "active_release_binding",
        "release_id",
        "bootstrap_epoch_id",
        "expected_topic",
        "expected_rule_version",
        "preauthorization_receipt",
        "preauthorization_capsule",
        "preproduction_receipt",
        "preproduction_capsule",
        "kafka_event_uid",
        "manual_success_identity",
        "manual_terminal_failure_identity",
        "runtime_content_sha256",
        "journal_root",
        "lock_path",
        "receipt_path",
    }
    if set(body) != expected or body.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise PostinstallActivationError("postinstall_manifest_shape_invalid")
    candidate_python = _absolute(body.get("candidate_python"), field="candidate_python")
    expected_python = cutover.CANONICAL_RUNTIME_ROOT / ".venv/bin/python"
    if candidate_python != expected_python:
        raise PostinstallActivationError("postinstall_candidate_python_invalid")
    preauth_receipt = _absolute(
        body.get("preauthorization_receipt"), field="preauthorization_receipt"
    )
    preauth_capsule = _absolute(
        body.get("preauthorization_capsule"), field="preauthorization_capsule"
    )
    preprod_receipt = _absolute(
        body.get("preproduction_receipt"), field="preproduction_receipt"
    )
    preprod_capsule = _absolute(
        body.get("preproduction_capsule"), field="preproduction_capsule"
    )
    event_uid = str(body.get("kafka_event_uid") or "")
    runtime_sha = str(body.get("runtime_content_sha256") or "")
    release_id = str(body.get("release_id") or "")
    bootstrap_epoch = str(body.get("bootstrap_epoch_id") or "")
    topic = str(body.get("expected_topic") or "")
    rule = str(body.get("expected_rule_version") or "")
    if (
        preauth_capsule != _capsule_path(preauth_receipt, "preauthorization")
        or preprod_capsule != _capsule_path(preprod_receipt, "preproduction")
        or activation._EVENT_UID_RE.fullmatch(event_uid) is None
        or event_uid.split(":", 1)[0] != topic
        or SHA256_RE.fullmatch(runtime_sha) is None
        or cutover.RELEASE_ID_RE.fullmatch(release_id) is None
        or activation._EPOCH_ID_RE.fullmatch(bootstrap_epoch) is None
        or not rule.strip()
    ):
        raise PostinstallActivationError("postinstall_manifest_binding_invalid")
    evidence = _absolute(body.get("evidence_dir"), field="evidence_dir")
    journal = _absolute(body.get("journal_root"), field="journal_root")
    lock_path = _absolute(body.get("lock_path"), field="lock_path")
    receipt_path = _absolute(body.get("receipt_path"), field="receipt_path")
    _owner_directory(evidence, field="evidence_dir")
    _owner_directory(journal, field="journal_root")
    for field, output in (
        ("preauthorization_receipt_parent", preauth_receipt),
        ("preproduction_receipt_parent", preprod_receipt),
        ("lock_parent", lock_path),
        ("receipt_parent", receipt_path),
    ):
        _owner_directory(output.parent, field=field)
    return PostinstallInputs(
        candidate_python=candidate_python,
        control_db=_absolute(body.get("control_db"), field="control_db"),
        evidence_dir=evidence,
        live_env=_absolute(body.get("live_env"), field="live_env"),
        active_release_binding=_absolute(
            body.get("active_release_binding"), field="active_release_binding"
        ),
        release_id=release_id,
        bootstrap_epoch_id=bootstrap_epoch,
        expected_topic=topic,
        expected_rule_version=rule,
        preauthorization_receipt=preauth_receipt,
        preauthorization_capsule=preauth_capsule,
        preproduction_receipt=preprod_receipt,
        preproduction_capsule=preprod_capsule,
        kafka_event_uid=event_uid,
        manual_success_identity=_absolute(
            body.get("manual_success_identity"), field="manual_success_identity"
        ),
        manual_terminal_failure_identity=_absolute(
            body.get("manual_terminal_failure_identity"),
            field="manual_terminal_failure_identity",
        ),
        runtime_content_sha256=runtime_sha,
        journal_root=journal,
        lock_path=lock_path,
        receipt_path=receipt_path,
    )


@contextmanager
def _session_lock(path: Path):
    parent = _owner_directory(path.parent, field="lock_parent")
    del parent
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        info = os.fstat(descriptor)
        lexical = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
            or stat.S_ISLNK(lexical.st_mode)
            or (info.st_dev, info.st_ino) != (lexical.st_dev, lexical.st_ino)
        ):
            raise PostinstallActivationError("postinstall_lock_invalid")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PostinstallActivationError("postinstall_lock_contended") from exc
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _strict_output(raw: str, *, code: str) -> Mapping[str, Any]:
    if len(raw.encode("utf-8")) > MAX_COMMAND_OUTPUT_BYTES:
        raise PostinstallActivationError(code)
    try:
        body = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise PostinstallActivationError(code) from exc
    if not isinstance(body, Mapping):
        raise PostinstallActivationError(code)
    return body


def _validate_step_result(
    body: Mapping[str, Any],
    *,
    activation_command: str | None,
    applied: bool | None,
) -> None:
    if body.get("ok") is not True:
        error_code = str(body.get("code") or "postinstall_command_failed")
        if re.fullmatch(r"[a-z][a-z0-9_]{2,127}", error_code) is None:
            error_code = "postinstall_command_failed"
        raise PostinstallActivationError(error_code)
    if activation_command is not None and (
        body.get("schema_version") != activation.ACTIVATION_CLI_SCHEMA_VERSION
        or body.get("command") != activation_command
        or body.get("applied") is not applied
        or body.get("mode") != ("apply" if applied else "plan")
        or not isinstance(body.get("result"), Mapping)
    ):
        raise PostinstallActivationError("postinstall_activation_result_invalid")


def _run_step(
    *,
    inputs: PostinstallInputs,
    runner: adapter.CommandRunner,
    index: int,
    name: str,
    argv: Sequence[str],
    activation_command: str | None = None,
    applied: bool | None = None,
) -> Mapping[str, Any]:
    normalized = tuple(str(item) for item in argv)
    path = inputs.journal_root / f"{index:02d}-{name}.json"
    argv_sha256 = hashlib.sha256(
        json.dumps(list(normalized), separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if path.exists() or path.is_symlink():
        prior = cutover._read_owned_json(
            path, artifact="postinstall_activation_step"
        ).body
        if (
            prior.get("schema_version") != JOURNAL_SCHEMA_VERSION
            or prior.get("index") != index
            or prior.get("name") != name
            or prior.get("argv_sha256") != argv_sha256
            or prior.get("argv") != list(normalized)
            or not isinstance(prior.get("result"), Mapping)
        ):
            raise PostinstallActivationError("postinstall_journal_conflict")
        prior_result = prior["result"]
        _validate_step_result(
            prior_result,
            activation_command=activation_command,
            applied=applied,
        )
        return prior_result
    result = runner.run(normalized)
    if result.argv != normalized:
        raise PostinstallActivationError("postinstall_command_identity_mismatch")
    body = _strict_output(
        result.stdout,
        code="postinstall_command_output_invalid",
    )
    if result.returncode != 0:
        body = dict(body)
        body["ok"] = False
    _validate_step_result(
        body,
        activation_command=activation_command,
        applied=applied,
    )
    cutover._publish_no_clobber(
        path,
        {
            "schema_version": JOURNAL_SCHEMA_VERSION,
            "index": index,
            "name": name,
            "argv": list(normalized),
            "argv_sha256": argv_sha256,
            "result": body,
        },
    )
    return body


def _activation_argv(inputs: PostinstallInputs, *arguments: str) -> list[str]:
    return [
        str(inputs.candidate_python),
        "-B",
        str(cutover.CANONICAL_RUNTIME_ROOT / "scripts/pnc_rca_activation.py"),
        "--control-db",
        str(inputs.control_db),
        *arguments,
    ]


def _run_activation_pair(
    *,
    inputs: PostinstallInputs,
    runner: adapter.CommandRunner,
    index: int,
    name: str,
    command: str,
    arguments: Sequence[str],
) -> tuple[Mapping[str, Any], int]:
    argv = _activation_argv(inputs, command, *arguments)
    _run_step(
        inputs=inputs,
        runner=runner,
        index=index,
        name=f"{name}-plan",
        argv=argv,
        activation_command=command,
        applied=False,
    )
    applied = _run_step(
        inputs=inputs,
        runner=runner,
        index=index + 1,
        name=f"{name}-apply",
        argv=[*argv, "--apply"],
        activation_command=command,
        applied=True,
    )
    return applied, index + 2


def _gate_argv(
    inputs: PostinstallInputs,
    *,
    mode: str,
    receipt: Path,
    preauthorization_capsule: Path | None = None,
    preproduction_capsule: Path | None = None,
) -> list[str]:
    if preauthorization_capsule is not None and preproduction_capsule is not None:
        raise PostinstallActivationError("postinstall_gate_capsule_ambiguous")
    argv = [
        str(inputs.candidate_python),
        "-B",
        str(cutover.CANONICAL_RUNTIME_ROOT / "scripts/pnc_rca_release_gate.py"),
        "--mode",
        mode,
        "--evidence-dir",
        str(inputs.evidence_dir),
        "--env-file",
        str(inputs.live_env),
        "--expected-topic",
        inputs.expected_topic,
        "--expected-rule-version",
        inputs.expected_rule_version,
        "--receipt",
        str(receipt),
    ]
    if preauthorization_capsule is not None:
        argv.extend(("--preauthorization-capsule", str(preauthorization_capsule)))
    if preproduction_capsule is not None:
        argv.extend(("--preproduction-capsule", str(preproduction_capsule)))
    return argv


def _require_gate_pair(receipt: Path, capsule: Path) -> None:
    for path in (receipt, capsule):
        cutover._read_owned_json(path, artifact="postinstall_gate_artifact")


def _validate_initial_resident_state(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "schema_version",
        "target_runtime_root",
        "labels",
        "jobs",
    }:
        raise PostinstallActivationError("postinstall_resident_initial_state_invalid")
    labels = value.get("labels")
    jobs = value.get("jobs")
    if (
        value.get("schema_version") != live.LIVE_SERVICE_STATE_SCHEMA_VERSION
        or value.get("target_runtime_root") != str(cutover.CANONICAL_RUNTIME_ROOT)
        or labels != list(cutover.RESIDENT_LABELS)
        or not isinstance(jobs, Mapping)
        or set(jobs) != set(cutover.RESIDENT_LABELS)
    ):
        raise PostinstallActivationError("postinstall_resident_initial_state_invalid")
    for label in cutover.RESIDENT_LABELS:
        entry = jobs.get(label)
        if not isinstance(entry, Mapping) or set(entry) != {"launchd", "plist"}:
            raise PostinstallActivationError(
                "postinstall_resident_initial_state_invalid"
            )
        launchd = entry.get("launchd")
        plist = entry.get("plist")
        if (
            not isinstance(launchd, Mapping)
            or launchd.get("label") != label
            or launchd.get("loaded") is not False
            or not isinstance(plist, Mapping)
        ):
            raise PostinstallActivationError(
                "postinstall_resident_initial_state_invalid"
            )
    return value


def run_postinstall_activation(
    inputs: PostinstallInputs,
    *,
    authorization_decision: str,
    operator: str,
    reason: str,
    runner: adapter.CommandRunner | None = None,
    service_controller: live.LaunchdServiceController | None = None,
) -> Mapping[str, Any]:
    if authorization_decision != AUTHORIZATION_DECISION:
        raise PostinstallActivationError("postinstall_authorization_decision_invalid")
    if not operator.strip() or not reason.strip():
        raise PostinstallActivationError("postinstall_audit_invalid")
    active_runner = runner or adapter.SubprocessArgvRunner()
    services = service_controller or live.LaunchdServiceController(
        evidence_root=inputs.evidence_dir / "postinstall-residents",
        runner=active_runner,
    )
    common_audit = ["--operator", operator.strip(), "--reason", reason.strip()]
    with _session_lock(inputs.lock_path):
        index = 1
        _run_step(
            inputs=inputs,
            runner=active_runner,
            index=index,
            name="release-gate-preauthorization",
            argv=_gate_argv(
                inputs,
                mode="preauthorization",
                receipt=inputs.preauthorization_receipt,
            ),
        )
        index += 1
        _require_gate_pair(
            inputs.preauthorization_receipt,
            inputs.preauthorization_capsule,
        )
        created, index = _run_activation_pair(
            inputs=inputs,
            runner=active_runner,
            index=index,
            name="activation-create",
            command="create",
            arguments=[
                "--preauthorization-capsule",
                str(inputs.preauthorization_capsule),
                *common_audit,
            ],
        )
        current_epoch = created["result"].get("current_epoch")
        epoch_id = (
            str(current_epoch.get("epoch_id") or "")
            if isinstance(current_epoch, Mapping)
            else ""
        )
        if activation._EPOCH_ID_RE.fullmatch(epoch_id) is None:
            raise PostinstallActivationError("postinstall_epoch_id_invalid")
        _run_step(
            inputs=inputs,
            runner=active_runner,
            index=index,
            name="release-gate-preproduction",
            argv=_gate_argv(
                inputs,
                mode="preproduction",
                receipt=inputs.preproduction_receipt,
                preauthorization_capsule=inputs.preauthorization_capsule,
            ),
        )
        index += 1
        _require_gate_pair(inputs.preproduction_receipt, inputs.preproduction_capsule)
        _preauthorized, index = _run_activation_pair(
            inputs=inputs,
            runner=active_runner,
            index=index,
            name="activation-preauthorized",
            command="transition-preauthorized",
            arguments=[
                "--preproduction-capsule",
                str(inputs.preproduction_capsule),
                *common_audit,
            ],
        )
        identities = (
            (
                "kafka-success",
                "kafka_success",
                ["--event-uid", inputs.kafka_event_uid],
            ),
            (
                "manual-success",
                "manual_success",
                ["--manual-identity-json", str(inputs.manual_success_identity)],
            ),
            (
                "manual-terminal-failure",
                "manual_terminal_failure",
                [
                    "--manual-identity-json",
                    str(inputs.manual_terminal_failure_identity),
                ],
            ),
        )
        for name, slot, identity_args in identities:
            _authorized, index = _run_activation_pair(
                inputs=inputs,
                runner=active_runner,
                index=index,
                name=f"authorize-{name}",
                command="authorize",
                arguments=[
                    "--epoch-id",
                    epoch_id,
                    "--slot-kind",
                    slot,
                    "--preproduction-capsule",
                    str(inputs.preproduction_capsule),
                    *identity_args,
                    *common_audit,
                ],
            )
        _bounded, index = _run_activation_pair(
            inputs=inputs,
            runner=active_runner,
            index=index,
            name="activation-bounded",
            command="transition-bounded",
            arguments=[
                "--epoch-id",
                epoch_id,
                "--preproduction-capsule",
                str(inputs.preproduction_capsule),
                *common_audit,
            ],
        )
        bootstrap, index = _run_activation_pair(
            inputs=inputs,
            runner=active_runner,
            index=index,
            name="bootstrap-producer",
            command="prepare-bootstrap-production",
            arguments=[
                "--epoch-id",
                epoch_id,
                "--preproduction-capsule",
                str(inputs.preproduction_capsule),
                "--active-release-binding",
                str(inputs.active_release_binding),
                "--live-env",
                str(inputs.live_env),
                "--release-id",
                inputs.release_id,
                "--bootstrap-epoch-id",
                inputs.bootstrap_epoch_id,
                *common_audit,
            ],
        )
        bootstrap_result = bootstrap["result"]
        if (
            bootstrap_result.get("producer_receipt_present") is not True
            or not bootstrap_result.get("producer_activation_receipt_sha256")
            or bootstrap_result.get("runtime_effective_state")
            not in {"BOOTSTRAP_PRODUCTION", "STEADY_READY"}
        ):
            raise PostinstallActivationError("postinstall_bootstrap_producer_invalid")
        intent_path = inputs.journal_root / "resident-start.intent.json"
        done_path = inputs.journal_root / "resident-start.done.json"
        if intent_path.exists() or intent_path.is_symlink():
            intent = cutover._read_owned_json(
                intent_path, artifact="postinstall_resident_start_intent"
            ).body
            initial_state = _validate_initial_resident_state(
                intent.get("initial_state")
            )
            if (
                intent.get("schema_version") != RESIDENT_INTENT_SCHEMA_VERSION
                or intent.get("epoch_id") != epoch_id
                or intent.get("runtime_content_sha256")
                != inputs.runtime_content_sha256
            ):
                raise PostinstallActivationError("postinstall_resident_intent_invalid")
        else:
            initial_state = _validate_initial_resident_state(
                services.capture_state(cutover.RESIDENT_LABELS)
            )
            intent = {
                "schema_version": RESIDENT_INTENT_SCHEMA_VERSION,
                "epoch_id": epoch_id,
                "runtime_content_sha256": inputs.runtime_content_sha256,
                "initial_state": initial_state,
            }
            cutover._publish_no_clobber(intent_path, intent)
        if done_path.exists() or done_path.is_symlink():
            done = cutover._read_owned_json(
                done_path, artifact="postinstall_resident_start_done"
            ).body
            try:
                health = services.verify(
                    cutover.RESIDENT_LABELS,
                    runtime_sha256=inputs.runtime_content_sha256,
                )
                if (
                    done.get("schema_version") != RESIDENT_DONE_SCHEMA_VERSION
                    or done.get("ok") is not True
                    or done.get("epoch_id") != epoch_id
                    or done.get("activated_labels") != list(cutover.RESIDENT_LABELS)
                    or done.get("resident_health") != health
                    or done.get("runtime_content_sha256")
                    != inputs.runtime_content_sha256
                    or done.get("bootstrap_producer") != bootstrap_result
                ):
                    raise PostinstallActivationError(
                        "postinstall_resident_done_invalid"
                    )
            except Exception:
                services.restore_state(initial_state)
                raise
        else:
            try:
                services.start_residents(cutover.RESIDENT_LABELS)
                health = services.verify(
                    cutover.RESIDENT_LABELS,
                    runtime_sha256=inputs.runtime_content_sha256,
                )
                done = {
                    "schema_version": RESIDENT_DONE_SCHEMA_VERSION,
                    "ok": True,
                    "epoch_id": epoch_id,
                    "activated_labels": list(cutover.RESIDENT_LABELS),
                    "resident_health": health,
                    "runtime_content_sha256": inputs.runtime_content_sha256,
                    "bootstrap_producer": bootstrap_result,
                }
                cutover._publish_no_clobber(done_path, done)
            except Exception:
                services.restore_state(initial_state)
                raise
        step_receipts = {
            path.name: cutover._read_owned_json(
                path, artifact="postinstall_step_receipt"
            ).sha256
            for path in sorted(inputs.journal_root.glob("[0-9][0-9]-*.json"))
        }
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "ok": True,
            "authorization_decision": authorization_decision,
            "operator": operator.strip(),
            "reason": reason.strip(),
            "release_id": inputs.release_id,
            "bootstrap_epoch_id": inputs.bootstrap_epoch_id,
            "activation_epoch_id": epoch_id,
            "activation_state": "bounded_active",
            "producer_receipt_present": True,
            "resident_health": health,
            "step_receipts": step_receipts,
            "real_canaries_completed": False,
            "next_phase": "execute_exact_kafka_and_manual_canaries",
        }
        cutover._publish_no_clobber(inputs.receipt_path, receipt)
        return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("schema")
    validate = commands.add_parser("validate-manifest")
    validate.add_argument("--manifest", type=Path, required=True)
    apply = commands.add_parser("apply")
    apply.add_argument("--manifest", type=Path, required=True)
    apply.add_argument("--authorization-decision", required=True)
    apply.add_argument("--operator", required=True)
    apply.add_argument("--reason", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "schema":
            result = {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "cli_apply_supported": True,
                "authorization_decision": AUTHORIZATION_DECISION,
                "real_canaries_completed_by_this_phase": False,
                "next_phase": "execute_exact_kafka_and_manual_canaries",
            }
        else:
            inputs = load_manifest(args.manifest)
            if args.command == "validate-manifest":
                result = {
                    "schema_version": MANIFEST_SCHEMA_VERSION,
                    "ok": True,
                    "release_id": inputs.release_id,
                    "production_effects_executed": False,
                }
            else:
                result = run_postinstall_activation(
                    inputs,
                    authorization_decision=args.authorization_decision,
                    operator=args.operator,
                    reason=args.reason,
                )
    except (OSError, ValueError) as exc:
        code = getattr(exc, "code", "postinstall_activation_failed")
        print(json.dumps({"ok": False, "code": code}, sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
