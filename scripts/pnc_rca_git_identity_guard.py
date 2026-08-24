#!/usr/bin/env python3
"""Reject undeclared Git identity probes and identity-kind regressions."""

from __future__ import annotations

import ast
from collections import Counter
import hashlib
import json
from pathlib import Path
from typing import Mapping


REV_PARSE = "rev" + "-parse"
_SELF = "scripts/pnc_rca_git_identity_guard.py"

# These are the latest production/rca call sites audited as Git-backed before
# this guard was introduced.  Hashing the complete physical line makes a move,
# edit, or duplicate a new call that must carry an explicit declaration.
REVIEWED_LINE_SHA256: Mapping[str, tuple[str, ...]] = {
    "gateway/admission/worktree_manager.py": (
        "da3f9cf54b25368ca9f0408d3f8da5556ae2f330edd985072ae1036a8c06cfc4",
        "06adeda2d6670885965cb6d013e841b878cb8d98780a5d711f89e7a7621fa2b5",
    ),
    "gateway/pnc_rca_direct_vm_transport.py": (
        "3d9551b16e8336ec669827c616baa8d6d0b2ece069254c41bba59cbbdf3255b0",
        "96d7635497605d2831b64041dbc957f1cf2dcaaa4b6f58505f018f5757d921dd",
        "61e98261172e4eacf15dba836af2d719a80a8f0802df48ff67287234c0862d2a",
    ),
    "scripts/hermes_safe_worktree_remove.py": (
        "aaec751d8b3aa7a8d769d5765f90b455c863c4940cf3130fbb504497269f098f",
    ),
    "scripts/pnc_l4_zero_impact_harness.py": (
        "b79c9a829782f464bc9670f0251aba30dbc5309baed4e485fb283ad77fce2ee7",
        "40d372a5bbb37e596c644c2ed3b9a584a9e4deef954ce16ef0356f9d43379774",
    ),
    "scripts/pnc_live_exec.py": (
        "0aee723365922ae76b375450d6e60f485fb28355cbc63070af2f7926451c1afe",
    ),
    "scripts/pnc_rca_batch_rerun.py": (
        "ab78d4076aecbc2808e7537ccd397287aeffab95473c0e57812c0f6ac8336386",
    ),
    "scripts/pnc_rca_delivery_collector.py": (
        "23637af446990e9f5358eb1f3fd8631e5ffe9536732982571f0c55e06d168a4c",
        "b21cc6ce9738d31b0cb34fabd08d55744f272a2ce750234a61a33249fd755a3b",
        "e0d14455baaa61796598890c00ee7b267db9bb6e3866b3d65dc5499bdc28b790",
        "b21cc6ce9738d31b0cb34fabd08d55744f272a2ce750234a61a33249fd755a3b",
        "e0d14455baaa61796598890c00ee7b267db9bb6e3866b3d65dc5499bdc28b790",
    ),
    "scripts/pnc_rca_feedback_offline_replay.py": (
        "6769035f6e38d3fba7f37c1c90d50b8bee8c17a9aa1a528ad84ef40d5f428033",
    ),
    "scripts/pnc_rca_minimal_release.py": (
        "41bebba134f37be52f8fea8cc1a42a7ccc3af7b93e0d1dfea583999257d57589",
        "f7938aeb9b23259c2bd5360727887dbb8a2888fb17e989eef4e92b4565727e59",
        "c9dba43d2d6f0da6e2df070df2d74c3e13944c324af267650b4cf5a2046fb55c",
        "75478047541f85a8e48a1a3814d1fd9bd2368a8f1fc5ceda7fb3fc3e9704b580",
        "b119e78908bf0b995f421cd1c2e17ccc8866c10974a44f9c81207cc7c1159ed2",
        "08174865a4b5f1338566499c43406c19b314d280e480f145d3f1052051faffb5",
        "32af07694622510a68ed1e1ffb36c07063b9172f50bc345c27766782099f0852",
    ),
    "scripts/pnc_rca_w4_registry_self_adjudicate.py": (
        "654dc203f036b46592aed68b2670b521d7a46ad5c33c12bd4493a6aab1ffad0c",
        "633373fc5e467b293a24bc7d76db8c4b270532e6ab7a01503497cfaa98f9eeed",
        "0f6880d2176b34c54f11c7cc2b98062e83c2daa5d6450acbe2dee1f154a0e818",
        "0cc4eade7dc9b04e5df8e2cf3a8cab7da2289c9f929fdcfc0e1399e071d840c3",
    ),
    "scripts/pnc_wrapper_transaction.py": (
        "d629604707526751400f534b900d9c79bae966ef2f1fbc2ca0ce3c1c123feb44",
        "3b5fc5244a5e2823437ab25c72078c5ad7108eb90685e1f56ea7ff17147609f0",
    ),
    "scripts/rca_issue_workload_export.py": (
        "3b84dfc5fc96ffe72ef64d306542e05f4fb258966891bf3478949ea6026abe6b",
    ),
}

# Location is part of the approval. Moving an otherwise identical probe is a
# new call site and must be reviewed together with its target declaration.
REVIEWED_LINE_NUMBERS: Mapping[str, tuple[int, ...]] = {
    "gateway/admission/worktree_manager.py": (61, 558),
    "gateway/pnc_rca_direct_vm_transport.py": (429, 430, 433),
    "scripts/hermes_safe_worktree_remove.py": (189,),
    "scripts/pnc_l4_zero_impact_harness.py": (1660, 1661),
    "scripts/pnc_live_exec.py": (243,),
    "scripts/pnc_rca_batch_rerun.py": (403,),
    "scripts/pnc_rca_delivery_collector.py": (1249, 1259, 1268, 1283, 1292),
    "scripts/pnc_rca_feedback_offline_replay.py": (406,),
    "scripts/pnc_rca_minimal_release.py": (759, 832, 968, 969, 970, 3851, 3871),
    "scripts/pnc_rca_w4_registry_self_adjudicate.py": (323, 324, 982, 983),
    "scripts/pnc_wrapper_transaction.py": (133, 140),
    "scripts/rca_issue_workload_export.py": (461,),
}

# These three calls live inside minimal_release's generated VM probe. They are
# reviewed legacy remote probes outside S1's collector/direct eight-call scope;
# this allowlist must not be reported as Host-local Git identity coverage.
REVIEWED_LEGACY_REMOTE_PROBE_LINES: Mapping[str, tuple[int, ...]] = {
    "scripts/pnc_rca_minimal_release.py": (968, 969, 970),
}

# Filled with AST call fingerprints for reviewed command aliases whose call
# site does not itself contain the literal token. This remains independent of
# the physical-line allowlist so string construction cannot bypass review.
REVIEWED_FOLDED_CALL_SHA256: Mapping[str, tuple[str, ...]] = {}


def _line_sha256(line: str) -> str:
    return hashlib.sha256(line.encode("utf-8")).hexdigest()


def _call_sha256(node: ast.Call) -> str:
    return hashlib.sha256(
        ast.dump(node, annotate_fields=True, include_attributes=False).encode(
            "utf-8"
        )
    ).hexdigest()


def _constant_string(node: ast.AST, bindings: Mapping[str, str]) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return bindings.get(node.id)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _constant_string(node.left, bindings)
        right = _constant_string(node.right, bindings)
        if left is not None and right is not None:
            return left + right
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.FormattedValue):
                parts.append("{value}")
                continue
            part = _constant_string(value, bindings)
            if part is None:
                return None
            parts.append(part)
        return "".join(parts)
    return None


def _string_bindings(tree: ast.AST) -> dict[str, str]:
    bindings: dict[str, str] = {}
    ambiguous: set[str] = set()
    assignments: list[tuple[str, ast.AST]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            value = node.value
            targets = node.targets if isinstance(node, ast.Assign) else (node.target,)
            for target in targets:
                if isinstance(target, ast.Name) and value is not None:
                    assignments.append((target.id, value))
    for _pass in range(len(assignments) + 1):
        changed = False
        for name, value_node in assignments:
            value = _constant_string(value_node, bindings)
            if value is None or name in ambiguous:
                continue
            previous = bindings.get(name)
            if previous is not None and previous != value:
                bindings.pop(name, None)
                ambiguous.add(name)
                changed = True
            elif previous is None:
                bindings[name] = value
                changed = True
        if not changed:
            break
    return bindings


def _value_bindings(tree: ast.AST) -> dict[str, ast.AST]:
    bindings: dict[str, ast.AST] = {}
    ambiguous: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)) or node.value is None:
            continue
        targets = node.targets if isinstance(node, ast.Assign) else (node.target,)
        for target in targets:
            if not isinstance(target, ast.Name) or target.id in ambiguous:
                continue
            previous = bindings.get(target.id)
            if previous is not None and ast.dump(previous) != ast.dump(node.value):
                bindings.pop(target.id, None)
                ambiguous.add(target.id)
            else:
                bindings[target.id] = node.value
    return bindings


def _resolved_strings(
    node: ast.AST,
    bindings: Mapping[str, str],
    value_bindings: Mapping[str, ast.AST],
    seen: frozenset[str] = frozenset(),
) -> list[str]:
    if isinstance(node, ast.Call):
        return []
    direct = _constant_string(node, bindings)
    if direct is not None:
        return [direct]
    if (
        isinstance(node, ast.Name)
        and node.id in value_bindings
        and node.id not in seen
    ):
        return _resolved_strings(
            value_bindings[node.id],
            bindings,
            value_bindings,
            seen | {node.id},
        )
    resolved: list[str] = []
    for child in ast.iter_child_nodes(node):
        resolved.extend(
            _resolved_strings(child, bindings, value_bindings, seen)
        )
    return resolved


def _folded_probe_findings(relative: str, source: str) -> list[str]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return [f"{relative}:1:git_identity_guard_parse_failed"]
    bindings = _string_bindings(tree)
    value_bindings = _value_bindings(tree)
    allowance = Counter(REVIEWED_FOLDED_CALL_SHA256.get(relative, ()))
    findings: list[str] = []
    observed: set[tuple[int, str]] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        values = []
        for argument in node.args:
            values.extend(_resolved_strings(argument, bindings, value_bindings))
        for keyword in node.keywords:
            values.extend(
                _resolved_strings(keyword.value, bindings, value_bindings)
            )
        if not any(REV_PARSE in value for value in values):
            continue
        segment = ast.get_source_segment(source, node) or ""
        if REV_PARSE in segment:
            # Literal occurrences are checked against the exact reviewed-line
            # allowlist below. This branch handles constant folding and aliases.
            continue
        digest = _call_sha256(node)
        marker = (getattr(node, "lineno", 1), digest)
        if marker in observed:
            continue
        observed.add(marker)
        if allowance[digest] > 0:
            allowance[digest] -= 1
            continue
        findings.append(
            f"{relative}:{getattr(node, 'lineno', 1)}:undeclared_git_identity_probe"
        )
    return findings


def bare_probe_findings(repo_root: Path) -> list[str]:
    reviewed = {
        path: Counter(zip(REVIEWED_LINE_NUMBERS.get(path, ()), digests))
        for path, digests in REVIEWED_LINE_SHA256.items()
    }
    findings: list[str] = []
    for directory in ("scripts", "gateway"):
        for path in sorted((repo_root / directory).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            relative = path.relative_to(repo_root).as_posix()
            if relative == _SELF:
                continue
            source = path.read_text(encoding="utf-8")
            lines = source.splitlines()
            allowance = reviewed.get(relative, Counter())
            for index, line in enumerate(lines):
                occurrences = line.count(REV_PARSE)
                for _occurrence in range(occurrences):
                    digest = _line_sha256(line)
                    reviewed_call = (index + 1, digest)
                    if allowance[reviewed_call] > 0:
                        allowance[reviewed_call] -= 1
                        continue
                    findings.append(
                        f"{relative}:{index + 1}:undeclared_git_identity_probe"
                    )
            findings.extend(_folded_probe_findings(relative, source))
    return findings


def remote_contract_findings(repo_root: Path) -> list[str]:
    findings: list[str] = []
    collector = (repo_root / "scripts/pnc_rca_delivery_collector.py").read_text()
    transport = (repo_root / "gateway/pnc_rca_direct_vm_transport.py").read_text()

    if "git_marker =" in collector or "posixpath.join(repo_root, '.git')" in collector:
        findings.append("collector_pipeline_identity_heuristic_present")
    for required in (
        "if identity_kind == IDENTITY_KIND_GIT_WORKTREE:",
        "if identity_kind == IDENTITY_KIND_SEALED_MATERIALIZED:",
        "g1q3_rca_vm_source_materialization_v1",
        "g1q3_rca_vm_worker_binding_v1",
        "g1q3_rca_service_provenance_v2",
    ):
        if required not in collector:
            findings.append("collector_identity_contract_missing")
            break

    unknown_guard = transport.find("direct_vm_humanizer_identity_kind_unknown")
    unsupported_guard = transport.find("direct_vm_humanizer_identity_kind_unsupported")
    first_probe = transport.find(REV_PARSE)
    if (
        "remote_humanizer_identity_kind: str = IDENTITY_KIND_GIT_WORKTREE"
        not in transport
        or min(unknown_guard, unsupported_guard, first_probe) < 0
        or not (unknown_guard < first_probe and unsupported_guard < first_probe)
    ):
        findings.append("direct_transport_identity_contract_missing")
    return findings


def audit_repository(repo_root: Path) -> list[str]:
    return bare_probe_findings(repo_root) + remote_contract_findings(repo_root)


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    findings = audit_repository(repo_root)
    print(
        json.dumps(
            {"ok": not findings, "findings": findings},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
