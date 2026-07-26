#!/usr/bin/env python3
"""Check Hermes context-budget guardrails without mutating runtime state."""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:  # pragma: no cover - exercised by CLI environment
    yaml = None  # type: ignore[assignment]


LEAN_SKILL_MODES = {"minimal", "lean", "tool"}
SAFE_SKILL_MODES = LEAN_SKILL_MODES | {"compact", "summary"}

SOURCE_MARKERS: dict[str, tuple[str, ...]] = {
    "agent/turn_context.py": (
        "def fallback_safe_preflight_threshold(",
        'reason = f"fallback-safe:{provider}/{model}"',
        "def _compression_made_progress(",
        "new_tokens < orig_tokens * 0.95",
    ),
    "agent/conversation_loop.py": (
        "def _restore_or_build_system_prompt(",
        "legacy_full_skills_prompt",
        "rebuilding_stored_prompt",
    ),
    "agent/prompt_builder.py": (
        "## Skills (mandatory)",
        "system_prompt_mode",
        "progressive disclosure",
    ),
    "tools/budget_config.py": (
        "skill_view",
        "session_search",
        "search_files",
        "terminal",
    ),
    "tools/tool_result_storage.py": (
        "build_historical_tool_compaction_message",
        "stdin_data=content",
    ),
}


@dataclass
class ContextBudgetReport:
    ok: bool
    issues: list[str]
    warnings: list[str]
    config_path: str
    source_root: str | None
    model_provider: str
    model: str
    model_context_length: int
    compression_enabled: bool
    compression_threshold: float
    compression_target_ratio: float
    skills_system_prompt_mode: str
    fallback_contexts: list[dict[str, Any]]
    effective_preflight_threshold_tokens: int | None
    synthetic_probe_tokens: int | None
    synthetic_would_preflight_compress: bool | None
    synthetic_fallback_safe_reason: str | None
    custom_providers_type: str
    source_markers_ok: bool | None = None
    recent_legacy_full_skills_sessions: int | None = None
    recent_large_tool_rows: int | None = None


def _load_yaml(path: Path) -> dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML is not available")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _fallback_entries(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    raw = cfg.get("fallback_providers") or []
    if isinstance(raw, dict):
        raw = [raw]
    return (
        [item for item in raw if isinstance(item, dict)]
        if isinstance(raw, list)
        else []
    )


def _check_source_markers(repo_root: Path) -> tuple[bool, list[str]]:
    """Verify the extracted context-budget implementation surfaces."""

    missing: list[str] = []
    for relative, markers in SOURCE_MARKERS.items():
        path = repo_root / relative
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            missing.append(f"{relative}:unreadable")
            continue
        for marker in markers:
            if marker not in text:
                missing.append(f"{relative}:{marker}")
    return not missing, missing


def _scan_recent_db(db_path: Path, recent_limit: int) -> tuple[int, int]:
    uri = f"{db_path.expanduser().resolve().as_uri()}?mode=ro"
    con = sqlite3.connect(uri, uri=True, isolation_level=None)
    try:
        con.execute("PRAGMA query_only = ON")
        legacy = con.execute(
            """
            select count(*)
            from (
                select id, system_prompt
                from sessions
                order by started_at desc
                limit ?
            )
            where coalesce(system_prompt, '')
                like '%## Skills (mandatory)%<available_skills>%'
            """,
            (recent_limit,),
        ).fetchone()[0]
        large_tool_rows = con.execute(
            """
            with recent as (
                select id from sessions order by started_at desc limit ?
            )
            select count(*)
            from messages m join recent r on r.id = m.session_id
            where m.role = 'tool' and length(coalesce(m.content, '')) > 20000
            """,
            (recent_limit,),
        ).fetchone()[0]
    finally:
        con.close()
    return int(legacy or 0), int(large_tool_rows or 0)


def build_report(
    *,
    config_path: Path,
    repo_root: Path | None = None,
    db_path: Path | None = None,
    recent_limit: int = 300,
    fail_on_historical_bloat: bool = False,
) -> ContextBudgetReport:
    cfg = _load_yaml(config_path)
    issues: list[str] = []
    warnings: list[str] = []

    model_cfg = cfg.get("model") if isinstance(cfg.get("model"), dict) else {}
    model_provider = str(model_cfg.get("provider") or "")
    model = str(model_cfg.get("default") or model_cfg.get("model") or "")
    model_context = _as_int(model_cfg.get("context_length"), 0)
    if not model_provider or not model:
        issues.append("model.provider/model.default missing")
    if model_context <= 0:
        issues.append("model.context_length missing or invalid")

    compression = (
        cfg.get("compression") if isinstance(cfg.get("compression"), dict) else {}
    )
    compression_enabled = bool(compression.get("enabled", False))
    threshold = _as_float(compression.get("threshold"), 0.0)
    target_ratio = _as_float(compression.get("target_ratio"), 0.0)
    if not compression_enabled:
        issues.append("compression.enabled must be true")
    if threshold <= 0 or threshold > 0.5:
        issues.append(
            "compression.threshold should be <=0.5 for fallback-safe "
            f"preflight, got {threshold}"
        )
    if target_ratio <= 0 or target_ratio > 0.25:
        warnings.append(f"compression.target_ratio is loose or invalid: {target_ratio}")

    skills = cfg.get("skills") if isinstance(cfg.get("skills"), dict) else {}
    skills_mode = str(skills.get("system_prompt_mode") or "full").strip().lower()
    if skills_mode not in SAFE_SKILL_MODES:
        issues.append(
            "skills.system_prompt_mode should be "
            f"minimal/lean/tool/compact/summary, got {skills_mode!r}"
        )
    elif skills_mode not in LEAN_SKILL_MODES:
        warnings.append(
            f"skills.system_prompt_mode={skills_mode!r} is safer than full "
            "but still carries an index"
        )

    custom_providers_type = type(cfg.get("custom_providers")).__name__
    if custom_providers_type != "list":
        issues.append(f"custom_providers must be a list, got {custom_providers_type}")

    fallback_contexts: list[dict[str, Any]] = []
    fallback_entries = _fallback_entries(cfg)
    if not fallback_entries:
        issues.append("fallback_providers must contain at least one explicit fallback")
    for item in fallback_entries:
        context_length = _as_int(item.get("context_length"), 0)
        max_tokens = _as_int(item.get("max_tokens"), 0)
        fallback_contexts.append(
            {
                "provider": item.get("provider") or "",
                "model": item.get("model") or "",
                "context_length": context_length,
                "max_tokens": max_tokens,
                "api_mode": item.get("api_mode") or "",
            }
        )
        if context_length <= 0:
            issues.append(
                f"fallback {item.get('provider')}/{item.get('model')} "
                "missing context_length"
            )
        if max_tokens <= 0:
            warnings.append(
                f"fallback {item.get('provider')}/{item.get('model')} "
                "missing max_tokens"
            )

    thresholds = []
    if model_context > 0 and threshold > 0:
        thresholds.append(int(model_context * threshold))
    for fallback in fallback_contexts:
        if fallback["context_length"] > 0 and threshold > 0:
            thresholds.append(int(fallback["context_length"] * threshold))
    effective = min(thresholds) if thresholds else None
    synthetic_probe_tokens: int | None = None
    synthetic_would_preflight_compress: bool | None = None
    synthetic_fallback_safe_reason: str | None = None
    if effective is not None and fallback_contexts:
        smallest_context = min(
            (
                fallback["context_length"]
                for fallback in fallback_contexts
                if fallback["context_length"] > 0
            ),
            default=0,
        )
        if smallest_context and effective > int(smallest_context * 0.5):
            issues.append(
                f"effective preflight threshold {effective} exceeds 50% of "
                f"smallest fallback context {smallest_context}"
            )
        synthetic_probe_tokens = effective + 1
        synthetic_would_preflight_compress = synthetic_probe_tokens >= effective
        min_fallback = min(
            (
                fallback
                for fallback in fallback_contexts
                if fallback["context_length"] > 0
            ),
            key=lambda fallback: fallback["context_length"],
            default=None,
        )
        if (
            min_fallback
            and effective == int(min_fallback["context_length"] * threshold)
        ):
            synthetic_fallback_safe_reason = (
                f"fallback-safe:{min_fallback['provider']}/{min_fallback['model']}"
            )
        else:
            synthetic_fallback_safe_reason = "primary"
        if not synthetic_would_preflight_compress or not str(
            synthetic_fallback_safe_reason
        ).startswith("fallback-safe:"):
            issues.append(
                "synthetic fallback-safe preflight probe did not select "
                "fallback-safe compression"
            )

    source_root: str | None = None
    source_markers_ok: bool | None = None
    if repo_root is not None:
        source_root = str(repo_root.expanduser().resolve())
        source_markers_ok, missing = _check_source_markers(Path(source_root))
        if missing:
            issues.extend(f"source marker missing: {item}" for item in missing)

    recent_legacy: int | None = None
    recent_large_tool_rows: int | None = None
    if db_path is not None and db_path.expanduser().exists():
        recent_legacy, recent_large_tool_rows = _scan_recent_db(
            db_path, recent_limit
        )
        if recent_legacy:
            message = (
                f"{recent_legacy} recent session(s) still carry legacy full "
                "skills prompt"
            )
            (issues if fail_on_historical_bloat else warnings).append(message)
        if recent_large_tool_rows:
            message = (
                f"{recent_large_tool_rows} recent large historical tool row(s) "
                ">20k chars"
            )
            (issues if fail_on_historical_bloat else warnings).append(message)

    return ContextBudgetReport(
        ok=not issues,
        issues=issues,
        warnings=warnings,
        config_path=str(config_path),
        source_root=source_root,
        model_provider=model_provider,
        model=model,
        model_context_length=model_context,
        compression_enabled=compression_enabled,
        compression_threshold=threshold,
        compression_target_ratio=target_ratio,
        skills_system_prompt_mode=skills_mode,
        fallback_contexts=fallback_contexts,
        effective_preflight_threshold_tokens=effective,
        synthetic_probe_tokens=synthetic_probe_tokens,
        synthetic_would_preflight_compress=synthetic_would_preflight_compress,
        synthetic_fallback_safe_reason=synthetic_fallback_safe_reason,
        custom_providers_type=custom_providers_type,
        source_markers_ok=source_markers_ok,
        recent_legacy_full_skills_sessions=recent_legacy,
        recent_large_tool_rows=recent_large_tool_rows,
    )


def main() -> int:
    home = Path.home()
    parser = argparse.ArgumentParser(description="Check Hermes context-budget guardrails")
    parser.add_argument("--config", type=Path, default=home / ".hermes" / "config.yaml")
    parser.add_argument("--repo-root", type=Path, default=None)
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--recent-limit", type=int, default=300)
    parser.add_argument("--fail-on-historical-bloat", action="store_true")
    parser.add_argument("--json", action="store_true", default=True)
    args = parser.parse_args()

    report = build_report(
        config_path=args.config,
        repo_root=args.repo_root,
        db_path=args.db,
        recent_limit=args.recent_limit,
        fail_on_historical_bloat=args.fail_on_historical_bloat,
    )
    print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
    return 0 if report.ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
