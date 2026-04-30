"""Reusable admission policy templates.

A PolicyTemplate captures a named set of admission parameters
(rate limits, queue depth thresholds, error rate thresholds)
that can be saved, shared, imported, and applied to a controller.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class PolicyTemplate:
    """A reusable admission policy configuration."""

    name: str
    description: str = ""
    rate_limit_per_user: int = 20
    rate_limit_window_seconds: int = 60
    depth_warning: int = 10
    depth_critical: int = 50
    error_rate_threshold: float = 0.2
    error_rate_critical: float = 0.5
    alert_cooldown_seconds: float = 300

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> PolicyTemplate:
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in known}
        return cls(**filtered)


class TemplateStore:
    """Persist and retrieve PolicyTemplates as JSON files."""

    def __init__(self, store_dir: Path | None = None):
        self._dir = store_dir or (Path.home() / ".hermes" / "admission" / "templates")
        self._dir.mkdir(parents=True, exist_ok=True)

    def _path(self, name: str) -> Path:
        safe = name.replace("/", "_").replace("\\", "_")
        return self._dir / f"{safe}.json"

    def save(self, template: PolicyTemplate) -> None:
        self._path(template.name).write_text(
            json.dumps(template.to_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def get(self, name: str) -> PolicyTemplate | None:
        p = self._path(name)
        if not p.exists():
            return None
        data = json.loads(p.read_text(encoding="utf-8"))
        return PolicyTemplate.from_dict(data)

    def list_names(self) -> list[str]:
        return sorted(
            p.stem for p in self._dir.glob("*.json")
        )

    def delete(self, name: str) -> bool:
        p = self._path(name)
        if not p.exists():
            return False
        p.unlink()
        return True

    def export_template(self, name: str, dest: Path) -> None:
        t = self.get(name)
        if t is None:
            raise ValueError(f"Template '{name}' not found")
        dest.write_text(
            json.dumps(t.to_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def import_template(self, src: Path) -> PolicyTemplate:
        data = json.loads(src.read_text(encoding="utf-8"))
        t = PolicyTemplate.from_dict(data)
        self.save(t)
        return t

    def seed_builtins(self) -> None:
        """Persist all built-in templates to the store."""
        for t in builtin_templates():
            self.save(t)


def builtin_templates() -> list[PolicyTemplate]:
    """Return the set of built-in policy templates."""
    return [
        PolicyTemplate(
            name="strict",
            description="Strict mode — low rate limits, tight thresholds",
            rate_limit_per_user=5,
            rate_limit_window_seconds=60,
            depth_warning=5,
            depth_critical=20,
            error_rate_threshold=0.1,
            error_rate_critical=0.3,
            alert_cooldown_seconds=120,
        ),
        PolicyTemplate(
            name="relaxed",
            description="Relaxed mode — generous limits for trusted environments",
            rate_limit_per_user=50,
            rate_limit_window_seconds=60,
            depth_warning=30,
            depth_critical=100,
            error_rate_threshold=0.3,
            error_rate_critical=0.6,
            alert_cooldown_seconds=600,
        ),
        PolicyTemplate(
            name="vip-priority",
            description="VIP priority — moderate limits, fast alerting",
            rate_limit_per_user=30,
            rate_limit_window_seconds=60,
            depth_warning=8,
            depth_critical=40,
            error_rate_threshold=0.15,
            error_rate_critical=0.4,
            alert_cooldown_seconds=180,
        ),
        PolicyTemplate(
            name="pnc-shared-vm",
            description="PNC shared Feishu -> Hermes -> VM shared-state control-plane policy",
            rate_limit_per_user=12,
            rate_limit_window_seconds=60,
            depth_warning=6,
            depth_critical=20,
            error_rate_threshold=0.15,
            error_rate_critical=0.35,
            alert_cooldown_seconds=180,
        ),
    ]
