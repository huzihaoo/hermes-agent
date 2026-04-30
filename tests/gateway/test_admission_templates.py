"""Tests for admission policy templates — P2-6.

Covers:
- PolicyTemplate: data model for reusable admission configs
- TemplateStore: CRUD + list + import/export
- Built-in templates: strict / relaxed / vip-priority
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from gateway.admission.templates import (
    PolicyTemplate,
    TemplateStore,
    builtin_templates,
)


# ── PolicyTemplate ───────────────────────────────────────────────


class TestPolicyTemplate:
    def test_create_template(self):
        t = PolicyTemplate(
            name="strict",
            description="Strict rate limiting",
            rate_limit_per_user=5,
            rate_limit_window_seconds=60,
            depth_warning=5,
            depth_critical=20,
            error_rate_threshold=0.1,
        )
        assert t.name == "strict"
        assert t.rate_limit_per_user == 5

    def test_to_dict_roundtrip(self):
        t = PolicyTemplate(
            name="test",
            description="Test template",
            rate_limit_per_user=10,
            rate_limit_window_seconds=30,
            depth_warning=8,
            depth_critical=40,
            error_rate_threshold=0.2,
        )
        d = t.to_dict()
        t2 = PolicyTemplate.from_dict(d)
        assert t2.name == t.name
        assert t2.rate_limit_per_user == t.rate_limit_per_user
        assert t2.depth_critical == t.depth_critical

    def test_from_dict_missing_optional_uses_defaults(self):
        d = {"name": "minimal", "description": "bare minimum"}
        t = PolicyTemplate.from_dict(d)
        assert t.rate_limit_per_user == 20  # default
        assert t.depth_warning == 10  # default


# ── TemplateStore ────────────────────────────────────────────────


class TestTemplateStore:
    def _make_store(self, tmpdir: str) -> TemplateStore:
        return TemplateStore(store_dir=Path(tmpdir))

    def test_save_and_get(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(tmpdir)
            t = PolicyTemplate(name="my-template", description="custom")
            store.save(t)
            loaded = store.get("my-template")
            assert loaded is not None
            assert loaded.name == "my-template"

    def test_list_templates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(tmpdir)
            store.save(PolicyTemplate(name="a", description="A"))
            store.save(PolicyTemplate(name="b", description="B"))
            names = store.list_names()
            assert "a" in names
            assert "b" in names

    def test_delete_template(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(tmpdir)
            store.save(PolicyTemplate(name="del-me", description="x"))
            assert store.delete("del-me") is True
            assert store.get("del-me") is None

    def test_delete_nonexistent_returns_false(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(tmpdir)
            assert store.delete("nope") is False

    def test_export_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(tmpdir)
            t = PolicyTemplate(
                name="export-test",
                description="for export",
                rate_limit_per_user=3,
            )
            store.save(t)

            export_path = Path(tmpdir) / "exported.json"
            store.export_template("export-test", export_path)
            assert export_path.exists()

            store2 = self._make_store(Path(tmpdir) / "store2")
            store2.import_template(export_path)
            loaded = store2.get("export-test")
            assert loaded is not None
            assert loaded.rate_limit_per_user == 3

    def test_overwrite_existing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(tmpdir)
            store.save(PolicyTemplate(name="x", description="v1", rate_limit_per_user=5))
            store.save(PolicyTemplate(name="x", description="v2", rate_limit_per_user=99))
            loaded = store.get("x")
            assert loaded.description == "v2"
            assert loaded.rate_limit_per_user == 99

    def test_seed_builtins_persists_templates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(tmpdir)
            store.seed_builtins()
            names = store.list_names()
            assert "strict" in names
            assert "relaxed" in names
            assert "vip-priority" in names
            assert "pnc-shared-vm" in names
            pnc = store.get("pnc-shared-vm")
            assert pnc is not None
            assert pnc.rate_limit_per_user == 12
            assert pnc.depth_warning == 6
            assert pnc.depth_critical == 20


# ── Built-in templates ───────────────────────────────────────────


class TestBuiltinTemplates:
    def test_builtins_exist(self):
        names = [t.name for t in builtin_templates()]
        assert "strict" in names
        assert "relaxed" in names
        assert "vip-priority" in names
        assert "pnc-shared-vm" in names

    def test_pnc_shared_vm_builtin_values(self):
        templates = {t.name: t for t in builtin_templates()}
        pnc = templates["pnc-shared-vm"]
        assert pnc.description == "PNC shared Feishu -> Hermes -> VM shared-state control-plane policy"
        assert pnc.rate_limit_per_user == 12
        assert pnc.rate_limit_window_seconds == 60
        assert pnc.depth_warning == 6
        assert pnc.depth_critical == 20
        assert pnc.error_rate_threshold == 0.15
        assert pnc.error_rate_critical == 0.35
        assert pnc.alert_cooldown_seconds == 180

    def test_builtins_are_valid(self):
        for t in builtin_templates():
            assert t.name
            assert t.description
            assert t.rate_limit_per_user > 0
            assert t.depth_warning > 0
            assert t.depth_critical > t.depth_warning
