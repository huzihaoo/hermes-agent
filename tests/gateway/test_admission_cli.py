"""Tests for admission CLI template commands."""

from __future__ import annotations

import io
import tempfile
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace

from gateway.admission.cli import cmd_template
from gateway.admission.templates import PolicyTemplate, TemplateStore


def test_cmd_template_list_outputs_names():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = TemplateStore(store_dir=Path(tmpdir))
        store.save(PolicyTemplate(name="alpha", description="A"))
        store.save(PolicyTemplate(name="beta", description="B"))

        args = SimpleNamespace(action="list", store_dir=tmpdir, name=None, path=None)
        buf = io.StringIO()
        with redirect_stdout(buf):
            cmd_template(args)
        out = buf.getvalue()
        assert "alpha" in out
        assert "beta" in out


def test_cmd_template_seed_populates_builtins():
    with tempfile.TemporaryDirectory() as tmpdir:
        args = SimpleNamespace(action="seed", store_dir=tmpdir, name=None, path=None)
        buf = io.StringIO()
        with redirect_stdout(buf):
            cmd_template(args)
        out = buf.getvalue()
        assert "Seeded" in out

        store = TemplateStore(store_dir=Path(tmpdir))
        names = store.list_names()
        assert "strict" in names
        assert "relaxed" in names
        assert "vip-priority" in names


def test_cmd_template_export_and_import():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = TemplateStore(store_dir=Path(tmpdir) / "store1")
        store.save(PolicyTemplate(name="shared", description="share me"))
        export_path = Path(tmpdir) / "shared.json"

        export_args = SimpleNamespace(
            action="export", store_dir=str(Path(tmpdir) / "store1"), name="shared", path=str(export_path)
        )
        with redirect_stdout(io.StringIO()):
            cmd_template(export_args)
        assert export_path.exists()

        import_args = SimpleNamespace(
            action="import", store_dir=str(Path(tmpdir) / "store2"), name=None, path=str(export_path)
        )
        with redirect_stdout(io.StringIO()):
            cmd_template(import_args)

        store2 = TemplateStore(store_dir=Path(tmpdir) / "store2")
        loaded = store2.get("shared")
        assert loaded is not None
        assert loaded.description == "share me"
