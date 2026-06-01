from argparse import Namespace
import contextlib
import io
import sys
import types

import pytest

from hermes_cli import doctor as doctor_mod


class TestDoctorLocalMem0SidecarSection:
    def _make_hermes_home(self, tmp_path, provider="local_mem0_sidecar"):
        home = tmp_path / ".hermes"
        home.mkdir(parents=True, exist_ok=True)
        (home / "config.yaml").write_text(f"memory:\n  provider: {provider}\n", encoding="utf-8")
        return home

    def _run_doctor_and_capture(self, monkeypatch, tmp_path, provider="local_mem0_sidecar"):
        home = self._make_hermes_home(tmp_path, provider)
        monkeypatch.setattr(doctor_mod, "HERMES_HOME", home)
        monkeypatch.setattr(doctor_mod, "PROJECT_ROOT", tmp_path / "project")
        monkeypatch.setattr(doctor_mod, "_DHH", str(home))
        (tmp_path / "project").mkdir(exist_ok=True)

        fake_model_tools = types.SimpleNamespace(
            check_tool_availability=lambda *a, **kw: ([], []),
            TOOLSET_REQUIREMENTS={},
        )
        monkeypatch.setitem(sys.modules, "model_tools", fake_model_tools)

        try:
            from hermes_cli import auth as _auth_mod
            monkeypatch.setattr(_auth_mod, "get_nous_auth_status", lambda: {})
            monkeypatch.setattr(_auth_mod, "get_codex_auth_status", lambda: {})
            monkeypatch.setattr(_auth_mod, "get_xai_oauth_auth_status", lambda: {})
        except Exception:
            pass

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            doctor_mod.run_doctor(Namespace(fix=False))
        return buf.getvalue()

    def test_local_mem0_sidecar_base_url_missing_shows_fail(self, monkeypatch, tmp_path):
        monkeypatch.delenv("LOCAL_MEM0_SIDECAR_BASE_URL", raising=False)
        out = self._run_doctor_and_capture(monkeypatch, tmp_path)
        assert "Memory Provider" in out
        assert "Local Mem0 sidecar base URL not set" in out

    def test_local_mem0_sidecar_configured_and_healthy(self, monkeypatch, tmp_path):
        monkeypatch.setenv("LOCAL_MEM0_SIDECAR_BASE_URL", "http://127.0.0.1:8765")
        monkeypatch.setenv("LOCAL_MEM0_USER_ID", "u1")
        monkeypatch.setenv("LOCAL_MEM0_AGENT_ID", "hermes")

        class FakeClient:
            def __init__(self, base_url, api_key="", timeout=5.0):
                self.base_url = base_url

            def health(self):
                return {"ok": True}

        import plugins.memory.local_mem0_sidecar as local_mod
        monkeypatch.setattr(local_mod, "_SidecarClient", FakeClient)

        out = self._run_doctor_and_capture(monkeypatch, tmp_path)
        assert "Local Mem0 sidecar configured" in out
        assert "Local Mem0 sidecar health check passed" in out
        assert "base_url=http://127.0.0.1:8765" in out

    def test_local_mem0_sidecar_warns_on_0_0_0_0_bind(self, monkeypatch, tmp_path):
        monkeypatch.setenv("LOCAL_MEM0_SIDECAR_BASE_URL", "http://0.0.0.0:8765")

        class FakeClient:
            def __init__(self, base_url, api_key="", timeout=5.0):
                self.base_url = base_url

            def health(self):
                return {"ok": True}

        import plugins.memory.local_mem0_sidecar as local_mod
        monkeypatch.setattr(local_mod, "_SidecarClient", FakeClient)

        out = self._run_doctor_and_capture(monkeypatch, tmp_path)
        assert "Local Mem0 sidecar binds to 0.0.0.0" in out
