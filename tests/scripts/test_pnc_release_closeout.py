from __future__ import annotations

import json

from scripts.pnc_release_closeout import run_closeout


def test_release_closeout_stops_on_failed_version_gate(monkeypatch):
    from scripts import pnc_release_closeout as mod

    class Fake:
        def __init__(self, rc=0, out="", err=""):
            self.returncode = rc
            self.stdout = out
            self.stderr = err

    calls = []

    def fake_run(cmd, cwd=None, text=None, capture_output=None):
        calls.append(cmd)
        if "check_versions.py" in cmd:
            return Fake(1, "", "boom")
        return Fake(0, "ok", "")

    monkeypatch.setattr(mod.subprocess, "run", fake_run)
    result = run_closeout("0.13.10")
    assert result["ok"] is False
    assert len(result["steps"]) == 1
    assert result["steps"][0]["name"] == "version_check"


def test_release_closeout_passes_when_all_gates_pass(monkeypatch):
    from scripts import pnc_release_closeout as mod

    class Fake:
        def __init__(self, rc=0, out="", err=""):
            self.returncode = rc
            self.stdout = out
            self.stderr = err

    def fake_run(cmd, cwd=None, text=None, capture_output=None):
        text_cmd = " ".join(cmd)
        if "pnc_release_html_gate.py" in text_cmd:
            return Fake(0, json.dumps({"ok": True, "version": "0.13.10"}), "")
        if "pnc_release_browser_gate.py" in text_cmd:
            return Fake(0, json.dumps({"ok": True, "version": "0.13.10"}), "")
        return Fake(0, "ok", "")

    monkeypatch.setattr(mod.subprocess, "run", fake_run)
    result = run_closeout("0.13.10")
    assert result["ok"] is True
    assert [s["name"] for s in result["steps"]] == [
        "version_check",
        "release_targeted_tests",
        "release_py_compile",
        "vm_html_publish_gate",
        "browser_interaction_gate",
        "feishu_release_target_gate",
    ]
