import sys
import types
from types import SimpleNamespace

from hermes_cli.main import cmd_version
from hermes_cli.version import format_version_report, iter_module_versions


def test_format_version_report_includes_release_flow_modules():
    report = format_version_report(include_python=False)

    assert report.startswith("Hermes Agent v")
    assert "Project:" not in report
    assert "Modules:" in report
    for label, version in iter_module_versions():
        assert f"{label}:" in report
        assert (f"v{version}" in report) or version == "unavailable"


def test_cmd_version_omits_project_and_prints_modules(capsys, monkeypatch):
    fake_banner = types.ModuleType("hermes_cli.banner")
    fake_banner.check_for_updates = lambda: None
    monkeypatch.setitem(sys.modules, "hermes_cli.banner", fake_banner)

    cmd_version(SimpleNamespace())

    out = capsys.readouterr().out
    assert "Project:" not in out
    assert "Modules:" in out
    assert "Admission Control: v" in out
    assert "Task Product Layer: v" in out
