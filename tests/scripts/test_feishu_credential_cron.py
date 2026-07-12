from types import SimpleNamespace

from scripts import feishu_credential_cron as cron


def test_orchestrate_health_then_escalate_dry_run(monkeypatch, tmp_path):
    calls = []
    rows = [{"surface": "doc", "health": "OK", "expires_at": 1}, {"surface": "project", "health": "OK"}]

    health = SimpleNamespace(
        run_health=lambda: calls.append("health") or rows,
        write_output=lambda got_rows, output_dir: calls.append(("write", got_rows, output_dir)) or (tmp_path / "out.json"),
    )
    escalate = SimpleNamespace(run=lambda got_rows, send=False: calls.append(("escalate", got_rows, send)) or {"results": [{"skipped": True}]})
    monkeypatch.setattr(cron, "load_module", lambda path, name: health if name == "feishu_credential_health" else escalate)

    rc, payload = cron.orchestrate(send=False, output_dir=tmp_path)

    assert rc == 0
    assert calls[0] == "health"
    assert calls[1][0] == "write"
    assert calls[2] == ("escalate", rows, False)
    assert payload["send"] is False
    assert payload["output_path"].endswith("out.json")


def test_orchestrate_send_true_is_passed(monkeypatch, tmp_path):
    rows = [{"surface": "doc", "health": "OK"}]
    health = SimpleNamespace(run_health=lambda: rows, write_output=lambda rows, output_dir: tmp_path / "out.json")
    seen = {}
    def fake_run(got_rows, send=False):
        seen["send"] = send
        return {"results": []}
    escalate = SimpleNamespace(run=fake_run)
    monkeypatch.setattr(cron, "load_module", lambda path, name: health if name == "feishu_credential_health" else escalate)

    rc, payload = cron.orchestrate(send=True, output_dir=tmp_path)

    assert rc == 0
    assert seen["send"] is True
    assert payload["send"] is True


def test_non_ok_health_returns_nonzero(monkeypatch, tmp_path):
    rows = [{"surface": "doc", "health": "REAUTH_REQUIRED"}]
    health = SimpleNamespace(run_health=lambda: rows, write_output=lambda rows, output_dir: tmp_path / "out.json")
    escalate = SimpleNamespace(run=lambda rows, send=False: {"results": [{"dry_run": True}]})
    monkeypatch.setattr(cron, "load_module", lambda path, name: health if name == "feishu_credential_health" else escalate)

    rc, payload = cron.orchestrate(send=False, output_dir=tmp_path)

    assert rc == 2
    assert payload["health_rows"][0]["health"] == "REAUTH_REQUIRED"


def test_refused_escalation_returns_nonzero(monkeypatch, tmp_path):
    rows = [{"surface": "doc", "health": "OK"}]
    health = SimpleNamespace(run_health=lambda: rows, write_output=lambda rows, output_dir: tmp_path / "out.json")
    escalate = SimpleNamespace(run=lambda rows, send=False: {"results": [{"refused": True, "reason": "no_explicit_target"}]})
    monkeypatch.setattr(cron, "load_module", lambda path, name: health if name == "feishu_credential_health" else escalate)

    rc, payload = cron.orchestrate(send=False, output_dir=tmp_path)

    assert rc == 2
    assert payload["escalation_results"][0]["refused"] is True


def test_main_loads_dotenv_before_orchestrate_send(monkeypatch, tmp_path, capsys):
    calls = []
    monkeypatch.setattr(cron, "load_send_environment", lambda: calls.append("load") or [])
    monkeypatch.setattr(cron, "orchestrate", lambda **kwargs: calls.append(("orchestrate", kwargs.get("send"))) or (0, {"send": kwargs.get("send")}))

    rc = cron.main(["--send", "--json", "--output-dir", str(tmp_path)])

    assert rc == 0
    assert calls[0] == "load"
    assert calls[1] == ("orchestrate", True)
