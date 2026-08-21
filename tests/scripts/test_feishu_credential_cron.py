from types import SimpleNamespace

import pytest

from scripts import feishu_credential_cron as cron


@pytest.fixture(autouse=True)
def _block_live_dotenv(monkeypatch):
    monkeypatch.setattr(
        cron,
        "load_send_environment",
        lambda: (_ for _ in ()).throw(AssertionError("live dotenv disabled in test")),
    )


def _health_rows(*, doc="OK", project="OK", meegle="OK"):
    return [
        {"surface": "doc", "health": doc, "expires_at": 1},
        {"surface": "project", "health": project},
        {"surface": "meegle_cli", "health": meegle},
    ]


def test_health_producer_is_bound_to_the_release_tree():
    assert cron.HEALTH_PATH == cron.REPO_ROOT / "scripts" / "feishu_credential_health.py"
    assert "/runtime/shared-state/" not in str(cron.HEALTH_PATH)


def test_release_local_health_import_closure():
    module = cron.load_module(cron.HEALTH_PATH, "feishu_credential_health_closure_test")

    assert module.KEEPWARM_PATH == cron.REPO_ROOT / "scripts" / "feishu_doc_token_keepwarm.py"


def test_orchestrate_health_then_escalate_dry_run(monkeypatch, tmp_path):
    calls = []
    rows = _health_rows()

    health = SimpleNamespace(
        run_health=lambda: calls.append("health") or rows,
        write_output=lambda got_rows, output_dir: calls.append(("write", got_rows, output_dir)) or (tmp_path / "out.json"),
    )
    escalate = SimpleNamespace(run=lambda got_rows, send=False: calls.append(("escalate", got_rows, send)) or {"ok": True, "results": [{"skipped": True}]})
    monkeypatch.setattr(cron, "load_module", lambda path, name: health if name == "feishu_credential_health" else escalate)

    rc, payload = cron.orchestrate(send=False, output_dir=tmp_path)

    assert rc == 0
    assert calls[0] == "health"
    assert calls[1][0] == "write"
    assert calls[2] == ("escalate", rows, False)
    assert payload["send"] is False
    assert payload["output_path"].endswith("out.json")


def test_orchestrate_send_true_is_passed(monkeypatch, tmp_path):
    rows = _health_rows()
    health = SimpleNamespace(run_health=lambda: rows, write_output=lambda rows, output_dir: tmp_path / "out.json")
    seen = {}
    def fake_run(got_rows, send=False):
        seen["send"] = send
        return {"ok": True, "results": []}
    escalate = SimpleNamespace(run=fake_run)
    monkeypatch.setattr(cron, "load_module", lambda path, name: health if name == "feishu_credential_health" else escalate)

    rc, payload = cron.orchestrate(send=True, output_dir=tmp_path)

    assert rc == 0
    assert seen["send"] is True
    assert payload["send"] is True


def test_non_ok_health_returns_nonzero(monkeypatch, tmp_path):
    rows = _health_rows(doc="REAUTH_REQUIRED")
    health = SimpleNamespace(run_health=lambda: rows, write_output=lambda rows, output_dir: tmp_path / "out.json")
    escalate = SimpleNamespace(run=lambda rows, send=False: {"results": [{"dry_run": True}]})
    monkeypatch.setattr(cron, "load_module", lambda path, name: health if name == "feishu_credential_health" else escalate)

    rc, payload = cron.orchestrate(send=False, output_dir=tmp_path)

    assert rc == 2
    assert payload["health_rows"][0]["health"] == "REAUTH_REQUIRED"


def test_refused_escalation_returns_nonzero(monkeypatch, tmp_path):
    rows = _health_rows()
    health = SimpleNamespace(run_health=lambda: rows, write_output=lambda rows, output_dir: tmp_path / "out.json")
    escalate = SimpleNamespace(run=lambda rows, send=False: {"results": [{"refused": True, "reason": "no_explicit_target"}]})
    monkeypatch.setattr(cron, "load_module", lambda path, name: health if name == "feishu_credential_health" else escalate)

    rc, payload = cron.orchestrate(send=False, output_dir=tmp_path)

    assert rc == 2
    assert payload["escalation_results"][0]["refused"] is True


def test_fallback_result_keeps_external_failure_and_all_rows(monkeypatch, tmp_path):
    rows = _health_rows(doc="REAUTH_REQUIRED")
    health = SimpleNamespace(
        run_health=lambda: rows,
        write_output=lambda rows, output_dir: tmp_path / "out.json",
    )
    escalate = SimpleNamespace(
        run=lambda rows, send=False: {
            "results": [
                {
                    "surface": "doc",
                    "sent": False,
                    "send_result": {"raw": "null"},
                    "fallback": {"persisted": True},
                },
                {"surface": "project", "sent": False, "state_error": "disk"},
            ]
        }
    )
    monkeypatch.setattr(
        cron,
        "load_module",
        lambda path, name: health if name == "feishu_credential_health" else escalate,
    )

    rc, payload = cron.orchestrate(send=True, output_dir=tmp_path)

    assert rc == 2
    assert len(payload["escalation_results"]) == 2
    assert payload["escalation_results"][0]["fallback"]["persisted"] is True


def test_fallback_cleanup_error_returns_nonzero(monkeypatch, tmp_path):
    rows = _health_rows()
    health = SimpleNamespace(
        run_health=lambda: rows,
        write_output=lambda rows, output_dir: tmp_path / "out.json",
    )
    escalate = SimpleNamespace(
        run=lambda rows, send=False: {
            "results": [
                {
                    "surface": "doc",
                    "skipped": True,
                    "fallback_error": "permission denied",
                }
            ]
        }
    )
    monkeypatch.setattr(
        cron,
        "load_module",
        lambda path, name: health if name == "feishu_credential_health" else escalate,
    )

    rc, payload = cron.orchestrate(send=True, output_dir=tmp_path)

    assert rc == 2
    assert payload["escalation_results"][0]["fallback_error"] == "permission denied"


def test_escalator_top_level_failure_returns_nonzero_with_no_rows(monkeypatch, tmp_path):
    health = SimpleNamespace(
        run_health=lambda: [],
        write_output=lambda rows, output_dir: tmp_path / "out.json",
    )
    escalate = SimpleNamespace(run=lambda rows, send=False: {"ok": False, "results": []})
    monkeypatch.setattr(
        cron,
        "load_module",
        lambda path, name: health if name == "feishu_credential_health" else escalate,
    )

    rc, payload = cron.orchestrate(send=True, output_dir=tmp_path)

    assert rc == 2
    assert payload["escalation_results"] == []


def test_malformed_escalator_ok_flag_fails_closed(monkeypatch, tmp_path):
    health = SimpleNamespace(
        run_health=lambda: [],
        write_output=lambda rows, output_dir: tmp_path / "out.json",
    )
    escalate = SimpleNamespace(run=lambda rows, send=False: {"ok": None, "results": []})
    monkeypatch.setattr(
        cron,
        "load_module",
        lambda path, name: health if name == "feishu_credential_health" else escalate,
    )

    rc, _payload = cron.orchestrate(send=True, output_dir=tmp_path)

    assert rc == 2


def test_empty_health_rows_fail_closed_even_if_escalator_claims_ok(
    monkeypatch, tmp_path
):
    health = SimpleNamespace(
        run_health=lambda: [],
        write_output=lambda rows, output_dir: tmp_path / "out.json",
    )
    escalate = SimpleNamespace(run=lambda rows, send=False: {"ok": True, "results": []})
    monkeypatch.setattr(
        cron,
        "load_module",
        lambda path, name: health if name == "feishu_credential_health" else escalate,
    )

    rc, payload = cron.orchestrate(send=False, output_dir=tmp_path)

    assert rc == 2
    assert payload["health_rows"] == []


def test_incomplete_health_surfaces_fail_closed_even_if_escalator_claims_ok(
    monkeypatch, tmp_path
):
    rows = [{"surface": "doc", "health": "OK"}]
    health = SimpleNamespace(
        run_health=lambda: rows,
        write_output=lambda rows, output_dir: tmp_path / "out.json",
    )
    escalate = SimpleNamespace(run=lambda rows, send=False: {"ok": True, "results": []})
    monkeypatch.setattr(
        cron,
        "load_module",
        lambda path, name: health if name == "feishu_credential_health" else escalate,
    )

    rc, _payload = cron.orchestrate(send=False, output_dir=tmp_path)

    assert rc == 2


def test_main_loads_dotenv_before_orchestrate_send(monkeypatch, tmp_path, capsys):
    calls = []
    monkeypatch.setattr(cron, "load_send_environment", lambda: calls.append("load") or [])
    monkeypatch.setattr(cron, "orchestrate", lambda **kwargs: calls.append(("orchestrate", kwargs.get("send"))) or (0, {"send": kwargs.get("send")}))

    rc = cron.main(["--send", "--json", "--output-dir", str(tmp_path)])

    assert rc == 0
    assert calls[0] == "load"
    assert calls[1] == ("orchestrate", True)


def test_main_dry_run_does_not_load_dotenv(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        cron,
        "load_send_environment",
        lambda: calls.append("load"),
    )
    monkeypatch.setattr(
        cron,
        "orchestrate",
        lambda **kwargs: calls.append(("orchestrate", kwargs.get("send")))
        or (0, {"send": kwargs.get("send")}),
    )

    rc = cron.main(["--json", "--output-dir", str(tmp_path)])

    assert rc == 0
    assert calls == [("orchestrate", False)]
