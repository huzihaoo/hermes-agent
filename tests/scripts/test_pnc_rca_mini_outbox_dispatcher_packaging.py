from __future__ import annotations

import json
from pathlib import Path
import plistlib

from dotenv import dotenv_values
import pytest

from gateway.pnc_rca_mini_store import MiniOutboxClaim
from scripts import pnc_rca_kafka_direct_consumer as direct_consumer
from scripts import pnc_rca_mini_outbox_dispatcher as dispatcher


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_RELATIVE = Path(".hermes/runtime/pnc_agent/feishu_issue_kafka_rca_direct")


def _runtime(tmp_path: Path) -> Path:
    root = tmp_path / RUNTIME_RELATIVE
    root.mkdir(parents=True)
    root.chmod(0o700)
    return root


def _env_file(tmp_path: Path, **updates: str) -> tuple[Path, dict[str, str]]:
    root = _runtime(tmp_path)
    values = {
        "HERMES_RCA_DIRECT_KAFKA_GROUP_ID": "rca_direct_path",
        "HERMES_RCA_DIRECT_OUTBOX_ENABLED": "false",
        "HERMES_RCA_DIRECT_OUTBOX_SUBMIT_ENABLED": "false",
    }
    values.update(updates)
    path = root / "direct.env"
    path.write_text(
        "".join(f"{key}={value}\n" for key, value in values.items()),
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path, values


def _claim(*, request_json: str = "") -> MiniOutboxClaim:
    return MiniOutboxClaim(
        outbox_id=1,
        submission_key="g1q3-rca-s1-" + "a" * 64,
        action="submit_rca_issue_intake",
        business_key="g1q3-rca-b1-" + "b" * 64,
        generation=1,
        trigger_kind="issue_created",
        source_event_id="feishu-project-workflow-event:0:10",
        origin_source_id="g1q3-rca-source-v1-" + "c" * 64,
        payload_json=json.dumps({"schema_version": "pnc_rca_mini_outbox_v2"}),
        request_json=request_json,
        request_sha256="",
        lease_owner=dispatcher.MINI_DISPATCHER_SERVICE_LABEL,
        lease_expires_at="2099-01-01T00:00:00+00:00",
        attempt_count=1,
    )


def _execution_request(claim: MiniOutboxClaim) -> dict[str, object]:
    return {
        "schema_version": "g1q3_rca_execution_request_v2",
        "request_kind": "issue_intake",
        "work_item": {
            "project_key": "project-key",
            "work_item_type": "problem-type",
            "work_item_id": "7041712812",
        },
        "data": {
            "data_access": {
                "schema_version": "g1q3_rca_remote_data_access_v1",
                "mode": "remote_read",
                "transport": "pdcl_pyclip",
                "references": [
                    {
                        "kind": "event",
                        "event_uuid": "event-7041712812",
                        "reader_class": "RemoteEventReader",
                    }
                ],
                "source": {
                    "field": "问题数据地址_PDCL",
                    "value_sha256": "d" * 64,
                },
                "reader_contract": {
                    "distribution": "pdcl_pyclip",
                    "required_version": "0.1.6+rca.2",
                    "mdi_download_allowed": False,
                    "fallback": "forbidden",
                    "completeness": "full_requested_scope",
                },
            },
            "artifact_root": f"/mnt/tmp/{claim.submission_key}/",
            "artifact_cifs_root": (
                f"//hfs1.example.test/rca/tmp/{claim.submission_key}/"
            ),
        },
        "execution_policy": {
            "data_access_mode": "remote_read",
            "allow_download": False,
            "input_materialization": "forbidden",
            "artifact_root": f"/mnt/tmp/{claim.submission_key}/",
        },
        "source_refs": {
            "origin_source_id": claim.origin_source_id,
            "source_event_id": claim.source_event_id,
            "generation": claim.generation,
            "business_key": claim.business_key,
            "submission_key": claim.submission_key,
        },
    }


def test_config_requires_explicit_two_level_safe_off_and_fixed_paths(tmp_path: Path):
    env_path, values = _env_file(tmp_path)
    source, loaded_path = dispatcher.load_mini_outbox_environment(
        env_path,
        environ={},
        hermes_home=tmp_path / ".hermes",
    )
    config = dispatcher.MiniOutboxDispatcherConfig.from_env(
        source,
        hermes_home=tmp_path / ".hermes",
        env_file=loaded_path,
    )
    assert config.enabled is False
    assert config.submit_enabled is False
    assert config.group_id == "rca_direct_path"
    assert config.request_builder == "prebuilt"
    assert config.db_path == loaded_path.parent / "mini.sqlite3"
    assert config.health_path == loaded_path.parent / "outbox_dispatcher_health.json"
    assert values["HERMES_RCA_DIRECT_OUTBOX_SUBMIT_ENABLED"] == "false"


def test_host_preread_builder_is_explicitly_selectable(tmp_path: Path):
    env_path, _ = _env_file(
        tmp_path,
        HERMES_RCA_DIRECT_OUTBOX_REQUEST_BUILDER="host_preread",
    )
    source, loaded_path = dispatcher.load_mini_outbox_environment(
        env_path,
        environ={},
        hermes_home=tmp_path / ".hermes",
    )
    config = dispatcher.MiniOutboxDispatcherConfig.from_env(
        source,
        hermes_home=tmp_path / ".hermes",
        env_file=loaded_path,
    )
    assert config.request_builder == "host_preread"


def test_unknown_request_builder_fails_closed(tmp_path: Path):
    env_path, _ = _env_file(
        tmp_path,
        HERMES_RCA_DIRECT_OUTBOX_REQUEST_BUILDER="legacy",
    )
    source, loaded_path = dispatcher.load_mini_outbox_environment(
        env_path,
        environ={},
        hermes_home=tmp_path / ".hermes",
    )
    with pytest.raises(dispatcher.MiniDispatcherConfigError, match="request_builder"):
        dispatcher.MiniOutboxDispatcherConfig.from_env(
            source,
            hermes_home=tmp_path / ".hermes",
            env_file=loaded_path,
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"HERMES_RCA_DIRECT_OUTBOX_ENABLED": "true"},
        {
            "HERMES_RCA_DIRECT_OUTBOX_ENABLED": "true",
            "HERMES_RCA_DIRECT_OUTBOX_SUBMIT_ENABLED": "true",
            "HERMES_RCA_DIRECT_KAFKA_GROUP_ID": "shadow-group",
        },
    ],
)
def test_enabled_dispatcher_rejects_incomplete_or_nonproduction_group(
    tmp_path: Path, updates: dict[str, str]
):
    env_path, _ = _env_file(tmp_path, **updates)
    source, loaded_path = dispatcher.load_mini_outbox_environment(
        env_path,
        environ={},
        hermes_home=tmp_path / ".hermes",
    )
    if updates.get("HERMES_RCA_DIRECT_OUTBOX_SUBMIT_ENABLED") == "true":
        with pytest.raises(
            dispatcher.MiniDispatcherConfigError,
            match="production_direct_group",
        ):
            dispatcher.MiniOutboxDispatcherConfig.from_env(
                source,
                hermes_home=tmp_path / ".hermes",
                env_file=loaded_path,
            )
    else:
        config = dispatcher.MiniOutboxDispatcherConfig.from_env(
            source,
            hermes_home=tmp_path / ".hermes",
            env_file=loaded_path,
        )
        assert config.submit_enabled is False


def test_env_file_mode_is_exactly_0600(tmp_path: Path):
    env_path, _ = _env_file(tmp_path)
    env_path.chmod(0o640)
    with pytest.raises(dispatcher.MiniDispatcherConfigError, match="0600"):
        dispatcher.load_mini_outbox_environment(
            env_path,
            environ={},
            hermes_home=tmp_path / ".hermes",
        )


def test_safe_off_does_not_open_db_or_transport_and_writes_private_health(
    tmp_path: Path,
):
    env_path, _ = _env_file(tmp_path)
    source, loaded_path = dispatcher.load_mini_outbox_environment(
        env_path,
        environ={},
        hermes_home=tmp_path / ".hermes",
    )
    config = dispatcher.MiniOutboxDispatcherConfig.from_env(
        source,
        hermes_home=tmp_path / ".hermes",
        env_file=loaded_path,
    )

    called = False

    def factory(_config):
        nonlocal called
        called = True
        raise AssertionError("safe-off must not construct dispatcher")

    assert (
        dispatcher.run_mini_outbox_dispatcher(
            config,
            dispatcher_factory=factory,
        )
        == 0
    )
    assert called is False
    assert not config.db_path.exists()
    health = config.health_path
    assert health.exists()
    assert (health.stat().st_mode & 0o777) == 0o600
    body = json.loads(health.read_text(encoding="utf-8"))
    assert body["state"] == "disabled"
    assert body["business_ready"] is False
    assert body["ok"] is False


def test_prebuilt_builder_never_synthesizes_old_payload_fields():
    claim = _claim()
    with pytest.raises(
        dispatcher.PrebuiltExecutionRequestError,
        match="prebuilt_execution_request_required",
    ):
        dispatcher.build_prebuilt_execution_request(
            {"admission": {"legacy": "metadata"}},
            claim,
        )


def test_prebuilt_request_requires_exact_claim_identity_and_direct_contract():
    claim = _claim()
    request = _execution_request(claim)
    dispatcher.validate_prebuilt_execution_request(request, claim)

    wrong = json.loads(json.dumps(request))
    wrong["source_refs"]["submission_key"] = "different"
    with pytest.raises(dispatcher.PrebuiltExecutionRequestError):
        dispatcher.validate_prebuilt_execution_request(wrong, claim)

    forbidden = json.loads(json.dumps(request))
    forbidden["execution_policy"]["resource_class"] = "standard"
    with pytest.raises(dispatcher.PrebuiltExecutionRequestError):
        dispatcher.validate_prebuilt_execution_request(forbidden, claim)


def test_existing_status_requires_matching_contract_identity():
    claim = _claim(request_json=json.dumps(_execution_request(_claim())))
    request = dispatcher.build_strict_direct_vm_request(
        json.loads(claim.request_json), claim
    )

    class Transport:
        def status(self, _task_id):
            return {
                "state": "existing",
                "task_id": claim.submission_key,
                "submission_key": claim.submission_key,
                "identity_sha256": "0" * 64,
            }

    config = dispatcher.MiniOutboxDispatcherConfig(
        enabled=True,
        submit_enabled=True,
        group_id=dispatcher.DIRECT_DEFAULT_GROUP_ID,
        db_path=Path("/tmp/direct-mini.sqlite3"),
        health_path=Path("/tmp/direct-health.json"),
        env_file=Path("/tmp/direct.env"),
    )
    boundary = dispatcher.DirectVmDispatcherBoundary(config, transport=Transport())
    with pytest.raises(dispatcher.IdentityMismatchError):
        boundary.status(claim.submission_key, claim)
    assert request.identity_sha256 != "0" * 64


def test_plists_are_direct_secret_free_and_shadow_is_not_packaged():
    expected = {
        "local.pnc.rca-kafka-direct.plist": (
            "local.pnc.rca-kafka-direct",
            "pnc_rca_kafka_direct_consumer.py",
            "consumer.stdout.log",
            "consumer.stderr.log",
        ),
        "local.pnc.rca-mini-outbox-dispatcher.plist": (
            "local.pnc.rca-mini-outbox-dispatcher",
            "pnc_rca_mini_outbox_dispatcher.py",
            "dispatcher.stdout.log",
            "dispatcher.stderr.log",
        ),
    }
    for filename, (label, target_name, stdout_name, stderr_name) in expected.items():
        path = REPO_ROOT / filename
        raw = path.read_bytes()
        payload = plistlib.loads(raw)
        root = "/Users/songying/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca_direct"
        assert payload["Label"] == label
        assert payload["ProgramArguments"] == [
            dispatcher.DIRECT_DISPATCHER_PYTHON,
            f"{root}/{target_name}",
            "--env-file",
            f"{root}/direct.env",
        ]
        assert payload["WorkingDirectory"] == root
        assert payload["Umask"] == 0o77
        assert payload["StandardOutPath"] == f"{root}/{stdout_name}"
        assert payload["StandardErrorPath"] == f"{root}/{stderr_name}"
        assert payload["KeepAlive"] == {"SuccessfulExit": False}
        text = raw.decode("utf-8")
        assert "pnc_live_exec" not in text
        assert "/runtime/releases/" not in text
        assert "PASSWORD" not in text.upper()
        assert "SECRET" not in text.upper()
        assert "HERMES_RCA_DIRECT_" not in text
    assert not list(REPO_ROOT.glob("*shadow*.plist"))


def test_env_example_has_explicit_safe_off_and_no_secrets():
    path = REPO_ROOT / "assets/pnc_rca_direct.env.example"
    text = path.read_text(encoding="utf-8")
    values = dotenv_values(path)
    required_consumer = {
        "HERMES_RCA_DIRECT_KAFKA_BOOTSTRAP_SERVERS",
        "HERMES_RCA_DIRECT_KAFKA_TOPIC",
        "HERMES_RCA_DIRECT_KAFKA_GROUP_ID",
        "HERMES_RCA_DIRECT_KAFKA_SECURITY_PROTOCOL",
        "HERMES_RCA_DIRECT_KAFKA_SASL_MECHANISM",
        "HERMES_RCA_DIRECT_KAFKA_SASL_USERNAME",
        "HERMES_RCA_DIRECT_KAFKA_SASL_PASSWORD",
        "HERMES_RCA_DIRECT_KAFKA_POLICY_JSON",
        "HERMES_RCA_DIRECT_KAFKA_COMMIT_ENABLED",
        "HERMES_RCA_DIRECT_KAFKA_DB_PATH",
        "HERMES_RCA_DIRECT_KAFKA_HEALTH_PATH",
    }
    assert required_consumer <= values.keys()
    assert values["HERMES_RCA_DIRECT_KAFKA_DB_PATH"].endswith(
        "/feishu_issue_kafka_rca_direct/mini.sqlite3"
    )
    assert values["HERMES_RCA_DIRECT_KAFKA_HEALTH_PATH"].endswith(
        "/feishu_issue_kafka_rca_direct/consumer_health.json"
    )
    assert values["HERMES_RCA_DIRECT_KAFKA_SASL_USERNAME"] in {None, ""}
    assert values["HERMES_RCA_DIRECT_KAFKA_SASL_PASSWORD"] in {None, ""}
    assert values["HERMES_RCA_DIRECT_KAFKA_COMMIT_ENABLED"] == "false"
    with pytest.raises(ValueError, match="SASL credentials"):
        credential_probe = dict(values)
        credential_probe["HERMES_RCA_DIRECT_KAFKA_COMMIT_ENABLED"] = "true"
        direct_consumer.DirectKafkaConfig.from_env(credential_probe)
    with pytest.raises(ValueError, match="SASL credentials"):
        direct_consumer.DirectKafkaConfig.from_env(values)
    commit_probe = dict(values)
    commit_probe["HERMES_RCA_DIRECT_KAFKA_SASL_USERNAME"] = "example-user"
    commit_probe["HERMES_RCA_DIRECT_KAFKA_SASL_PASSWORD"] = "example-password"
    with pytest.raises(ValueError, match="shadow mode"):
        direct_consumer.DirectKafkaConfig.from_env(commit_probe)
    assert "HERMES_RCA_DIRECT_OUTBOX_ENABLED=false" in text
    assert "HERMES_RCA_DIRECT_OUTBOX_SUBMIT_ENABLED=false" in text
    assert "HERMES_RCA_DIRECT_OUTBOX_REQUEST_BUILDER=prebuilt" in text
    assert "HERMES_RCA_DIRECT_KAFKA_GROUP_ID=rca_direct_path" in text
    assert "HERMES_RCA_DIRECT_OUTBOX_DB_PATH=" in text
    assert "HERMES_RCA_DIRECT_OUTBOX_HEALTH_PATH=" in text
    for key, value in values.items():
        if any(marker in key.upper() for marker in ("PASSWORD", "SECRET", "TOKEN")):
            assert value in {None, ""}, key
