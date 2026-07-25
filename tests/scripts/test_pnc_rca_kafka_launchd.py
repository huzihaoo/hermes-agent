import plistlib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PLIST_PATH = REPO_ROOT / "local.pnc.rca-kafka-consumer.plist"
OUTBOX_PLIST_PATH = REPO_ROOT / "local.pnc.rca-outbox-dispatcher.plist"
RCA_RESIDENT_PLIST_PATHS = (
    PLIST_PATH,
    OUTBOX_PLIST_PATH,
    REPO_ROOT / "local.pnc.rca-delivery-collector.plist",
    REPO_ROOT / "local.pnc.rca-delivery-dispatcher.plist",
)
LIVE_EXEC = "/Users/songying/.hermes/runtime/governance-tools/pnc_live_exec.py"


def _expected_arguments(label: str) -> list[str]:
    return ["/usr/bin/python3", LIVE_EXEC, label]


def test_kafka_launchd_production_is_secret_free_and_crash_restarting():
    raw = PLIST_PATH.read_bytes()
    payload = plistlib.loads(raw)

    assert payload["Label"] == "local.pnc.rca-kafka-consumer"
    assert payload["ProgramArguments"] == _expected_arguments(
        "local.pnc.rca-kafka-consumer"
    )
    assert payload["RunAtLoad"] is True
    assert payload["KeepAlive"] == {"SuccessfulExit": False}
    assert payload["Umask"] == 0o77
    assert "PASSWORD" not in raw.decode("utf-8")
    assert "consumer_timeout" not in raw.decode("utf-8").lower()
    assert set(payload["EnvironmentVariables"]) == {
        "HERMES_HOME",
        "HOME",
        "PATH",
        "PYTHONDONTWRITEBYTECODE",
        "PYTHONNOUSERSITE",
        "PYTHONUNBUFFERED",
    }
    assert "/runtime/releases/" not in raw.decode("utf-8")
    assert "/runtime/venvs/" not in raw.decode("utf-8")


def test_outbox_launchd_production_is_secret_free_and_crash_restarting():
    raw = OUTBOX_PLIST_PATH.read_bytes()
    payload = plistlib.loads(raw)

    assert payload["Label"] == "local.pnc.rca-outbox-dispatcher"
    assert payload["ProgramArguments"] == _expected_arguments(
        "local.pnc.rca-outbox-dispatcher"
    )
    assert payload["RunAtLoad"] is True
    assert payload["KeepAlive"] == {"SuccessfulExit": False}
    assert payload["Umask"] == 0o77
    assert "PASSWORD" not in raw.decode("utf-8")
    assert set(payload["EnvironmentVariables"]) == {
        "HERMES_HOME",
        "HOME",
        "PATH",
        "PYTHONDONTWRITEBYTECODE",
        "PYTHONNOUSERSITE",
        "PYTHONUNBUFFERED",
    }
    assert "/runtime/releases/" not in raw.decode("utf-8")
    assert "/runtime/venvs/" not in raw.decode("utf-8")


def test_all_rca_residents_resolve_the_active_manifest_without_runtime_literals():
    for path in RCA_RESIDENT_PLIST_PATHS:
        payload = plistlib.loads(path.read_bytes())
        label = payload["Label"]
        assert payload["ProgramArguments"][:3] == _expected_arguments(label)
        assert "VIRTUAL_ENV" not in payload["EnvironmentVariables"]
        text = path.read_text(encoding="utf-8")
        assert "/runtime/releases/" not in text
        assert "/runtime/venvs/" not in text
        assert "/runtime/hermes-live" not in text
