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
EXPECTED_VIRTUAL_ENV = "/Users/songying/.hermes/runtime/hermes-live/.venv"


def test_kafka_launchd_production_is_secret_free_and_crash_restarting():
    raw = PLIST_PATH.read_bytes()
    payload = plistlib.loads(raw)

    assert payload["Label"] == "local.pnc.rca-kafka-consumer"
    assert payload["ProgramArguments"] == [
        "/Users/songying/.hermes/runtime/hermes-live/.venv/bin/python",
        "/Users/songying/.hermes/runtime/hermes-live/scripts/pnc_rca_kafka_consumer.py",
    ]
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
        "VIRTUAL_ENV",
    }


def test_outbox_launchd_production_is_secret_free_and_crash_restarting():
    raw = OUTBOX_PLIST_PATH.read_bytes()
    payload = plistlib.loads(raw)

    assert payload["Label"] == "local.pnc.rca-outbox-dispatcher"
    assert payload["ProgramArguments"] == [
        "/Users/songying/.hermes/runtime/hermes-live/.venv/bin/python",
        "/Users/songying/.hermes/runtime/hermes-live/scripts/pnc_rca_outbox_dispatcher.py",
    ]
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
        "VIRTUAL_ENV",
    }


def test_all_rca_residents_bind_the_canonical_virtual_environment():
    for path in RCA_RESIDENT_PLIST_PATHS:
        payload = plistlib.loads(path.read_bytes())
        assert payload["EnvironmentVariables"]["VIRTUAL_ENV"] == (
            EXPECTED_VIRTUAL_ENV
        )
