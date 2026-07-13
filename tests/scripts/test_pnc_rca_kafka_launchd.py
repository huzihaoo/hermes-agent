import plistlib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PLIST_PATH = REPO_ROOT / "local.pnc.rca-kafka-consumer.candidate.plist"
OUTBOX_PLIST_PATH = REPO_ROOT / "local.pnc.rca-outbox-dispatcher.candidate.plist"


def test_kafka_launchd_candidate_is_secret_free_and_crash_restarting():
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
        "PATH",
        "PYTHONNOUSERSITE",
        "PYTHONUNBUFFERED",
    }


def test_outbox_launchd_candidate_is_secret_free_and_crash_restarting():
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
        "PATH",
        "PYTHONNOUSERSITE",
        "PYTHONUNBUFFERED",
    }
