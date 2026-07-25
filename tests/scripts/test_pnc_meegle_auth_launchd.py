import plistlib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PLIST_PATH = REPO_ROOT / "local.pnc.meegle-auth-watchdog.plist"
LIVE_EXEC = Path("/Users/songying/.hermes/runtime/governance-tools/pnc_live_exec.py")


def test_meegle_auth_watchdog_resolves_the_active_manifest():
    with PLIST_PATH.open("rb") as handle:
        payload = plistlib.load(handle)

    assert payload["Label"] == "local.pnc.meegle-auth-watchdog"
    assert payload["ProgramArguments"] == [
        "/usr/bin/python3",
        str(LIVE_EXEC),
        "local.pnc.meegle-auth-watchdog",
        "--once",
        "--json",
    ]
    assert payload["WorkingDirectory"] == "/Users/songying/.hermes/runtime"
    environment = payload["EnvironmentVariables"]
    assert "VIRTUAL_ENV" not in environment
    text = PLIST_PATH.read_text(encoding="utf-8")
    assert "/runtime/releases/" not in text
    assert "/runtime/venvs/" not in text
    assert "/runtime/hermes-live" not in text
