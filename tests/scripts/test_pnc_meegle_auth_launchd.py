import plistlib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PLIST_PATH = REPO_ROOT / "local.pnc.meegle-auth-watchdog.plist"
LIVE_ROOT = Path("/Users/songying/.hermes/runtime/hermes-live")


def test_meegle_auth_watchdog_uses_live_runtime_paths():
    with PLIST_PATH.open("rb") as handle:
        payload = plistlib.load(handle)

    assert payload["Label"] == "local.pnc.meegle-auth-watchdog"
    assert payload["ProgramArguments"] == [
        str(LIVE_ROOT / ".venv/bin/python"),
        str(LIVE_ROOT / "scripts/pnc_meegle_auth_watchdog.py"),
        "--once",
        "--json",
    ]
    assert payload["WorkingDirectory"] == str(LIVE_ROOT)
    environment = payload["EnvironmentVariables"]
    assert environment["VIRTUAL_ENV"] == str(LIVE_ROOT / ".venv")
    assert environment["PATH"].split(":", 1)[0] == str(
        LIVE_ROOT / ".venv/bin"
    )
