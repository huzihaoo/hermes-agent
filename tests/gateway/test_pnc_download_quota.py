import json
from datetime import date

from gateway.pnc_download_quota import consume_g1q3_download_grant


def test_grant_disabled_when_quota_zero(tmp_path):
    result = consume_g1q3_download_grant(quota_dir=tmp_path, task_id="t1", daily_quota=0)

    assert result == {"granted": False, "used": 0, "quota": 0, "reason": "auto_download_disabled"}
    assert list(tmp_path.glob("*.json")) == []


def test_grants_consume_and_exhaust_daily_quota(tmp_path):
    day = date(2026, 6, 11)

    first = consume_g1q3_download_grant(quota_dir=tmp_path, task_id="t1", daily_quota=2, today=day)
    second = consume_g1q3_download_grant(quota_dir=tmp_path, task_id="t2", daily_quota=2, today=day)
    third = consume_g1q3_download_grant(quota_dir=tmp_path, task_id="t3", daily_quota=2, today=day)

    assert first["granted"] is True and first["used"] == 1
    assert second["granted"] is True and second["used"] == 2
    assert third["granted"] is False
    assert third["reason"] == "daily_quota_exhausted"
    ledger = json.loads((tmp_path / "g1q3_auto_download-2026-06-11.json").read_text())
    assert ledger["used"] == 2
    assert [g["task_id"] for g in ledger["grants"]] == ["t1", "t2"]


def test_quota_resets_per_day(tmp_path):
    exhausted = consume_g1q3_download_grant(quota_dir=tmp_path, task_id="t1", daily_quota=1, today=date(2026, 6, 11))
    denied = consume_g1q3_download_grant(quota_dir=tmp_path, task_id="t2", daily_quota=1, today=date(2026, 6, 11))
    next_day = consume_g1q3_download_grant(quota_dir=tmp_path, task_id="t3", daily_quota=1, today=date(2026, 6, 12))

    assert exhausted["granted"] is True
    assert denied["granted"] is False
    assert next_day["granted"] is True


def test_env_quota_default_off(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", raising=False)
    assert consume_g1q3_download_grant(quota_dir=tmp_path, task_id="t1")["granted"] is False

    monkeypatch.setenv("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", "3")
    assert consume_g1q3_download_grant(quota_dir=tmp_path, task_id="t1")["granted"] is True

    monkeypatch.setenv("HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA", "not-a-number")
    assert consume_g1q3_download_grant(quota_dir=tmp_path, task_id="t1")["granted"] is False


def test_corrupt_ledger_is_reset_not_fatal(tmp_path):
    day = date(2026, 6, 11)
    (tmp_path / "g1q3_auto_download-2026-06-11.json").write_text("{corrupt", encoding="utf-8")

    result = consume_g1q3_download_grant(quota_dir=tmp_path, task_id="t1", daily_quota=1, today=day)

    assert result["granted"] is True
    ledger = json.loads((tmp_path / "g1q3_auto_download-2026-06-11.json").read_text())
    assert ledger["used"] == 1
