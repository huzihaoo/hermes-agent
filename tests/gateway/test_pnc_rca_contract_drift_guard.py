from pathlib import Path

from scripts.pnc_rca_contract_drift_guard import check_contract_drift, default_host_path

BEGIN = "# === RCA_REQUEST_CONTRACT:BEGIN (do not edit between markers without updating host copy) ==="
END = "# === RCA_REQUEST_CONTRACT:END ==="


def _write_contract(path: Path, body: str) -> None:
    path.write_text(f"prefix\n{BEGIN}\n{body}{END}\nsuffix\n", encoding="utf-8")


def test_contract_drift_guard_passes_for_identical_marked_blocks(tmp_path):
    host = tmp_path / "host.py"
    vm = tmp_path / "vm.py"
    _write_contract(host, "VALUE = 1\n")
    _write_contract(vm, "VALUE = 1\n")

    result = check_contract_drift(host, vm)

    assert result["ok"] is True
    assert result["status"] == "pass"
    assert result["sha256"]


def test_contract_drift_guard_reports_contract_drift(tmp_path):
    host = tmp_path / "host.py"
    vm = tmp_path / "vm.py"
    _write_contract(host, "VALUE = 1\n")
    _write_contract(vm, "VALUE = 2\n")

    result = check_contract_drift(host, vm)

    assert result["ok"] is False
    assert result["status"] == "contract_drift"
    assert result["host_sha256"] != result["vm_sha256"]


def test_contract_drift_guard_fails_closed_when_counterpart_missing(tmp_path):
    host = tmp_path / "host.py"
    vm = tmp_path / "missing.py"
    _write_contract(host, "VALUE = 1\n")

    result = check_contract_drift(host, vm)

    assert result["ok"] is False
    assert result["status"] == "contract_unverified"
    assert result["error"] == "counterpart_missing"


def test_contract_drift_guard_can_explicitly_allow_missing_for_development(tmp_path):
    host = default_host_path()
    vm = tmp_path / "missing" / "rca_request_contract.py"

    assert host.is_file()
    result = check_contract_drift(host, vm, allow_missing=True)

    assert result["ok"] is True
    assert result["status"] == "skip"
    assert result["error"] == "counterpart_missing"
    assert result["missing"] == [str(vm)]
