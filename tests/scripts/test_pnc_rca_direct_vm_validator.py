from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

from gateway.pnc_rca_direct_vm_submit import (
    DirectVmSubmitError,
    validate_direct_vm_request,
)
from tests.gateway.test_pnc_rca_direct_vm_submit import _request


VALIDATOR_PATH = Path("scripts/pnc_rca_direct_vm_validator.py").resolve()


def _sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_flat_validator():
    spec = importlib.util.spec_from_file_location(
        "flat_direct_validator", VALIDATOR_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_flat_validator_matches_host_for_valid_request() -> None:
    flat = _load_flat_validator()
    request = _request().to_dict()

    host = validate_direct_vm_request(request).to_dict()
    observed = flat.validate_direct_vm_request(request)

    assert observed == host
    assert flat.DIRECT_VM_VALIDATOR_SCHEMA_VERSION == "g1q3_rca_direct_vm_validator_v1"


def test_flat_validator_rejects_the_same_malformed_identity_as_host() -> None:
    flat = _load_flat_validator()
    request = _request().to_dict()
    request["source_refs"] = dict(request["source_refs"])
    request["source_refs"]["generation"] = 2

    with pytest.raises(DirectVmSubmitError):
        validate_direct_vm_request(request)
    with pytest.raises(flat.DirectVmValidatorError):
        flat.validate_direct_vm_request(request)


@pytest.mark.parametrize(
    "field",
    ("raw", "raw_payload", "raw_feishu_payload", "full_payload", "secret", "token"),
)
def test_flat_validator_rejects_resealed_sensitive_payloads(field: str) -> None:
    flat = _load_flat_validator()
    request = _request().to_dict()
    execution = dict(request["execution_request"])
    execution["data"] = {**execution["data"], field: "sensitive-value"}
    request["execution_request"] = execution
    request["contract_sha256"] = _sha256(execution)
    identity = dict(request)
    identity.pop("identity_sha256")
    request["identity_sha256"] = _sha256(identity)

    with pytest.raises(DirectVmSubmitError):
        validate_direct_vm_request(request)
    with pytest.raises(
        flat.DirectVmValidatorError,
        match="direct_vm_sensitive_field_forbidden",
    ):
        flat.validate_direct_vm_request(request)


def test_flat_validator_imports_without_host_gateway_package() -> None:
    request = json.dumps(_request().to_dict(), ensure_ascii=False)
    probe = (
        "import importlib.util,json,sys;"
        f"spec=importlib.util.spec_from_file_location('v',{str(VALIDATOR_PATH)!r});"
        "m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);"
        "assert not any(name == 'gateway' or name.startswith('gateway.') for name in sys.modules);"
        "print(m.validate_direct_vm_request(json.loads(sys.stdin.read()))['task_id'])"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", probe],
        input=request,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == _request().task_id


def test_flat_validator_has_no_host_package_import_or_side_effect_surface() -> None:
    source = VALIDATOR_PATH.read_text(encoding="utf-8")
    assert "from gateway" not in source
    assert "import gateway" not in source
    assert "subprocess" not in source
    assert "socket" not in source
