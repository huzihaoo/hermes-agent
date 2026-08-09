from gateway import pnc_rca_derived_capacity_reservation as derived
from gateway import pnc_rca_prod_admission as admission
from gateway import pnc_rca_runtime_identity as runtime_identity
from gateway import pnc_rca_vm_release_binding as binding
from tools import vm_task_tool


def test_current_rca_vm_release_binding_is_shared_by_all_active_host_paths():
    assert binding.RCA_PROD_VM_RELEASE_ROOT == (
        "/home/mini/.hermes/rca-prod-runtime/releases/"
        "rca-platform-20260809.installed-6a8c5e3"
    )
    assert derived.REMOTE_VM_REPO_ROOT == binding.RCA_PROD_VM_RELEASE_ROOT
    assert derived.REMOTE_DERIVED_RESERVATION_MODULE == (
        f"{binding.RCA_PROD_VM_RELEASE_ROOT}/"
        "api/g1q3_rca/derived_capacity_reservation.py"
    )
    assert vm_task_tool._RCA_VM_REPO_ROOT == binding.RCA_PROD_VM_RELEASE_ROOT
    assert vm_task_tool._RCA_FIXED_CLI_RELATIVE_PATH == (
        "./api/g1q3_rca/scripts/run_rca_service_request.py"
    )
    assert not hasattr(admission, "VM_REPO_ROOT")
    assert "gateway/pnc_rca_vm_release_binding.py" in (
        runtime_identity.RCA_RUNTIME_RELATIVE_FILES
    )
    assert "gateway/pnc_rca_vm_release_binding.py" in (
        runtime_identity.GATEWAY_RCA_RUNTIME_RELATIVE_FILES
    )
