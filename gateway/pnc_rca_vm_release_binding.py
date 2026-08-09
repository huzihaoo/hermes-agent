"""One explicit VM release binding for the governed RCA production path."""

from __future__ import annotations


RCA_PROD_VM_RELEASE_ROOT = (
    "/home/mini/.hermes/rca-prod-runtime/releases/"
    "rca-platform-20260809.installed-eeb1bb9"
)
RCA_PROD_VM_FIXED_CLI_RELATIVE_PATH = "api/g1q3_rca/scripts/run_rca_service_request.py"
RCA_PROD_VM_DERIVED_RESERVATION_MODULE = (
    f"{RCA_PROD_VM_RELEASE_ROOT}/api/g1q3_rca/derived_capacity_reservation.py"
)
