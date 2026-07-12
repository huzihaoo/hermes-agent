"""Candidate-only outbound recording transport."""

from gateway.record_only.runtime import (
    get_record_only_transport,
    record_only_enabled,
)

__all__ = ["get_record_only_transport", "record_only_enabled"]
