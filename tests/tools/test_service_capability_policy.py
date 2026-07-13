import json

from tools import permission_policy


CAPABILITY = "submit_g1q3_rca_issue_intake"
SERVICE_ID = "root_cause_analysis_agent"


def _configure(monkeypatch, tmp_path, entry):
    path = tmp_path / "user-roles.json"
    path.write_text(
        json.dumps(
            {
                "users": {"default": "member"},
                "permission_matrix": {"member": {}},
                "service_capabilities": {SERVICE_ID: entry},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", path)
    monkeypatch.setattr(permission_policy, "_config", None)


def test_service_capability_requires_exact_service_actor_and_capability(monkeypatch, tmp_path):
    _configure(
        monkeypatch,
        tmp_path,
        {"actor_kind": "service", "enabled": True, "capabilities": [CAPABILITY]},
    )

    assert permission_policy.service_capability_allows(SERVICE_ID, CAPABILITY) is True
    assert permission_policy.service_capability_allows(SERVICE_ID, "vm_task_submit") is False
    assert permission_policy.service_capability_allows("other_service", CAPABILITY) is False


def test_service_capability_has_no_wildcard_or_human_role_fallback(monkeypatch, tmp_path):
    _configure(
        monkeypatch,
        tmp_path,
        {"actor_kind": "service", "capabilities": ["*"]},
    )
    assert permission_policy.service_capability_allows(SERVICE_ID, CAPABILITY) is False

    _configure(
        monkeypatch,
        tmp_path,
        {"actor_kind": "human", "capabilities": [CAPABILITY]},
    )
    assert permission_policy.service_capability_allows(SERVICE_ID, CAPABILITY) is False


def test_disabled_or_malformed_service_grants_fail_closed(monkeypatch, tmp_path):
    _configure(
        monkeypatch,
        tmp_path,
        {"actor_kind": "service", "enabled": False, "capabilities": [CAPABILITY]},
    )
    assert permission_policy.service_capability_allows(SERVICE_ID, CAPABILITY) is False

    _configure(
        monkeypatch,
        tmp_path,
        {"actor_kind": "service", "capabilities": [CAPABILITY]},
    )
    assert permission_policy.service_capability_allows(SERVICE_ID, CAPABILITY) is False

    _configure(
        monkeypatch,
        tmp_path,
        {"actor_kind": "service", "capabilities": CAPABILITY},
    )
    assert permission_policy.service_capability_allows(SERVICE_ID, CAPABILITY) is False
