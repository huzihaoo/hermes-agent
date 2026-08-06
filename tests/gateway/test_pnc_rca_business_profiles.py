from gateway.pnc_rca_business_profiles import resolve_business_profile


def _brief(*option_ids: int):
    return {
        "work_item_fields": [
            {
                "key": "field_052f23",
                "name": "所属项目",
                "value": [
                    {"id": option_id, "name": f"display-{option_id}"}
                    for option_id in option_ids
                ],
            }
        ]
    }


def test_resolves_g1q3_and_mdrive4_by_stable_option_id_only():
    g1q3 = resolve_business_profile(
        project_key="t03o4q",
        work_item_type_key="issue",
        work_item_brief=_brief(6670325063),
    )
    mdrive4 = resolve_business_profile(
        project_key="t03o4q",
        work_item_type_key="issue",
        work_item_brief=_brief(7019637554),
    )

    assert g1q3.status == "matched"
    assert g1q3.profile.profile_id == "g1q3"
    assert g1q3.profile.execution_readiness == "ready"
    assert mdrive4.status == "matched"
    assert mdrive4.profile.profile_id == "mdrive4"
    assert mdrive4.profile.evaluator_scope == "ct_evaluator_217_20260722"
    assert mdrive4.profile.execution_readiness == "input_adapter_pending"
    assert g1q3.profile.artifact_namespace != mdrive4.profile.artifact_namespace
    assert g1q3.profile.evidence_contract != mdrive4.profile.evidence_contract


def test_unsupported_project_never_falls_back_to_g1q3():
    result = resolve_business_profile(
        project_key="t03o4q",
        work_item_type_key="issue",
        work_item_brief=_brief(6440037529),
    )

    assert result.status == "unsupported"
    assert result.profile is None
    assert result.reason == "project_option_not_registered"


def test_missing_or_conflicting_project_field_fails_closed():
    missing = resolve_business_profile(
        project_key="t03o4q",
        work_item_type_key="issue",
        work_item_brief={"work_item_fields": []},
    )
    conflict = resolve_business_profile(
        project_key="t03o4q",
        work_item_type_key="issue",
        work_item_brief=_brief(6670325063, 7019637554),
    )

    assert missing.status == "unresolved"
    assert missing.reason == "business_project_field_missing"
    assert conflict.status == "conflict"
    assert conflict.profile is None


def test_duplicate_project_fields_are_merged_without_first_value_bias():
    result = resolve_business_profile(
        project_key="t03o4q",
        work_item_type_key="issue",
        work_item_brief={
            "work_item_fields": [
                {"key": "field_052f23", "value": [{"id": 6670325063}]},
                {"key": "field_052f23", "value": [{"id": 6670325063}]},
            ]
        },
    )

    assert result.status == "matched"
    assert result.project_option_ids == ("6670325063",)


def test_duplicate_project_fields_with_different_values_are_conflict():
    result = resolve_business_profile(
        project_key="t03o4q",
        work_item_type_key="issue",
        work_item_brief={
            "work_item_fields": [
                {"key": "field_052f23", "value": [{"id": 6670325063}]},
                {"key": "field_052f23", "value": [{"id": 6841983153}]},
            ]
        },
    )

    assert result.status == "conflict"
    assert result.project_option_ids == ("6670325063", "6841983153")


def test_empty_work_item_fields_falls_back_to_canonical_fields():
    result = resolve_business_profile(
        project_key="t03o4q",
        work_item_type_key="issue",
        work_item_brief={
            "work_item_fields": [],
            "fields": [
                {
                    "field_key": "field_052f23",
                    "field_value": ["6670325063"],
                }
            ],
        },
    )

    assert result.status == "matched"
    assert result.profile.profile_id == "g1q3"


def test_title_and_owner_cannot_route_without_project_option_id():
    result = resolve_business_profile(
        project_key="t03o4q",
        work_item_type_key="issue",
        work_item_brief={
            "work_item_attribute": {
                "work_item_name": "Mdrive4 Y1M4 G1Q3",
                "role_members": [{"name": "胡子豪"}],
            },
            "work_item_fields": [],
        },
    )

    assert result.status == "unresolved"
    assert result.profile is None
