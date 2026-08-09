import builtins
import json

import pytest

from gateway.pnc_rca_abstention_projection import (
    build_gate_a_identifier_binding,
    build_gate_a_public_result,
    RcaEvidenceProjectionError,
    UNMATERIALIZED_FAILURE_MESSAGES,
    project_gate_a_report as _project_gate_a_report,
    project_materialized_evaluator_evidence,
    project_unmaterialized_case_anchor,
)


def _test_identifier_binding(source):
    evaluators = source.get("rca_evaluators") or []
    signals = set()
    fields = set()
    actual_evaluators = []
    for evaluator in evaluators:
        actual_evaluators.append({
            "evaluator_id": evaluator.get("key"),
            "status": evaluator.get("status"),
        })
        fields.update(evaluator.get("missing_fields") or [])
        for check in evaluator.get("checks") or []:
            evidence = check.get("evidence") or {}
            fields.update(evidence.get("fields") or [])
        for reference in evaluator.get("evidence_refs") or []:
            if reference.get("signal") is not None:
                signals.add(reference["signal"])
            if reference.get("field") is not None:
                fields.add(reference["field"])
            fields.update(reference.get("fields") or [])
    return build_gate_a_identifier_binding({
        "actual_evaluators": actual_evaluators,
        "actual_signals": sorted(signals),
        "actual_fields": sorted(fields),
    })


def project_gate_a_report(source):
    binding = _test_identifier_binding(source) if source.get("rca_evaluators") else None
    return _project_gate_a_report(source, identifier_binding=binding)


def _forbid_open(*_args, **_kwargs):
    raise AssertionError("projection must not perform I/O")


def test_materialized_projection_preserves_refutation_without_defect_label(monkeypatch):
    monkeypatch.setattr(builtins, "open", _forbid_open)
    report = {
        "rca_evaluators": [
            {
                "key": "lcc_path",
                "domain": "lcc",
                "pattern": "lateral_offset",
                "status": "refuted",
                "window": {"start_us": 10, "end_us": 20},
                "checks": [
                    {
                        "thresholds": {"max_offset_m": 0.2},
                        "evidence": {"fields": ["lateral_offset_m"]},
                    }
                ],
                "evidence_refs": [{"evidence": "offset stayed below 0.2m"}],
                "defect": "must not cross projection boundary",
            }
        ]
    }

    result = project_materialized_evaluator_evidence(report)

    entry = result["evaluators"][0]
    assert entry["status"] == "refuted"
    assert "evidence" not in entry
    assert "evidence_refs" not in entry
    assert entry["window"] == {"start_us": 10, "end_us": 20}
    assert "defect" not in entry
    assert report["rca_evaluators"][0]["defect"] == "must not cross projection boundary"


def test_materialized_projection_keeps_exact_missing_fields_without_synthesis():
    result = project_materialized_evaluator_evidence({
        "rca_evaluators": [
            {
                "key": "aeb",
                "status": "need_fields",
                "missing_fields": ["object_speed_mps", "aeb_state"],
                "checks": [],
                "evidence_refs": [],
            }
        ]
    })

    entry = result["evaluators"][0]
    assert entry["missing_fields"] == ["object_speed_mps", "aeb_state"]
    assert "checks" not in entry
    assert "evidence" not in entry
    assert entry["source_field_absent"] == ["domain", "pattern"]


@pytest.mark.parametrize("failure_class", sorted(UNMATERIALIZED_FAILURE_MESSAGES))
def test_unmaterialized_projection_keeps_each_failure_class_distinct(failure_class):
    result = project_unmaterialized_case_anchor({
        "input_materialized": False,
        "failure_class": failure_class,
        "frame_lookup": {"management_timestamp": 1_783_841_476_000_000},
        "marker_time": "2026-07-30T10:00:00+08:00",
        "event_uuid": "event-123",
    })

    assert result["failure_class"] == failure_class
    assert result["message"] == UNMATERIALIZED_FAILURE_MESSAGES[failure_class]
    assert result["anchors"] == {
        "management_timestamp": 1_783_841_476_000_000,
        "marker_time": "2026-07-30T10:00:00+08:00",
        "event_uuid": "event-123",
    }
    assert set(result) == {
        "schema_version",
        "input_materialized",
        "failure_class",
        "message",
        "anchors",
    }


@pytest.mark.parametrize(
    "extra",
    [
        {"foxglove_url": "https://foxglove.dev/view"},
        {"report_url": "https://reports.example.test/rca"},
        {"confidence": "high"},
        {"conclusion": "planner fault"},
        {"rca_evaluators": [{"status": "supported"}]},
    ],
)
def test_unmaterialized_projection_rejects_disclosures_and_materialized_evidence(extra):
    case = {
        "input_materialized": False,
        "failure_class": "remote_event_not_found",
    }
    case.update(extra)

    with pytest.raises(RcaEvidenceProjectionError):
        project_unmaterialized_case_anchor(case)


def test_unmaterialized_projection_requires_explicit_unmaterialized_input():
    with pytest.raises(
        RcaEvidenceProjectionError, match="unmaterialized_input_required"
    ):
        project_unmaterialized_case_anchor({"failure_class": "remote_event_not_found"})


def test_gate_a_materialized_projection_is_observation_only():
    projection = project_gate_a_report({
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": "lcc_path",
                "domain": "lcc",
                "pattern": "lateral_offset",
                "status": "supported",
                "candidate_responsibility": "must not cross",
                "confidence": "high",
                "evidence_refs": [{"signal": "lateral_offset_m"}],
            }
        ],
    })

    public = build_gate_a_public_result(projection)
    assert projection["level"] == "L1_observation"
    assert public["gate_a_level"] == "L1_observation"
    assert public["responsibility"]["candidate"] == "暂无法判断"
    assert "candidate_responsibility" not in public["evaluator_observations"][0]
    assert "confidence" not in public["evaluator_observations"][0]
    assert "责任归因" in public["summary"]["short_conclusion"]


def test_gate_a_rejects_l2_fields_in_detached_projection():
    projection = project_gate_a_report({
        "input_materialized": False,
        "failure_class": "remote_event_not_found",
    })
    projection["evaluator_projection"] = {
        "schema_version": "pnc_rca_materialized_evaluator_projection_v1",
        "input_materialized": True,
        "evaluators": [{"status": "supported", "candidate": "ACC"}],
    }
    with pytest.raises(RcaEvidenceProjectionError, match="gate_a_projection_invalid"):
        build_gate_a_public_result(projection)


def test_gate_a_rejects_materialization_level_mismatch():
    projection = project_gate_a_report({
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": "aeb",
                "status": "supported",
                "evidence_refs": [{"signal": "AEBReq"}],
            },
        ],
    })
    projection["input_materialized"] = False

    with pytest.raises(RcaEvidenceProjectionError, match="gate_a_projection_invalid"):
        build_gate_a_public_result(projection)


def test_gate_a_rejects_decision_text_forged_into_observation_status():
    projection = project_gate_a_report({
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": "aeb",
                "status": "supported",
                "evidence_refs": [{"signal": "AEBReq"}],
            },
        ],
    })
    projection["evaluator_projection"]["evaluators"][0]["status"] = (
        "ACC is responsible"
    )

    with pytest.raises(RcaEvidenceProjectionError, match="gate_a_projection_invalid"):
        build_gate_a_public_result(projection)


def test_gate_a_rejects_forged_l0_fixed_sentence():
    projection = project_gate_a_report({
        "input_materialized": False,
        "failure_class": "remote_event_not_found",
    })
    projection["abstention"]["message"] = "ACC is responsible"

    with pytest.raises(RcaEvidenceProjectionError, match="gate_a_projection_invalid"):
        build_gate_a_public_result(projection)


@pytest.mark.parametrize("status", ["supported", "refuted"])
def test_gate_a_public_status_requires_safe_evidence_refs(status):
    with pytest.raises(
        RcaEvidenceProjectionError, match="gate_a_observation_evidence_missing"
    ):
        project_gate_a_report({
            "input_materialized": True,
            "rca_evaluators": [{"key": "aeb", "status": status}],
        })


def test_gate_a_all_need_fields_fails_closed():
    with pytest.raises(
        RcaEvidenceProjectionError, match="gate_a_observation_evidence_missing"
    ):
        project_gate_a_report({
            "input_materialized": True,
            "rca_evaluators": [
                {
                    "key": "aeb",
                    "status": "need_fields",
                    "missing_fields": ["AEBReq"],
                },
                {
                    "key": "fcw",
                    "status": "need_fields",
                    "missing_fields": ["FCWReq"],
                },
            ],
        })


def test_gate_a_need_fields_is_not_public_when_safe_observation_exists():
    projection = project_gate_a_report({
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": "aeb",
                "status": "need_fields",
                "missing_fields": ["private_signal"],
            },
            {
                "key": "fcw",
                "status": "refuted",
                "evidence_refs": [
                    {
                        "signal": "FCWReq",
                        "window": [0.1, 0.3],
                        "evidence": "窗口内未观测到 FCW 请求。",
                    }
                ],
            },
        ],
    })

    public = build_gate_a_public_result(projection)

    assert [item["key"] for item in public["evaluator_observations"]] == ["fcw"]
    rendered = json.dumps(public, ensure_ascii=False, sort_keys=True)
    assert "need_fields" not in rendered
    assert "private_signal" not in rendered
    assert "现有证据不支持评测项 fcw" in rendered


def test_gate_a_public_fact_copy_is_capped_at_eight_items():
    projection = project_gate_a_report({
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": f"evaluator_{index}",
                "status": "supported" if index % 2 == 0 else "refuted",
                "evidence_refs": [
                    {
                        "signal": f"signal_{index}",
                        "evidence": f"第 {index} 项观测事实。",
                    }
                ],
            }
            for index in range(10)
        ],
    })

    public = build_gate_a_public_result(projection)
    fact_lines = [
        item["text"]
        for item in public["causal_chain"]["narrative"]
        if item["text"].startswith(("已观测到评测项", "现有证据不支持评测项"))
    ]

    assert len(public["evaluator_observations"]) == 8
    assert len(fact_lines) == 8
    assert public["evaluator_observation_count"] == 10
    assert public["evaluator_observation_omitted_count"] == 2
    assert public["causal_chain"]["narrative"][-1]["text"].startswith("另有 2 项")


def test_gate_a_numeric_evidence_refs_survive_detached_validation():
    projection = project_gate_a_report({
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": "lane_jump",
                "status": "supported",
                "evidence_refs": [
                    {
                        "signal": "Lane2D_C0",
                        "duration_s": 0.5,
                        "max_delta": 0.12,
                        "threshold": 0.075,
                    }
                ],
            }
        ],
    })

    public = build_gate_a_public_result(projection)

    assert public["evaluator_observations"][0]["metrics"] == {
        "duration_s": 0.5,
        "max_delta": 0.12,
        "threshold": 0.075,
    }
    assert "max_delta=0.12" in public["causal_chain"]["narrative"][0]["text"]


def test_gate_a_requires_materialization_attestation_when_flag_is_absent():
    with pytest.raises(
        RcaEvidenceProjectionError, match="gate_a_materialization_state_missing"
    ):
        project_gate_a_report({
            "rca_evaluators": [
                {
                    "key": "aeb",
                    "status": "supported",
                    "evidence_refs": [{"signal": "AEBReq"}],
                }
            ]
        })

    projection = project_gate_a_report({
        "materialization_attested": True,
        "rca_evaluators": [
            {
                "key": "aeb",
                "status": "supported",
                "evidence_refs": [{"signal": "AEBReq"}],
            }
        ],
    })
    assert projection["level"] == "L1_observation"


@pytest.mark.parametrize("bad_field", ["https://internal/foo", "candidate_owner"])
def test_gate_a_rejects_unsafe_signal_field_text(bad_field):
    with pytest.raises(
        RcaEvidenceProjectionError, match="evaluator_evidence_text_forbidden"
    ):
        _project_gate_a_report({
            "input_materialized": True,
            "rca_evaluators": [
                {
                    "key": "aeb",
                    "status": "supported",
                    "evidence_refs": [{"fields": [bad_field]}],
                }
            ],
        }, identifier_binding=build_gate_a_identifier_binding({
            "actual_evaluators": [{"evaluator_id": "aeb", "status": "supported"}],
            "actual_signals": ["AEBReq"],
            "actual_fields": [],
        }))


@pytest.mark.parametrize(
    ("field", "prose"),
    [
        ("evidence", "ACC is at fault."),
        ("reason", "ACC 是责任方。"),
        ("result", "The planner caused this event."),
        ("check", "规划模块导致该问题。"),
        ("name", "Responsibility belongs to perception."),
        ("verdict", "感知应承担责任。"),
    ],
)
def test_gate_a_drops_free_prose_even_when_typed_evidence_exists(field, prose):
    projection = project_gate_a_report({
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": "aeb",
                "status": "supported",
                "evidence_refs": [{"signal": "AEBReq", field: prose}],
            }
        ],
    })

    public = build_gate_a_public_result(projection)
    serialized = json.dumps(
        {"projection": projection, "public": public},
        ensure_ascii=False,
        sort_keys=True,
    )

    assert prose not in serialized
    assert (
        field
        not in projection["evaluator_projection"]["evaluators"][0]["evidence_refs"][0]
    )
    assert "AEBReq" in serialized


@pytest.mark.parametrize(
    "reference",
    [
        {"evidence": "ACC is at fault."},
        {"reason": "ACC 是责任方。"},
    ],
)
def test_gate_a_free_prose_only_evidence_fails_closed(reference):
    with pytest.raises(
        RcaEvidenceProjectionError, match="gate_a_observation_evidence_missing"
    ):
        project_gate_a_report({
            "input_materialized": True,
            "rca_evaluators": [
                {
                    "key": "aeb",
                    "status": "supported",
                    "evidence_refs": [reference],
                }
            ],
        })


def test_gate_a_isolates_unprojectable_evaluator_when_bound_observation_exists():
    binding = build_gate_a_identifier_binding({
        "actual_evaluators": [
            {"evaluator_id": "acc_jerk", "status": "supported"},
            {"evaluator_id": "object_kinematics", "status": "refuted"},
        ],
        "actual_signals": ["ACC_AccelerationRequestMps2"],
        "actual_fields": [],
    })
    projection = _project_gate_a_report({
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": "object_kinematics",
                "status": "refuted",
                "evidence_refs": [{
                    "signal": "OPK_PosX,OPK_RelSpeed",
                    "fields": ["OPK_PosX", "OPK_RelSpeed"],
                }],
            },
            {
                "key": "acc_jerk",
                "status": "supported",
                "evidence_refs": [{"signal": "ACC_AccelerationRequestMps2"}],
            },
        ],
    }, identifier_binding=binding)

    assert [
        item["key"] for item in projection["evaluator_projection"]["evaluators"]
    ] == ["acc_jerk"]


def test_gate_a_quarantines_unbound_responsibility_bytes_from_every_public_surface():
    malicious_key = "ACC_is_at_fault"
    malicious_signal = "control_team_should_own"
    binding = build_gate_a_identifier_binding({
        "actual_evaluators": [
            {"evaluator_id": "acc_jerk", "status": "supported"},
        ],
        "actual_signals": ["ACC_AccelerationRequestMps2"],
        "actual_fields": [],
    })
    projection = _project_gate_a_report({
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": malicious_key,
                "status": "supported",
                "evidence_refs": [{"signal": malicious_signal}],
            },
            {
                "key": "acc_jerk",
                "status": "supported",
                "evidence_refs": [{"signal": "ACC_AccelerationRequestMps2"}],
            },
        ],
    }, identifier_binding=binding)

    serialized = json.dumps(
        {
            "projection": projection,
            "public": build_gate_a_public_result(projection),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    assert malicious_key not in serialized
    assert malicious_signal not in serialized
    assert "ACC_AccelerationRequestMps2" in serialized


@pytest.mark.parametrize(
    ("evaluator", "expected_code"),
    [
        (
            {
                "key": "object_kinematics",
                "status": "refuted",
                "evidence_refs": [{"signal": "OPK_PosX,OPK_RelSpeed"}],
            },
            "evaluator_evidence_text_forbidden",
        ),
        (
            {
                "key": "object_track_quality",
                "status": "refuted",
                "evidence_refs": [{"signal": "OPK_PosX"}],
                "checks": [{"thresholds": {"stable": "yes"}}],
            },
            "evaluator_check_thresholds_invalid",
        ),
    ],
)
def test_gate_a_invalid_only_rethrows_first_exact_projection_error(
    evaluator, expected_code
):
    binding = build_gate_a_identifier_binding({
        "actual_evaluators": [
            {"evaluator_id": evaluator["key"], "status": evaluator["status"]},
        ],
        "actual_signals": ["OPK_PosX"],
        "actual_fields": [],
    })

    with pytest.raises(RcaEvidenceProjectionError, match=expected_code):
        _project_gate_a_report({
            "input_materialized": True,
            "rca_evaluators": [evaluator],
        }, identifier_binding=binding)


def test_gate_a_threshold_metadata_ignores_bounded_structural_facts():
    evaluator = {
        "key": "object_track_quality",
        "status": "refuted",
        "evidence_refs": [{"signal": "OPK_PosX"}],
        "checks": [{
            "thresholds": {
                "must_cover_issue_time": True,
                "min_presence_ratio": 0.3,
                "event_window_s": [-5.0, 1.0],
            },
        }],
    }
    binding = build_gate_a_identifier_binding({
        "actual_evaluators": [
            {"evaluator_id": evaluator["key"], "status": evaluator["status"]},
        ],
        "actual_signals": ["OPK_PosX"],
        "actual_fields": [],
    })

    projection = _project_gate_a_report(
        {"input_materialized": True, "rca_evaluators": [evaluator]},
        identifier_binding=binding,
    )
    assert projection["evaluator_projection"]["evaluators"][0]["checks"][0][
        "thresholds"
    ] == {
        "min_presence_ratio": 0.3,
    }


@pytest.mark.parametrize(
    "threshold_value",
    ([0.0, "later"], [True, 1.0], [], [0.0] * 9),
)
def test_gate_a_threshold_metadata_rejects_unsafe_sequences(threshold_value):
    evaluator = {
        "key": "object_track_quality",
        "status": "refuted",
        "evidence_refs": [{"signal": "OPK_PosX"}],
        "checks": [{"thresholds": {"event_window_s": threshold_value}}],
    }
    binding = build_gate_a_identifier_binding({
        "actual_evaluators": [
            {"evaluator_id": evaluator["key"], "status": evaluator["status"]},
        ],
        "actual_signals": ["OPK_PosX"],
        "actual_fields": [],
    })

    with pytest.raises(
        RcaEvidenceProjectionError, match="evaluator_check_thresholds_invalid"
    ):
        _project_gate_a_report(
            {"input_materialized": True, "rca_evaluators": [evaluator]},
            identifier_binding=binding,
        )


@pytest.mark.parametrize("identifier", ["ACC is at fault.", "规划 模块导致问题"])
def test_gate_a_rejects_prose_disguised_as_structural_identifier(identifier):
    with pytest.raises(
        RcaEvidenceProjectionError, match="evaluator_evidence_text_forbidden"
    ):
        _project_gate_a_report({
            "input_materialized": True,
            "rca_evaluators": [
                {
                    "key": "aeb",
                    "status": "supported",
                    "evidence_refs": [{"signal": identifier}],
                }
            ],
        }, identifier_binding=build_gate_a_identifier_binding({
            "actual_evaluators": [{"evaluator_id": "aeb", "status": "supported"}],
            "actual_signals": ["AEBReq"],
            "actual_fields": [],
        }))


def test_gate_a_requires_sealed_identifier_binding_for_l1():
    source = {
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": "aeb",
                "status": "supported",
                "evidence_refs": [{"signal": "AEBReq"}],
            }
        ],
    }
    with pytest.raises(
        RcaEvidenceProjectionError, match="gate_a_identifier_binding_missing"
    ):
        _project_gate_a_report(source)


def test_gate_a_rejects_responsibility_encoded_in_unbound_identifiers():
    source = {
        "input_materialized": True,
        "rca_evaluators": [
            {
                "key": "ACC_is_at_fault",
                "status": "supported",
                "evidence_refs": [{"signal": "control_team_should_own"}],
            }
        ],
    }
    binding = build_gate_a_identifier_binding({
        "actual_evaluators": [{"evaluator_id": "aeb", "status": "supported"}],
        "actual_signals": ["AEBReq"],
        "actual_fields": [],
    })
    with pytest.raises(
        RcaEvidenceProjectionError, match="gate_a_identifier_binding_mismatch"
    ):
        _project_gate_a_report(source, identifier_binding=binding)
