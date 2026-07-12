"""Tests for gateway.integration_tools_intake field extraction.

Corpus mirrors real integration_tools failure modes (de-identified). The point is
to lock in two root-cause fixes:
  R1: messages WITHOUT the literal "mdrive4" still carry signals (no short-circuit).
  P3: fields the user already gave are extracted (so the clarifier won't re-ask).
"""
from gateway.integration_tools_intake import IntakeFields, extract_intake_fields


def test_clean_request_without_mdrive4_literal_has_signal():
    # R1: no "mdrive4" word, but a clean action + mcap path => must carry a signal.
    f = extract_intake_fields("清洗这个 /mnt/tmp/a.mcap")
    assert f.signals_any() is True
    assert f.action == "clean"
    assert f.mcap_path == "/mnt/tmp/a.mcap"


def test_foxglove_no_planning_topic_without_mdrive4_has_signal():
    # R1 + diagnostic: foxglove/topic phrasing, no "mdrive4".
    f = extract_intake_fields("foxglove 打开没有 planning topic，包在 /mnt/x.mcap")
    assert f.signals_any() is True
    assert f.action == "diagnostic"
    assert f.mcap_path == "/mnt/x.mcap"
    assert f.topic_or_tick is not None


def test_translate_beats_clean_when_both_present():
    f = extract_intake_fields("把 /mnt/tmp/b.mcap 转成 foxglove 可用格式")
    assert f.action == "translate"
    assert f.mcap_path == "/mnt/tmp/b.mcap"


def test_owner_seeded_from_originator():
    # P3: originator is the default acceptor => owner extracted, clarifier skips owner.
    f = extract_intake_fields("清洗 /mnt/tmp/c.mcap", originator="ou_abc")
    assert f.owner == "ou_abc"


def test_owner_explicit_field_wins():
    f = extract_intake_fields("owner: 胡子豪 清洗 /mnt/tmp/d.mcap", originator="ou_abc")
    assert f.owner == "胡子豪"


def test_pure_greeting_has_no_signal():
    # Must NOT over-trigger: a greeting carries no integration_tools signal.
    f = extract_intake_fields("在吗 上午好")
    assert f.signals_any() is False
    assert f.action is None
    assert f.mcap_path is None


def test_build_request_signal():
    f = extract_intake_fields("帮我看下 mdrive4 编译报错 gflags 找不到")
    assert f.signals_any() is True
    assert f.action == "build"
    assert f.project == "mdrive4"


def test_branch_extracted_when_given():
    f = extract_intake_fields("编译 mdrive4 分支: dev-d4q2-dnp-release-cdr-260420")
    assert f.branch == "dev-d4q2-dnp-release-cdr-260420"


def test_as_dict_roundtrip_keys():
    f = extract_intake_fields("清洗 /mnt/tmp/e.mcap")
    d = f.as_dict()
    assert set(d) == {
        "mcap_path", "owner", "project", "action",
        "branch", "output_req", "topic_or_tick",
    }


def test_empty_message_is_inert():
    f = extract_intake_fields("")
    assert f == IntakeFields()
    assert f.signals_any() is False


# --- v2 classifier tests (R1: no short-circuit; P3: no re-ask) --------------
from gateway.integration_tools_intake import classify_integration_tools_intake_v2 as cls


def test_v2_clean_without_mdrive4_not_general():
    # R1: the exact bug — "清洗 /mnt/..." with no "mdrive4" used to return general/''.
    r = cls("清洗这个 /mnt/tmp/a.mcap", originator="ou_x")
    assert r["kind"] != "general"
    assert r["reply_hint"] != ""
    assert r["extracted_fields"]["mcap_path"] == "/mnt/tmp/a.mcap"


def test_v2_diagnostic_does_not_reask_given_path():
    # P3: path + topic provided => missing_fields must not ask for path again.
    r = cls("foxglove 没有 planning topic，包在 /mnt/x.mcap")
    assert r["kind"] == "mcap_diagnostic_request"
    assert "mcap/转换产物绝对路径" not in r["missing_fields"]


def test_v2_complete_clean_auto_dispatches():
    r = cls("owner: 胡子豪 项目 mdrive4 清洗 /mnt/tmp/b.mcap", originator="ou_x")
    assert r["kind"] == "mcap-clean_execution"
    assert r["auto_dispatch"]["cli"] == "mcap-clean"
    assert r["auto_dispatch"]["input"] == "/mnt/tmp/b.mcap"


def test_v2_greeting_stays_general():
    r = cls("在吗 上午好")
    assert r["kind"] == "general"
    assert r["status"] == "intake_checked"


def test_v2_question_closed_no_task():
    r = cls("mdrive4 这个工具能用吗？")
    assert r["kind"] == "question"
    assert r["status"] == "closed"


def test_v2_build_skips_branch_when_given():
    r = cls("编译 mdrive4 分支: dev-d4q2-260420")
    assert r["kind"] == "mdrive4_decision_and_planning_build"
    assert "目标分支/commit" not in r["missing_fields"]


def test_v2_every_branch_has_extracted_fields():
    for msg in ["在吗", "清洗 /mnt/a.mcap", "mdrive4 能用吗？", "拉取 drs 产物"]:
        assert "extracted_fields" in cls(msg)
