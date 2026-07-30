from __future__ import annotations

from scripts import pnc_rca_answer_key as answer_key


class _Client:
    def detail(self, work_item_id: str) -> dict[str, str]:
        return {
            "pdcl_data": f"mdi download event -u {work_item_id} -s ./",
            "root_cause_text": f"模块{work_item_id}现象{work_item_id}证据{work_item_id}",
        }


def _manifest() -> dict:
    return answer_key.fetch_fixed_answer_key(_Client())


def test_fetch_uses_correct_truth_and_pdcl_fields_only():
    manifest = _manifest()

    assert manifest["truth_field"] == "field_842fc8"
    assert manifest["forbidden_truth_field"] == "field_9193cb"
    assert manifest["case_count"] == 10
    assert all(row["pdcl_command_valid"] for row in manifest["cases"])
    assert all(row["human_root_cause_present"] for row in manifest["cases"])


def test_compare_requires_no_report_and_abstention_for_fixed_cases():
    own_results = []
    for work_item_id, policy in answer_key.FIXED_CASE_POLICIES.items():
        row = {"work_item_id": work_item_id, "action": policy["expected_action"]}
        if policy["expected_action"] == "report":
            row["own_conclusion"] = {
                "module": f"模块{work_item_id}",
                "phenomenon": f"现象{work_item_id}",
                "evidence_anchors": [f"证据{work_item_id}"],
            }
        own_results.append(row)

    report = answer_key.compare_answer_key(_manifest(), own_results)

    assert report["counts"] == {"一致": 9, "不一致": 0, "我们弃权": 1}
    assert next(row for row in report["rows"] if row["work_item_id"] == "7056845775")["result"] == "一致"
    assert next(row for row in report["rows"] if row["work_item_id"] == "7055295349")["result"] == "我们弃权"


def test_compare_fails_when_negative_case_is_reported():
    own_results = []
    for work_item_id, policy in answer_key.FIXED_CASE_POLICIES.items():
        action = policy["expected_action"]
        row = {"work_item_id": work_item_id, "action": action}
        if action == "report":
            row["own_conclusion"] = {
                "module": f"模块{work_item_id}",
                "phenomenon": f"现象{work_item_id}",
                "evidence_anchors": [f"证据{work_item_id}"],
            }
        own_results.append(row)
    next(row for row in own_results if row["work_item_id"] == "7057052984")["action"] = "report"

    report = answer_key.compare_answer_key(_manifest(), own_results)

    assert next(row for row in report["rows"] if row["work_item_id"] == "7057052984")["result"] == "不一致"
