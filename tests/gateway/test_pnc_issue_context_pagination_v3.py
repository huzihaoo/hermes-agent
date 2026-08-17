"""Focused production-shape tests for Meegle ``--fields _all`` pagination."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from typing import Any

import pytest

from gateway import pnc_issue_context


PROJECT = "68ef617fb371dc80a10641f7"
ISSUE = "7060326398"
ADDRESS = "mdi download event -u 7d59cb46 -s ./"
ADDR_FIELD = {"key": "field_93aa63", "name": "问题数据地址_PDCL", "value": ADDRESS}
MISSING = object()


def _page(
    fields: Any,
    *,
    has_more: Any,
    token: Any = None,
    total: Any = MISSING,
    issue_id: str | None = ISSUE,
    pagination: Any = MISSING,
) -> tuple[int, str, str]:
    body: dict[str, Any] = {"work_item_fields": fields}
    if issue_id is not None:
        body["work_item_attribute"] = {"work_item_id": issue_id}
    if pagination is MISSING:
        pagination = {"has_more": has_more, "next_page_token": token}
        if total is not MISSING:
            pagination["total"] = total
    body["pagination"] = pagination
    return 0, json.dumps({"data": body}, ensure_ascii=False), ""


def _runner(
    responses: list[Any], *, comments: list[dict[str, Any]] | None = None
) -> tuple[Callable[[list[str]], tuple[int, str, str]], list[list[str]]]:
    calls: list[list[str]] = []
    workitems = iter(responses)

    def run(args: list[str]) -> tuple[int, str, str]:
        calls.append(list(args))
        if args[:2] == ["auth", "status"]:
            return 0, json.dumps({"authenticated": True}), ""
        if args[:2] == ["comment", "list"]:
            return 0, json.dumps({"comments": comments or []}), ""
        response = next(workitems)
        if isinstance(response, BaseException):
            raise response
        return response

    return run, calls


def _read(
    responses: list[Any], *, comments: list[dict[str, Any]] | None = None
) -> tuple[pnc_issue_context.G1Q3IssueReadResult, list[list[str]]]:
    runner, calls = _runner(responses, comments=comments)
    return (
        pnc_issue_context.fetch_g1q3_issue_context_result_via_meegle(
            project_key=PROJECT, work_item_id=ISSUE, runner=runner
        ),
        calls,
    )


def _assert_failed(result: pnc_issue_context.G1Q3IssueReadResult) -> None:
    assert result.status == "read_failed"
    assert result.context_text == ""
    assert result.source == ""
    assert any(item.get("error_class") == "PaginationError" for item in result.errors or [])


def test_real_shape_total_156_with_31_plus_25_fields_succeeds() -> None:
    fields = [
        {"key": f"field_{index:03d}", "name": f"field {index}", "value": index}
        for index in range(55)
    ] + [ADDR_FIELD]
    result, calls = _read([
        _page(fields[:31], has_more=True, token="p2", total=156),
        _page(fields[31:], has_more=False, token="", total=156),
    ])

    assert result.status == "fields_extracted"
    assert ADDRESS in result.context_text
    workitem_calls = [call for call in calls if call[:2] == ["workitem", "get"]]
    assert "--page-token" not in workitem_calls[0]
    assert workitem_calls[1][-2:] == ["--page-token", "p2"]


def test_cli_timeout_exception_and_bad_json_fail_uniformly() -> None:
    for failure in (
        (7, "", "CLI failed"),
        subprocess.TimeoutExpired("meegle", 1),
        RuntimeError("runner crashed"),
        (0, "not-json", ""),
    ):
        result, _ = _read([failure])
        _assert_failed(result)


def test_comments_cannot_upgrade_midstream_failure() -> None:
    result, _ = _read(
        [
            _page([{"key": "field_1", "value": 1}], has_more=True, token="p2"),
            (8, "", "transport failed"),
        ],
        comments=[{"content": f"- 数据地址: {ADDRESS}"}],
    )
    _assert_failed(result)


def test_bad_page_and_field_shapes_fail() -> None:
    failures = [
        (0, json.dumps([]), ""),
        _page([], has_more=False, pagination=[]),
        _page({}, has_more=False),
        _page(["not-an-object"], has_more=False),
    ]
    for response in failures:
        result, _ = _read([response])
        _assert_failed(result)


def test_has_more_is_exact_bool_and_every_page_identity_matches() -> None:
    for response in (
        _page([], has_more="false"),
        _page([], has_more=False, issue_id="999"),
        _page([], has_more=False, issue_id=None),
    ):
        result, _ = _read([response])
        _assert_failed(result)


def test_page_token_must_exist_and_advance() -> None:
    result, _ = _read([_page([], has_more=True, token=None)])
    _assert_failed(result)
    result, _ = _read([
        _page([{"key": "field_1", "value": 1}], has_more=True, token="p2"),
        _page([{"key": "field_2", "value": 2}], has_more=True, token="p2"),
    ])
    _assert_failed(result)


def test_page_limit_is_fail_closed() -> None:
    responses = [
        _page([{"key": f"field_{index}"}], has_more=True, token=f"p{index + 2}")
        for index in range(pnc_issue_context._MEEGLE_FIELD_PAGE_LIMIT)
    ]
    result, calls = _read(responses)
    _assert_failed(result)
    assert len([call for call in calls if call[:2] == ["workitem", "get"]]) == 12


def test_total_is_optional_but_must_be_valid_and_stable_when_present() -> None:
    result, _ = _read([
        _page([{"key": "field_1"}], has_more=True, token="p2"),
        _page([ADDR_FIELD], has_more=False),
    ])
    assert result.status == "fields_extracted"
    for responses in (
        [_page([], has_more=False, total="156")],
        [
            _page([{"key": "field_1"}], has_more=True, token="p2", total=156),
            _page([ADDR_FIELD], has_more=False, total=157),
        ],
    ):
        result, _ = _read(responses)
        _assert_failed(result)


def test_conflicting_duplicate_key_is_rejected() -> None:
    result, _ = _read([
        _page([{"key": "field_1", "value": 1}], has_more=True, token="p2"),
        _page([{"key": "field_1", "value": 2}], has_more=False),
    ])
    _assert_failed(result)


def test_only_legacy_fields_mapping_may_omit_pagination() -> None:
    result, _ = _read([
        (0, json.dumps({"id": ISSUE, "fields": {"问题数据地址_PDCL": ADDRESS}}), "")
    ])
    assert result.status == "fields_extracted"
    result, _ = _read([
        (0, json.dumps({"data": {"work_item_id": ISSUE, "work_item_fields": [ADDR_FIELD]}}), "")
    ])
    _assert_failed(result)


def test_incomplete_pagination_never_calls_mcp() -> None:
    runner, _ = _runner([
        _page([{"key": "field_1"}], has_more=True, token="p2"),
        (1, "", "midstream failure"),
    ])
    mcp_calls: list[str] = []
    result = pnc_issue_context.fetch_g1q3_issue_context_result(
        project_key=PROJECT,
        work_item_id=ISSUE,
        meegle_runner=runner,
        tool_caller=lambda name, _args: mcp_calls.append(name),
        use_mcp_fallback=True,
    )
    _assert_failed(result)
    assert mcp_calls == []


@pytest.mark.parametrize("meegle_failure", ["unauthenticated", "startup"])
def test_mcp_fallback_requires_direct_nonempty_pdcl_field(
    monkeypatch: pytest.MonkeyPatch, meegle_failure: str
) -> None:
    monkeypatch.delenv("HERMES_G1Q3_MCP_FALLBACK", raising=False)
    monkeypatch.delenv("HERMES_G1Q3_MCP_AUTODEGRADE", raising=False)
    captures: list[dict[str, Any]] = []
    monkeypatch.setattr(
        pnc_issue_context,
        "_capture_g1q3_issue_context",
        lambda **kwargs: captures.append(kwargs),
    )

    def failed_meegle(args: list[str]) -> tuple[int, str, str]:
        if meegle_failure == "startup":
            raise FileNotFoundError("meegle unavailable")
        return 0, json.dumps({"authenticated": False}), ""

    def partial_mcp(name: str, _args: dict[str, Any]) -> dict[str, str]:
        if name == "mcp_feishu_project_get_workitem_brief":
            payload = {
                "work_item_attribute": {"work_item_id": ISSUE, "work_item_name": "partial"},
                "work_item_fields": [{"key": "field_842fc8", "value": "partial"}],
            }
        else:
            payload = {"comments": [{"content": f"- 数据地址: {ADDRESS}"}]}
        return {"result": json.dumps(payload, ensure_ascii=False)}

    result = pnc_issue_context.fetch_g1q3_issue_context_result(
        project_key=PROJECT,
        work_item_id=ISSUE,
        meegle_runner=failed_meegle,
        tool_caller=partial_mcp,
    )
    assert result.status == "read_failed"
    assert result.context_text == ""
    assert captures == []
    assert any(
        item.get("error_class") == "FieldSetCompletenessUnproven"
        for item in result.errors or []
    )
