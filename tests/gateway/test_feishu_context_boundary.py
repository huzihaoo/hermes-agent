import unittest

from gateway.config import Platform
from gateway.feishu_reply import (
    FEISHU_CONTEXT_AUTO_COMPACT_TEXT,
    sanitize_feishu_final_response,
    sanitize_feishu_inbound_text,
)


class TestFeishuContextBoundary(unittest.TestCase):
    def test_synthetic_topic_history_with_bot_noise_is_compacted_to_latest_user_text(self):
        raw = """@胡子豪的小助手 你好
5 条话题回复
胡子豪的小助手
机器人
网关
19:55
你好，我在。有什么要我处理的？
邬乐飞
19:55
想问一下当前RCA归因相关的设计和现状，以及有哪些相关的skills，可以清楚的讲解一下
邬乐飞
19:56
@胡子豪的小助手 想问一下当前RCA归因相关的设计和现状，以及有哪些相关的skills，可以清楚的讲解一下
胡子豪的小助手
机器人
网关
19:56
⚙️ 正在处理请求... (×8)
胡子豪的小助手
机器人
网关
19:58
这轮对话太长了，模型已经放不下完整上下文。
可以先发 /compact 压缩一下，或者 /reset 开一个新会话。"""

        cleaned = sanitize_feishu_inbound_text(Platform.FEISHU, raw)

        self.assertIn("想问一下当前RCA归因相关的设计和现状", cleaned)
        self.assertIn("[Feishu topic history omitted", cleaned)
        self.assertLess(len(cleaned), 260)
        self.assertNotIn("⚙️ 正在处理请求", cleaned)
        self.assertNotIn("可以先发 /compact", cleaned)
        self.assertNotIn("胡子豪的小助手\n机器人", cleaned)

    def test_synthetic_topic_history_prefers_latest_new_message_over_old_root_request(self):
        raw = """刘孝炜
20:17
@胡子豪的小助手 参考文档告诉我如何查看mcap文件的消息列表和消息内容
4 条话题回复
胡子豪的小助手
机器人
网关
20:18
✅ Approved for session
✅ Approved for session by 胡子豪
胡子豪的小助手
机器人
网关
20:19
这轮上下文接近上限，我会先压缩后继续，只保留任务边界和关键状态。
刘孝炜
20:20
@胡子豪的小助手 继续
胡子豪的小助手
机器人
网关
20:21
这轮上下文接近上限，我会先压缩后继续，只保留任务边界和关键状态。
新消息
刘孝炜
20:21
这个上下文为什么超了?以及之前治理的VM coding agent的有符合预期生效么"""

        cleaned = sanitize_feishu_inbound_text(Platform.FEISHU, raw)

        self.assertIn("这个上下文为什么超了", cleaned)
        self.assertIn("VM coding agent", cleaned)
        self.assertNotIn("参考文档告诉我如何查看mcap文件的消息列表和消息内容", cleaned)
        self.assertNotIn("Approved for session", cleaned)
        self.assertNotIn("这轮上下文接近上限", cleaned)
        self.assertNotIn("新消息", cleaned)
        self.assertIn("[Feishu topic history omitted", cleaned)

    def test_non_feishu_text_is_unchanged(self):
        raw = "5 条话题回复\n胡子豪的小助手\n⚙️ 正在处理请求...\n用户问题"
        self.assertEqual(sanitize_feishu_inbound_text(Platform.TELEGRAM, raw), raw)

    def test_normal_feishu_text_is_unchanged(self):
        raw = "想问一下当前RCA归因相关的设计和现状"
        self.assertEqual(sanitize_feishu_inbound_text(Platform.FEISHU, raw), raw)

    def test_feishu_context_failure_text_is_auto_compact_not_user_choice(self):
        response = sanitize_feishu_final_response(
            Platform.FEISHU,
            "human",
            "⚠️ Session too large for the model's context window.\nUse /compact to compress the conversation, or /reset to start fresh.",
            failed=True,
        )

        self.assertEqual(response, FEISHU_CONTEXT_AUTO_COMPACT_TEXT)
        self.assertNotIn("/compact", response)
        self.assertNotIn("/reset", response)
        self.assertNotIn("这轮上下文接近上限", response)


    def test_feishu_final_response_keeps_normal_context_prose(self):
        response = sanitize_feishu_final_response(
            Platform.FEISHU,
            "human",
            "因果链为空的原因是 viewer 读 rca_receipt 路径而产物只写了顶层，context 不是这次结论的根因。",
            failed=False,
        )

        self.assertEqual(
            response,
            "因果链为空的原因是 viewer 读 rca_receipt 路径而产物只写了顶层，context 不是这次结论的根因。",
        )

    def test_feishu_final_response_replaces_true_context_failure_with_honest_copy(self):
        response = sanitize_feishu_final_response(
            Platform.FEISHU,
            "human",
            "Session too large for the model's context window",
            failed=True,
        )

        self.assertEqual(response, FEISHU_CONTEXT_AUTO_COMPACT_TEXT)
        self.assertIn("这条我没处理完", response)
        self.assertIn("请 @我重发", response)
        self.assertNotIn("不需要你手动处理", response)

    def test_feishu_context_interim_status_is_suppressed(self):
        from gateway.feishu_reply import sanitize_feishu_visible_text

        response = sanitize_feishu_visible_text(
            Platform.FEISHU,
            "human",
            "Session too large for the model's context window; compacting",
        )

        self.assertEqual(response, "")

    def test_feishu_context_interim_status_suppresses_old_manual_compact_copy(self):
        from gateway.feishu_reply import sanitize_feishu_visible_text

        response = sanitize_feishu_visible_text(
            Platform.FEISHU,
            "human",
            "这轮上下文接近上限，我会先压缩后继续，只保留任务边界和关键状态。可以先发 /compact 压缩一下，或者 /reset 开一个新会话。",
        )

        self.assertEqual(response, "")

    def test_feishu_final_response_never_leaks_old_manual_compact_copy(self):
        response = sanitize_feishu_final_response(
            Platform.FEISHU,
            "human",
            "这轮上下文接近上限，我会先压缩后继续，只保留任务边界和关键状态。可以先发 /compact 压缩一下，或者 /reset 开一个新会话。",
            failed=True,
        )

        self.assertEqual(response, FEISHU_CONTEXT_AUTO_COMPACT_TEXT)
        self.assertNotIn("这轮上下文接近上限", response)
        self.assertNotIn("/compact", response)
        self.assertNotIn("/reset", response)

    def test_feishu_visible_text_suppresses_generic_processing_bubble(self):
        from gateway.feishu_reply import sanitize_feishu_visible_text

        response = sanitize_feishu_visible_text(
            Platform.FEISHU,
            "human",
            "⚙️ 正在处理请求... (×8)",
        )

        self.assertEqual(response, "")


if __name__ == "__main__":
    unittest.main()


def test_feishu_inbound_long_issue_card_is_folded_to_structured_fields():
    long_description = "车辆在乡村道路雨天正常行驶时，对向车压中心线，自车误触发；" * 80
    raw = f"""转发飞书问题卡片
work_item_id：7017699515
title：G1Q3_0938-AWB-宁德-乡村道路雨天正常行驶对向车道小轿车压中心线行驶，自车误触发
状态：处理中
优先级：P1
严重程度：S2
负责人：刘旭
缺陷描述：{long_description}
复现步骤：打开附件 mcap 后从 100s 开始查看，多次重复触发。
实际结果：主车 AEB 误触发。
期望结果：不触发。
"""

    cleaned = sanitize_feishu_inbound_text(Platform.FEISHU, raw)

    assert cleaned.splitlines()[0] == "飞书问题 7017699515"
    assert "work_item_id：7017699515" not in cleaned
    assert "title：G1Q3_0938-AWB" in cleaned
    assert "status：处理中" in cleaned
    assert "priority：P1" in cleaned
    assert "severity：S2" in cleaned
    assert "assignee：刘旭" in cleaned
    assert "[问题卡片正文已折叠，work_item_id=7017699515]" in cleaned
    assert "车辆在乡村道路雨天正常行驶" not in cleaned
    assert len(cleaned) < 500


def test_feishu_inbound_short_user_followup_is_not_folded():
    raw = "为啥因果链空？卡在哪一步了"

    cleaned = sanitize_feishu_inbound_text(Platform.FEISHU, raw)

    assert cleaned == raw


def test_g1q3_main_session_handoff_remains_dispatch_only():
    from pathlib import Path

    source = Path("gateway/run.py").read_text(encoding="utf-8")
    start = source.index("def _submit_g1q3_rca_status_handoff")
    end = source.index("def _resolve_runtime_agent_kwargs", start)
    handoff_source = source[start:end]
    forbidden = ["report_builder", "decode_raw", "mcap_service", "mcap_data_translate"]

    for token in forbidden:
        assert token not in handoff_source
    assert "vm_task_submit" in handoff_source
    assert "run_rca_auto_pipeline.py" in handoff_source  # VM command suggestion string is allowed.


def test_feishu_topic_history_sanitizer_still_omits_bot_self_history_after_issue_card_change():
    raw = """继续
3 条话题回复
胡子豪的小助手
机器人
网关
19:55
⚙️ 正在处理请求... (×2)
同事A: 保留一点人类上下文
Cronjob Response: watchdog
"""

    cleaned = sanitize_feishu_inbound_text(Platform.FEISHU, raw)

    assert cleaned.startswith("继续")
    assert "[Feishu topic history omitted." in cleaned
    assert "同事A" in cleaned
    assert "Cronjob Response" not in cleaned
    assert "⚙️ 正在处理请求" not in cleaned


def test_folded_issue_card_still_feeds_g1q3_handoff_work_item_id():
    from gateway.pnc_group_binding import _extract_issue_work_item_id, evaluate_pnc_group_request

    long_description = "车辆在乡村道路雨天正常行驶时，对向车压中心线，自车误触发；" * 80
    raw = f"""转发飞书问题卡片
work_item_id：7017699515
title：G1Q3_0938-AWB-宁德-乡村道路雨天正常行驶对向车道小轿车压中心线行驶，自车误触发
状态：处理中
优先级：P1
严重程度：S2
负责人：刘旭
缺陷描述：{long_description}
"""

    folded = sanitize_feishu_inbound_text(Platform.FEISHU, raw)
    decision = evaluate_pnc_group_request(
        platform="feishu",
        chat_id="oc_6cfc782212009ff4cd815349909dd423",
        text=folded,
    )

    assert folded.splitlines()[0] == "飞书问题 7017699515"
    assert _extract_issue_work_item_id(folded) == "7017699515"
    assert decision.decision == "accepted"
    assert decision.template_id == "rca_issue_intake"
    assert decision.handoff_contract["work_item_id"] == "7017699515"
