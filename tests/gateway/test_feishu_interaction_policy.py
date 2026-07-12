from __future__ import annotations

from gateway.feishu_interaction_policy import (
    FeishuInteractionContext,
    build_intake_ack,
    build_integration_tools_runbook_fast_reply,
    classify_integration_tools_intent,
)


def test_common_policy_generic_keeps_existing_feishu_behavior_silent():
    ctx = FeishuInteractionContext(business_line="generic", intent="general")
    assert build_intake_ack(ctx) is None


def test_integration_tools_runbook_question_classifies_as_qa_and_has_safe_fast_reply():
    text = "我想用 logsim 回放一包 mcap，脚本没有纯 help 路径，怎么安全发起？"
    assert classify_integration_tools_intent(text) == "qa_runbook"
    reply = build_integration_tools_runbook_fast_reply(text)
    assert reply is not None
    assert "不要在主仓直接执行业务脚本" in reply
    assert "/mnt/tmp/<task_id>/" in reply
    assert "clean-only 才传 `-co`" in reply
    assert "受限 runner" in reply


def test_integration_tools_execution_request_gets_execution_ack_not_qa_reply():
    text = "帮我执行 mcap 清洗并导出产物"
    assert classify_integration_tools_intent(text) == "execution"
    assert build_integration_tools_runbook_fast_reply(text) is None
    ack = build_intake_ack(FeishuInteractionContext(business_line="integration_tools", intent="execution"))
    assert ack is not None
    assert "任务/卡片" in ack
    assert "受限 runner" in ack


def test_integration_tools_gflags_build_question_gets_specific_candidate_reply():
    text = "我在 mdrive4 分支编译 common 时遇到 find_package(gflags REQUIRED CONFIG) 找不到 gflagsConfig.cmake。环境是 VM clean clone，linux x86_64。这个应该怎么判断和修？"
    assert classify_integration_tools_intent(text) == "qa_runbook"
    reply = build_integration_tools_runbook_fast_reply(text)
    assert reply is not None
    assert "candidate(high)" in reply
    assert "gflags_DIR=/home/mini/.local/minieye-vm-deps/apt-gflags-2.2.2/usr/lib/x86_64-linux-gnu/cmake/gflags" in reply
    assert "ARM runtime" in reply
    assert "伪造 header" in reply
    assert "受限 runner" in reply


def test_integration_tools_ci_pthread_configure_failure_gets_specific_triage_reply():
    text = "GitLab CI 里 dnp build 报错只看到 cmake configure failed 和后面一堆 pthread 检测 failed，我应该先看哪几类日志？怎么分类？"
    assert classify_integration_tools_intent(text) == "qa_runbook"
    reply = build_integration_tools_runbook_fast_reply(text)
    assert reply is not None
    assert "candidate(high)" in reply
    assert "第一个 CMake 配置错误" in reply
    assert "CMakeFiles/CMakeError.log" in reply
    assert "pthread/feature check" in reply
    assert "trigger_pipeline.sh" in reply
    assert "build-repro independent clone" in reply


def test_integration_tools_foxglove_planning_topic_question_gets_specific_fast_reply():
    text = "foxglove 打开后没有 planning topic，应该收集哪些信息？是不是可以直接跑 run_planning_visualization.sh 看看？"
    assert classify_integration_tools_intent(text) == "qa_runbook"
    reply = build_integration_tools_runbook_fast_reply(text)
    assert reply is not None
    assert "candidate(high)" in reply
    assert "不要直接在主仓裸跑 `run_planning_visualization.sh`" in reply
    assert "原始 mcap topic list" in reply
    assert "/mnt/tmp/<task_id>/" in reply
    assert "受治理任务卡片" in reply
