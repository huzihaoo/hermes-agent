"""Shared Feishu interaction policy for PNC business adapters.

This module is intentionally pure/deterministic: it extracts the mature common
interaction contract used by PNC task-card flows (topic anchoring, visible
intake, timeout/failure visibility, and execution-vs-QA separation) without
calling Feishu APIs or mutating shared-state.  Business lines may provide small
adapters (intent/text), but the gateway should enter through the same policy
surface instead of maintaining separate Feishu ingress semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Lane = Literal["fast", "standard", "heavy"]
BusinessLine = Literal["generic", "integration_tools", "g1q3_rca"]
Intent = Literal["qa_runbook", "execution", "ambiguous", "general"]


@dataclass(frozen=True)
class FeishuInteractionContext:
    business_line: BusinessLine = "generic"
    intent: Intent = "general"
    chat_id: str = ""
    thread_id: str = ""
    request_message_id: str = ""
    lane: Lane = "standard"


def classify_integration_tools_intent(text: str) -> Intent:
    """Classify common integration-tools asks without invoking tools/VM.

    The goal is not final semantic routing for every request; it is a safe gate
    that prevents known runbook Q&A from being treated as execution and trying
    long VM commands before a human-visible answer exists.
    """
    compact = str(text or "").lower()
    qa_terms = (
        "怎么", "如何", "注意", "需要注意", "说明", "解释", "安全", "help", "--help",
        "runbook", "脚本没有纯 help", "没有纯help", "无纯 help", "无纯help",
        "收集哪些", "哪些信息", "能不能", "是不是可以", "可以直接", "直接跑",
        "找不到", "cannot find", "not found", "failed", "error", "报错", "遇到", "判断", "修",
    )
    runbook_terms = (
        "logsim", "mcap", "replay", "回放", "mcap-clean", "mcap-translate",
        "foxglove", "build-repro", "编译", "ci", "pipeline", "触发", "flag",
        "gflags", "find_package", "gflagsconfig", "cmake", "common",
    )
    execution_terms = (
        "执行", "跑一下", "跑通", "帮我跑", "开始跑", "清洗", "转换", "真实执行",
        "提交", "部署", "重载", "重启", "产物", "生成", "导出",
    )
    if any(term in compact for term in runbook_terms) and any(term in compact for term in qa_terms):
        return "qa_runbook"
    if any(term in compact for term in execution_terms):
        return "execution"
    if any(term in compact for term in runbook_terms):
        return "ambiguous"
    return "general"


def build_intake_ack(ctx: FeishuInteractionContext) -> str | None:
    """Return a user-visible intake acknowledgement for business Feishu flows.

    Generic Feishu chat keeps existing behavior (None). Business adapters opt in
    to the shared style by supplying a non-generic business_line.
    """
    if ctx.business_line == "generic":
        return None
    if ctx.intent == "qa_runbook":
        return "收到，我按工具知识页/治理规则先给答疑结论；这类问题不会直接触发 VM 长任务。"
    if ctx.intent == "execution":
        return "收到，我会按受治理执行流程接手：先建任务/卡片并校验输入，真实执行只走受限 runner。"
    if ctx.intent == "ambiguous":
        return "收到，我先按问题澄清/方案建议处理；如果要真实执行，会先确认输入、权限和验收口径。"
    return "收到，已接手，会在原话题内回复。"


def build_admission_timeout_fallback(ctx: FeishuInteractionContext) -> str | None:
    """Common fallback text for admission-level stalls.

    This is separate from completion_notice relay fallback: it covers the gap
    before a shared-state/VM task has produced sidecars.
    """
    if ctx.business_line == "generic":
        return None
    return (
        "[PNC intake fallback] 已接手但后端答复超时/未完成。"
        "我会保留在原话题处理；如涉及执行类任务，将转为受治理任务卡片继续跟踪。"
    )


def build_integration_tools_runbook_fast_reply(text: str) -> str | None:
    """Deterministic fast reply for high-confidence integration-tools Q&A.

    Returns None unless the request is clearly a known runbook/safety question.
    This keeps the common Feishu entrypoint responsive while preserving business
    differences in a tiny adapter.
    """
    if classify_integration_tools_intent(text) != "qa_runbook":
        return None
    compact = str(text or "").lower()
    if "gflags" in compact or "gflagsconfig" in compact or "find_package" in compact:
        return (
            "结论（candidate(high)）：这是 linux x86_64 clean clone 缺少合规 gflags dev/config 包或构建链路未注入 gflags prefix，不应靠 ARM runtime `.so`、伪造 header 或裸改主仓绕过。\n"
            "- 项目 CMake 使用 `find_package(gflags REQUIRED CONFIG)` 并链接 `gflags::gflags_shared`，所以需要同时有 header、`gflagsConfig.cmake/gflags-config.cmake` 和匹配的 x86_64 CMake target。\n"
            "- 独立 clone 里若只有 `libgflags*.so` 运行库，尤其是 ARM/aarch64 包，不能用于本机 linux x86_64 构建；Conan/tools/sysroot 也要核 target 名和架构是否匹配。\n"
            "- 已知 VM 候选前缀：`gflags_DIR=/home/mini/.local/minieye-vm-deps/apt-gflags-2.2.2/usr/lib/x86_64-linux-gnu/cmake/gflags`；runner 可注入该 `gflags_DIR` 或对应 `CMAKE_PREFIX_PATH` 后重跑。\n"
            "- 合规修复路径：安装/准备真实 x86_64 gflags dev/config 包，或在项目 CMake 中显式引入兼容 target shim；不要使用 ARM runtime so 或伪造 header。\n"
            "如果要我真实复现，请给 build-repro ref/commit、目标模块和验收口径；我会走 independent clone + 受限 runner，不在主仓直接跑脚本。"
        )
    if ("pthread" in compact and ("cmake" in compact or "configure" in compact)) or ("dnp build" in compact and ("ci" in compact or "gitlab" in compact)):
        return (
            "结论（candidate(high)）：`cmake configure failed` 后面连续 pthread 检测失败，首轮不要直接把 pthread 当根因；先按 CI/build-repro 分类树找“第一个 CMake 配置错误”和对应日志。\n"
            "- 先看 GitLab job raw log 中第一处 `CMake Error`、`-- Configuring incomplete` 之前 100~200 行；后面的 pthread/feature check 往往是 toolchain/linker/依赖探测连带症状。\n"
            "- 同步看 build 目录里的 `CMakeFiles/CMakeError.log` 和 `CMakeFiles/CMakeOutput.log`，确认失败是编译器不可用、链接器/sysroot 问题、包 config 缺失，还是目标模块自身 CMake 断言。\n"
            "- 分类优先级：1) profile/platform/buildtype/module 是否选错；2) third_party/CMake config 包缺失或 target 名不匹配；3) toolchain/sysroot/架构混用；4) 子模块分支/commit 未同步；5) stale cache/build dir；6) release/status artifact 传递失败。\n"
            "- 对 `minieye_dnp_nop`/mdrive4，结论先标 `candidate(high)`；没有 pipeline artifact/job log/VM 回执前不要标 verified，也不要在主仓直接跑 `trigger_pipeline.sh`、`ci_pipeline_orchestrator.py` 或业务脚本 `--help`。\n"
            "如果要我真实复现，请给 pipeline URL/job log、branch/ref、profile/platform/buildtype/module；我会走 build-repro independent clone + 受治理 runner。"
        )
    if "foxglove" in compact or "run_planning_visualization" in compact or "planning topic" in compact:
        return (
            "结论（candidate(high)）：foxglove 打开后没有 planning topic，先收集输入/转换/显示链路证据，不要直接在主仓裸跑 `run_planning_visualization.sh`。\n"
            "- 先确认数据侧：mcap 绝对路径、对应日期/clip、是否经过 mcap-clean/mcap-translate/logsim replay、产物目录和 `/mnt/tmp/<task_id>/`/CIFS 输出路径。\n"
            "- 再确认 topic 侧：原始 mcap topic list、是否存在 planning 相关 topic、topic 名称是否被 translate/remap、时间戳范围、消息数量是否为 0。\n"
            "- 再确认 foxglove 侧：打开的是原始包还是转换产物、layout/config、过滤条件、数据源 URL/本地路径、控制台/导入错误。\n"
            "- `run_planning_visualization.sh` 属业务脚本，不能在主仓直接试跑或用 `--help` 探测；真实排查应走受治理 fixed-CLI/VM runner，资源类步骤放 `/mnt/tmp/<task_id>/`，不要写主仓 flag/cache。\n"
            "如果要我真实排查，请给 mcap/转换产物绝对路径、期望 planning topic 名、foxglove 打开方式和验收口径；我会转成受治理任务卡片。"
        )
    if "logsim" in compact or "mcap" in compact or "回放" in compact:
        return (
            "结论：logsim/mcap 回放应按受治理 fixed-CLI 发起，不要在主仓直接执行业务脚本或 `--help`。\n"
            "- 输入/中间/输出默认放 `/mnt/tmp/<task_id>/`，对外路径对应 `//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/`。\n"
            "- logsim replay 路径需要先 staging input 到 `/mnt/tmp/<task_id>/input/`，清洗产物不要写回主仓或 flag 文件。\n"
            "- clean-only 才传 `-co`；replay 成功判定应包含 `logs/logsim_log.txt` 等产物/日志存在性。\n"
            "- MCAP/foxglove/translate 属 VM heavy 资源类，只能走受限 runner/guarded wrapper，不能裸跑 docker、mcap_service 或业务脚本。\n"
            "如果你要我真实执行，请补充 mcap 绝对路径、期望任务名/输出和验收口径；我会转为任务卡片跟踪。"
        )
    return (
        "结论：这类工具问题先走知识页/源码契约答疑，不直接执行业务脚本。"
        "如需真实执行，请给出输入路径、目标动作和验收口径，我会转为受治理任务卡片。"
    )
