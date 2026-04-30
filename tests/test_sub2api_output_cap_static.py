from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUN_AGENT = ROOT / "run_agent.py"


def test_sub2api_gpt5_default_output_caps_are_explicit():
    text = RUN_AGENT.read_text(encoding="utf-8")

    assert "HERMES_DEFAULT_MAX_OUTPUT_TOKENS" in text
    assert "def _default_max_tokens_for_current_model" in text
    assert "sub2api.minieye.tech" in text
    assert "return 32768" in text
    assert "return 65536" in text


def test_responses_and_chat_paths_use_effective_default_max_tokens():
    text = RUN_AGENT.read_text(encoding="utf-8")

    assert "self.max_tokens if self.max_tokens is not None else self._default_max_tokens_for_current_model()" in text
    assert 'kwargs["max_output_tokens"] = effective_max_tokens' in text
    assert "api_kwargs.update(self._max_tokens_param(effective_max_tokens))" in text


def test_fallback_activation_precompresses_before_retrying_smaller_context():
    text = RUN_AGENT.read_text(encoding="utf-8")

    assert "self._fallback_just_activated = True" in text
    assert "Fallback context requires compaction" in text
    assert "_fallback_compressor.should_compress(approx_tokens)" in text
    assert "restart_with_compressed_messages = True" in text


def test_truncated_tool_call_retry_raises_one_call_output_cap():
    text = RUN_AGENT.read_text(encoding="utf-8")

    assert "def _consume_ephemeral_or_default_max_tokens" in text
    assert "def _next_truncated_tool_call_max_tokens" in text
    assert "truncated_tool_call_retries < 2" in text
    assert "Tool call hit output limit — retrying with larger max_tokens=" in text
    assert "self._ephemeral_max_output_tokens = next_cap" in text


def test_length_continuation_retry_raises_one_call_output_cap():
    text = RUN_AGENT.read_text(encoding="utf-8")

    assert "Requesting continuation " in text
    assert "with max_tokens=" in text
    assert "self._ephemeral_max_output_tokens = next_cap" in text


def test_repeated_length_truncation_degrades_to_bounded_partial_not_error():
    text = RUN_AGENT.read_text(encoding="utf-8")

    assert "Stop expanding" in text
    assert "<=1200 words" in text
    assert "showing a bounded partial answer instead of failing the turn" in text
    assert '"warning": "Response remained truncated after 3 continuation attempts"' in text
    assert '"error": "Response remained truncated after 3 continuation attempts"' not in text


def test_truncation_recovery_messages_are_not_hard_error_wording():
    text = RUN_AGENT.read_text(encoding="utf-8")

    assert "Provider stopped at output limit; Hermes is auto-recovering" in text
    assert "Tool call hit output limit — retrying with larger max_tokens=" in text
    assert "Response truncated (finish_reason='length') - model hit max output tokens" not in text


def test_tool_result_persistence_happens_before_callbacks_and_verbose_logging():
    text = RUN_AGENT.read_text(encoding="utf-8")

    concurrent_start = text.index("# ── Post-execution: display per-tool results")
    sequential_start = text.index("def _execute_tool_calls_sequential")
    concurrent_block = text[concurrent_start:sequential_start]
    sequential_block = text[sequential_start:text.index("    def _emit_context_pressure", sequential_start)]

    for block in (concurrent_block, sequential_block):
        persist_idx = block.index("function_result = maybe_persist_tool_result")
        verbose_idx = block.index("if self.verbose_logging:", persist_idx)
        callback_idx = block.index("if self.tool_complete_callback:", persist_idx)
        message_idx = block.index('"role": "tool"', persist_idx)
        assert persist_idx < verbose_idx < callback_idx < message_idx


def test_preflight_compression_is_fallback_aware():
    text = RUN_AGENT.read_text(encoding="utf-8")

    assert "_preflight_threshold_reason = \"fallback\"" in text
    assert "_fb_threshold = int(_fb_ctx * getattr(self.context_compressor" in text
    assert "primary transient failure → fallback activation" in text
    assert "_preflight_threshold_tokens" in text


def test_post_tool_empty_recovery_is_compact_and_history_aware():
    text = RUN_AGENT.read_text(encoding="utf-8")

    assert "def _should_nudge_after_empty_tool_response" in text
    assert "def _is_tool_output_followed_by_empty_recovery" in text
    assert "def _build_post_tool_empty_recovery_message" in text
    assert "Provider returned an empty post-tool message" in text
    assert "recovering with a compact continuation prompt" in text
    assert "Do not return an empty message" in text
    assert "You just executed tool calls but returned an empty response" not in text
    assert "nudging to continue" not in text


def test_hidden_truncated_tool_call_json_retries_with_larger_cap_not_hard_error():
    text = RUN_AGENT.read_text(encoding="utf-8")

    assert "Tool call arguments were truncated" in text
    assert "finish_reason='tool_calls'" in text
    assert "retrying with larger" in text
    assert "Tool call arguments remained truncated after recovery attempts" in text
    assert "I did not execute incomplete tool calls" in text
    assert '"warning": "Tool call arguments remained truncated after recovery attempts"' in text


def test_first_response_truncation_degrades_to_concise_summary_not_hard_fail():
    text = RUN_AGENT.read_text(encoding="utf-8")

    assert "First response hit output limit" in text
    assert "retrying with concise summary" in text
    assert "Your first response was truncated by the provider output limit" in text
    assert "First response remained truncated after recovery attempts" in text
    assert "First response truncated - cannot recover" not in text
    assert "First response truncated due to output length limit" not in text


def test_long_task_state_sidecar_records_artifacts_and_recovery_fields():
    text = RUN_AGENT.read_text(encoding="utf-8")

    assert "def _update_long_task_state_sidecar" in text
    assert "def _long_task_state_path" in text
    assert "task-state" in text
    assert '"recent_events"' in text
    assert '"artifacts"' in text
    assert '"verification"' in text
    assert '"blockers"' in text
    assert "_update_long_task_state_sidecar(" in text
    assert "Use recent_events/artifacts/verification/blockers as recovery hints" in text


def test_long_task_sidecar_summary_is_injected_before_compression():
    text = RUN_AGENT.read_text(encoding="utf-8")

    assert "def _load_long_task_state_summary" in text
    assert "def _inject_long_task_state_for_compression" in text
    assert "Long-task structured recovery state" in text
    assert "Preserve these facts during compression/fallback" in text
    assert "_sidecar_injected = self._inject_long_task_state_for_compression(messages, task_id)" in text
    assert 'm.get("_long_task_state_sidecar")' in text
