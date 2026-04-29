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
    assert "retrying API call with max_tokens=" in text
    assert "self._ephemeral_max_output_tokens = next_cap" in text
