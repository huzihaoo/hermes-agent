"""Tests for token pricing calculator."""

import pytest

from gateway.observability.pricing import calculate_cost, get_model_price


def test_calculate_cost_claude_opus():
    cost = calculate_cost(1000, 500, "claude-opus-4-6")
    assert cost > 0
    assert cost == pytest.approx(1000 * 15.0 / 1e6 + 500 * 75.0 / 1e6)


def test_calculate_cost_claude_sonnet():
    cost = calculate_cost(1000, 500, "claude-sonnet-4")
    assert cost == pytest.approx(1000 * 3.0 / 1e6 + 500 * 15.0 / 1e6)


def test_calculate_cost_unknown_model_uses_default():
    cost = calculate_cost(1000, 500, "unknown-model-xyz")
    assert cost > 0  # Should use default pricing


def test_calculate_cost_zero_tokens():
    cost = calculate_cost(0, 0, "claude-opus-4-6")
    assert cost == 0.0


def test_calculate_cost_with_provider_prefix():
    cost = calculate_cost(1000, 500, "anthropic/claude-opus-4-6")
    assert cost > 0  # Should strip provider prefix


def test_get_model_price_returns_dict():
    price = get_model_price("claude-opus-4-6")
    assert "input" in price
    assert "output" in price
    assert price["input"] > 0
    assert price["output"] > 0
