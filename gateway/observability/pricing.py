"""Token pricing calculator for LLM cost tracking."""

from typing import Dict

# Prices per 1M tokens (USD)
PRICING: Dict[str, Dict[str, float]] = {
    # Anthropic
    "claude-opus-4-6": {"input": 15.0, "output": 75.0},
    "claude-sonnet-4": {"input": 3.0, "output": 15.0},
    "claude-haiku-3.5": {"input": 0.80, "output": 4.0},
    # OpenAI
    "gpt-4o": {"input": 2.5, "output": 10.0},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "o3": {"input": 10.0, "output": 40.0},
    "o4-mini": {"input": 1.10, "output": 4.40},
    # Google
    "gemini-2.5-pro": {"input": 1.25, "output": 10.0},
    "gemini-2.5-flash": {"input": 0.15, "output": 0.60},
    "gemini-3-flash-preview": {"input": 0.15, "output": 0.60},
    # DeepSeek
    "deepseek-r1": {"input": 0.55, "output": 2.19},
    "deepseek-v3": {"input": 0.27, "output": 1.10},
}

# Default pricing for unknown models
DEFAULT_PRICE = {"input": 5.0, "output": 15.0}


def _normalize_model_name(model: str) -> str:
    """Strip provider prefix (e.g. 'anthropic/claude-opus-4-6' -> 'claude-opus-4-6')."""
    if "/" in model:
        model = model.split("/", 1)[1]
    return model.strip().lower()


def get_model_price(model: str) -> Dict[str, float]:
    """Get pricing for a model. Returns default if unknown."""
    normalized = _normalize_model_name(model)
    
    # Exact match
    if normalized in PRICING:
        return PRICING[normalized]
    
    # Partial match (e.g. "claude-opus" matches "claude-opus-4-6")
    for key, price in PRICING.items():
        if normalized.startswith(key) or key.startswith(normalized):
            return price
    
    return DEFAULT_PRICE


def calculate_cost(input_tokens: int, output_tokens: int, model: str) -> float:
    """Calculate cost in USD for a given number of tokens."""
    price = get_model_price(model)
    return (input_tokens * price["input"] + output_tokens * price["output"]) / 1_000_000
