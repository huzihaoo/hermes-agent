"""Tests for concurrency control and token quota."""

import asyncio
import pytest

from gateway.concurrency import UserConcurrencyLimiter, TokenQuotaManager


# --- Concurrency Limiter ---

@pytest.mark.asyncio
async def test_limiter_allows_within_limit():
    limiter = UserConcurrencyLimiter(max_concurrent=3)
    assert await limiter.acquire("alice") is True
    assert await limiter.acquire("alice") is True
    assert await limiter.acquire("alice") is True
    assert limiter.get_active("alice") == 3


@pytest.mark.asyncio
async def test_limiter_blocks_over_limit():
    limiter = UserConcurrencyLimiter(max_concurrent=2)
    assert await limiter.acquire("alice") is True
    assert await limiter.acquire("alice") is True
    assert await limiter.acquire("alice") is False  # Over limit


@pytest.mark.asyncio
async def test_limiter_release_frees_slot():
    limiter = UserConcurrencyLimiter(max_concurrent=1)
    assert await limiter.acquire("alice") is True
    assert await limiter.acquire("alice") is False
    
    await limiter.release("alice")
    assert await limiter.acquire("alice") is True


@pytest.mark.asyncio
async def test_limiter_users_independent():
    limiter = UserConcurrencyLimiter(max_concurrent=1)
    assert await limiter.acquire("alice") is True
    assert await limiter.acquire("bob") is True  # Different user


# --- Token Quota ---

def test_quota_default_limit(tmp_path):
    mgr = TokenQuotaManager(db_path=tmp_path / "quota.db", default_monthly_limit=100_000)
    assert mgr.get_limit("alice") == 100_000


def test_quota_custom_limit(tmp_path):
    mgr = TokenQuotaManager(db_path=tmp_path / "quota.db")
    mgr.set_limit("alice", 200_000)
    assert mgr.get_limit("alice") == 200_000


def test_quota_consume_within_limit(tmp_path):
    mgr = TokenQuotaManager(db_path=tmp_path / "quota.db", default_monthly_limit=10_000)
    assert mgr.consume("alice", 5_000, cost_usd=0.25) is True
    
    usage = mgr.get_usage("alice")
    assert usage["tokens_used"] == 5_000
    assert usage["tokens_remaining"] == 5_000
    assert usage["over_limit"] is False


def test_quota_consume_over_limit(tmp_path):
    mgr = TokenQuotaManager(db_path=tmp_path / "quota.db", default_monthly_limit=10_000)
    mgr.consume("alice", 8_000)
    assert mgr.consume("alice", 5_000) is False  # Over limit
    
    usage = mgr.get_usage("alice")
    assert usage["over_limit"] is True


def test_quota_check(tmp_path):
    mgr = TokenQuotaManager(db_path=tmp_path / "quota.db", default_monthly_limit=10_000)
    assert mgr.check_quota("alice") is True
    
    mgr.consume("alice", 10_000)
    assert mgr.check_quota("alice") is False


def test_quota_usage_percent(tmp_path):
    mgr = TokenQuotaManager(db_path=tmp_path / "quota.db", default_monthly_limit=10_000)
    mgr.consume("alice", 7_500)
    
    usage = mgr.get_usage("alice")
    assert usage["usage_percent"] == 75.0
