"""Tests for the API server bind-address startup guard.

Validates that is_network_accessible() correctly classifies addresses and
that connect() refuses to start on non-loopback without API_SERVER_KEY.
"""

import socket
from unittest.mock import AsyncMock, patch

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter
from gateway.platforms.base import is_network_accessible


@pytest.fixture(autouse=True)
def _isolate_api_server_env(monkeypatch):
    """Keep live gateway config/env from changing API server bind-guard defaults.

    gateway.run can be imported by earlier tests and bridge the live
    ~/.hermes config into os.environ. Tests in this module construct
    APIServerAdapter directly and expect explicit PlatformConfig values to be
    the only configuration source, so clear API server env both before and
    after each test. The post-yield cleanup prevents tests that set these
    variables internally from leaking into following tests in a full suite run.
    """
    keys = (
        "API_SERVER_HOST",
        "API_SERVER_PORT",
        "API_SERVER_KEY",
        "API_SERVER_CORS_ORIGINS",
    )
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    yield
    for key in keys:
        monkeypatch.delenv(key, raising=False)


# ---------------------------------------------------------------------------
# Unit tests: is_network_accessible()
# ---------------------------------------------------------------------------


class TestIsNetworkAccessible:
    """Direct tests for the address classification helper."""

    # -- Loopback (safe, should return False) --

    def test_ipv4_loopback(self):
        assert is_network_accessible("127.0.0.1") is False

    def test_ipv6_loopback(self):
        assert is_network_accessible("::1") is False

    def test_ipv4_mapped_loopback(self):
        # ::ffff:127.0.0.1 — Python's is_loopback returns False for mapped
        # addresses; the helper must unwrap and check ipv4_mapped.
        assert is_network_accessible("::ffff:127.0.0.1") is False

    # -- Network-accessible (should return True) --

    def test_ipv4_wildcard(self):
        assert is_network_accessible("0.0.0.0") is True

    def test_ipv6_wildcard(self):
        # This is the bypass vector that the string-based check missed.
        assert is_network_accessible("::") is True

    def test_ipv4_mapped_unspecified(self):
        assert is_network_accessible("::ffff:0.0.0.0") is True

    def test_private_ipv4(self):
        assert is_network_accessible("10.0.0.1") is True

    def test_private_ipv4_class_c(self):
        assert is_network_accessible("192.168.1.1") is True

    def test_public_ipv4(self):
        assert is_network_accessible("8.8.8.8") is True

    # -- Hostname resolution --

    def test_localhost_resolves_to_loopback(self):
        loopback_result = [
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 0)),
        ]
        with patch("gateway.platforms.base._socket.getaddrinfo", return_value=loopback_result):
            assert is_network_accessible("localhost") is False

    def test_hostname_resolving_to_non_loopback(self):
        non_loopback_result = [
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("10.0.0.1", 0)),
        ]
        with patch("gateway.platforms.base._socket.getaddrinfo", return_value=non_loopback_result):
            assert is_network_accessible("my-server.local") is True

    def test_hostname_mixed_resolution(self):
        """If a hostname resolves to both loopback and non-loopback, it's
        network-accessible (any non-loopback address is enough)."""
        mixed_result = [
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 0)),
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("10.0.0.1", 0)),
        ]
        with patch("gateway.platforms.base._socket.getaddrinfo", return_value=mixed_result):
            assert is_network_accessible("dual-host.local") is True

    def test_dns_failure_fails_closed(self):
        """Unresolvable hostnames should require an API key (fail closed)."""
        with patch(
            "gateway.platforms.base._socket.getaddrinfo",
            side_effect=socket.gaierror("Name resolution failed"),
        ):
            assert is_network_accessible("nonexistent.invalid") is True


# ---------------------------------------------------------------------------
# Integration tests: connect() startup guard
# ---------------------------------------------------------------------------


class TestConnectBindGuard:
    """Verify that connect() refuses dangerous configurations."""

    @pytest.mark.asyncio
    async def test_refuses_ipv4_wildcard_without_key(self):
        adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"host": "0.0.0.0"}))
        result = await adapter.connect()
        assert result is False

    @pytest.mark.asyncio
    async def test_refuses_ipv6_wildcard_without_key(self):
        adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"host": "::"}))
        result = await adapter.connect()
        assert result is False

    def test_allows_loopback_without_key(self):
        """Loopback with no key should pass the guard."""
        adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"host": "127.0.0.1"}))
        assert adapter._api_key == ""
        # The guard condition: is_network_accessible(host) AND NOT api_key
        # For loopback, is_network_accessible is False so the guard does not block.
        assert is_network_accessible(adapter._host) is False

    @pytest.mark.asyncio
    async def test_allows_wildcard_with_key(self):
        """Non-loopback with a key should pass the guard."""
        adapter = APIServerAdapter(
            PlatformConfig(enabled=True, extra={"host": "0.0.0.0", "key": "sk-test"})
        )
        # The guard checks: is_network_accessible(host) AND NOT api_key
        # With a key set, the guard should not block.
        assert adapter._api_key == "sk-test"
        assert is_network_accessible("0.0.0.0") is True
        # Combined: the guard condition is False (key is set), so it passes
