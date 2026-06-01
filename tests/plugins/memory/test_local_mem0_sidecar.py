"""Tests for the local Mem0 sidecar memory provider."""

from __future__ import annotations

import json

import pytest


class FakeSidecarClient:
    def __init__(self, search_response=None, list_response=None, capture_response=None):
        self.search_response = search_response or {"ok": True, "results": []}
        self.list_response = list_response or {"ok": True, "results": []}
        self.capture_response = capture_response or {"ok": True, "results": []}
        self.calls = []

    def post(self, path, payload):
        self.calls.append({"path": path, "payload": payload})
        if path == "/v1/memory/search":
            return self.search_response
        if path == "/v1/memory/list":
            return self.list_response
        if path == "/v1/memory/capture":
            return self.capture_response
        return {"ok": True}


@pytest.fixture
def clean_local_mem0_env(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    for key in (
        "LOCAL_MEM0_SIDECAR_BASE_URL",
        "LOCAL_MEM0_SIDECAR_API_KEY",
        "LOCAL_MEM0_USER_ID",
        "LOCAL_MEM0_AGENT_ID",
        "LOCAL_MEM0_TIMEOUT",
        "LOCAL_MEM0_DEFAULT_LIMIT",
        "LOCAL_MEM0_DEFAULT_THRESHOLD",
    ):
        monkeypatch.delenv(key, raising=False)
    return tmp_path


def test_provider_name_is_local_mem0_sidecar(clean_local_mem0_env):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()

    assert provider.name == "local_mem0_sidecar"


def test_provider_is_unavailable_without_config(clean_local_mem0_env):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()

    assert provider.is_available() is False


def test_provider_is_available_from_hermes_home_config(clean_local_mem0_env, tmp_path):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    (tmp_path / "local_mem0_sidecar.json").write_text(
        json.dumps({"base_url": "http://127.0.0.1:8765"}),
        encoding="utf-8",
    )

    provider = LocalMem0SidecarMemoryProvider()

    assert provider.is_available() is True


def test_discover_and_load_provider(clean_local_mem0_env):
    from plugins.memory import discover_memory_providers, load_memory_provider

    discovered = {name for name, _, _ in discover_memory_providers()}
    provider = load_memory_provider("local_mem0_sidecar")

    assert "local_mem0_sidecar" in discovered
    assert provider is not None
    assert provider.name == "local_mem0_sidecar"


def test_initialize_prefers_gateway_user_id_over_config(monkeypatch, tmp_path):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    (tmp_path / "local_mem0_sidecar.json").write_text(
        json.dumps({"base_url": "http://127.0.0.1:8765", "user_id": "configured-user", "agent_id": "configured-agent"}),
        encoding="utf-8",
    )
    provider = LocalMem0SidecarMemoryProvider()

    provider.initialize(
        "session-1",
        hermes_home=str(tmp_path),
        platform="feishu",
        user_id="gateway-user",
        chat_id="chat-1",
        thread_id="thread-1",
    )

    assert provider._user_id == "gateway-user"
    assert provider._agent_id == "configured-agent"
    assert provider._session_id == "session-1"
    assert provider._platform == "feishu"


def test_tool_schemas_reuse_mem0_tool_names(clean_local_mem0_env):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()
    names = {schema["name"] for schema in provider.get_tool_schemas()}

    assert {"mem0_profile", "mem0_search", "mem0_conclude", "mem0_promote"}.issubset(names)


def test_search_tool_requires_query(tmp_path):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path), user_id="u1")
    provider._client = FakeSidecarClient()

    result = json.loads(provider.handle_tool_call("mem0_search", {}))

    assert "error" in result
    assert "query" in result["error"].lower()


def test_conclude_tool_requires_conclusion(tmp_path):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path), user_id="u1")
    provider._client = FakeSidecarClient()

    result = json.loads(provider.handle_tool_call("mem0_conclude", {}))

    assert "error" in result
    assert "conclusion" in result["error"].lower()


def test_search_tool_sends_scoped_sidecar_request(tmp_path):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path), user_id="u1")
    fake = FakeSidecarClient(search_response={"ok": True, "results": [{"memory": "alpha", "score": 0.8}]})
    provider._client = fake

    result = json.loads(provider.handle_tool_call("mem0_search", {"query": "alpha", "top_k": 3}))

    assert result["count"] == 1
    assert result["results"][0]["memory"] == "alpha"
    assert fake.calls == [
        {
            "path": "/v1/memory/search",
            "payload": {
                "query": "alpha",
                "user_id": "u1",
                "session_id": "session-1",
                "limit": 3,
                "threshold": 0.25,
                "metadata_filters": {"scope_type": "user", "scope_id": "u1"},
            },
        }
    ]


def test_search_tool_default_excludes_candidate_memories(tmp_path):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path), user_id="u1")
    fake = FakeSidecarClient(
        search_response={
            "ok": True,
            "results": [
                {"memory": "candidate memory", "metadata": {"approval_status": "candidate"}},
                {"memory": "reviewed memory", "metadata": {"approval_status": "reviewed"}},
            ],
        }
    )
    provider._client = fake

    result = json.loads(provider.handle_tool_call("mem0_search", {"query": "alpha", "top_k": 3}))

    assert result["count"] == 1
    assert [item["memory"] for item in result["results"]] == ["reviewed memory"]


def test_search_tool_can_include_candidate_memories_explicitly(tmp_path):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path), user_id="u1")
    fake = FakeSidecarClient(
        search_response={
            "ok": True,
            "results": [
                {"memory": "candidate memory", "metadata": {"approval_status": "candidate"}},
                {"memory": "reviewed memory", "metadata": {"approval_status": "reviewed"}},
            ],
        }
    )
    provider._client = fake

    result = json.loads(
        provider.handle_tool_call(
            "mem0_search",
            {"query": "alpha", "top_k": 3, "include_candidates": True},
        )
    )

    assert result["count"] == 2
    assert [item["memory"] for item in result["results"]] == ["candidate memory", "reviewed memory"]


def test_profile_tool_excludes_candidate_memories(tmp_path):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path), user_id="u1")
    fake = FakeSidecarClient(
        list_response={
            "ok": True,
            "results": [
                {"memory": "candidate memory", "metadata": {"approval_status": "candidate"}},
                {"memory": "reviewed memory", "metadata": {"approval_status": "reviewed"}},
            ],
        }
    )
    provider._client = fake

    result = json.loads(provider.handle_tool_call("mem0_profile", {"limit": 10}))

    assert result["count"] == 1
    assert "reviewed memory" in result["result"]
    assert "candidate memory" not in result["result"]


def test_promote_tool_requires_memory(tmp_path):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path), user_id="u1")
    provider._client = FakeSidecarClient()

    result = json.loads(provider.handle_tool_call("mem0_promote", {}))

    assert "error" in result
    assert "memory" in result["error"].lower()


def test_promote_tool_recaptures_reviewed_memory_with_source_candidate(tmp_path):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path), platform="cli", user_id="u1")
    fake = FakeSidecarClient(capture_response={"ok": True, "results": [{"id": "reviewed-1"}]})
    provider._client = fake

    result = json.loads(
        provider.handle_tool_call(
            "mem0_promote",
            {"memory": "User prefers concise updates", "candidate_id": "candidate-1", "rationale": "stable preference"},
        )
    )

    assert result["ok"] is True
    assert fake.calls[0]["path"] == "/v1/memory/capture"
    payload = fake.calls[0]["payload"]
    assert payload["messages"] == [{"role": "system", "content": "User prefers concise updates"}]
    assert payload["metadata"]["approval_status"] == "reviewed"
    assert payload["metadata"]["recall_policy"] == "auto_recall"
    assert payload["metadata"]["source_candidate_id"] == "candidate-1"
    assert payload["metadata"]["review_rationale"] == "stable preference"


def test_conclude_tool_captures_candidate_with_metadata(tmp_path):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path), platform="cli", user_id="u1")
    fake = FakeSidecarClient(capture_response={"ok": True, "results": [{"id": "m1"}]})
    provider._client = fake

    result = json.loads(provider.handle_tool_call("mem0_conclude", {"conclusion": "User prefers concise updates"}))

    assert result["ok"] is True
    assert fake.calls[0]["path"] == "/v1/memory/capture"
    payload = fake.calls[0]["payload"]
    assert payload["user_id"] == "u1"
    assert payload["session_id"] == "session-1"
    assert payload["infer"] is False
    assert payload["messages"] == [{"role": "system", "content": "User prefers concise updates"}]
    assert payload["metadata"]["approval_status"] == "candidate"
    assert payload["metadata"]["scope_type"] == "user"
    assert payload["metadata"]["recall_policy"] == "manual_only"


def test_prefetch_formats_search_results(tmp_path):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path), user_id="u1")
    provider._client = FakeSidecarClient(search_response={"ok": True, "results": [{"memory": "remember me"}]})

    result = provider.prefetch("memory")

    assert "Local Mem0 Memory" in result
    assert "remember me" in result


def test_prefetch_skips_candidate_only_memories(tmp_path):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path), user_id="u1")
    provider._client = FakeSidecarClient(
        search_response={
            "ok": True,
            "results": [
                {"memory": "candidate memory", "metadata": {"approval_status": "candidate"}},
                {"memory": "reviewed memory", "metadata": {"approval_status": "reviewed"}},
            ],
        }
    )

    result = provider.prefetch("memory")

    assert "reviewed memory" in result
    assert "candidate memory" not in result


def test_sync_turn_is_noop_by_default(tmp_path):
    from plugins.memory.local_mem0_sidecar import LocalMem0SidecarMemoryProvider

    provider = LocalMem0SidecarMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path), user_id="u1")
    fake = FakeSidecarClient()
    provider._client = fake

    provider.sync_turn("user", "assistant", session_id="session-1")

    assert fake.calls == []
