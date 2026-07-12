"""Feishu approval operator display-name regressions."""

import importlib.util
import importlib.machinery
import sys
import time
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

_repo = str(Path(__file__).resolve().parents[2])
if _repo not in sys.path:
    sys.path.insert(0, _repo)


def _fake_module(name: str) -> ModuleType:
    mod = ModuleType(name)
    mod.__spec__ = importlib.machinery.ModuleSpec(name, loader=None)
    return mod


def _ensure_feishu_mocks():
    if importlib.util.find_spec("lark_oapi") is None and "lark_oapi" not in sys.modules:
        mod = _fake_module("lark_oapi")
        for name in (
            "lark_oapi", "lark_oapi.api.im.v1",
            "lark_oapi.event", "lark_oapi.event.callback_type",
        ):
            sys.modules.setdefault(name, mod)
    if importlib.util.find_spec("aiohttp") is None and "aiohttp" not in sys.modules:
        aio = MagicMock()
        sys.modules.setdefault("aiohttp", aio)
        sys.modules.setdefault("aiohttp.web", aio.web)


_ensure_feishu_mocks()

from gateway.config import PlatformConfig
from gateway.platforms.feishu import FeishuAdapter


def test_approval_operator_name_uses_local_user_id_mapping_when_cache_misses(monkeypatch):
    import tools.permission_policy as permission_policy

    adapter = FeishuAdapter(PlatformConfig(enabled=True))
    operator = SimpleNamespace(open_id="ou_admin", user_id="on_admin")
    monkeypatch.setattr(
        permission_policy,
        "_load_config",
        lambda: {"user_id_mapping": {"ou_admin": "胡子豪"}, "users": {"胡子豪": "owner", "default": "member"}},
    )

    assert adapter._resolve_approval_operator_display_name(operator) == "胡子豪"


def test_approval_operator_name_checks_nested_user_id_identities(monkeypatch):
    import tools.permission_policy as permission_policy

    adapter = FeishuAdapter(PlatformConfig(enabled=True))
    operator = SimpleNamespace(
        open_id="",
        user_id=SimpleNamespace(open_id="ou_nested", user_id="u_nested", union_id="on_nested"),
    )
    monkeypatch.setattr(
        permission_policy,
        "_load_config",
        lambda: {"user_id_mapping": {"on_nested": "胡子豪"}, "users": {"胡子豪": "owner", "default": "member"}},
    )

    assert adapter._resolve_approval_operator_display_name(operator) == "胡子豪"


def test_approval_operator_name_prefers_fresh_cache_over_mapping(monkeypatch):
    import tools.permission_policy as permission_policy

    adapter = FeishuAdapter(PlatformConfig(enabled=True))
    adapter._sender_name_cache["ou_admin"] = ("胡子豪", time.time() + 60)
    operator = SimpleNamespace(open_id="ou_admin", user_id="on_admin")
    monkeypatch.setattr(
        permission_policy,
        "_load_config",
        lambda: {"user_id_mapping": {"ou_admin": "旧名字"}, "users": {"旧名字": "owner", "default": "member"}},
    )

    assert adapter._resolve_approval_operator_display_name(operator) == "胡子豪"
