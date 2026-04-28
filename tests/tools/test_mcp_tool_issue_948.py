import asyncio
import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tools.mcp_tool import MCPServerTask, _format_connect_error, _resolve_stdio_command, _MCP_AVAILABLE

# Ensure the mcp module symbols exist for patching even when the SDK isn't installed
if not _MCP_AVAILABLE:
    import tools.mcp_tool as _mcp_mod
    if not hasattr(_mcp_mod, "StdioServerParameters"):
        _mcp_mod.StdioServerParameters = MagicMock
    if not hasattr(_mcp_mod, "stdio_client"):
        _mcp_mod.stdio_client = MagicMock
    if not hasattr(_mcp_mod, "ClientSession"):
        _mcp_mod.ClientSession = MagicMock


def test_resolve_stdio_command_falls_back_to_hermes_node_bin(tmp_path):
    node_bin = tmp_path / "node" / "bin"
    node_bin.mkdir(parents=True)
    npx_path = node_bin / "npx"
    npx_path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    npx_path.chmod(0o755)

    with patch("tools.mcp_tool.shutil.which", return_value=None), \
         patch.dict("os.environ", {"HERMES_HOME": str(tmp_path)}, clear=False):
        command, env = _resolve_stdio_command("npx", {"PATH": "/usr/bin"})

    assert command == str(npx_path)
    assert env["PATH"].split(os.pathsep)[0] == str(node_bin)


def test_resolve_stdio_command_respects_explicit_empty_path():
    seen_paths = []

    def _fake_which(_cmd, path=None):
        seen_paths.append(path)
        return None

    with patch("tools.mcp_tool.shutil.which", side_effect=_fake_which):
        command, env = _resolve_stdio_command("python", {"PATH": ""})

    assert command == "python"
    assert env["PATH"] == ""
    assert seen_paths == [""]


def test_format_connect_error_unwraps_exception_group():
    error = ExceptionGroup(
        "unhandled errors in a TaskGroup",
        [FileNotFoundError(2, "No such file or directory", "node")],
    )

    message = _format_connect_error(error)

    assert "missing executable 'node'" in message


def test_run_stdio_uses_resolved_command_and_prepended_path(tmp_path):
    node_bin = tmp_path / "node" / "bin"
    node_bin.mkdir(parents=True)
    npx_path = node_bin / "npx"
    npx_path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    npx_path.chmod(0o755)

    mock_session = MagicMock()
    mock_session.initialize = AsyncMock()
    mock_session.list_tools = AsyncMock(return_value=SimpleNamespace(tools=[]))

    mock_stdio_cm = MagicMock()
    mock_stdio_cm.__aenter__ = AsyncMock(return_value=(object(), object()))
    mock_stdio_cm.__aexit__ = AsyncMock(return_value=False)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)

    async def _test():
        with patch("tools.mcp_tool.shutil.which", return_value=None), \
             patch.dict("os.environ", {"HERMES_HOME": str(tmp_path), "PATH": "/usr/bin", "HOME": str(tmp_path)}, clear=False), \
             patch("tools.mcp_tool.StdioServerParameters") as mock_params, \
             patch("tools.mcp_tool.stdio_client", return_value=mock_stdio_cm), \
             patch("tools.mcp_tool.ClientSession", return_value=mock_session_cm):
            server = MCPServerTask("srv")
            await server.start({"command": "npx", "args": ["-y", "pkg"], "env": {"PATH": "/usr/bin"}})

            call_kwargs = mock_params.call_args.kwargs
            assert call_kwargs["command"] == str(npx_path)
            assert call_kwargs["env"]["PATH"].split(os.pathsep)[0] == str(node_bin)

            await server.shutdown()

    asyncio.run(_test())


def test_run_stdio_redirects_child_stderr_to_configured_file(tmp_path):
    log_path = tmp_path / "mcp" / "srv.stderr.log"

    mock_session = MagicMock()
    mock_session.initialize = AsyncMock()
    mock_session.list_tools = AsyncMock(return_value=SimpleNamespace(tools=[]))

    mock_stdio_cm = MagicMock()
    mock_stdio_cm.__aenter__ = AsyncMock(return_value=(object(), object()))
    mock_stdio_cm.__aexit__ = AsyncMock(return_value=False)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)

    async def _test():
        with patch("tools.osv_check.check_package_for_malware", return_value=None), \
             patch("tools.mcp_tool._resolve_stdio_command", return_value=("/bin/echo", {"PATH": "/usr/bin"})), \
             patch("tools.mcp_tool.stdio_client", return_value=mock_stdio_cm) as mock_stdio, \
             patch("tools.mcp_tool.ClientSession", return_value=mock_session_cm):
            server = MCPServerTask("srv")
            await server.start({
                "command": "echo",
                "args": [],
                "env": {"PATH": "/usr/bin"},
                "stderr_log_path": str(log_path),
            })

            _, call_kwargs = mock_stdio.call_args
            assert "errlog" in call_kwargs
            assert call_kwargs["errlog"].name == str(log_path)
            assert log_path.parent.exists()

            await server.shutdown()

    asyncio.run(_test())


def test_run_stdio_can_opt_in_to_inherited_stderr(tmp_path):
    mock_session = MagicMock()
    mock_session.initialize = AsyncMock()
    mock_session.list_tools = AsyncMock(return_value=SimpleNamespace(tools=[]))

    mock_stdio_cm = MagicMock()
    mock_stdio_cm.__aenter__ = AsyncMock(return_value=(object(), object()))
    mock_stdio_cm.__aexit__ = AsyncMock(return_value=False)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)

    async def _test():
        with patch("tools.osv_check.check_package_for_malware", return_value=None), \
             patch("tools.mcp_tool._resolve_stdio_command", return_value=("/bin/echo", {"PATH": "/usr/bin"})), \
             patch("tools.mcp_tool.stdio_client", return_value=mock_stdio_cm) as mock_stdio, \
             patch("tools.mcp_tool.ClientSession", return_value=mock_session_cm):
            server = MCPServerTask("srv")
            await server.start({
                "command": "echo",
                "args": [],
                "env": {"PATH": "/usr/bin"},
                "stderr": "inherit",
            })

            _, call_kwargs = mock_stdio.call_args
            assert "errlog" not in call_kwargs

            await server.shutdown()

    asyncio.run(_test())


def test_run_stdio_uses_default_stderr_log_path_under_hermes_home(tmp_path):
    mock_session = MagicMock()
    mock_session.initialize = AsyncMock()
    mock_session.list_tools = AsyncMock(return_value=SimpleNamespace(tools=[]))

    mock_stdio_cm = MagicMock()
    mock_stdio_cm.__aenter__ = AsyncMock(return_value=(object(), object()))
    mock_stdio_cm.__aexit__ = AsyncMock(return_value=False)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)

    async def _test():
        with patch.dict("os.environ", {"HERMES_HOME": str(tmp_path)}, clear=False), \
             patch("tools.osv_check.check_package_for_malware", return_value=None), \
             patch("tools.mcp_tool._resolve_stdio_command", return_value=("/bin/echo", {"PATH": "/usr/bin"})), \
             patch("tools.mcp_tool.stdio_client", return_value=mock_stdio_cm) as mock_stdio, \
             patch("tools.mcp_tool.ClientSession", return_value=mock_session_cm):
            server = MCPServerTask("srv/name")
            await server.start({"command": "echo", "args": [], "env": {"PATH": "/usr/bin"}})

            _, call_kwargs = mock_stdio.call_args
            expected = tmp_path / "logs" / "mcp" / "srv_name.stderr.log"
            assert call_kwargs["errlog"].name == str(expected)
            assert expected.parent.exists()

            await server.shutdown()

    asyncio.run(_test())


def test_run_stdio_can_discard_child_stderr_with_devnull(tmp_path):
    mock_session = MagicMock()
    mock_session.initialize = AsyncMock()
    mock_session.list_tools = AsyncMock(return_value=SimpleNamespace(tools=[]))

    mock_stdio_cm = MagicMock()
    mock_stdio_cm.__aenter__ = AsyncMock(return_value=(object(), object()))
    mock_stdio_cm.__aexit__ = AsyncMock(return_value=False)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)

    async def _test():
        with patch("tools.osv_check.check_package_for_malware", return_value=None), \
             patch("tools.mcp_tool._resolve_stdio_command", return_value=("/bin/echo", {"PATH": "/usr/bin"})), \
             patch("tools.mcp_tool.stdio_client", return_value=mock_stdio_cm) as mock_stdio, \
             patch("tools.mcp_tool.ClientSession", return_value=mock_session_cm):
            server = MCPServerTask("srv")
            await server.start({
                "command": "echo",
                "args": [],
                "env": {"PATH": "/usr/bin"},
                "stderr": "null",
            })

            _, call_kwargs = mock_stdio.call_args
            assert call_kwargs["errlog"].name == os.devnull

            await server.shutdown()

    asyncio.run(_test())
