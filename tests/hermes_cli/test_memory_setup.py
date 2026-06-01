"""Focused tests for hermes_cli.memory_setup."""

from __future__ import annotations

from argparse import Namespace
from unittest.mock import patch

import pytest


def _make_args(memory_command=None):
    return Namespace(memory_command=memory_command)


def test_cmd_setup_builtin_only_saves_empty_provider(capsys):
    from hermes_cli.memory_setup import cmd_setup

    class DummyProvider:
        pass

    with patch(
        "hermes_cli.memory_setup._get_available_providers",
        return_value=[("honcho", "requires API key", DummyProvider())],
    ), patch(
        "hermes_cli.memory_setup._curses_select",
        return_value=1,
    ), patch(
        "hermes_cli.config.load_config",
        return_value={},
    ), patch(
        "hermes_cli.config.save_config",
    ) as mock_save:
        cmd_setup(_make_args("setup"))

    saved = mock_save.call_args.args[0]
    assert saved["memory"]["provider"] == ""
    out = capsys.readouterr().out
    assert "built-in only" in out


def test_cmd_setup_provider_unknown_prints_not_found_and_returns(capsys):
    from hermes_cli.memory_setup import cmd_setup_provider

    with patch(
        "hermes_cli.memory_setup._get_available_providers",
        return_value=[],
    ):
        cmd_setup_provider("ghost")

    out = capsys.readouterr().out
    assert "Memory provider 'ghost' not found." in out


def test_cmd_status_with_active_uninstalled_provider_reports_not_installed(capsys):
    from hermes_cli.memory_setup import cmd_status

    with patch(
        "hermes_cli.config.load_config",
        return_value={"memory": {"provider": "ghost"}},
    ), patch(
        "hermes_cli.memory_setup._get_available_providers",
        return_value=[],
    ):
        cmd_status(_make_args("status"))

    out = capsys.readouterr().out
    assert "Memory status" in out
    assert "Provider:  ghost" in out
    assert "Plugin:    NOT installed" in out


@pytest.mark.parametrize(
    ("subcommand", "setup_calls", "status_calls"),
    [
        ("setup", 1, 0),
        ("status", 0, 1),
        (None, 0, 1),
    ],
)
def test_memory_command_routes_setup_status_and_default_to_status(
    subcommand, setup_calls, status_calls
):
    from hermes_cli.memory_setup import memory_command

    args = _make_args(subcommand)

    with patch("hermes_cli.memory_setup.cmd_setup") as mock_setup, patch(
        "hermes_cli.memory_setup.cmd_status"
    ) as mock_status:
        memory_command(args)

    assert mock_setup.call_count == setup_calls
    assert mock_status.call_count == status_calls
