"""Tests for FeishuAdapter admission auto-start features."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from gateway.config import PlatformConfig


@pytest.fixture
def mock_admission_modules():
    """Mock admission modules to avoid real initialization."""
    with (
        patch("gateway.admission.AdmissionController") as mock_ctrl_cls,
        patch("gateway.admission.worker.QueueWorker") as mock_worker_cls,
        patch("gateway.admission.templates.TemplateStore") as mock_store_cls,
        patch("gateway.admission.metrics_export.MetricsExporter") as mock_exporter_cls,
        patch("gateway.admission.metrics_server.MetricsServer") as mock_server_cls,
    ):
        mock_ctrl = MagicMock()
        mock_ctrl.validate_config.return_value = (True, [])
        mock_ctrl_cls.return_value = mock_ctrl

        mock_worker = MagicMock()
        mock_worker.start = AsyncMock()
        mock_worker.stop = AsyncMock()
        mock_worker_cls.return_value = mock_worker

        mock_store = MagicMock()
        mock_template = MagicMock()
        mock_template.name = "strict"
        mock_store.get.return_value = mock_template
        mock_store_cls.return_value = mock_store

        mock_exporter = MagicMock()
        mock_exporter_cls.return_value = mock_exporter

        mock_server = MagicMock()
        mock_server.port = 9090
        mock_server.start = MagicMock()
        mock_server.stop = MagicMock()
        mock_server_cls.return_value = mock_server

        yield {
            "controller_cls": mock_ctrl_cls,
            "controller": mock_ctrl,
            "worker_cls": mock_worker_cls,
            "worker": mock_worker,
            "store_cls": mock_store_cls,
            "store": mock_store,
            "template": mock_template,
            "exporter_cls": mock_exporter_cls,
            "exporter": mock_exporter,
            "server_cls": mock_server_cls,
            "server": mock_server,
        }


class TestAdmissionTemplateAutoLoad:
    """Test automatic policy template loading on adapter init."""

    def test_template_loaded_when_specified(self, mock_admission_modules):
        from gateway.platforms.feishu import FeishuAdapter

        config = PlatformConfig(
            enabled=True,
            extra={
                "admission_control_enabled": True,
                "admission_template": "strict",
            }
        )

        adapter = FeishuAdapter(config)

        # Template should be loaded and applied
        mock_admission_modules["store_cls"].assert_called_once()
        mock_admission_modules["store"].get.assert_called_once_with("strict")
        mock_admission_modules["controller"].apply_template.assert_called_once_with(
            mock_admission_modules["template"]
        )

    def test_no_template_loaded_when_not_specified(self, mock_admission_modules):
        from gateway.platforms.feishu import FeishuAdapter

        config = PlatformConfig(
            enabled=True,
            extra={
                "admission_control_enabled": True,
            }
        )

        adapter = FeishuAdapter(config)

        # Template store should not be accessed
        mock_admission_modules["store_cls"].assert_not_called()
        mock_admission_modules["controller"].apply_template.assert_not_called()

    def test_template_load_failure_does_not_crash(self, mock_admission_modules):
        from gateway.platforms.feishu import FeishuAdapter

        # Simulate template not found
        mock_admission_modules["store"].get.return_value = None

        config = PlatformConfig(
            enabled=True,
            extra={
                "admission_control_enabled": True,
                "admission_template": "nonexistent",
            }
        )

        # Should not raise
        adapter = FeishuAdapter(config)

        # apply_template should not be called
        mock_admission_modules["controller"].apply_template.assert_not_called()


class TestMetricsServerAutoStart:
    """Test automatic metrics server lifecycle management."""

    def test_metrics_server_configured_when_port_specified(self, mock_admission_modules):
        from gateway.platforms.feishu import FeishuAdapter

        config = PlatformConfig(
            enabled=True,
            extra={
                "admission_control_enabled": True,
                "admission_metrics_port": 9090,
            }
        )

        adapter = FeishuAdapter(config)

        # Metrics server should be created
        mock_admission_modules["exporter_cls"].assert_called_once_with(
            mock_admission_modules["controller"]
        )
        mock_admission_modules["server_cls"].assert_called_once_with(
            mock_admission_modules["exporter"],
            port=9090
        )
        assert adapter._metrics_server is not None

    def test_metrics_server_not_configured_when_port_not_specified(self, mock_admission_modules):
        from gateway.platforms.feishu import FeishuAdapter

        config = PlatformConfig(
            enabled=True,
            extra={
                "admission_control_enabled": True,
            }
        )

        adapter = FeishuAdapter(config)

        # Metrics server should not be created
        mock_admission_modules["exporter_cls"].assert_not_called()
        mock_admission_modules["server_cls"].assert_not_called()
        assert adapter._metrics_server is None

    @pytest.mark.asyncio
    async def test_metrics_server_started_on_connect(self, mock_admission_modules):
        from gateway.platforms.feishu import FeishuAdapter

        config = PlatformConfig(
            enabled=True,
            extra={
                "app_id": "cli_test",
                "app_secret": "secret_test",
                "admission_control_enabled": True,
                "admission_metrics_port": 9090,
            }
        )

        adapter = FeishuAdapter(config)

        with (
            patch("gateway.platforms.feishu.FEISHU_AVAILABLE", True),
            patch("gateway.platforms.feishu.acquire_scoped_lock", return_value=(True, None)),
            patch.object(adapter, "_connect_with_retry", new=AsyncMock()),
        ):
            await adapter.connect()

        # Metrics server should be started
        mock_admission_modules["server"].start.assert_called_once()

    @pytest.mark.asyncio
    async def test_metrics_server_stopped_on_disconnect(self, mock_admission_modules):
        from gateway.platforms.feishu import FeishuAdapter

        config = PlatformConfig(
            enabled=True,
            extra={
                "app_id": "cli_test",
                "app_secret": "secret_test",
                "admission_control_enabled": True,
                "admission_metrics_port": 9090,
            }
        )

        adapter = FeishuAdapter(config)

        with (
            patch("gateway.platforms.feishu.FEISHU_AVAILABLE", True),
            patch("gateway.platforms.feishu.acquire_scoped_lock", return_value=(True, None)),
            patch("gateway.platforms.feishu.release_scoped_lock"),
            patch.object(adapter, "_connect_with_retry", new=AsyncMock()),
        ):
            await adapter.connect()
            await adapter.disconnect()

        # Metrics server should be stopped
        mock_admission_modules["server"].stop.assert_called_once()


class TestCombinedAutoStart:
    """Test template + metrics server together."""

    def test_both_features_enabled_together(self, mock_admission_modules):
        from gateway.platforms.feishu import FeishuAdapter

        config = PlatformConfig(
            enabled=True,
            extra={
                "admission_control_enabled": True,
                "admission_template": "strict",
                "admission_metrics_port": 9090,
            }
        )

        adapter = FeishuAdapter(config)

        # Both should be initialized
        mock_admission_modules["controller"].apply_template.assert_called_once()
        mock_admission_modules["server_cls"].assert_called_once()
        assert adapter._metrics_server is not None
