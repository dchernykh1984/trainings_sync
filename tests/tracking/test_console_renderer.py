from unittest.mock import patch

from app.tracking.console_renderer import ConsoleRenderer


class TestConsoleRendererContextManager:
    def test_exit_calls_stop(self) -> None:
        with patch("app.tracking.console_renderer.Progress"):
            renderer = ConsoleRenderer()
            with patch.object(renderer, "stop") as mock_stop:
                with renderer:
                    pass
            mock_stop.assert_called_once()
