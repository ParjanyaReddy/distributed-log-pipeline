"""Apex Stream - Web Server & SOC Telemetry Console.

Serves static UI assets and routes API requests via the modular APIRouter layer.
"""

from __future__ import annotations

import http.server
import os
import sys
from typing import Any

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import WEB_HOST, WEB_PORT
from common.logging import get_logger
from web.api.base import APIResponse
from web.api.routes import create_default_router
from web.services.bridge import WebPipelineBridge

logger = get_logger("web.server")

# Global singleton bridge instance (used by test suites and server runtime)
bridge = WebPipelineBridge()
api_router = create_default_router(bridge)


class PipelineRequestHandler(http.server.SimpleHTTPRequestHandler):
    """Custom HTTP request handler serving static dashboard assets and routing API calls."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        super().__init__(*args, directory=static_dir, **kwargs)

    def do_GET(self) -> None:
        """Handles incoming HTTP GET requests."""
        response = api_router.dispatch_get(self.path)
        if response is not None:
            self._send_api_response(response)
            return

        # Fallback to static asset serving (index.html, style.css, js files)
        super().do_GET()

    def do_POST(self) -> None:
        """Handles incoming HTTP POST requests."""
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b""

        response = api_router.dispatch_post(self.path, body)
        if response is not None:
            self._send_api_response(response)
            return

        self.send_response(404)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"error": "Endpoint not found"}')

    def _send_api_response(self, response: APIResponse) -> None:
        """Helper to send standardized API response."""
        data = response.serialize()
        self.send_response(response.status_code)
        self.send_header("Content-Type", response.content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args: Any) -> None:
        """Suppresses default noisy stdout logging, channeling into structured logger."""
        logger.debug("%s - - [%s] %s" % (self.address_string(), self.log_date_time_string(), format % args))


def start_server(host: str = WEB_HOST, port: int = WEB_PORT) -> None:
    """Starts the multi-threaded HTTP server."""
    server = http.server.ThreadingHTTPServer((host, port), PipelineRequestHandler)
    logger.info(f"Pipeline SOC Console running live at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Web server stopping...")
    finally:
        server.server_close()


if __name__ == "__main__":
    start_server(port=WEB_PORT)
