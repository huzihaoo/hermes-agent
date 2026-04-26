"""Standalone HTTP server exposing /metrics for admission control.

Sidecar-style, zero-intrusion: does not modify gateway core server.
Uses Python stdlib http.server.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class MetricsServer:
    def __init__(self, exporter, host: str = "127.0.0.1", port: int = 9099):
        self._exporter = exporter
        self._host = host
        self._port = port
        self._server = None
        self._thread = None

    @property
    def port(self) -> int:
        if self._server is None:
            return self._port
        return self._server.server_address[1]

    def start(self) -> None:
        if self._server is not None:
            return

        exporter = self._exporter

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path != "/metrics":
                    self.send_response(404)
                    self.end_headers()
                    self.wfile.write(b"not found")
                    return
                body = exporter.export().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                return  # silent

        self._server = ThreadingHTTPServer((self._host, self._port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is None:
            return
        try:
            self._server.shutdown()
            self._server.server_close()
        finally:
            self._server = None
            self._thread = None
