# =============================================================================
# Copyright (c) 2025 by Laszlo Anka. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional, Any

from pypz.core.commons.parameters import OptionalParameter
from pypz.core.specs.plugin import ServicePlugin


class HttpHealthCheckPlugin(ServicePlugin):
    port = OptionalParameter(int)

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.port = 8081
        self._server: Optional[HTTPServer] = None
        self._server_thread: Optional[threading.Thread] = None

    # --------------------------
    # Private: HTTP server setup
    # --------------------------
    def _start_http_server(self) -> None:
        plugin = self
        plugin.get_logger().debug("Starting HTTP server ...")

        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/check":
                    status = 200
                    body = b"OK"
                else:
                    status = 404
                    body = b"NOT_FOUND"

                self.send_response(status)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer(("0.0.0.0", self.port), HealthHandler)

        self._server_thread = threading.Thread(
            target=self._server.serve_forever,
            name="healthcheck-server",
            daemon=True,
        )
        self._server_thread.start()

        plugin.get_logger().debug(f"HTTP server started and listening on port: {self.port}")

    def _stop_http_server(self) -> None:
        if self._server:
            self.get_logger().debug("Shutting down HTTP server ...")
            self._server.shutdown()
            self.get_logger().debug("Closing HTTP server ...")
            self._server.server_close()
            self._server = None
            self.get_logger().debug("HTTP server closed")
        if self._server_thread and self._server_thread.is_alive():
            self.get_logger().debug("Cleaning up thread ...")
            self._server_thread.join(timeout=2.0)
        self._server_thread = None

    # --------------------------
    # ServicePlugin lifecycle
    # --------------------------
    def _on_service_start(self) -> bool:
        self._start_http_server()
        return True

    def _on_service_shutdown(self) -> bool:
        self._stop_http_server()
        return True

    def _on_interrupt(self, system_signal: int = None) -> None:
        pass

    def _on_error(self, source: Any, exception: Exception) -> None:
        pass
