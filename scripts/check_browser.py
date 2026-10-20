from __future__ import annotations

from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from bs4 import BeautifulSoup

from flixpatrol_scraper.__main__ import _env_flag
from flixpatrol_scraper.browser import BrowserClient


class _PageHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = b'<html><head><script>document.documentElement.setAttribute("data-browser-ready", "yes")</script></head><body>Browser startup check</body></html>'
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args) -> None:
        pass


def main() -> int:
    with HTTPServer(("127.0.0.1", 0), _PageHandler) as server:
        worker = Thread(target=server.serve_forever, daemon=True)
        worker.start()
        client = BrowserClient(
            headless=not _env_flag("FLIXPATROL_SOLVE_HEADED"), timeout_ms=15_000
        )
        try:
            url = f"http://127.0.0.1:{server.server_port}/"
            page = client.fetch(url, deadline_seconds=45)
            soup = BeautifulSoup(page.text, "html.parser")
            if page.status_code != 200 or soup.html.get("data-browser-ready") != "yes":
                raise RuntimeError(
                    "Browser did not execute the local page's JavaScript"
                )
            print(
                "Browser launched, rendered HTML, and executed JavaScript successfully",
                flush=True,
            )
        finally:
            client.close()
            server.shutdown()
            worker.join(2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
