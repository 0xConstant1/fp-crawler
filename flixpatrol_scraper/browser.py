from __future__ import annotations

import multiprocessing
import os
import signal
import subprocess
import threading
from dataclasses import dataclass
from multiprocessing.connection import Connection
from typing import Any

from curl_cffi.requests.exceptions import HTTPError


class BrowserError(RuntimeError):
    pass


class BrowserTimeoutError(BrowserError):
    pass


@dataclass(slots=True)
class BrowserResponse:
    status_code: int
    url: str
    text: str
    headers: dict[str, str]

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise HTTPError(f"HTTP {self.status_code}")


def _serialize_page(page, document: dict) -> BrowserResponse:
    return BrowserResponse(
        status_code=document.get("status", page.status),
        url=str(page.url),
        text=page.body.decode("utf-8", errors="replace"),
        headers={
            str(k).lower(): str(v)
            for k, v in document.get("headers", page.headers).items()
        },
    )


def _fetch_page(session, url: str):
    document = {}
    listeners = []

    def observe(page) -> None:
        def capture(response) -> None:
            if (
                response.request.is_navigation_request()
                and response.frame == page.main_frame
            ):
                document.update(status=response.status, headers=response.all_headers())

        page.on("response", capture)
        listeners.append((page, capture))

    try:
        # Scrapling returns initial-response headers alongside final-page HTML.
        return session.fetch(url, page_setup=observe), document
    finally:
        for page, listener in listeners:
            page.remove_listener("response", listener)


def _browser_worker(connection: Connection, options: dict[str, Any]) -> None:
    if os.name != "nt":
        os.setsid()
    try:
        from scrapling.fetchers import StealthySession

        with StealthySession(**options) as session:
            while True:
                command = connection.recv()
                if command is None:
                    return
                url, credentials_only = command
                page, document = _fetch_page(session, url)
                if credentials_only:
                    from .cf import extract_cf_clearance, extract_user_agent

                    payload = (
                        document.get("status", page.status),
                        extract_cf_clearance(page),
                        extract_user_agent(page),
                    )
                else:
                    payload = _serialize_page(page, document)
                connection.send((True, payload))
    except EOFError:
        pass
    except Exception as exc:  # noqa: BLE001
        try:
            connection.send((False, f"{type(exc).__name__}: {exc}"))
        except (BrokenPipeError, EOFError, OSError):
            pass
    finally:
        connection.close()


class BrowserClient:
    """Keep the browser on one process/thread and bound every navigation."""

    def __init__(
        self, *, headless: bool, timeout_ms: int, proxy: str | None = None
    ) -> None:
        self.options = {
            "headless": headless,
            "timeout": timeout_ms,
            "solve_cloudflare": True,
            "google_search": False,
            **({"proxy": proxy} if proxy else {}),
        }
        self._process: multiprocessing.Process | None = None
        self._connection: Connection | None = None
        self._lock = threading.RLock()

    def fetch(
        self, url: str, *, deadline_seconds: float, credentials_only: bool = False
    ):
        with self._lock:
            if self._process is None:
                context = multiprocessing.get_context("spawn")
                parent, child = context.Pipe()
                self._process = context.Process(
                    target=_browser_worker, args=(child, self.options), daemon=True
                )
                self._connection = parent
                self._process.start()
                child.close()
            try:
                self._connection.send((url, credentials_only))
                if not self._connection.poll(deadline_seconds):
                    raise BrowserTimeoutError(
                        f"Browser exceeded {deadline_seconds:.0f}s for {url!r}"
                    )
                success, payload = self._connection.recv()
                if not success:
                    raise BrowserError(payload)
                return payload
            except (EOFError, BrokenPipeError, OSError) as exc:
                self.close()
                raise BrowserError(f"Browser worker exited for {url!r}") from exc
            except BrowserError:
                self.close()
                raise

    def close(self) -> None:
        with self._lock:
            process, connection = self._process, self._connection
            self._process = self._connection = None
            if process is None:
                return
            if process.is_alive():
                try:
                    connection.send(None)
                except (BrokenPipeError, EOFError, OSError):
                    pass
                process.join(1)
                if process.is_alive():
                    if os.name == "nt":
                        subprocess.run(
                            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            creationflags=subprocess.CREATE_NO_WINDOW,
                            check=False,
                        )
                    else:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            process.kill()
                    process.join(5)
            connection.close()
            process.close()
