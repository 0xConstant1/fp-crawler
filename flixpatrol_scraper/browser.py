from __future__ import annotations

import asyncio
import multiprocessing
import os
import signal
import subprocess
import threading
from collections.abc import Callable
from dataclasses import dataclass
from multiprocessing.connection import Connection
from typing import Any

from curl_cffi.requests.exceptions import HTTPError


TITLE_DETAIL_WORKERS = 5


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
    source_html: str | None = None

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise HTTPError(f"HTTP {self.status_code}")


def _serialize_page(page, document: dict) -> BrowserResponse:
    return BrowserResponse(
        status_code=document.get("status", page.status),
        url=document.get("url", str(page.url)),
        text=page.body.decode("utf-8", errors="replace"),
        headers={
            str(k).lower(): str(v)
            for k, v in document.get("headers", page.headers).items()
        },
        source_html=document.get("source_html"),
    )


async def _fetch_page(session, url: str, *, include_source: bool):
    document = {}
    listeners = []

    async def observe(page) -> None:
        document.clear()

        async def capture(response) -> None:
            if (
                response.request.is_navigation_request()
                and response.frame == page.main_frame
            ):
                document.update(
                    status=response.status,
                    url=response.url,
                    headers=response.headers,
                    navigation=response,
                    source_html=None,
                )
                headers = await response.all_headers()
                if document.get("navigation") is response:
                    document["headers"] = headers

        page.on("response", capture)
        listeners.append((page, capture))

    async def capture_source(_page) -> None:
        navigation = document.get("navigation")
        if navigation is not None and navigation.status == 200:
            source_html = await navigation.text()
            if navigation is document.get("navigation"):
                document["source_html"] = source_html

    try:
        # Read the response body before Scrapling releases the page. Its own HTML is rendered DOM.
        return await session.fetch(
            url, page_setup=observe, page_action=capture_source if include_source else None
        ), document
    finally:
        for page, listener in listeners:
            page.remove_listener("response", listener)


@dataclass(slots=True)
class _TitleRequest:
    url: str
    timeout_ms: int
    reply: Connection
    navigate: bool


async def _request_title(session, command: _TitleRequest) -> None:
    try:
        if command.navigate:
            page = await session.context.new_page()
            try:
                response = await page.goto(
                    command.url, timeout=command.timeout_ms, wait_until="domcontentloaded"
                )
                if response is None:
                    raise BrowserError("Title navigation returned no document")
                payload = BrowserResponse(
                    response.status, response.url, await response.text(), await response.all_headers()
                )
            finally:
                await page.close()
        else:
            response = await session.context.request.get(
                command.url, timeout=command.timeout_ms, max_redirects=0
            )
            try:
                payload = BrowserResponse(
                    response.status, response.url, await response.text(), response.headers
                )
            finally:
                await response.dispose()
        result = True, payload
    except Exception as exc:  # noqa: BLE001
        result = False, f"Browser HTTP request failed ({type(exc).__name__})"

    try:
        await asyncio.to_thread(command.reply.send, result)
    except (BrokenPipeError, EOFError, OSError):
        pass
    finally:
        command.reply.close()


async def _run_browser(connection: Connection, options: dict[str, Any]) -> None:
    from scrapling.fetchers import AsyncStealthySession

    async with AsyncStealthySession(**options) as session:
        requests = set()
        title_slots = asyncio.Semaphore(TITLE_DETAIL_WORKERS)

        async def fetch_title(command: _TitleRequest) -> None:
            async with title_slots:
                await _request_title(session, command)

        try:
            while True:
                command = await asyncio.to_thread(connection.recv)
                if command is None:
                    return
                if isinstance(command, _TitleRequest):
                    task = asyncio.create_task(fetch_title(command))
                    requests.add(task)
                    task.add_done_callback(requests.discard)
                    continue
                url, credentials_only, include_source = command
                page, document = await _fetch_page(
                    session, url, include_source=include_source and not credentials_only
                )
                if credentials_only:
                    from .cf import extract_cf_clearance, extract_user_agent

                    payload = (
                        document.get("status", page.status),
                        extract_cf_clearance(page),
                        extract_user_agent(page),
                    )
                else:
                    payload = _serialize_page(page, document)
                await asyncio.to_thread(connection.send, (True, payload))
        finally:
            if requests:
                await asyncio.gather(*requests, return_exceptions=True)


def _browser_worker(connection: Connection, options: dict[str, Any]) -> None:
    if os.name != "nt":
        os.setsid()
    try:
        asyncio.run(_run_browser(connection, options))
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
        self,
        *,
        headless: bool,
        timeout_ms: int,
        proxy: str | None = None,
        pace_request: Callable[[], None] | None = None,
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
        self._pace_request = pace_request

    def fetch(
        self,
        url: str,
        *,
        deadline_seconds: float,
        credentials_only: bool = False,
        include_source: bool = True,
        before_send: Callable[[], None] | None = None,
        pace: bool = True,
    ):
        with self._lock:
            if self._process is None:
                self._start()
            try:
                if pace and self._pace_request is not None:
                    self._pace_request()
                if before_send is not None:
                    before_send()
                self._connection.send((url, credentials_only, include_source))
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

    def request(
        self,
        url: str,
        *,
        timeout_seconds: float,
        before_send: Callable[[], None],
        navigate: bool = False,
    ) -> BrowserResponse:
        """Fetch title HTML in the persistent browser context."""
        with self._lock:
            if self._process is None:
                self._start()
            before_send()
            reader, writer = multiprocessing.Pipe(duplex=False)
            try:
                self._connection.send(_TitleRequest(url, int(timeout_seconds * 1000), writer, navigate))
            except (BrokenPipeError, EOFError, OSError) as exc:
                reader.close()
                self.close()
                raise BrowserError(f"Browser worker exited for {url!r}") from exc
            finally:
                writer.close()

        with reader:
            try:
                if not reader.poll(timeout_seconds + 5):
                    raise BrowserTimeoutError(f"Browser HTTP request timed out for {url!r}")
                success, payload = reader.recv()
            except (BrokenPipeError, EOFError, OSError) as exc:
                raise BrowserError(f"Browser worker exited for {url!r}") from exc
        if not success:
            raise BrowserError(payload)
        return payload

    def _start(self) -> None:
        context = multiprocessing.get_context("spawn")
        try:
            parent, child = context.Pipe()
        except OSError as exc:
            raise BrowserError("Could not open the browser worker connection") from exc
        try:
            process = context.Process(
                target=_browser_worker, args=(child, self.options), daemon=True
            )
            process.start()
        except (OSError, RuntimeError) as exc:
            parent.close()
            raise BrowserError("Could not start the browser worker") from exc
        finally:
            child.close()
        self._process, self._connection = process, parent

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
