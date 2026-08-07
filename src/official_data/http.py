from __future__ import annotations

import hashlib
import os
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import requests

DEFAULT_HEADERS = {
    "User-Agent": "cpl-data-foundry/0.1 (+https://www.congreso.es/es/datos-abiertos)",
    "Accept": "application/json,text/csv,application/xml,text/html,*/*",
}


@dataclass(frozen=True)
class FetchResult:
    url: str
    status_code: int
    headers: Mapping[str, str]
    content: bytes

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()


@dataclass(frozen=True)
class StreamFetchResult:
    """Metadata for a response persisted incrementally to a local file."""

    url: str
    status_code: int
    headers: Mapping[str, str]
    sha256: str
    bytes: int


class RequestRateLimiter:
    """Thread-safe request pacing with a shared server-directed cooldown."""

    def __init__(self, *, min_interval_seconds: float = 0.0) -> None:
        if min_interval_seconds < 0:
            raise ValueError("min_interval_seconds must be non-negative")
        self.min_interval_seconds = min_interval_seconds
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                delay = self._next_allowed - now
                if delay <= 0:
                    self._next_allowed = now + self.min_interval_seconds
                    return
            time.sleep(delay)

    def defer(self, seconds: float) -> None:
        if seconds <= 0:
            return
        with self._lock:
            self._next_allowed = max(
                self._next_allowed,
                time.monotonic() + seconds,
            )


class CongresoHttpClient:
    """Small HTTP client with polite defaults and deterministic retries."""

    def __init__(
        self,
        *,
        timeout_seconds: int = 60,
        max_retries: int = 3,
        sleep_seconds: float = 0.6,
        headers: Mapping[str, str] | None = None,
        rate_limiter: RequestRateLimiter | None = None,
        throttle_backoff_seconds: float = 60.0,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.sleep_seconds = sleep_seconds
        self.rate_limiter = rate_limiter
        self.throttle_backoff_seconds = throttle_backoff_seconds
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS | dict(headers or {}))

    def get(self, url: str) -> FetchResult:
        return self._request("GET", url)

    def post(self, url: str, *, data: Mapping[str, str] | None = None) -> FetchResult:
        return self._request("POST", url, data=data)

    def _request(
        self,
        method: str,
        url: str,
        *,
        data: Mapping[str, str] | None = None,
        allow_official_pdf_fallback: bool = True,
    ) -> FetchResult:
        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                if self.rate_limiter is not None:
                    self.rate_limiter.wait()
                response = self.session.request(
                    method,
                    url,
                    data=data,
                    timeout=self.timeout_seconds,
                )
                should_retry = self._should_retry_response(response, attempt=attempt)
                if should_retry:
                    self._wait_for_retry(response, attempt=attempt)
                    continue
                if (
                    method == "GET"
                    and response.status_code == 404
                    and allow_official_pdf_fallback
                    and (fallback_url := official_diary_pdf_fallback_url(url))
                ):
                    return self._request(
                        method,
                        fallback_url,
                        data=data,
                        allow_official_pdf_fallback=False,
                    )
                response.raise_for_status()
                return FetchResult(
                    url=response.url,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    content=response.content,
                )
            except requests.RequestException as exc:
                last_exc = exc
                if attempt + 1 < self.max_retries:
                    time.sleep(self.sleep_seconds * (2**attempt))
        assert last_exc is not None
        raise last_exc

    def _should_retry_response(self, response: requests.Response, *, attempt: int) -> bool:
        return (
            response.status_code in {403, 429, 500, 502, 503, 504}
            and attempt + 1 < self.max_retries
        )

    def _wait_for_retry(self, response: requests.Response, *, attempt: int) -> None:
        if response.status_code in {403, 429}:
            retry_after = response.headers.get("Retry-After")
            try:
                server_delay = float(retry_after) if retry_after else 0.0
            except ValueError:
                server_delay = 0.0
            delay = max(server_delay, self.throttle_backoff_seconds * (2**attempt))
        else:
            delay = self.sleep_seconds * (2**attempt)
        if self.rate_limiter is not None:
            self.rate_limiter.defer(delay)
            return
        time.sleep(delay)

    def download(self, url: str, destination: Path) -> FetchResult:
        result = self.get(url)
        destination.parent.mkdir(parents=True, exist_ok=True)
        tmp = destination.with_suffix(destination.suffix + ".tmp")
        tmp.write_bytes(result.content)
        tmp.replace(destination)
        return result

    def download_to_file(self, url: str, destination: Path) -> StreamFetchResult:
        """Download a response in bounded memory and atomically publish it."""

        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            response = None
            tmp = destination.with_suffix(destination.suffix + ".tmp")
            try:
                if self.rate_limiter is not None:
                    self.rate_limiter.wait()
                response = self.session.get(url, stream=True, timeout=self.timeout_seconds)
                should_retry = self._should_retry_response(response, attempt=attempt)
                if should_retry:
                    response.close()
                    self._wait_for_retry(response, attempt=attempt)
                    continue
                response.raise_for_status()
                destination.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                byte_count = 0
                with tmp.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        handle.write(chunk)
                        digest.update(chunk)
                        byte_count += len(chunk)
                    handle.flush()
                    os.fsync(handle.fileno())
                tmp.replace(destination)
                return StreamFetchResult(
                    url=response.url,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    sha256=digest.hexdigest(),
                    bytes=byte_count,
                )
            except requests.RequestException as exc:
                last_exc = exc
                tmp.unlink(missing_ok=True)
                if attempt + 1 < self.max_retries:
                    time.sleep(self.sleep_seconds * (2**attempt))
            except OSError as exc:
                last_exc = exc
                tmp.unlink(missing_ok=True)
                if attempt + 1 < self.max_retries:
                    time.sleep(self.sleep_seconds * (2**attempt))
            finally:
                if response is not None:
                    response.close()
        assert last_exc is not None
        raise last_exc


def official_diary_pdf_fallback_url(url: str) -> str | None:
    """Return the official alternate path for known Congreso diary URL defects.

    Historical open-data rows sometimes label a Congreso commission diary as the
    Cortes ``CM`` series, or use the legacy ``CI`` filename even though the official
    object is stored under ``CONG/DS/CO/CO``. The caller must only use this candidate
    after the supplied URL returns 404; valid mixed-commission URLs remain untouched.
    """

    parsed = urlparse(url)
    if parsed.hostname not in {"congreso.es", "www.congreso.es"}:
        return None
    replacements = (
        ("/CORT/DS/CM/CM_", "/CONG/DS/CO/CO_"),
        ("/CONG/DS/CO/CI_", "/CONG/DS/CO/CO_"),
    )
    for source, target in replacements:
        if source in parsed.path:
            return url.replace(source, target, 1)
    return None
