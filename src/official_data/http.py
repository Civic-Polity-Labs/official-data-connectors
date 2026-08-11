"""Bounded, provider-neutral HTTP transport for official statistical sources."""

from __future__ import annotations

import hashlib
import os
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Self

import requests

DEFAULT_HEADERS = {
    "User-Agent": "official-data-connectors/1.0 (+https://github.com/Civic-Polity-Labs)",
    "Accept": "application/json,text/csv,application/xml,text/html,*/*",
}
DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024 * 1024
DEFAULT_DOWNLOAD_BYTES = 8 * 1024 * 1024 * 1024


class ResponseTooLargeError(ValueError):
    """Raised before a response can exceed its configured byte budget."""


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
            self._next_allowed = max(self._next_allowed, time.monotonic() + seconds)


class OfficialDataHttpClient:
    """Session-based HTTP client with explicit timeouts and bounded responses."""

    def __init__(
        self,
        *,
        timeout_seconds: float | tuple[float, float] = (10.0, 60.0),
        max_retries: int = 3,
        sleep_seconds: float = 0.6,
        headers: Mapping[str, str] | None = None,
        rate_limiter: RequestRateLimiter | None = None,
        throttle_backoff_seconds: float = 60.0,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        max_download_bytes: int = DEFAULT_DOWNLOAD_BYTES,
        session: requests.Session | None = None,
    ) -> None:
        self.timeout_seconds = _validated_timeout(timeout_seconds)
        if max_retries < 1:
            raise ValueError("max_retries must be at least 1")
        if sleep_seconds < 0:
            raise ValueError("sleep_seconds must be non-negative")
        if throttle_backoff_seconds <= 0:
            raise ValueError("throttle_backoff_seconds must be positive")
        if max_response_bytes < 1 or max_download_bytes < 1:
            raise ValueError("response byte limits must be positive")
        self.max_retries = max_retries
        self.sleep_seconds = sleep_seconds
        self.rate_limiter = rate_limiter
        self.throttle_backoff_seconds = throttle_backoff_seconds
        self.max_response_bytes = max_response_bytes
        self.max_download_bytes = max_download_bytes
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS | dict(headers or {}))

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

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
    ) -> FetchResult:
        last_exc: requests.RequestException | None = None
        for attempt in range(self.max_retries):
            response: requests.Response | None = None
            try:
                if self.rate_limiter is not None:
                    self.rate_limiter.wait()
                response = self.session.request(
                    method,
                    url,
                    data=data,
                    timeout=self.timeout_seconds,
                    stream=True,
                )
                if self._should_retry_response(response, attempt=attempt):
                    self._wait_for_retry(response, attempt=attempt)
                    continue
                response.raise_for_status()
                return FetchResult(
                    url=response.url,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    content=_read_bounded_response(response, max_bytes=self.max_response_bytes),
                )
            except requests.RequestException as exc:
                last_exc = exc
                if attempt + 1 < self.max_retries:
                    time.sleep(self.sleep_seconds * (2**attempt))
            finally:
                if response is not None:
                    _close_response(response)
        if last_exc is None:
            raise RuntimeError("HTTP request exhausted without a response")
        raise last_exc

    def _should_retry_response(self, response: requests.Response, *, attempt: int) -> bool:
        return (
            response.status_code in {403, 429, 500, 502, 503, 504}
            and attempt + 1 < self.max_retries
        )

    def _wait_for_retry(self, response: requests.Response, *, attempt: int) -> None:
        if response.status_code in {403, 429}:
            delay = max(
                _retry_after_seconds(response.headers.get("Retry-After")),
                self.throttle_backoff_seconds * (2**attempt),
            )
        else:
            delay = self.sleep_seconds * (2**attempt)
        if self.rate_limiter is not None:
            self.rate_limiter.defer(delay)
        else:
            time.sleep(delay)

    def download(self, url: str, destination: Path) -> FetchResult:
        result = self.get(url)
        destination.parent.mkdir(parents=True, exist_ok=True)
        tmp = destination.with_suffix(destination.suffix + ".tmp")
        tmp.write_bytes(result.content)
        tmp.replace(destination)
        return result

    def download_to_file(
        self,
        url: str,
        destination: Path,
        *,
        max_bytes: int | None = None,
    ) -> StreamFetchResult:
        """Download in bounded memory and atomically publish the completed file."""

        byte_limit = self.max_download_bytes if max_bytes is None else max_bytes
        if byte_limit < 1:
            raise ValueError("max_bytes must be positive")
        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            response: requests.Response | None = None
            tmp = destination.with_suffix(destination.suffix + ".tmp")
            try:
                if self.rate_limiter is not None:
                    self.rate_limiter.wait()
                response = self.session.get(url, stream=True, timeout=self.timeout_seconds)
                if self._should_retry_response(response, attempt=attempt):
                    self._wait_for_retry(response, attempt=attempt)
                    continue
                response.raise_for_status()
                _validate_content_length(response, max_bytes=byte_limit)
                destination.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                byte_count = 0
                with tmp.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        if byte_count + len(chunk) > byte_limit:
                            raise ResponseTooLargeError(
                                f"Response exceeded configured limit of {byte_limit} bytes"
                            )
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
            except (requests.RequestException, OSError) as exc:
                last_exc = exc
                tmp.unlink(missing_ok=True)
                if attempt + 1 < self.max_retries:
                    time.sleep(self.sleep_seconds * (2**attempt))
            except ResponseTooLargeError:
                tmp.unlink(missing_ok=True)
                raise
            finally:
                if response is not None:
                    _close_response(response)
        if last_exc is None:
            raise RuntimeError("HTTP download exhausted without a response")
        raise last_exc


def _validated_timeout(value: float | tuple[float, float]) -> float | tuple[float, float]:
    values = value if isinstance(value, tuple) else (value,)
    if len(values) not in {1, 2} or any(float(item) <= 0 for item in values):
        raise ValueError("timeout_seconds must contain positive values")
    return tuple(float(item) for item in values) if isinstance(value, tuple) else float(value)


def _close_response(response: requests.Response) -> None:
    try:
        response.close()
    except AttributeError:
        if response.raw is not None:
            raise


def _validate_content_length(response: requests.Response, *, max_bytes: int) -> None:
    raw = response.headers.get("Content-Length")
    if raw and raw.isdigit() and int(raw) > max_bytes:
        raise ResponseTooLargeError(
            f"Response Content-Length {raw} exceeds configured limit of {max_bytes} bytes"
        )


def _read_bounded_response(response: requests.Response, *, max_bytes: int) -> bytes:
    _validate_content_length(response, max_bytes=max_bytes)
    if response.raw is None:
        content = response.content
        if len(content) > max_bytes:
            raise ResponseTooLargeError(f"Response exceeded configured limit of {max_bytes} bytes")
        return content
    content = bytearray()
    for chunk in response.iter_content(chunk_size=1024 * 1024):
        if not chunk:
            continue
        if len(content) + len(chunk) > max_bytes:
            raise ResponseTooLargeError(f"Response exceeded configured limit of {max_bytes} bytes")
        content.extend(chunk)
    return bytes(content)


def _retry_after_seconds(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            moment = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return 0.0
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        return max(0.0, (moment - datetime.now(UTC)).total_seconds())

__all__ = [
    "FetchResult",
    "OfficialDataHttpClient",
    "RequestRateLimiter",
    "ResponseTooLargeError",
    "StreamFetchResult",
]
