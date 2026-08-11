from __future__ import annotations

import hashlib
import io
from pathlib import Path

import pytest
import requests

from official_data.http import (
    DEFAULT_HEADERS,
    OfficialDataHttpClient,
    ResponseTooLargeError,
    _retry_after_seconds,
)


class _FakeSession:
    def __init__(self, responses: list[requests.Response | Exception]) -> None:
        self.responses = responses
        self.headers: dict[str, str] = {}
        self.calls: list[tuple[str, str, dict]] = []
        self.closed = False

    def request(self, *args, **kwargs) -> requests.Response:
        self.calls.append((str(args[0]), str(args[1]), kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def get(self, url: str, **kwargs) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def close(self) -> None:
        self.closed = True


def _response(status: int, body: bytes, **headers: str) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = body
    response.url = "https://example.test/data"
    response.headers.update(headers)
    response.raw = _Raw(body)
    response.was_closed = False
    original_close = response.close

    def tracked_close() -> None:
        response.was_closed = True
        original_close()

    response.close = tracked_close
    return response


class _Raw(io.BytesIO):
    def stream(self, amount: int, decode_content: bool = False):
        while chunk := self.read(amount):
            yield chunk


def test_transport_is_provider_neutral_and_bounds_buffered_responses() -> None:
    assert "Congreso" not in DEFAULT_HEADERS["User-Agent"]
    client = OfficialDataHttpClient(max_retries=1, max_response_bytes=3)
    client.session = _FakeSession([_response(200, b"four")])

    with pytest.raises(ResponseTooLargeError, match="configured limit"):
        client.get("https://example.test/data")


def test_transport_rejects_invalid_resource_controls() -> None:
    with pytest.raises(ValueError, match="max_retries"):
        OfficialDataHttpClient(max_retries=0)
    with pytest.raises(ValueError, match="timeout_seconds"):
        OfficialDataHttpClient(timeout_seconds=-1)
    with pytest.raises(ValueError, match="sleep_seconds"):
        OfficialDataHttpClient(sleep_seconds=-1)
    with pytest.raises(ValueError, match="throttle_backoff_seconds"):
        OfficialDataHttpClient(throttle_backoff_seconds=0)
    with pytest.raises(ValueError, match="byte limits"):
        OfficialDataHttpClient(max_response_bytes=0)


def test_request_retries_transient_status_and_closes_every_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _response(503, b"busy")
    second = _response(200, b'{"ok":true}', **{"Content-Type": "application/json"})
    session = _FakeSession([first, second])
    sleeps: list[float] = []
    monkeypatch.setattr("official_data.http.time.sleep", sleeps.append)
    client = OfficialDataHttpClient(
        session=session,
        max_retries=2,
        sleep_seconds=0.25,
    )

    result = client.post("https://example.test/data", data={"query": "value"})

    assert result.content == b'{"ok":true}'
    assert result.sha256 == hashlib.sha256(result.content).hexdigest()
    assert sleeps == [0.25]
    assert len(session.calls) == 2
    assert session.calls[0][2]["timeout"] == (10.0, 60.0)
    assert session.calls[0][2]["stream"] is True
    assert first.was_closed and second.was_closed


def test_request_retries_network_error_and_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("official_data.http.time.sleep", lambda seconds: None)
    recovered = _FakeSession(
        [requests.ConnectionError("offline"), _response(200, b"recovered")]
    )
    assert OfficialDataHttpClient(session=recovered, max_retries=2).get(
        "https://example.test/data"
    ).content == b"recovered"

    failed = _FakeSession([requests.Timeout("slow"), requests.Timeout("still slow")])
    with pytest.raises(requests.Timeout, match="still slow"):
        OfficialDataHttpClient(session=failed, max_retries=2).get(
            "https://example.test/data"
        )


def test_retry_after_parses_seconds_and_http_dates() -> None:
    assert _retry_after_seconds("2.5") == 2.5
    assert _retry_after_seconds("invalid") == 0.0
    assert _retry_after_seconds(None) == 0.0
    assert _retry_after_seconds("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0


def test_stream_download_is_atomic_hashed_and_context_closes_session(tmp_path: Path) -> None:
    content = b"0123456789"
    response = _response(200, content, **{"Content-Length": str(len(content))})
    session = _FakeSession([response])
    destination = tmp_path / "downloads" / "data.csv"

    with OfficialDataHttpClient(session=session, max_download_bytes=32) as client:
        result = client.download_to_file("https://example.test/data.csv", destination)

    assert destination.read_bytes() == content
    assert result.bytes == len(content)
    assert result.sha256 == hashlib.sha256(content).hexdigest()
    assert not destination.with_suffix(".csv.tmp").exists()
    assert session.closed


def test_stream_download_refuses_header_or_body_over_limit_and_cleans_temp(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "data.csv"
    header_session = _FakeSession([_response(200, b"1234", **{"Content-Length": "10"})])
    with pytest.raises(ResponseTooLargeError, match="Content-Length"):
        OfficialDataHttpClient(session=header_session).download_to_file(
            "https://example.test/data.csv", destination, max_bytes=4
        )
    assert not destination.exists()
    assert not destination.with_suffix(".csv.tmp").exists()

    body_session = _FakeSession([_response(200, b"12345")])
    with pytest.raises(ResponseTooLargeError, match="configured limit"):
        OfficialDataHttpClient(session=body_session).download_to_file(
            "https://example.test/data.csv", destination, max_bytes=4
        )
    assert not destination.exists()
    assert not destination.with_suffix(".csv.tmp").exists()


def test_download_wrapper_publishes_only_complete_buffered_response(tmp_path: Path) -> None:
    session = _FakeSession([_response(200, b"small")])
    destination = tmp_path / "nested" / "data.json"
    result = OfficialDataHttpClient(session=session).download(
        "https://example.test/data.json", destination
    )

    assert result.content == destination.read_bytes() == b"small"
    assert not destination.with_suffix(".json.tmp").exists()
