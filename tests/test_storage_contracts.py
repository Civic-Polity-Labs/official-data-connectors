from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest

from official_data.catalog import DatasetResource
from official_data.http import FetchResult, StreamFetchResult
from official_data.storage import (
    bronze_payload_is_valid,
    content_matches_format_contract,
    content_type_matches_format,
    persist_bronze,
    persist_bronze_stream,
)


def _resource(format_name: str = "json") -> DatasetResource:
    return DatasetResource(
        family="ine",
        dataset="test",
        format=format_name,
        url=f"https://example.test/data.{format_name}",
        snapshot_token="test",
    )


def test_persisted_json_is_content_addressed_and_stream_validated(tmp_path: Path) -> None:
    content = json.dumps([{"id": index} for index in range(5)]).encode()
    resource = _resource()
    manifest = persist_bronze(
        root=tmp_path,
        resource=resource,
        run_date="2026-08-08",
        result=FetchResult(
            url=resource.url,
            status_code=200,
            headers={"Content-Type": "application/json"},
            content=content,
        ),
    )

    assert manifest.sha256 == hashlib.sha256(content).hexdigest()
    assert bronze_payload_is_valid(root=tmp_path, manifest=manifest)

    (tmp_path / manifest.bronze_path).write_bytes(b"[broken")
    assert not bronze_payload_is_valid(root=tmp_path, manifest=manifest)


def test_stream_publish_checks_bytes_hash_and_format(tmp_path: Path) -> None:
    content = b"[{\"id\":1},{\"id\":2}]"
    downloaded = tmp_path / ".staging" / "download.part"
    downloaded.parent.mkdir(parents=True)
    downloaded.write_bytes(content)
    result = StreamFetchResult(
        url="https://example.test/data.json",
        status_code=200,
        headers={"Content-Type": "application/json"},
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )

    manifest = persist_bronze_stream(
        root=tmp_path,
        resource=_resource(),
        run_date="2026-08-08",
        result=result,
        downloaded_path=downloaded,
    )

    assert not downloaded.exists()
    assert bronze_payload_is_valid(root=tmp_path, manifest=manifest)


@pytest.mark.parametrize(
    ("format_name", "content", "expected"),
    [
        ("json", b'{"rows":[]}', True),
        ("json", b'"scalar"', False),
        ("csv", b"code,value\nES,1\nFR,2\n", True),
        ("csv", b"<html><body>error</body></html>", False),
        ("xml", b"<dataset><row/></dataset>", True),
        ("xml", b"<error>unavailable</error>", False),
        ("pdf", b"prefix%PDF-1.7\n", True),
        ("pdf", b"<html>login</html>", False),
        ("html", b"<html><body>Official statistical dataset with enough useful text.</body></html>", True),
        ("html", b"<html><body>Error 404</body></html>", False),
    ],
)
def test_in_memory_format_contracts(
    format_name: str,
    content: bytes,
    expected: bool,
) -> None:
    assert content_matches_format_contract(content=content, format_name=format_name) is expected


def test_zip_contract_rejects_empty_and_traversal_archives() -> None:
    valid_stream = io.BytesIO()
    with zipfile.ZipFile(valid_stream, "w") as archive:
        archive.writestr("dataset/data.csv", "code,value\nES,1\n")
    assert content_matches_format_contract(content=valid_stream.getvalue(), format_name="zip")

    traversal_stream = io.BytesIO()
    with zipfile.ZipFile(traversal_stream, "w") as archive:
        archive.writestr("../escape.csv", "bad")
    assert not content_matches_format_contract(
        content=traversal_stream.getvalue(), format_name="zip"
    )

    empty_stream = io.BytesIO()
    with zipfile.ZipFile(empty_stream, "w"):
        pass
    assert not content_matches_format_contract(content=empty_stream.getvalue(), format_name="zip")


def test_content_types_are_strict_for_known_formats_and_neutral_for_extensions() -> None:
    assert content_type_matches_format("application/json; charset=utf-8", "json")
    assert not content_type_matches_format("text/html", "json")
    assert content_type_matches_format("text/tab-separated-values", "tsv")
    assert not content_type_matches_format(None, "json")


def test_stream_publish_rejects_wrong_bytes_hash_and_payload_format(tmp_path: Path) -> None:
    downloaded = tmp_path / "download.part"
    downloaded.write_bytes(b'{"rows":[]}')
    resource = _resource()

    with pytest.raises(OSError, match="byte count mismatch"):
        persist_bronze_stream(
            root=tmp_path,
            resource=resource,
            run_date="2026-08-08",
            result=StreamFetchResult(
                url=resource.url,
                status_code=200,
                headers={},
                sha256=hashlib.sha256(downloaded.read_bytes()).hexdigest(),
                bytes=downloaded.stat().st_size + 1,
            ),
            downloaded_path=downloaded,
        )

    with pytest.raises(OSError, match="checksum mismatch"):
        persist_bronze_stream(
            root=tmp_path,
            resource=resource,
            run_date="2026-08-08",
            result=StreamFetchResult(
                url=resource.url,
                status_code=200,
                headers={},
                sha256="0" * 64,
                bytes=downloaded.stat().st_size,
            ),
            downloaded_path=downloaded,
        )

    invalid = b"not-json"
    downloaded.write_bytes(invalid)
    with pytest.raises(ValueError, match="violates json contract"):
        persist_bronze_stream(
            root=tmp_path,
            resource=resource,
            run_date="2026-08-08",
            result=StreamFetchResult(
                url=resource.url,
                status_code=200,
                headers={},
                sha256=hashlib.sha256(invalid).hexdigest(),
                bytes=len(invalid),
            ),
            downloaded_path=downloaded,
        )


def test_stream_validation_handles_csv_tsv_gzip_xml_html_zip_and_unknown(
    tmp_path: Path,
) -> None:
    cases = {
        "csv": b"code,value\nES,1\nFR,2\n",
        "tsv": b"code\tvalue\nES\t1\n",
        "xml": b"<dataset><row code='ES'/></dataset>",
        "html": b"<html><body>Official statistical dataset with enough useful text.</body></html>",
        "bin": b"opaque-but-nonempty",
    }
    zip_stream = io.BytesIO()
    with zipfile.ZipFile(zip_stream, "w") as archive:
        archive.writestr("data.csv", "code,value\nES,1\n")
    cases["zip"] = zip_stream.getvalue()

    import gzip

    cases["csv.gz"] = gzip.compress(b"code,value\nES,1\n")

    for format_name, content in cases.items():
        resource = _resource(format_name)
        downloaded = tmp_path / ".staging" / f"{format_name.replace('.', '-')}.part"
        downloaded.parent.mkdir(parents=True, exist_ok=True)
        downloaded.write_bytes(content)
        result = StreamFetchResult(
            url=resource.url,
            status_code=200,
            headers={},
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )
        manifest = persist_bronze_stream(
            root=tmp_path,
            resource=resource,
            run_date="2026-08-08",
            result=result,
            downloaded_path=downloaded,
        )
        assert bronze_payload_is_valid(root=tmp_path, manifest=manifest)
