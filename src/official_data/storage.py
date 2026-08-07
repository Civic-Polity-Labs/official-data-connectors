from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import zipfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from lxml import etree

from official_data.catalog import DatasetResource
from official_data.durable_io import write_json_atomically
from official_data.http import FetchResult, StreamFetchResult

_CONTENT_TYPES_BY_FORMAT = {
    "html": {"text/html", "application/xhtml+xml"},
    "htm": {"text/html", "application/xhtml+xml"},
    "json": {"application/json", "text/json", "application/download", "application/octet-stream"},
    "csv": {
        "text/csv",
        "application/csv",
        "application/vnd.ms-excel",
        "application/download",
        "application/octet-stream",
    },
    "xml": {
        "application/xml",
        "text/xml",
        "application/download",
        "application/octet-stream",
    },
    "pdf": {"application/pdf", "application/octet-stream"},
    "png": {"image/png", "application/octet-stream"},
    "zip": {
        "application/zip",
        "application/x-zip-compressed",
        "application/octet-stream",
    },
}


@dataclass(frozen=True)
class BronzeManifest:
    family: str
    dataset: str
    format: str
    source_url: str
    snapshot_token: str | None
    run_date: str
    extracted_at: str
    sha256: str
    bytes: int
    bronze_path: str
    status_code: int
    requested_url: str | None = None
    legislature: str | None = None
    session: str | None = None
    vote_number: str | None = None
    content_type: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    request_method: str = "GET"
    request_parameters_json: str | None = None
    request_parameters_sha256: str | None = None


def content_type_matches_format(content_type: str | None, format_name: str) -> bool:
    """Require a declared MIME compatible with the requested official format."""

    value = str(content_type or "").split(";", 1)[0].strip().casefold()
    expected = _CONTENT_TYPES_BY_FORMAT.get(format_name.casefold())
    return bool(value) and (expected is None or value in expected)


def decode_official_csv_value(value: str) -> str:
    """Decode the extra quote layer used by Congreso CSV exports."""

    normalized = str(value)
    if normalized.endswith('""') and normalized.count('""') == 1:
        normalized = '"' + normalized[:-2] + '"'
    return normalized.replace('""', '"')


def bronze_manifest_from_dict(raw: dict) -> BronzeManifest:
    return BronzeManifest(
        family=raw["family"],
        dataset=raw["dataset"],
        format=raw["format"],
        source_url=raw["source_url"],
        snapshot_token=raw.get("snapshot_token"),
        run_date=raw["run_date"],
        extracted_at=raw["extracted_at"],
        sha256=raw["sha256"],
        bytes=raw["bytes"],
        bronze_path=raw["bronze_path"],
        status_code=raw["status_code"],
        requested_url=raw.get("requested_url"),
        legislature=raw.get("legislature"),
        session=raw.get("session"),
        vote_number=raw.get("vote_number"),
        content_type=raw.get("content_type"),
        etag=raw.get("etag"),
        last_modified=raw.get("last_modified"),
        request_method=raw.get("request_method") or "GET",
        request_parameters_json=raw.get("request_parameters_json"),
        request_parameters_sha256=raw.get("request_parameters_sha256"),
    )


def read_bronze_manifest(path: Path) -> BronzeManifest:
    # Windows tooling may emit a UTF-8 BOM (PowerShell's default `utf8`
    # encoding).  Manifests are JSON, so accepting the signature is lossless
    # and prevents a valid official payload from becoming an unexplained
    # provenance gap during replay.
    return bronze_manifest_from_dict(json.loads(path.read_text(encoding="utf-8-sig")))


def bronze_payload_is_valid(
    *,
    root: Path,
    manifest: BronzeManifest,
    verify_checksum: bool = True,
) -> bool:
    """Verify that a reusable Bronze object still matches its content contract."""

    path = root / manifest.bronze_path
    try:
        if path.stat().st_size != manifest.bytes:
            return False
        if manifest.format.casefold() == "pdf":
            with path.open("rb") as handle:
                header = handle.read(1_024)
            if b"%PDF-" not in header:
                return False
            return not verify_checksum or _sha256_path(path) == manifest.sha256
        content = path.read_bytes()
    except OSError:
        return False
    if verify_checksum and hashlib.sha256(content).hexdigest() != manifest.sha256:
        return False
    return content_matches_format_contract(
        content=content,
        format_name=manifest.format,
    )


def content_matches_format_contract(*, content: bytes, format_name: str) -> bool:
    """Apply cheap content checks before trusting an official response or checkpoint."""

    normalized_format = format_name.casefold()
    if normalized_format == "json":
        try:
            payload = json.loads(content.decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return False
        if not isinstance(payload, (dict, list)):
            return False
    elif normalized_format == "pdf":
        # PDF readers accept the header within the first 1,024 bytes. This catches
        # error/login HTML that the official site can return with HTTP status 200.
        if b"%PDF-" not in content[:1024]:
            return False
    elif normalized_format in {"html", "htm"}:
        try:
            decoded = content.decode("utf-8-sig", errors="replace")
        except (LookupError, UnicodeError):
            return False
        folded = decoded.casefold()
        if not re.search(r"<(?:!doctype\s+html|html|body)\b", folded):
            return False
        if not re.search(r"<body\b[^>]*>[\s\S]*\S[\s\S]*</body\s*>", folded):
            return False
        error_signatures = (
            "access denied",
            "acceso denegado",
            "service unavailable",
            "servicio no disponible",
            "internal server error",
            "error 404",
            "página no encontrada",
            "pagina no encontrada",
        )
        body_text = re.sub(r"<[^>]+>", " ", folded)
        body_text = re.sub(r"\s+", " ", body_text).strip()
        if len(body_text) < 40 or any(
            signature in body_text and len(body_text) < 2_000 for signature in error_signatures
        ):
            return False
    elif normalized_format == "xml":
        try:
            root = etree.fromstring(
                content,
                parser=etree.XMLParser(
                    resolve_entities=False,
                    no_network=True,
                    recover=False,
                    huge_tree=False,
                ),
            )
        except (etree.XMLSyntaxError, ValueError):
            return False
        if (
            root is None
            or not isinstance(root.tag, str)
            or etree.QName(root).localname.casefold()
            in {
                "body",
                "error",
                "exception",
                "fault",
                "html",
                "login",
                "serviceunavailable",
            }
        ):
            return False
    elif normalized_format == "csv":
        if b"\x00" in content or not content.strip():
            return False
        decoded = None
        for encoding in ("utf-8-sig", "cp1252"):
            try:
                decoded = content.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        if decoded is None:
            return False
        folded = decoded.lstrip().casefold()
        if folded.startswith(("<!doctype", "<html", "<?xml")):
            return False
        stream = io.StringIO(decoded, newline="")
        first_line = next((line for line in stream if line.strip()), None)
        if first_line is None:
            return False
        delimiter = ";" if first_line.count(";") >= first_line.count(",") else ","
        try:
            stream.seek(0)
            rows = csv.reader(stream, delimiter=delimiter)
            header = next((row for row in rows if any(cell.strip() for cell in row)), None)
        except csv.Error:
            return False
        if header is None:
            return False
        width = len(header)
        if width < 2 or any(not str(cell).strip() for cell in header):
            return False
        try:
            for row in rows:
                if not row or not any(cell.strip() for cell in row):
                    continue
                if len(row) != width:
                    return False
        except csv.Error:
            return False
    elif normalized_format == "zip":
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                members = archive.infolist()
                files = [member for member in members if not member.is_dir()]
                if not files or len(members) > 100_000:
                    return False
                total_uncompressed = 0
                seen_members: set[str] = set()
                for member in members:
                    member_path = Path(member.filename)
                    if (
                        member_path.is_absolute()
                        or ".." in member_path.parts
                        or member.filename.replace("\\", "/") in seen_members
                        or bool(member.flag_bits & 0x1)
                        or member.file_size < 0
                        or member.compress_size < 0
                    ):
                        return False
                    seen_members.add(member.filename.replace("\\", "/"))
                    total_uncompressed += member.file_size
                    if member.file_size > 512 * 1024 * 1024:
                        return False
                    if (
                        member.file_size > 1024 * 1024
                        and member.compress_size > 0
                        and member.file_size / member.compress_size > 1_000
                    ):
                        return False
                if total_uncompressed > 1024 * 1024 * 1024:
                    return False
                if archive.testzip() is not None:
                    return False
        except (OSError, zipfile.BadZipFile):
            return False
    elif normalized_format == "png":
        if not content.startswith(b"\x89PNG\r\n\x1a\n"):
            return False
        try:
            from PIL import Image, UnidentifiedImageError

            with Image.open(io.BytesIO(content)) as image:
                if image.format != "PNG" or image.width <= 0 or image.height <= 0:
                    return False
                image.verify()
        except ImportError:
            return True
        except (OSError, UnidentifiedImageError, Image.DecompressionBombError):
            return False
    return True


def bronze_relative_path(resource: DatasetResource, run_date: str, sha256: str) -> Path:
    ext = resource.format if resource.format != "json" else "json"
    token = resource.snapshot_token or "no-token"
    request_parameters_json = canonical_request_parameters(resource)
    request_suffix = (
        f"_{hashlib.sha256(request_parameters_json.encode()).hexdigest()[:12]}"
        if request_parameters_json is not None
        else ""
    )
    return (
        Path("bronze")
        / resource.family
        / resource.dataset
        / f"snapshot_date={run_date}"
        / f"{token}_{sha256[:12]}{request_suffix}.{ext}"
    )


def canonical_request_parameters(resource: DatasetResource) -> str | None:
    if resource.post_data is None:
        return None
    return json.dumps(
        {str(key): str(value) for key, value in resource.post_data.items()},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def persist_bronze(
    *,
    root: Path,
    resource: DatasetResource,
    run_date: str,
    result: FetchResult,
) -> BronzeManifest:
    relative = bronze_relative_path(resource, run_date, result.sha256)
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    if not _path_matches_content(target, result.content, result.sha256):
        tmp = target.with_suffix(target.suffix + ".tmp")
        with tmp.open("wb") as handle:
            handle.write(result.content)
            handle.flush()
            os.fsync(handle.fileno())
        tmp.replace(target)
    if not _path_matches_content(target, result.content, result.sha256):
        raise OSError(f"Persisted Bronze payload failed checksum verification: {target}")

    request_parameters_json = canonical_request_parameters(resource)
    manifest = BronzeManifest(
        family=resource.family,
        dataset=resource.dataset,
        format=resource.format,
        source_url=result.url,
        snapshot_token=resource.snapshot_token,
        run_date=run_date,
        extracted_at=datetime.now(UTC).isoformat(),
        sha256=result.sha256,
        bytes=len(result.content),
        bronze_path=str(relative).replace("\\", "/"),
        status_code=result.status_code,
        requested_url=resource.url if result.url != resource.url else None,
        legislature=resource.legislature,
        session=resource.session,
        vote_number=resource.vote_number,
        content_type=result.headers.get("Content-Type"),
        etag=result.headers.get("ETag"),
        last_modified=result.headers.get("Last-Modified"),
        request_method="POST" if resource.post_data is not None else "GET",
        request_parameters_json=request_parameters_json,
        request_parameters_sha256=(
            hashlib.sha256(request_parameters_json.encode()).hexdigest()
            if request_parameters_json is not None
            else None
        ),
    )
    manifest_path = target.with_suffix(target.suffix + ".manifest.json")
    write_json_atomically(manifest_path, asdict(manifest))
    return manifest


def _path_matches_content(path: Path, content: bytes, expected_sha256: str) -> bool:
    try:
        if path.stat().st_size != len(content):
            return False
        return _sha256_path(path) == expected_sha256
    except OSError:
        return False


def _sha256_path(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def persist_bronze_stream(
    *,
    root: Path,
    resource: DatasetResource,
    run_date: str,
    result: StreamFetchResult,
    downloaded_path: Path,
) -> BronzeManifest:
    """Publish a previously streamed download and write its manifest."""

    relative = bronze_relative_path(resource, run_date, result.sha256)
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    if downloaded_path.stat().st_size != result.bytes:
        raise OSError(f"Streamed Bronze byte count mismatch: {downloaded_path}")
    if _sha256_path(downloaded_path) != result.sha256:
        raise OSError(f"Streamed Bronze checksum mismatch: {downloaded_path}")
    if not _path_matches_format_contract(downloaded_path, resource.format):
        raise ValueError(
            f"Streamed Bronze payload violates {resource.format} contract: {downloaded_path}"
        )
    if target.exists() and _path_matches_stream_result(target, result, resource.format):
        downloaded_path.unlink(missing_ok=True)
    else:
        downloaded_path.replace(target)
    if not _path_matches_stream_result(target, result, resource.format):
        raise OSError(f"Published streamed Bronze object failed verification: {target}")

    request_parameters_json = canonical_request_parameters(resource)
    manifest = BronzeManifest(
        family=resource.family,
        dataset=resource.dataset,
        format=resource.format,
        source_url=result.url,
        snapshot_token=resource.snapshot_token,
        run_date=run_date,
        extracted_at=datetime.now(UTC).isoformat(),
        sha256=result.sha256,
        bytes=result.bytes,
        bronze_path=str(relative).replace("\\", "/"),
        status_code=result.status_code,
        requested_url=resource.url if result.url != resource.url else None,
        legislature=resource.legislature,
        session=resource.session,
        vote_number=resource.vote_number,
        content_type=result.headers.get("Content-Type"),
        etag=result.headers.get("ETag"),
        last_modified=result.headers.get("Last-Modified"),
        request_method="POST" if resource.post_data is not None else "GET",
        request_parameters_json=request_parameters_json,
        request_parameters_sha256=(
            hashlib.sha256(request_parameters_json.encode()).hexdigest()
            if request_parameters_json is not None
            else None
        ),
    )
    manifest_path = target.with_suffix(target.suffix + ".manifest.json")
    write_json_atomically(manifest_path, asdict(manifest))
    return manifest


def _path_matches_stream_result(
    path: Path,
    result: StreamFetchResult,
    format_name: str,
) -> bool:
    try:
        return (
            path.stat().st_size == result.bytes
            and _sha256_path(path) == result.sha256
            and _path_matches_format_contract(path, format_name)
        )
    except OSError:
        return False


def _path_matches_format_contract(path: Path, format_name: str) -> bool:
    normalized = format_name.casefold()
    if normalized == "pdf":
        with path.open("rb") as handle:
            return b"%PDF-" in handle.read(1_024)
    return content_matches_format_contract(content=path.read_bytes(), format_name=format_name)
