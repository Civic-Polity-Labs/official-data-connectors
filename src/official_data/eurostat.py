from __future__ import annotations

import csv
import gzip
import json
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from dateutil import parser as date_parser

from official_data.catalog import DatasetResource
from official_data.durable_io import write_json_atomically
from official_data.http import FetchResult, OfficialDataHttpClient, StreamFetchResult
from official_data.normalization import normalize_key, stable_id
from official_data.storage import (
    BronzeManifest,
    persist_bronze,
    persist_bronze_stream,
    read_bronze_manifest,
)

EUROSTAT_API_BASE = "https://ec.europa.eu/eurostat/api/dissemination"
EUROSTAT_LANGUAGE = "en"
EUROSTAT_FAMILY = "eurostat"
EUROSTAT_CATALOG_DATASET = "Catalog"
EUROSTAT_TOC_DATASET = "Toc"
EUROSTAT_TOC_XML_DATASET = "TocXml"
EUROSTAT_CODELIST_INVENTORY_DATASET = "CodelistInventory"
EUROSTAT_CODELIST_DATASET = "Codelist"
EUROSTAT_DATASET_DATA = "DatasetData"
EUROSTAT_DATASET_STRUCTURE = "DatasetStructure"

EUROSTAT_OBSERVATION_COLUMNS = {
    "DATAFLOW",
    "LAST UPDATE",
    "TIME_PERIOD",
    "OBS_VALUE",
    "OBS_STATUS",
    "OBS_FLAG",
    "CONF_STATUS",
    "DECIMALS",
}
EUROSTAT_OBSERVATION_ID_PREFIX = "es_"
EUROSTAT_OBSERVATION_ID_HEX_LENGTH = 32
EUROSTAT_VALUE_COLUMNS = {"OBS_VALUE"}
EUROSTAT_ATTRIBUTE_COLUMNS = {"OBS_STATUS", "OBS_FLAG", "CONF_STATUS", "DECIMALS"}


@dataclass(frozen=True)
class EurostatApiRequest:
    url: str
    kind: str
    params: dict[str, Any] | None = None


@dataclass(frozen=True)
class EurostatDatasetPlan:
    dataset_code: str
    data_url: str
    structure_url: str | None
    last_data_change_at: str | None
    last_structural_change_at: str | None


class EurostatApiClient:
    def __init__(
        self,
        *,
        language: str = EUROSTAT_LANGUAGE,
        http_client: OfficialDataHttpClient | None = None,
    ) -> None:
        self.language = language
        self.http_client = http_client or OfficialDataHttpClient(
            timeout_seconds=180,
            max_retries=4,
            sleep_seconds=1.2,
            headers={
                "User-Agent": (
                    "cpl-data-foundry/0.1 "
                    "(Eurostat API; https://ec.europa.eu/eurostat/api/dissemination)"
                )
            },
        )

    def inventory_url(self, inventory_type: str) -> str:
        return f"{EUROSTAT_API_BASE}/files/inventory?" + urlencode(
            {"type": inventory_type, "lang": self.language}
        )

    def toc_url(self) -> str:
        return f"{EUROSTAT_API_BASE}/catalogue/toc/txt?" + urlencode({"lang": self.language})

    def toc_xml_url(self) -> str:
        return f"{EUROSTAT_API_BASE}/catalogue/toc/xml"

    def get(self, url: str) -> FetchResult:
        return self.http_client.get(url)

    def get_resolving_async(
        self,
        url: str,
        *,
        wait_seconds: int = 1800,
        poll_seconds: int = 30,
    ) -> FetchResult:
        result = self.get(url)
        async_id = _async_request_id(result.content)
        if not async_id:
            return result

        deadline = time.monotonic() + max(0, wait_seconds)
        while time.monotonic() <= deadline:
            status = self.async_status(async_id)
            if status == "AVAILABLE":
                return self.get(f"{EUROSTAT_API_BASE}/1.0/async/data/{async_id}")
            if status in {"EXPIRED", "UNKNOWN_REQUEST", "ERROR"}:
                raise RuntimeError(f"Eurostat async request {async_id} ended with status {status}")
            time.sleep(max(1, poll_seconds))
        raise TimeoutError(f"Eurostat async request {async_id} was not ready in {wait_seconds}s")

    def download_resolving_async(
        self,
        url: str,
        *,
        destination: Path,
        wait_seconds: int = 1800,
        poll_seconds: int = 30,
    ) -> StreamFetchResult:
        """Resolve Eurostat async responses while keeping dataset bytes on disk."""

        initial_path = destination.with_suffix(destination.suffix + ".initial")
        initial = self.http_client.download_to_file(url, initial_path)
        try:
            with initial_path.open("rb") as handle:
                async_id = _async_request_id(handle.read(64 * 1024))
            if not async_id:
                initial_path.replace(destination)
                return initial
            initial_path.unlink(missing_ok=True)
        except Exception:
            initial_path.unlink(missing_ok=True)
            raise

        deadline = time.monotonic() + max(0, wait_seconds)
        while time.monotonic() <= deadline:
            status = self.async_status(async_id)
            if status == "AVAILABLE":
                return self.http_client.download_to_file(
                    f"{EUROSTAT_API_BASE}/1.0/async/data/{async_id}",
                    destination,
                )
            if status in {"EXPIRED", "UNKNOWN_REQUEST", "ERROR"}:
                raise RuntimeError(f"Eurostat async request {async_id} ended with status {status}")
            time.sleep(max(1, poll_seconds))
        raise TimeoutError(f"Eurostat async request {async_id} was not ready in {wait_seconds}s")

    def async_status(self, async_id: str) -> str:
        result = self.get(f"{EUROSTAT_API_BASE}/1.0/async/status/{async_id}")
        return _async_status(result.content) or "UNKNOWN_REQUEST"


def discover_eurostat_catalog(
    *,
    client: EurostatApiClient | None = None,
    language: str = EUROSTAT_LANGUAGE,
) -> dict[str, Any]:
    client = client or EurostatApiClient(language=language)
    result = client.get(client.inventory_url("data"))
    return eurostat_catalog_payload_from_content(result.content, language=language)


def discover_eurostat_codelist_inventory(
    *,
    client: EurostatApiClient | None = None,
    language: str = EUROSTAT_LANGUAGE,
) -> dict[str, Any]:
    client = client or EurostatApiClient(language=language)
    result = client.get(client.inventory_url("codelist"))
    return eurostat_codelist_inventory_payload_from_content(result.content, language=language)


def discover_eurostat_toc(
    *,
    client: EurostatApiClient | None = None,
    language: str = EUROSTAT_LANGUAGE,
) -> dict[str, Any]:
    client = client or EurostatApiClient(language=language)
    result = client.get(client.toc_url())
    return eurostat_toc_payload_from_content(result.content, language=language)


def eurostat_catalog_payload_from_content(
    content: bytes,
    *,
    language: str = EUROSTAT_LANGUAGE,
) -> dict[str, Any]:
    return {
        "source": "eurostat_inventory_api",
        "language": language,
        "discovered_at": datetime.now(UTC).isoformat(),
        "datasets": _read_tsv_rows(_decode_text(content)),
    }


def eurostat_codelist_inventory_payload_from_content(
    content: bytes,
    *,
    language: str = EUROSTAT_LANGUAGE,
) -> dict[str, Any]:
    return {
        "source": "eurostat_codelist_inventory_api",
        "language": language,
        "discovered_at": datetime.now(UTC).isoformat(),
        "codelists": _read_tsv_rows(_decode_text(content)),
    }


def eurostat_toc_payload_from_content(
    content: bytes,
    *,
    language: str = EUROSTAT_LANGUAGE,
) -> dict[str, Any]:
    return {
        "source": "eurostat_toc_api",
        "language": language,
        "discovered_at": datetime.now(UTC).isoformat(),
        "toc": _read_tsv_rows(_decode_text(content)),
    }


def eurostat_catalog_payload_from_manifest(
    *,
    lake_root: Path,
    manifest: BronzeManifest,
    language: str = EUROSTAT_LANGUAGE,
) -> dict[str, Any]:
    return eurostat_catalog_payload_from_content(
        (lake_root / manifest.bronze_path).read_bytes(),
        language=language,
    )


def eurostat_toc_payload_from_manifest(
    *,
    lake_root: Path,
    manifest: BronzeManifest,
    language: str = EUROSTAT_LANGUAGE,
) -> dict[str, Any]:
    return eurostat_toc_payload_from_content(
        (lake_root / manifest.bronze_path).read_bytes(),
        language=language,
    )


def eurostat_codelist_inventory_payload_from_manifest(
    *,
    lake_root: Path,
    manifest: BronzeManifest,
    language: str = EUROSTAT_LANGUAGE,
) -> dict[str, Any]:
    return eurostat_codelist_inventory_payload_from_content(
        (lake_root / manifest.bronze_path).read_bytes(),
        language=language,
    )


def extract_eurostat_catalog_resource(
    *,
    run_date: str,
    output_root: Path,
    client: EurostatApiClient | None = None,
    language: str = EUROSTAT_LANGUAGE,
) -> BronzeManifest:
    client = client or EurostatApiClient(language=language)
    url = client.inventory_url("data")
    result = client.get(url)
    resource = DatasetResource(
        family=EUROSTAT_FAMILY,
        dataset=EUROSTAT_CATALOG_DATASET,
        format="tsv",
        url=result.url,
        snapshot_token=_snapshot_token("catalog", language),
    )
    return persist_bronze(root=output_root, resource=resource, run_date=run_date, result=result)


def extract_eurostat_toc_resource(
    *,
    run_date: str,
    output_root: Path,
    client: EurostatApiClient | None = None,
    language: str = EUROSTAT_LANGUAGE,
) -> BronzeManifest:
    client = client or EurostatApiClient(language=language)
    result = client.get(client.toc_url())
    resource = DatasetResource(
        family=EUROSTAT_FAMILY,
        dataset=EUROSTAT_TOC_DATASET,
        format="tsv",
        url=result.url,
        snapshot_token=_snapshot_token("toc", language),
    )
    return persist_bronze(root=output_root, resource=resource, run_date=run_date, result=result)


def extract_eurostat_toc_xml_resource(
    *,
    run_date: str,
    output_root: Path,
    client: EurostatApiClient | None = None,
) -> BronzeManifest:
    client = client or EurostatApiClient()
    result = client.get(client.toc_xml_url())
    resource = DatasetResource(
        family=EUROSTAT_FAMILY,
        dataset=EUROSTAT_TOC_XML_DATASET,
        format="xml",
        url=result.url,
        snapshot_token=_snapshot_token("toc-xml"),
    )
    return persist_bronze(root=output_root, resource=resource, run_date=run_date, result=result)


def extract_eurostat_codelist_inventory_resource(
    *,
    run_date: str,
    output_root: Path,
    client: EurostatApiClient | None = None,
    language: str = EUROSTAT_LANGUAGE,
) -> BronzeManifest:
    client = client or EurostatApiClient(language=language)
    url = client.inventory_url("codelist")
    result = client.get(url)
    resource = DatasetResource(
        family=EUROSTAT_FAMILY,
        dataset=EUROSTAT_CODELIST_INVENTORY_DATASET,
        format="tsv",
        url=result.url,
        snapshot_token=_snapshot_token("codelist-inventory", language),
    )
    return persist_bronze(root=output_root, resource=resource, run_date=run_date, result=result)


def build_eurostat_dataset_plans(
    *,
    catalog_payload: dict[str, Any],
    datasets: set[str] | None = None,
    changed_since: str | None = None,
    include_unchanged: bool = False,
    max_datasets: int | None = None,
    prefer_format: str = "sdmx-csv",
    compressed: bool = True,
) -> list[dict[str, Any]]:
    dataset_filters = {item.upper() for item in datasets or set()}
    rows = catalog_payload.get("datasets") or catalog_payload.get("payload") or []
    plans: list[dict[str, Any]] = []
    for row in rows:
        descriptor = _dataset_descriptor(row)
        dataset_code = descriptor["dataset_code"]
        if dataset_filters and dataset_code.upper() not in dataset_filters:
            continue
        if (
            not include_unchanged
            and changed_since
            and not _changed_since(
                descriptor["last_data_change_at"],
                descriptor["last_structural_change_at"],
                changed_since,
            )
        ):
            continue
        data_url = _dataset_download_url(row, prefer_format=prefer_format)
        if not data_url:
            continue
        plan = asdict(
            EurostatDatasetPlan(
                dataset_code=dataset_code,
                data_url=_with_compressed(data_url, compressed=compressed),
                structure_url=_text(row.get("Data structure download url")),
                last_data_change_at=descriptor["last_data_change_at"],
                last_structural_change_at=descriptor["last_structural_change_at"],
            )
        )
        plans.append(plan)
        if max_datasets is not None and len(plans) >= max_datasets:
            break
    return plans


def build_eurostat_codelist_plans(
    *,
    codelist_payload: dict[str, Any],
    codelists: set[str] | None = None,
    max_codelists: int | None = None,
) -> list[dict[str, Any]]:
    filters = {item.upper() for item in codelists or set()}
    rows = codelist_payload.get("codelists") or codelist_payload.get("payload") or []
    plans: list[dict[str, Any]] = []
    for row in rows:
        descriptor = _codelist_descriptor(row)
        code = descriptor["codelist_code"]
        if filters and code.upper() not in filters:
            continue
        url = descriptor["specific_tsv_url"] or descriptor["latest_tsv_url"]
        if not url:
            continue
        plans.append({"codelist": descriptor, "url": url})
        if max_codelists is not None and len(plans) >= max_codelists:
            break
    return plans


def extract_eurostat_dataset_resource(
    *,
    plan: dict[str, Any],
    run_date: str,
    output_root: Path,
    skip_existing: bool = False,
    async_wait_seconds: int = 1800,
    async_poll_seconds: int = 30,
    client: EurostatApiClient | None = None,
) -> BronzeManifest:
    client = client or EurostatApiClient()
    dataset_code = str(plan["dataset_code"])
    token = eurostat_dataset_data_snapshot_token(dataset_code)
    if skip_existing:
        for extension in ("csv.gz", "csv"):
            existing = find_eurostat_manifest(
                output_root=output_root,
                run_date=run_date,
                dataset=EUROSTAT_DATASET_DATA,
                token=token,
                extension=extension,
            )
            if existing is not None:
                return existing

    temporary_path = (
        output_root / "bronze" / EUROSTAT_FAMILY / ".downloads" / f"{token}_{run_date}.download"
    )
    try:
        result = client.download_resolving_async(
            plan["data_url"],
            destination=temporary_path,
            wait_seconds=async_wait_seconds,
            poll_seconds=async_poll_seconds,
        )
        resource = DatasetResource(
            family=EUROSTAT_FAMILY,
            dataset=EUROSTAT_DATASET_DATA,
            format="csv.gz" if _is_gzip_file(temporary_path) else "csv",
            url=result.url,
            snapshot_token=token,
        )
        return persist_bronze_stream(
            root=output_root,
            resource=resource,
            run_date=run_date,
            result=result,
            downloaded_path=temporary_path,
        )
    finally:
        temporary_path.unlink(missing_ok=True)


def extract_eurostat_dataset_structure_resource(
    *,
    plan: dict[str, Any],
    run_date: str,
    output_root: Path,
    skip_existing: bool = False,
    client: EurostatApiClient | None = None,
) -> BronzeManifest | None:
    structure_url = plan.get("structure_url")
    if not structure_url:
        return None
    client = client or EurostatApiClient()
    dataset_code = str(plan["dataset_code"])
    token = _snapshot_token("dataset-structure", dataset_code)
    if skip_existing:
        existing = find_eurostat_manifest(
            output_root=output_root,
            run_date=run_date,
            dataset=EUROSTAT_DATASET_STRUCTURE,
            token=token,
            extension="xml",
        )
        if existing is not None:
            return existing
    result = client.get(structure_url)
    resource = DatasetResource(
        family=EUROSTAT_FAMILY,
        dataset=EUROSTAT_DATASET_STRUCTURE,
        format="xml",
        url=result.url,
        snapshot_token=token,
    )
    return persist_bronze(root=output_root, resource=resource, run_date=run_date, result=result)


def extract_eurostat_codelist_resource(
    *,
    plan: dict[str, Any],
    run_date: str,
    output_root: Path,
    skip_existing: bool = False,
    client: EurostatApiClient | None = None,
) -> BronzeManifest:
    client = client or EurostatApiClient()
    codelist = plan["codelist"]
    codelist_code = str(codelist["codelist_code"])
    version = codelist.get("version")
    token = _snapshot_token("codelist", codelist_code, version or "latest")
    if skip_existing:
        existing = find_eurostat_manifest(
            output_root=output_root,
            run_date=run_date,
            dataset=EUROSTAT_CODELIST_DATASET,
            token=token,
            extension="tsv",
        )
        if existing is not None:
            return existing
    result = client.get(plan["url"])
    resource = DatasetResource(
        family=EUROSTAT_FAMILY,
        dataset=EUROSTAT_CODELIST_DATASET,
        format="tsv",
        url=result.url,
        snapshot_token=token,
    )
    return persist_bronze(root=output_root, resource=resource, run_date=run_date, result=result)


def extract_eurostat_dataset_batch(
    *,
    run_date: str,
    output_root: Path,
    catalog_payload: dict[str, Any],
    datasets: set[str] | None = None,
    changed_since: str | None = None,
    include_unchanged: bool = False,
    max_datasets: int | None = None,
    include_structures: bool = False,
    skip_existing: bool = False,
    continue_on_error: bool = False,
    report_path: Path | None = None,
    client: EurostatApiClient | None = None,
) -> list[BronzeManifest]:
    client = client or EurostatApiClient()
    plans = build_eurostat_dataset_plans(
        catalog_payload=catalog_payload,
        datasets=datasets,
        changed_since=changed_since,
        include_unchanged=include_unchanged,
        max_datasets=max_datasets,
    )
    manifests: list[BronzeManifest] = []
    items: list[dict[str, Any]] = []
    started_at = datetime.now(UTC).isoformat()
    for index, plan in enumerate(plans, start=1):
        item = {
            "index": index,
            "dataset_code": plan["dataset_code"],
            "last_data_change_at": plan.get("last_data_change_at"),
            "last_structural_change_at": plan.get("last_structural_change_at"),
        }
        try:
            manifest = extract_eurostat_dataset_resource(
                plan=plan,
                run_date=run_date,
                output_root=output_root,
                skip_existing=skip_existing,
                client=client,
            )
            manifests.append(manifest)
            related = [asdict(manifest)]
            if include_structures:
                structure_manifest = extract_eurostat_dataset_structure_resource(
                    plan=plan,
                    run_date=run_date,
                    output_root=output_root,
                    skip_existing=skip_existing,
                    client=client,
                )
                if structure_manifest is not None:
                    manifests.append(structure_manifest)
                    related.append(asdict(structure_manifest))
            items.append(item | {"status": "downloaded", "manifests": related})
        except Exception as exc:
            items.append(item | {"status": "failed", "error": f"{type(exc).__name__}: {exc}"})
            if not continue_on_error:
                _write_report(
                    report_path=report_path,
                    report=_batch_report(
                        run_date=run_date,
                        output_root=output_root,
                        plans=plans,
                        items=items,
                        started_at=started_at,
                    ),
                )
                raise
    _write_report(
        report_path=report_path,
        report=_batch_report(
            run_date=run_date,
            output_root=output_root,
            plans=plans,
            items=items,
            started_at=started_at,
        ),
    )
    return manifests


def extract_eurostat_codelist_batch(
    *,
    run_date: str,
    output_root: Path,
    codelist_payload: dict[str, Any],
    codelists: set[str] | None = None,
    max_codelists: int | None = None,
    skip_existing: bool = False,
    continue_on_error: bool = False,
    report_path: Path | None = None,
    client: EurostatApiClient | None = None,
) -> list[BronzeManifest]:
    client = client or EurostatApiClient()
    plans = build_eurostat_codelist_plans(
        codelist_payload=codelist_payload,
        codelists=codelists,
        max_codelists=max_codelists,
    )
    manifests: list[BronzeManifest] = []
    items: list[dict[str, Any]] = []
    started_at = datetime.now(UTC).isoformat()
    for index, plan in enumerate(plans, start=1):
        codelist = plan["codelist"]
        item = {
            "index": index,
            "codelist_code": codelist["codelist_code"],
            "version": codelist.get("version"),
        }
        try:
            manifest = extract_eurostat_codelist_resource(
                plan=plan,
                run_date=run_date,
                output_root=output_root,
                skip_existing=skip_existing,
                client=client,
            )
            manifests.append(manifest)
            items.append(item | {"status": "downloaded", "manifest": asdict(manifest)})
        except Exception as exc:
            items.append(item | {"status": "failed", "error": f"{type(exc).__name__}: {exc}"})
            if not continue_on_error:
                _write_report(
                    report_path=report_path,
                    report=_batch_report(
                        run_date=run_date,
                        output_root=output_root,
                        plans=plans,
                        items=items,
                        started_at=started_at,
                    ),
                )
                raise
    _write_report(
        report_path=report_path,
        report=_batch_report(
            run_date=run_date,
            output_root=output_root,
            plans=plans,
            items=items,
            started_at=started_at,
        ),
    )
    return manifests


def find_eurostat_manifest(
    *,
    output_root: Path,
    run_date: str,
    dataset: str,
    token: str,
    extension: str,
) -> BronzeManifest | None:
    manifest_dir = output_root / "bronze" / EUROSTAT_FAMILY / dataset / f"snapshot_date={run_date}"
    if not manifest_dir.exists():
        return None
    matches = sorted(
        manifest_dir.glob(f"{token}_*.{extension}.manifest.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in matches:
        try:
            return read_bronze_manifest(path)
        except (OSError, KeyError, json.JSONDecodeError):
            continue
    return None


def eurostat_dataset_data_snapshot_token(dataset_code: str) -> str:
    return _snapshot_token("dataset-data", dataset_code)


def eurostat_catalog_normalized_records(
    content: bytes,
    *,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, list[dict[str, Any]]]:
    rows = _read_tsv_rows(_decode_text(content))
    return {
        "datasets": [
            _dataset_row(row, snapshot_date=snapshot_date, source_file_sha256=source_file_sha256)
            for row in rows
        ]
    }


def eurostat_codelist_inventory_normalized_records(
    content: bytes,
    *,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, list[dict[str, Any]]]:
    rows = _read_tsv_rows(_decode_text(content))
    return {
        "codelists": [
            _codelist_row(row, snapshot_date=snapshot_date, source_file_sha256=source_file_sha256)
            for row in rows
        ]
    }


def eurostat_toc_normalized_records(
    content: bytes,
    *,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, list[dict[str, Any]]]:
    rows = _read_tsv_rows(_decode_text(content))
    return {
        "toc": [
            _toc_row(
                row,
                ordinal=ordinal,
                snapshot_date=snapshot_date,
                source_file_sha256=source_file_sha256,
            )
            for ordinal, row in enumerate(rows, start=1)
            if _row_value(row, "code")
        ]
    }


def eurostat_toc_xml_normalized_records(
    content: bytes,
    *,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, list[dict[str, Any]]]:
    root = ET.fromstring(_decode_text(content))
    rows_by_code: dict[str, dict[str, Any]] = {}
    for leaf in root.iter():
        if _xml_local_name(leaf.tag) != "leaf":
            continue
        code = _xml_child_text(leaf, "code")
        if not code:
            continue
        code = code.strip().upper()
        titles = _xml_localized_children(leaf, "title")
        sources = _xml_localized_children(leaf, "source")
        units = _xml_localized_children(leaf, "unit")
        descriptions = _xml_localized_children(leaf, "shortDescription")
        metadata_urls = _xml_link_children(leaf, "metadata")
        download_urls = _xml_link_children(leaf, "downloadLink")
        candidate = {
            "metadata_id": "eurostat_toc_metadata_" + stable_id(code, snapshot_date),
            "code": code,
            "title_en": titles.get("en"),
            "title_fr": titles.get("fr"),
            "title_de": titles.get("de"),
            "source_en": sources.get("en"),
            "source_fr": sources.get("fr"),
            "source_de": sources.get("de"),
            "unit_en": units.get("en"),
            "unit_fr": units.get("fr"),
            "unit_de": units.get("de"),
            "short_description_en": descriptions.get("en"),
            "short_description_fr": descriptions.get("fr"),
            "short_description_de": descriptions.get("de"),
            "metadata_html_url": metadata_urls.get("html"),
            "metadata_sdmx_url": metadata_urls.get("sdmx"),
            "download_tsv_url": download_urls.get("tsv"),
            "download_sdmx_url": download_urls.get("sdmx"),
            "source_file_sha256": source_file_sha256,
            "snapshot_date": snapshot_date,
        }
        existing = rows_by_code.get(code)
        if existing is None:
            rows_by_code[code] = candidate
        else:
            for key, value in candidate.items():
                if existing.get(key) in (None, "") and value not in (None, ""):
                    existing[key] = value
    return {"toc_metadata": list(rows_by_code.values())}


def eurostat_codelist_normalized_records(
    content: bytes,
    *,
    manifest: BronzeManifest,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, list[dict[str, Any]]]:
    codelist_code, version = _codelist_token_parts(manifest.snapshot_token)
    rows = _read_tsv_rows(_decode_text(content))
    return {
        "codelist_values": [
            _codelist_value_row(
                row,
                codelist_code=codelist_code,
                version=version,
                snapshot_date=snapshot_date,
                source_file_sha256=source_file_sha256,
            )
            for row in rows
            if _row_code(row)
        ]
    }


def eurostat_dataset_data_normalized_records(
    content: bytes,
    *,
    manifest: BronzeManifest,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, list[dict[str, Any]]]:
    dataset_code = eurostat_dataset_code_from_manifest(manifest)
    text = _decode_text(content)
    reader = csv.DictReader(StringIO(text))
    columns = list(reader.fieldnames or [])
    observations: list[dict[str, Any]] = []
    for row in reader:
        if not any((value or "").strip() for value in row.values()):
            continue
        observations.append(
            _observation_row(
                row,
                dataset_code=dataset_code,
                snapshot_date=snapshot_date,
                source_file_sha256=source_file_sha256,
            )
        )
    return {
        "dataset_columns": eurostat_dataset_column_normalized_records(
            dataset_code=dataset_code,
            columns=columns,
            snapshot_date=snapshot_date,
            source_file_sha256=source_file_sha256,
        ),
        "observations": observations,
    }


def eurostat_dataset_code_from_manifest(manifest: BronzeManifest) -> str:
    return _dataset_code_from_manifest(manifest)


def eurostat_dataset_column_normalized_records(
    *,
    dataset_code: str,
    columns: list[str],
    snapshot_date: str,
    source_file_sha256: str,
) -> list[dict[str, Any]]:
    return [
        _dataset_column_row(
            dataset_code=dataset_code,
            column_name=column,
            ordinal=ordinal,
            snapshot_date=snapshot_date,
            source_file_sha256=source_file_sha256,
        )
        for ordinal, column in enumerate(columns, start=1)
    ]


def eurostat_observation_normalized_record(
    row: dict[str, Any],
    *,
    dataset_code: str,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, Any]:
    return _observation_row(
        row,
        dataset_code=dataset_code,
        snapshot_date=snapshot_date,
        source_file_sha256=source_file_sha256,
    )


def eurostat_dataset_structure_normalized_records(
    content: bytes,
    *,
    manifest: BronzeManifest,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, list[dict[str, Any]]]:
    dataset_code = _dataset_code_from_manifest(manifest)
    text = _decode_text(content)
    structure_id = "eurostat_structure_" + stable_id(dataset_code, source_file_sha256)
    return {
        "dataset_structures": [
            {
                "structure_id": structure_id,
                "dataset_code": dataset_code,
                "structure_format": manifest.format,
                "structure_url": manifest.source_url,
                "structure_sha256": source_file_sha256,
                "structure_xml": text,
                "source_file_sha256": source_file_sha256,
                "snapshot_date": snapshot_date,
            }
        ]
    }


def _dataset_descriptor(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset_code": _text(row.get("Code")),
        "dataset_type": _text(row.get("Type")),
        "source_dataset": _text(row.get("Source dataset")),
        "last_data_change_at": _iso_datetime(row.get("Last data change")),
        "last_structural_change_at": _iso_datetime(row.get("Last structural change")),
        "data_tsv_url": _text(row.get("Data download url (tsv)")),
        "data_csv_url": _text(row.get("Data download url (csv)")),
        "data_sdmx_url": _text(row.get("Data download url (sdmx)")),
        "structure_url": _text(row.get("Data structure download url")),
        "browser_url": _text(row.get("Open in Data Browser url")),
    }


def _dataset_row(
    row: dict[str, Any],
    *,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, Any]:
    descriptor = _dataset_descriptor(row)
    return descriptor | {
        "source_file_sha256": source_file_sha256,
        "snapshot_date": snapshot_date,
    }


def _toc_row(
    row: dict[str, Any],
    *,
    ordinal: int,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, Any]:
    raw_title = _row_raw_value(row, "title") or ""
    code = (_row_value(row, "code") or "").strip().upper()
    node_type = _row_value(row, "type")
    return {
        "toc_id": "eurostat_toc_" + stable_id(ordinal, code, raw_title, node_type or ""),
        "ordinal": ordinal,
        "code": code,
        "title": raw_title.strip(),
        "raw_title": raw_title,
        "node_type": node_type,
        "depth": _toc_depth(raw_title),
        "last_data_update": _row_value(row, "last update of data"),
        "last_structure_change": _row_value(row, "last table structure change"),
        "data_start": _row_value(row, "data start"),
        "data_end": _row_value(row, "data end"),
        "values": _row_value(row, "values"),
        "source_file_sha256": source_file_sha256,
        "snapshot_date": snapshot_date,
    }


def _codelist_descriptor(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "codelist_code": _text(row.get("Code")),
        "source": _text(row.get("Source")),
        "version": _text(row.get("Version")),
        "label": _text(row.get("Label")),
        "specific_tsv_url": _text(row.get("Specific tsv download url")),
        "specific_sdmx_url": _text(row.get("Specific sdmx download url")),
        "latest_tsv_url": _text(row.get("Latest tsv download url")),
        "latest_sdmx_url": _text(row.get("Latest sdmx download url")),
        "mapping_file": _text(row.get("mapping file")),
    }


def _codelist_row(
    row: dict[str, Any],
    *,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, Any]:
    descriptor = _codelist_descriptor(row)
    return descriptor | {
        "codelist_id": "eurostat_codelist_"
        + stable_id(descriptor["codelist_code"], descriptor["version"] or "latest"),
        "source_file_sha256": source_file_sha256,
        "snapshot_date": snapshot_date,
    }


def _codelist_value_row(
    row: dict[str, Any],
    *,
    codelist_code: str,
    version: str | None,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, Any]:
    code = _row_code(row)
    label_en = (
        _row_value(row, "Label - English") or _row_value(row, "label") or _row_value(row, "Label")
    )
    label_fr = _row_value(row, "Label - French")
    label_de = _row_value(row, "Label - German")
    return {
        "codelist_value_id": "eurostat_codelist_value_"
        + stable_id(codelist_code, version or "", code),
        "codelist_code": codelist_code,
        "version": version,
        "code": code,
        "label_en": label_en or "",
        "label_fr": label_fr,
        "label_de": label_de,
        "standard_code": _row_value(row, "Standard code"),
        "note_en": _row_value(row, "Note (EN only)"),
        "corporate_code": _row_value(row, "EC_corporate_code"),
        "corporate_uri": _row_value(row, "EC_corporate_uri"),
        "raw_metadata_json": _json_dumps(row),
        "source_file_sha256": source_file_sha256,
        "snapshot_date": snapshot_date,
    }


def _dataset_column_row(
    *,
    dataset_code: str,
    column_name: str,
    ordinal: int,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, Any]:
    return {
        "column_id": "eurostat_column_" + stable_id(dataset_code, column_name, snapshot_date),
        "dataset_code": dataset_code,
        "column_name": column_name,
        "normalized_column_name": normalize_key(column_name),
        "column_role": _column_role(column_name),
        "ordinal": ordinal,
        "source_file_sha256": source_file_sha256,
        "snapshot_date": snapshot_date,
    }


def _observation_row(
    row: dict[str, Any],
    *,
    dataset_code: str,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, Any]:
    dimensions = {
        key: _clean_cell(value)
        for key, value in row.items()
        if key not in EUROSTAT_OBSERVATION_COLUMNS and _clean_cell(value) is not None
    }
    attributes = {
        key: _clean_cell(value)
        for key, value in row.items()
        if key in EUROSTAT_ATTRIBUTE_COLUMNS and _clean_cell(value) is not None
    }
    time_period = _clean_cell(row.get("TIME_PERIOD"))
    dataflow_id = _clean_cell(row.get("DATAFLOW"))
    obs_value = _float_or_none(row.get("OBS_VALUE"))
    return {
        # 128 bits are collision-safe for this grain while avoiding a 64-byte
        # SHA-256 string repeated on every row of the large fact table.
        "observation_id": EUROSTAT_OBSERVATION_ID_PREFIX
        + stable_id(dataset_code, time_period, _json_dumps(dimensions))[
            :EUROSTAT_OBSERVATION_ID_HEX_LENGTH
        ],
        "dataset_code": dataset_code,
        "dataflow_id": dataflow_id,
        "last_updated_at": _iso_datetime(row.get("LAST UPDATE")),
        "frequency": _clean_cell(row.get("freq") or row.get("FREQ")),
        "geo": _clean_cell(row.get("geo") or row.get("GEO")),
        "unit": _clean_cell(row.get("unit") or row.get("UNIT")),
        "time_period": time_period,
        "reference_date": _reference_date(time_period),
        "obs_value": obs_value,
        "obs_status": _clean_cell(row.get("OBS_STATUS") or row.get("OBS_FLAG")),
        "conf_status": _clean_cell(row.get("CONF_STATUS")),
        "dimensions_json": _json_dumps(dimensions),
        "attributes_json": _json_dumps(attributes),
        "source_file_sha256": source_file_sha256,
        "snapshot_date": snapshot_date,
    }


def _batch_report(
    *,
    run_date: str,
    output_root: Path,
    plans: list[dict[str, Any]],
    items: list[dict[str, Any]],
    started_at: str,
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for item in items:
        status = str(item.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return {
        "run_date": run_date,
        "output_root": str(output_root),
        "planned": len(plans),
        "completed": len(items),
        "downloaded": counts.get("downloaded", 0),
        "failed": counts.get("failed", 0),
        "status_counts": counts,
        "started_at": started_at,
        "finished_at": datetime.now(UTC).isoformat(),
        "items": items,
    }


def _write_report(*, report_path: Path | None, report: dict[str, Any]) -> None:
    if report_path is None:
        return
    write_json_atomically(report_path, report)


def _dataset_download_url(row: dict[str, Any], *, prefer_format: str) -> str | None:
    if prefer_format == "tsv":
        return _text(row.get("Data download url (tsv)"))
    if prefer_format in {"sdmx-csv", "csv"}:
        return _text(row.get("Data download url (csv)"))
    if prefer_format == "sdmx":
        return _text(row.get("Data download url (sdmx)"))
    raise ValueError("prefer_format must be 'sdmx-csv', 'tsv' or 'sdmx'")


def _with_compressed(url: str, *, compressed: bool) -> str:
    if not compressed:
        return url
    parsed = urlparse(url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    params["compressed"] = "true"
    return urlunparse(parsed._replace(query=urlencode(params)))


def _changed_since(
    last_data_change_at: str | None,
    last_structural_change_at: str | None,
    changed_since: str,
) -> bool:
    threshold = _comparable_datetime(changed_since)
    if threshold is None:
        threshold_date = _parse_date(changed_since)
        if threshold_date is None:
            return True
        return any(
            value and _parse_date(value[:10]) and _parse_date(value[:10]) >= threshold_date
            for value in (last_data_change_at, last_structural_change_at)
        )
    return any(
        value and _comparable_datetime(value) and _comparable_datetime(value) >= threshold
        for value in (last_data_change_at, last_structural_change_at)
    )


def _read_tsv_rows(text: str) -> list[dict[str, Any]]:
    sample = text[:2048]
    has_header = "\t" in sample and any(
        name in sample.splitlines()[0]
        for name in ("Code", "CODE", "code", "title", "Label", "DATAFLOW")
    )
    if has_header:
        return list(csv.DictReader(StringIO(text), delimiter="\t"))
    rows: list[dict[str, Any]] = []
    for raw in csv.reader(StringIO(text), delimiter="\t"):
        if not raw or not any(cell.strip() for cell in raw):
            continue
        rows.append({"CODE": raw[0], "Label - English": raw[1] if len(raw) > 1 else ""})
    return rows


def _decode_text(content: bytes) -> str:
    raw = gzip.decompress(content) if _is_gzip(content) else content
    return raw.decode("utf-8-sig")


def _xml_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _xml_child_text(element: ET.Element, name: str) -> str | None:
    for child in element:
        if _xml_local_name(child.tag) == name:
            text = (child.text or "").strip()
            return text or None
    return None


def _xml_localized_children(element: ET.Element, name: str) -> dict[str, str | None]:
    values: dict[str, str | None] = {}
    for child in element:
        if _xml_local_name(child.tag) != name:
            continue
        language = (child.attrib.get("language") or "").lower()
        values[language] = (child.text or "").strip() or None
    return values


def _xml_link_children(element: ET.Element, name: str) -> dict[str, str | None]:
    values: dict[str, str | None] = {}
    for child in element:
        if _xml_local_name(child.tag) != name:
            continue
        link_format = (child.attrib.get("format") or "").lower()
        values[link_format] = (child.text or "").strip() or None
    return values


def _is_gzip(content: bytes) -> bool:
    return len(content) >= 2 and content[:2] == b"\x1f\x8b"


def _is_gzip_file(path: Path) -> bool:
    with path.open("rb") as handle:
        return handle.read(2) == b"\x1f\x8b"


def _async_request_id(content: bytes) -> str | None:
    text = content[:4096].decode("utf-8", errors="ignore")
    if "syncResponse" not in text or "<queued" not in text:
        return None
    match = re.search(r"<(?:[^:>]+:)?id>([^<]+)</(?:[^:>]+:)?id>", text)
    return match.group(1).strip() if match else None


def _async_status(content: bytes) -> str | None:
    text = content.decode("utf-8", errors="ignore")
    matches = re.findall(r"<(?:[^:>]+:)?status>([^<]+)</(?:[^:>]+:)?status>", text)
    return matches[-1].strip().upper() if matches else None


def _snapshot_token(*parts: Any) -> str:
    return "-".join(
        normalize_key(str(part)) for part in parts if part is not None and str(part).strip() != ""
    )


def _dataset_code_from_manifest(manifest: BronzeManifest) -> str:
    for source_url in (manifest.source_url, manifest.requested_url):
        parsed = urlparse(source_url or "")
        match = re.search(r"/data/([^/?#]+)/?", parsed.path)
        if match:
            return match.group(1).upper()
    token = manifest.snapshot_token or ""
    for prefix in (
        "dataset-data-",
        "dataset_data-",
        "dataset-structure-",
        "dataset_structure-",
    ):
        if token.startswith(prefix):
            return token[len(prefix) :].upper()
    return "unknown"


def _codelist_token_parts(token: str | None) -> tuple[str, str | None]:
    if not token or not token.startswith("codelist-"):
        return "unknown", None
    remainder = token[len("codelist-") :]
    if "-" not in remainder:
        return remainder.upper(), None
    code, version = remainder.rsplit("-", 1)
    if re.fullmatch(r"\d+(?:_\d+)+", version):
        version = version.replace("_", ".")
    return code.upper(), version or None


def _row_code(row: dict[str, Any]) -> str:
    return _clean_cell(row.get("CODE") or row.get("Code") or row.get("code")) or ""


def _row_value(row: dict[str, Any], key: str) -> str | None:
    if key in row:
        return _clean_cell(row.get(key))
    normalized = normalize_key(key)
    for candidate, value in row.items():
        if normalize_key(candidate) == normalized:
            return _clean_cell(value)
    return None


def _row_raw_value(row: dict[str, Any], key: str) -> str | None:
    if key in row:
        value = row.get(key)
        return None if value is None else str(value)
    normalized = normalize_key(key)
    for candidate, value in row.items():
        if normalize_key(candidate) == normalized:
            return None if value is None else str(value)
    return None


def _column_role(column_name: str) -> str:
    if column_name in {"TIME_PERIOD"}:
        return "time"
    if column_name in EUROSTAT_VALUE_COLUMNS:
        return "measure"
    if column_name in EUROSTAT_ATTRIBUTE_COLUMNS:
        return "attribute"
    if column_name in {"DATAFLOW", "LAST UPDATE"}:
        return "metadata"
    return "dimension"


def _clean_cell(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _text(value: Any) -> str | None:
    return _clean_cell(value)


def _float_or_none(value: Any) -> float | None:
    text = _clean_cell(value)
    if text is None or text.casefold() in {":", "nan"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _iso_datetime(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else None


def _parse_datetime(value: Any) -> datetime | None:
    text = _clean_cell(value)
    if text is None:
        return None
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}", text):
            return date_parser.isoparse(text)
        return date_parser.parse(text, dayfirst=True)
    except (TypeError, ValueError, OverflowError):
        return None


def _comparable_datetime(value: Any) -> datetime | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_date(value: Any) -> date | None:
    parsed = _parse_datetime(value)
    return parsed.date() if parsed else None


def _reference_date(time_period: str | None) -> str | None:
    if not time_period:
        return None
    if re.fullmatch(r"\d{4}", time_period):
        return f"{time_period}-01-01"
    if re.fullmatch(r"\d{4}-\d{2}", time_period):
        return f"{time_period}-01"
    quarter = re.fullmatch(r"(\d{4})-Q([1-4])", time_period)
    if quarter:
        month = (int(quarter.group(2)) - 1) * 3 + 1
        return f"{quarter.group(1)}-{month:02d}-01"
    semester = re.fullmatch(r"(\d{4})-S([1-2])", time_period)
    if semester:
        month = 1 if semester.group(2) == "1" else 7
        return f"{semester.group(1)}-{month:02d}-01"
    week = re.fullmatch(r"(\d{4})-W(\d{2})", time_period)
    if week:
        try:
            return date.fromisocalendar(int(week.group(1)), int(week.group(2)), 1).isoformat()
        except ValueError:
            return None
    return None


def _toc_depth(raw_title: str) -> int:
    leading_spaces = len(raw_title) - len(raw_title.lstrip(" "))
    return leading_spaces // 4


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
