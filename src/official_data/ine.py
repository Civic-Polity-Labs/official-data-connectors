from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from official_data.catalog import DatasetResource
from official_data.durable_io import write_json_atomically
from official_data.http import CongresoHttpClient, FetchResult
from official_data.normalization import normalize_text, stable_id
from official_data.storage import BronzeManifest, persist_bronze, read_bronze_manifest

INE_API_BASE = "https://servicios.ine.es/wstempus"
INE_LANGUAGE = "ES"
INE_FAMILY = "ine"
INE_CATALOG_DATASET = "Catalog"
INE_TABLE_DATASET = "TableData"
DEFAULT_DATA_DETAIL = 2
DEFAULT_DATA_TIP = "AM"


@dataclass(frozen=True)
class IneApiRequest:
    language: str
    function: str
    identifier: tuple[str, ...] = ()
    params: dict[str, Any] | None = None
    cache: bool = False


class IneApiClient:
    def __init__(
        self,
        *,
        language: str = INE_LANGUAGE,
        http_client: CongresoHttpClient | None = None,
    ) -> None:
        self.language = language
        self.http_client = http_client or CongresoHttpClient(
            headers={
                "User-Agent": (
                    "cpl-data-foundry/0.1 (INE JSON API; https://www.ine.es/datosabiertos/)"
                )
            }
        )

    def url(
        self,
        function: str,
        *identifier: Any,
        params: dict[str, Any] | None = None,
        cache: bool = False,
    ) -> str:
        endpoint_root = "jsCache" if cache else "js"
        parts = [
            INE_API_BASE.rstrip("/"),
            endpoint_root,
            self.language,
            function,
            *(str(part).strip("/") for part in identifier if part is not None),
        ]
        url = "/".join(parts)
        clean_params = {
            key: value for key, value in (params or {}).items() if value is not None and value != ""
        }
        if clean_params:
            url += "?" + urlencode(clean_params)
        return url

    def get(
        self,
        function: str,
        *identifier: Any,
        params: dict[str, Any] | None = None,
        cache: bool = False,
    ) -> FetchResult:
        return self.http_client.get(self.url(function, *identifier, params=params, cache=cache))

    def get_json(
        self,
        function: str,
        *identifier: Any,
        params: dict[str, Any] | None = None,
        cache: bool = False,
    ) -> Any:
        return _loads_json(self.get(function, *identifier, params=params, cache=cache).content)


def discover_ine_operations(
    *,
    client: IneApiClient | None = None,
    language: str = INE_LANGUAGE,
) -> list[dict[str, Any]]:
    client = client or IneApiClient(language=language)
    return _paged_endpoint(
        client,
        "OPERACIONES_DISPONIBLES",
        params={"det": 2},
    )


def discover_ine_tables(
    operation: dict[str, Any] | str,
    *,
    client: IneApiClient | None = None,
    language: str = INE_LANGUAGE,
) -> list[dict[str, Any]]:
    client = client or IneApiClient(language=language)
    operation_key = _operation_request_key(operation)
    payload = client.get_json("TABLAS_OPERACION", operation_key, params={"det": 2})
    return [item for item in _as_dict_list(payload)]


def discover_ine_table_groups(
    table_id: Any,
    *,
    client: IneApiClient | None = None,
    language: str = INE_LANGUAGE,
) -> list[dict[str, Any]]:
    client = client or IneApiClient(language=language)
    return _as_dict_list(client.get_json("GRUPOS_TABLA", table_id))


def discover_ine_table_group_values(
    table_id: Any,
    group_id: Any,
    *,
    client: IneApiClient | None = None,
    language: str = INE_LANGUAGE,
) -> list[dict[str, Any]]:
    client = client or IneApiClient(language=language)
    return _as_dict_list(
        client.get_json("VALORES_GRUPOSTABLA", table_id, group_id, params={"det": 2})
    )


def discover_ine_table_series(
    table_id: Any,
    *,
    client: IneApiClient | None = None,
    language: str = INE_LANGUAGE,
) -> list[dict[str, Any]]:
    client = client or IneApiClient(language=language)
    return _paged_endpoint(
        client,
        "SERIES_TABLA",
        table_id,
        params={"det": 2, "tip": "AM"},
        cache=True,
    )


def discover_ine_catalog(
    *,
    operations: set[str] | None = None,
    tables: set[str] | None = None,
    categories: set[str] | None = None,
    include_table_groups: bool = False,
    include_table_series: bool = False,
    max_operations: int | None = None,
    max_tables: int | None = None,
    client: IneApiClient | None = None,
    language: str = INE_LANGUAGE,
) -> dict[str, Any]:
    client = client or IneApiClient(language=language)
    operation_filters = set(operations or set()) | set(categories or set())
    operation_rows = [
        operation
        for operation in discover_ine_operations(client=client, language=language)
        if _operation_selected(operation, operation_filters)
    ]
    if max_operations is not None:
        operation_rows = operation_rows[:max_operations]

    table_rows: list[dict[str, Any]] = []
    group_rows: list[dict[str, Any]] = []
    group_value_rows: list[dict[str, Any]] = []
    series_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    table_count = 0

    for operation in operation_rows:
        try:
            operation_tables = discover_ine_tables(
                operation,
                client=client,
                language=language,
            )
        except Exception as exc:
            errors.append(
                {
                    "scope": "tables",
                    "operation": _operation_descriptor(operation),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue
        for table in operation_tables:
            if not _table_selected(table, tables or set()):
                continue
            table_item = {
                "operation": _operation_descriptor(operation),
                "table": table,
            }
            table_rows.append(table_item)
            table_count += 1
            if include_table_groups:
                _append_table_group_catalog(
                    group_rows=group_rows,
                    group_value_rows=group_value_rows,
                    table_item=table_item,
                    client=client,
                    language=language,
                    errors=errors,
                )
            if include_table_series:
                _append_table_series_catalog(
                    series_rows=series_rows,
                    table_item=table_item,
                    client=client,
                    language=language,
                    errors=errors,
                )
            if max_tables is not None and table_count >= max_tables:
                break
        if max_tables is not None and table_count >= max_tables:
            break

    return {
        "source": "ine_json_api",
        "language": language,
        "discovered_at": datetime.now(UTC).isoformat(),
        "operations": [_operation_descriptor(operation) for operation in operation_rows],
        "tables": table_rows,
        "groups": group_rows,
        "group_values": group_value_rows,
        "series": series_rows,
        "errors": errors,
    }


def build_ine_table_plans(
    *,
    catalog_payload: dict[str, Any] | None = None,
    operations: set[str] | None = None,
    tables: set[str] | None = None,
    categories: set[str] | None = None,
    max_tables: int | None = None,
    client: IneApiClient | None = None,
    language: str = INE_LANGUAGE,
) -> list[dict[str, Any]]:
    catalog_payload = catalog_payload or discover_ine_catalog(
        operations=operations,
        tables=tables,
        categories=categories,
        max_tables=max_tables,
        client=client,
        language=language,
    )
    operation_filters = set(operations or set()) | set(categories or set())
    table_filters = tables or set()
    plans: list[dict[str, Any]] = []
    for item in catalog_payload.get("tables") or []:
        operation = item.get("operation") or {}
        table = item.get("table") or {}
        if not _operation_selected(operation, operation_filters):
            continue
        if not _table_selected(table, table_filters):
            continue
        plans.append(
            {
                "operation": _operation_descriptor(operation),
                "table": _table_descriptor(table),
            }
        )
        if max_tables is not None and len(plans) >= max_tables:
            break
    return plans


def extract_ine_catalog_resource(
    *,
    run_date: str,
    output_root: Path,
    operations: set[str] | None = None,
    tables: set[str] | None = None,
    categories: set[str] | None = None,
    include_table_groups: bool = False,
    include_table_series: bool = False,
    max_operations: int | None = None,
    max_tables: int | None = None,
    client: IneApiClient | None = None,
    language: str = INE_LANGUAGE,
) -> BronzeManifest:
    client = client or IneApiClient(language=language)
    payload = discover_ine_catalog(
        operations=operations,
        tables=tables,
        categories=categories,
        include_table_groups=include_table_groups,
        include_table_series=include_table_series,
        max_operations=max_operations,
        max_tables=max_tables,
        client=client,
        language=language,
    )
    request = IneApiRequest(
        language=language,
        function="OPERACIONES_DISPONIBLES",
        params={"det": 2},
    )
    content = _json_bytes({"request": asdict(request), "payload": payload})
    resource = DatasetResource(
        family=INE_FAMILY,
        dataset=INE_CATALOG_DATASET,
        format="json",
        url=client.url("OPERACIONES_DISPONIBLES", params={"det": 2}),
        snapshot_token=_snapshot_token("catalog", run_date),
    )
    return persist_bronze(
        root=output_root,
        resource=resource,
        run_date=run_date,
        result=FetchResult(
            url=resource.url,
            status_code=200,
            headers={"content-type": "application/json"},
            content=content,
        ),
    )


def extract_ine_table_data_resource(
    *,
    operation: dict[str, Any],
    table: dict[str, Any],
    run_date: str,
    output_root: Path,
    start_date: str | None = None,
    end_date: str | None = None,
    nult: int | None = None,
    det: int = DEFAULT_DATA_DETAIL,
    tip: str = DEFAULT_DATA_TIP,
    skip_existing: bool = False,
    client: IneApiClient | None = None,
    language: str = INE_LANGUAGE,
) -> BronzeManifest:
    client = client or IneApiClient(language=language)
    table_id = str(table["table_id"])
    if skip_existing:
        existing = find_ine_table_data_manifest(
            output_root=output_root,
            run_date=run_date,
            table_id=table_id,
            start_date=start_date,
            end_date=end_date,
            nult=nult,
        )
        if existing is not None:
            return existing
    params: dict[str, Any] = {"det": det, "tip": tip}
    if start_date or end_date:
        params["date"] = f"{_compact_date(start_date)}:{_compact_date(end_date)}"
    elif nult is not None:
        params["nult"] = nult
    result = client.get("DATOS_TABLA", table_id, params=params)
    payload = _loads_json(result.content)
    wrapped = {
        "request": {
            "language": language,
            "function": "DATOS_TABLA",
            "operation": _operation_descriptor(operation),
            "table": _table_descriptor(table),
            "params": params,
            "source_url": result.url,
            "requested_at": datetime.now(UTC).isoformat(),
        },
        "payload": payload,
    }
    resource = DatasetResource(
        family=INE_FAMILY,
        dataset=INE_TABLE_DATASET,
        format="json",
        url=result.url,
        snapshot_token=_snapshot_token(
            "datos-tabla",
            table_id,
            _window_token(start_date=start_date, end_date=end_date, nult=nult),
        ),
    )
    return persist_bronze(
        root=output_root,
        resource=resource,
        run_date=run_date,
        result=FetchResult(
            url=result.url,
            status_code=result.status_code,
            headers=result.headers,
            content=_json_bytes(wrapped),
        ),
    )


def find_ine_table_data_manifest(
    *,
    output_root: Path,
    run_date: str,
    table_id: Any,
    start_date: str | None = None,
    end_date: str | None = None,
    nult: int | None = None,
) -> BronzeManifest | None:
    token = ine_table_data_snapshot_token(
        table_id,
        start_date=start_date,
        end_date=end_date,
        nult=nult,
    )
    manifest_dir = (
        output_root / "bronze" / INE_FAMILY / INE_TABLE_DATASET / f"snapshot_date={run_date}"
    )
    if not manifest_dir.exists():
        return None
    matches = sorted(
        manifest_dir.glob(f"{token}_*.json.manifest.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in matches:
        try:
            return read_bronze_manifest(path)
        except (OSError, KeyError, json.JSONDecodeError):
            continue
    return None


def ine_table_data_snapshot_token(
    table_id: Any,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    nult: int | None = None,
) -> str:
    return _snapshot_token(
        "datos-tabla",
        str(table_id),
        _window_token(start_date=start_date, end_date=end_date, nult=nult),
    )


def extract_ine_table_data_batch(
    *,
    run_date: str,
    output_root: Path,
    catalog_payload: dict[str, Any] | None = None,
    operations: set[str] | None = None,
    tables: set[str] | None = None,
    categories: set[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    nult: int | None = None,
    max_tables: int | None = None,
    skip_existing: bool = False,
    continue_on_error: bool = False,
    report_path: Path | None = None,
    client: IneApiClient | None = None,
    language: str = INE_LANGUAGE,
) -> list[BronzeManifest]:
    client = client or IneApiClient(language=language)
    plans = build_ine_table_plans(
        catalog_payload=catalog_payload,
        operations=operations,
        tables=tables,
        categories=categories,
        max_tables=max_tables,
        client=client,
        language=language,
    )
    manifests: list[BronzeManifest] = []
    items: list[dict[str, Any]] = []
    started_at = datetime.now(UTC).isoformat()
    for index, plan in enumerate(plans, start=1):
        table = plan["table"]
        operation = plan["operation"]
        table_id = table["table_id"]
        item = _batch_report_item(
            index=index,
            operation=operation,
            table=table,
        )
        try:
            existing = (
                find_ine_table_data_manifest(
                    output_root=output_root,
                    run_date=run_date,
                    table_id=table_id,
                    start_date=start_date,
                    end_date=end_date,
                    nult=nult,
                )
                if skip_existing
                else None
            )
            if existing is not None:
                manifests.append(existing)
                items.append(
                    item
                    | {
                        "status": "skipped_existing",
                        "manifest": asdict(existing),
                    }
                )
                continue
            manifest = extract_ine_table_data_resource(
                operation=operation,
                table=table,
                run_date=run_date,
                output_root=output_root,
                start_date=start_date,
                end_date=end_date,
                nult=nult,
                client=client,
                language=language,
            )
            manifests.append(manifest)
            items.append(item | {"status": "downloaded", "manifest": asdict(manifest)})
        except Exception as exc:
            items.append(
                item
                | {
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            if not continue_on_error:
                _write_ine_batch_report(
                    report_path=report_path,
                    report=_ine_batch_report(
                        run_date=run_date,
                        output_root=output_root,
                        start_date=start_date,
                        end_date=end_date,
                        nult=nult,
                        plans=plans,
                        items=items,
                        started_at=started_at,
                    ),
                )
                raise
    _write_ine_batch_report(
        report_path=report_path,
        report=_ine_batch_report(
            run_date=run_date,
            output_root=output_root,
            start_date=start_date,
            end_date=end_date,
            nult=nult,
            plans=plans,
            items=items,
            started_at=started_at,
        ),
    )
    return manifests


def _batch_report_item(
    *,
    index: int,
    operation: dict[str, Any],
    table: dict[str, Any],
) -> dict[str, Any]:
    return {
        "index": index,
        "operation_id": operation.get("operation_id"),
        "operation_code": operation.get("operation_code"),
        "operation_name": operation.get("operation_name"),
        "table_id": table.get("table_id"),
        "table_name": table.get("table_name"),
        "table_code": table.get("table_code"),
    }


def _ine_batch_report(
    *,
    run_date: str,
    output_root: Path,
    start_date: str | None,
    end_date: str | None,
    nult: int | None,
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
        "window": {
            "start_date": start_date,
            "end_date": end_date,
            "nult": nult,
        },
        "planned_tables": len(plans),
        "completed_tables": len(items),
        "downloaded_tables": counts.get("downloaded", 0),
        "skipped_existing_tables": counts.get("skipped_existing", 0),
        "failed_tables": counts.get("failed", 0),
        "status_counts": counts,
        "started_at": started_at,
        "finished_at": datetime.now(UTC).isoformat(),
        "items": items,
    }


def _write_ine_batch_report(
    *,
    report_path: Path | None,
    report: dict[str, Any],
) -> None:
    if report_path is None:
        return
    write_json_atomically(report_path, report, default=str)


def ine_catalog_silver_rows(
    payload: dict[str, Any],
    *,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, list[dict[str, Any]]]:
    catalog = payload.get("payload") or payload
    operation_rows = [
        _operation_row(
            operation,
            snapshot_date=snapshot_date,
            source_file_sha256=source_file_sha256,
        )
        for operation in catalog.get("operations") or []
    ]
    table_rows = [
        _table_row(
            item.get("operation") or {},
            item.get("table") or {},
            snapshot_date=snapshot_date,
            source_file_sha256=source_file_sha256,
        )
        for item in catalog.get("tables") or []
    ]
    group_rows = [
        _group_row(item, snapshot_date=snapshot_date, source_file_sha256=source_file_sha256)
        for item in catalog.get("groups") or []
    ]
    group_value_rows = [
        _group_value_row(item, snapshot_date=snapshot_date, source_file_sha256=source_file_sha256)
        for item in catalog.get("group_values") or []
    ]
    series_rows: list[dict[str, Any]] = []
    series_metadata_rows: list[dict[str, Any]] = []
    for item in catalog.get("series") or []:
        table = item.get("table") or {}
        operation = item.get("operation") or {}
        series = item.get("series") or {}
        series_rows.append(
            _series_row(
                series,
                table=table,
                operation=operation,
                snapshot_date=snapshot_date,
                source_file_sha256=source_file_sha256,
            )
        )
        series_metadata_rows.extend(
            _series_metadata_rows(
                series,
                table=table,
                snapshot_date=snapshot_date,
                source_file_sha256=source_file_sha256,
            )
        )
    return {
        "silver_ine_operations": operation_rows,
        "silver_ine_tables": table_rows,
        "silver_ine_table_groups": group_rows,
        "silver_ine_table_group_values": group_value_rows,
        "silver_ine_series": series_rows,
        "silver_ine_series_metadata_values": series_metadata_rows,
    }


def ine_table_data_silver_rows(
    payload: dict[str, Any],
    *,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, list[dict[str, Any]]]:
    request = payload.get("request") or {}
    table = request.get("table") or {}
    operation = request.get("operation") or {}
    series_payload = _as_dict_list(payload.get("payload"))
    series_rows: list[dict[str, Any]] = []
    metadata_rows: list[dict[str, Any]] = []
    observation_rows: list[dict[str, Any]] = []
    for series in series_payload:
        series_rows.append(
            _series_row(
                series,
                table=table,
                operation=operation,
                snapshot_date=snapshot_date,
                source_file_sha256=source_file_sha256,
            )
        )
        metadata_rows.extend(
            _series_metadata_rows(
                series,
                table=table,
                snapshot_date=snapshot_date,
                source_file_sha256=source_file_sha256,
            )
        )
        observation_rows.extend(
            _observation_rows(
                series,
                table=table,
                operation=operation,
                snapshot_date=snapshot_date,
                source_file_sha256=source_file_sha256,
            )
        )
    return {
        "silver_ine_series": series_rows,
        "silver_ine_series_metadata_values": metadata_rows,
        "silver_ine_observations": observation_rows,
    }


def _append_table_group_catalog(
    *,
    group_rows: list[dict[str, Any]],
    group_value_rows: list[dict[str, Any]],
    table_item: dict[str, Any],
    client: IneApiClient,
    language: str,
    errors: list[dict[str, Any]],
) -> None:
    table = table_item["table"]
    table_id = table.get("Id")
    try:
        groups = discover_ine_table_groups(table_id, client=client, language=language)
    except Exception as exc:
        errors.append(
            {
                "scope": "groups",
                "table_id": table_id,
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        return
    for group in groups:
        group_rows.append({**table_item, "group": group})
        try:
            values = discover_ine_table_group_values(
                table_id,
                group.get("Id"),
                client=client,
                language=language,
            )
        except Exception as exc:
            errors.append(
                {
                    "scope": "group_values",
                    "table_id": table_id,
                    "group_id": group.get("Id"),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue
        group_value_rows.extend({**table_item, "group": group, "value": value} for value in values)


def _append_table_series_catalog(
    *,
    series_rows: list[dict[str, Any]],
    table_item: dict[str, Any],
    client: IneApiClient,
    language: str,
    errors: list[dict[str, Any]],
) -> None:
    table_id = table_item["table"].get("Id")
    try:
        series = discover_ine_table_series(table_id, client=client, language=language)
    except Exception as exc:
        errors.append(
            {
                "scope": "series",
                "table_id": table_id,
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        return
    series_rows.extend({**table_item, "series": item} for item in series)


def _paged_endpoint(
    client: IneApiClient,
    function: str,
    *identifier: Any,
    params: dict[str, Any] | None = None,
    cache: bool = False,
    max_pages: int = 1000,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen_signatures: set[str] = set()
    for page in range(1, max_pages + 1):
        payload = client.get_json(
            function,
            *identifier,
            params={**(params or {}), "page": page},
            cache=cache,
        )
        page_items = _as_dict_list(payload)
        if not page_items:
            break
        signature = stable_id(json.dumps(page_items, ensure_ascii=False, sort_keys=True))
        if signature in seen_signatures:
            break
        seen_signatures.add(signature)
        items.extend(page_items)
    return items


def _operation_descriptor(operation: dict[str, Any] | str) -> dict[str, Any]:
    if not isinstance(operation, dict):
        return {
            "operation_id": str(operation),
            "operation_code": str(operation),
            "ioe_code": None,
            "operation_name": str(operation),
            "url": None,
        }
    return {
        "operation_id": _operation_id(operation),
        "operation_code": _text(operation.get("Codigo") or operation.get("operation_code")),
        "ioe_code": _text(
            operation.get("Cod_IOE") or operation.get("CodIOE") or operation.get("ioe_code")
        ),
        "operation_name": _text(
            operation.get("Nombre") or operation.get("name") or operation.get("operation_name")
        ),
        "url": _text(operation.get("Url") or operation.get("url")),
    }


def _table_descriptor(table: dict[str, Any] | str) -> dict[str, Any]:
    if not isinstance(table, dict):
        return {
            "table_id": str(table),
            "table_name": str(table),
            "table_code": None,
        }
    return {
        "table_id": _table_id(table),
        "table_name": _text(table.get("Nombre") or table.get("name") or table.get("table_name")),
        "table_code": _text(table.get("Codigo") or table.get("table_code")),
        "raw": table,
    }


def _operation_row(
    operation: dict[str, Any],
    *,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, Any]:
    descriptor = _operation_descriptor(operation)
    return {
        "operation_id": descriptor["operation_id"],
        "operation_code": descriptor["operation_code"],
        "ioe_code": descriptor["ioe_code"],
        "name": descriptor["operation_name"] or descriptor["operation_id"],
        "url": descriptor["url"],
        "source_file_sha256": source_file_sha256,
        "snapshot_date": snapshot_date,
    }


def _table_row(
    operation: dict[str, Any],
    table: dict[str, Any],
    *,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, Any]:
    operation_descriptor = _operation_descriptor(operation)
    table_descriptor = _table_descriptor(table)
    raw_table = table.get("raw") if "raw" in table else table
    publication = raw_table.get("Publicacion") or {}
    periodicity = raw_table.get("Periodicidad") or {}
    period_start = raw_table.get("Periodo_ini") or {}
    pub_date = publication.get("PubFechaAct") or {}
    return {
        "table_id": table_descriptor["table_id"],
        "operation_id": operation_descriptor["operation_id"],
        "operation_code": operation_descriptor["operation_code"],
        "ioe_code": operation_descriptor["ioe_code"],
        "name": table_descriptor["table_name"] or table_descriptor["table_id"],
        "table_code": table_descriptor["table_code"],
        "periodicity_id": _text(periodicity.get("Id")),
        "periodicity_code": _text(periodicity.get("Codigo")),
        "periodicity_name": _text(periodicity.get("Nombre")),
        "publication_id": _text(publication.get("Id")),
        "publication_name": _text(publication.get("Nombre")),
        "publication_updated_at": _timestamp(pub_date.get("Fecha")),
        "publication_update_name": _text(pub_date.get("Nombre")),
        "reference_start_year": _int(raw_table.get("Anyo_Periodo_ini")),
        "reference_start_period_code": _text(period_start.get("Codigo")),
        "reference_start_period_name": _text(
            period_start.get("Nombre_largo") or period_start.get("Nombre")
        ),
        "reference_end": _none_text(raw_table.get("FechaRef_fin")),
        "last_modified_at": _timestamp(raw_table.get("Ultima_Modificacion")),
        "raw_metadata_json": _json_text(raw_table),
        "source_file_sha256": source_file_sha256,
        "snapshot_date": snapshot_date,
    }


def _group_row(
    item: dict[str, Any],
    *,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, Any]:
    group = item.get("group") or {}
    table = _table_descriptor(item.get("table") or {})
    group_id = _text(group.get("Id")) or stable_id(table["table_id"], group.get("Nombre"))[:16]
    return {
        "table_id": table["table_id"],
        "group_id": group_id,
        "group_name": _text(group.get("Nombre")) or "",
        "source_file_sha256": source_file_sha256,
        "snapshot_date": snapshot_date,
    }


def _group_value_row(
    item: dict[str, Any],
    *,
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, Any]:
    group = item.get("group") or {}
    value = item.get("value") or {}
    variable = value.get("Variable") or {}
    table = _table_descriptor(item.get("table") or {})
    group_id = _text(group.get("Id")) or stable_id(table["table_id"], group.get("Nombre"))[:16]
    value_id = (
        _text(value.get("Id"))
        or stable_id(
            table["table_id"],
            group_id,
            value.get("Nombre"),
        )[:24]
    )
    return {
        "table_id": table["table_id"],
        "group_id": group_id,
        "value_id": value_id,
        "variable_id": _text(variable.get("Id")),
        "variable_name": _text(variable.get("Nombre")),
        "variable_code": _text(variable.get("Codigo")),
        "value_name": _text(value.get("Nombre")) or value_id,
        "value_code": _text(value.get("Codigo")),
        "note": _text(value.get("Nota")),
        "raw_metadata_json": _json_text(value),
        "source_file_sha256": source_file_sha256,
        "snapshot_date": snapshot_date,
    }


def _series_row(
    series: dict[str, Any],
    *,
    table: dict[str, Any],
    operation: dict[str, Any],
    snapshot_date: str,
    source_file_sha256: str,
) -> dict[str, Any]:
    table_descriptor = _table_descriptor(table)
    operation_descriptor = _operation_descriptor(
        operation or (series.get("Operacion") if isinstance(series.get("Operacion"), dict) else {})
    )
    publication = series.get("Publicacion") or {}
    pub_date = publication.get("PubFechaAct") or {}
    classification = series.get("Clasificacion") or {}
    unit = series.get("Unidad") or {}
    scale = series.get("Escala") or {}
    periodicity = series.get("Periodicidad") or {}
    series_id = _series_id(series)
    return {
        "series_id": series_id,
        "table_id": table_descriptor["table_id"],
        "operation_id": operation_descriptor["operation_id"],
        "operation_code": operation_descriptor["operation_code"],
        "ioe_code": operation_descriptor["ioe_code"],
        "name": _text(series.get("Nombre")) or series_id,
        "decimals": _int(series.get("Decimales")),
        "periodicity_code": _text(periodicity.get("Codigo")),
        "periodicity_name": _text(periodicity.get("Nombre")),
        "publication_name": _text(publication.get("Nombre")),
        "publication_updated_at": _timestamp(pub_date.get("Fecha")),
        "classification_name": _text(classification.get("Nombre")),
        "classification_date": _date_value(classification.get("Fecha")),
        "unit_name": _text(unit.get("Nombre")),
        "unit_code": _text(unit.get("Codigo")),
        "unit_abbrev": _text(unit.get("Abrev")),
        "scale_name": _text(scale.get("Nombre")),
        "scale_factor": _float(scale.get("Factor")),
        "metadata_signature": _metadata_signature(series.get("MetaData")),
        "raw_metadata_json": _json_text(
            {key: value for key, value in series.items() if key != "Data"}
        ),
        "source_file_sha256": source_file_sha256,
        "snapshot_date": snapshot_date,
    }


def _series_metadata_rows(
    series: dict[str, Any],
    *,
    table: dict[str, Any],
    snapshot_date: str,
    source_file_sha256: str,
) -> list[dict[str, Any]]:
    table_id = _table_descriptor(table)["table_id"]
    series_id = _series_id(series)
    rows: list[dict[str, Any]] = []
    for item in _as_dict_list(series.get("MetaData")):
        variable = item.get("Variable") or {}
        variable_id = _text(variable.get("Id")) or ""
        value_id = (
            _text(item.get("Id"))
            or stable_id(
                series_id,
                variable_id,
                item.get("Nombre"),
            )[:24]
        )
        rows.append(
            {
                "series_id": series_id,
                "table_id": table_id,
                "variable_id": variable_id,
                "variable_name": _text(variable.get("Nombre")),
                "variable_code": _text(variable.get("Codigo")),
                "value_id": value_id,
                "value_name": _text(item.get("Nombre")) or value_id,
                "value_code": _text(item.get("Codigo")),
                "note": _text(item.get("Nota")),
                "source_file_sha256": source_file_sha256,
                "snapshot_date": snapshot_date,
            }
        )
    return rows


def _observation_rows(
    series: dict[str, Any],
    *,
    table: dict[str, Any],
    operation: dict[str, Any],
    snapshot_date: str,
    source_file_sha256: str,
) -> list[dict[str, Any]]:
    table_descriptor = _table_descriptor(table)
    operation_descriptor = _operation_descriptor(operation)
    unit = series.get("Unidad") or {}
    scale = series.get("Escala") or {}
    metadata_json = _json_text(series.get("MetaData") or [])
    rows: list[dict[str, Any]] = []
    series_id = _series_id(series)
    series_name = _text(series.get("Nombre")) or series_id
    for item in _as_dict_list(series.get("Data")):
        period = item.get("Periodo") or {}
        data_type = item.get("TipoDato") or {}
        reference_date = _date_value(item.get("Fecha"))
        period_code = _text(period.get("Codigo"))
        data_type_code = _text(data_type.get("Codigo"))
        observation_id = (
            "ine_obs_"
            + stable_id(
                table_descriptor["table_id"],
                series_id,
                reference_date,
                item.get("Anyo"),
                period_code,
                data_type_code,
            )[:32]
        )
        rows.append(
            {
                "observation_id": observation_id,
                "table_id": table_descriptor["table_id"],
                "series_id": series_id,
                "operation_id": operation_descriptor["operation_id"],
                "operation_code": operation_descriptor["operation_code"],
                "series_name": series_name,
                "reference_date": reference_date,
                "period_code": period_code,
                "period_name": _text(period.get("Nombre")),
                "year": _int(item.get("Anyo")),
                "value": _float(item.get("Valor")),
                "data_type_code": data_type_code,
                "data_type_name": _text(data_type.get("Nombre")),
                "unit_name": _text(unit.get("Nombre")),
                "scale_factor": _float(scale.get("Factor")),
                "metadata_json": metadata_json,
                "source_file_sha256": source_file_sha256,
                "snapshot_date": snapshot_date,
            }
        )
    return rows


def ine_series_observation_rows(
    series: dict[str, Any],
    *,
    table: dict[str, Any],
    operation: dict[str, Any],
    snapshot_date: str,
    source_file_sha256: str,
) -> list[dict[str, Any]]:
    """Normalize one bounded INE series without materializing the full table payload."""

    return _observation_rows(
        series,
        table=table,
        operation=operation,
        snapshot_date=snapshot_date,
        source_file_sha256=source_file_sha256,
    )


def _operation_selected(operation: dict[str, Any], filters: set[str]) -> bool:
    if not filters:
        return True
    candidates = _candidate_values(
        operation.get("operation_id"),
        operation.get("operation_code"),
        operation.get("ioe_code"),
        operation.get("operation_name"),
        operation.get("Id"),
        operation.get("Codigo"),
        operation.get("Cod_IOE"),
        operation.get("Nombre"),
    )
    return bool(candidates & _normalized_filters(filters))


def _table_selected(table: dict[str, Any], filters: set[str]) -> bool:
    if not filters:
        return True
    candidates = _candidate_values(
        table.get("table_id"),
        table.get("table_code"),
        table.get("table_name"),
        table.get("Id"),
        table.get("Codigo"),
        table.get("Nombre"),
    )
    return bool(candidates & _normalized_filters(filters))


def _candidate_values(*values: Any) -> set[str]:
    return {_normalize_filter(value) for value in values if value not in (None, "")}


def _normalized_filters(values: set[str]) -> set[str]:
    return {_normalize_filter(value) for value in values if value}


def _normalize_filter(value: Any) -> str:
    return str(value).strip().casefold()


def _operation_request_key(operation: dict[str, Any] | str) -> str:
    if not isinstance(operation, dict):
        return str(operation)
    return str(
        operation.get("Codigo")
        or operation.get("operation_code")
        or operation.get("Id")
        or operation.get("operation_id")
        or operation.get("Cod_IOE")
        or operation.get("ioe_code")
    )


def _operation_id(operation: dict[str, Any]) -> str:
    value = operation.get("Id") or operation.get("operation_id")
    if value not in (None, ""):
        return str(value)
    value = operation.get("Codigo") or operation.get("Cod_IOE") or operation.get("Nombre")
    return str(value) if value not in (None, "") else "unknown_operation"


def _table_id(table: dict[str, Any]) -> str:
    value = table.get("Id") or table.get("table_id")
    if value not in (None, ""):
        return str(value)
    value = table.get("Codigo") or table.get("Nombre")
    return str(value) if value not in (None, "") else "unknown_table"


def _series_id(series: dict[str, Any]) -> str:
    value = series.get("COD") or series.get("Id") or series.get("series_id")
    return str(value) if value not in (None, "") else "series_" + stable_id(series)[:24]


def _metadata_signature(metadata: Any) -> str | None:
    values = []
    for item in _as_dict_list(metadata):
        variable = item.get("Variable") or {}
        values.append(f"{variable.get('Id')}:{item.get('Id')}")
    if not values:
        return None
    return stable_id(*sorted(values))[:32]


def _loads_json(content: bytes) -> Any:
    text = content.decode("utf-8-sig").strip()
    if not text:
        return []
    return json.loads(text)


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8")


def _json_text(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _as_dict_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(normalize_text(value))


def _none_text(value: Any) -> str | None:
    text = _text(value)
    if text is None or text.casefold() == "null":
        return None
    return text


def _int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, int | float):
        return float(value)
    text = str(value).strip().replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _timestamp(value: Any) -> datetime | None:
    if value in (None, "", "null"):
        return None
    if isinstance(value, int | float):
        seconds = float(value) / 1000 if abs(float(value)) > 10_000_000_000 else float(value)
        try:
            return datetime.fromtimestamp(seconds, UTC)
        except (OSError, OverflowError, ValueError):
            return None
    text = str(value).replace("Z", "+00:00")
    if text.casefold() == "null":
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _date_value(value: Any) -> date | None:
    if value in (None, "", "null"):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if timestamp := _timestamp(value):
        return timestamp.date()
    text = str(value)
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _compact_date(value: str | None) -> str:
    if not value:
        return ""
    return date.fromisoformat(value[:10]).strftime("%Y%m%d")


def _window_token(
    *,
    start_date: str | None,
    end_date: str | None,
    nult: int | None,
) -> str:
    if start_date or end_date:
        return f"date-{_compact_date(start_date) or 'start'}-{_compact_date(end_date) or 'end'}"
    if nult is not None:
        return f"nult-{nult}"
    return "full-history"


def _snapshot_token(*parts: Any) -> str:
    return "-".join(str(part).replace("/", "-").replace("\\", "-") for part in parts if part)
