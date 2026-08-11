"""Provider-neutral public client backed by native INE and Eurostat connectors."""

from __future__ import annotations

import csv
import gzip
import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import IO, Any

import ijson

from official_data.eurostat import (
    EUROSTAT_CATALOG_DATASET,
    EUROSTAT_DATASET_DATA,
    EurostatApiClient,
    eurostat_catalog_normalized_records,
    eurostat_catalog_payload_from_content,
    eurostat_dataset_code_from_manifest,
    eurostat_observation_normalized_record,
    extract_eurostat_catalog_resource,
    extract_eurostat_dataset_batch,
)
from official_data.ine import (
    INE_CATALOG_DATASET,
    INE_TABLE_DATASET,
    IneApiClient,
    extract_ine_catalog_resource,
    extract_ine_table_data_batch,
    ine_catalog_normalized_records,
    ine_series_observation_rows,
)
from official_data.models import (
    ArtifactManifest,
    EurostatCatalogItem,
    EurostatObservation,
    IneObservation,
    IneOperation,
    IneSeries,
    IneTable,
    NativeCatalogRecord,
    NativeObservation,
    Observation,
    OfficialExtractionPlan,
)
from official_data.protocols import OfficialSourceConnector
from official_data.registry import default_registry
from official_data.storage import BronzeManifest


def public_manifest(provider: str, manifest: BronzeManifest) -> ArtifactManifest:
    return ArtifactManifest(
        provider=provider,
        family=manifest.family,
        dataset=manifest.dataset,
        format=manifest.format,
        source_url=manifest.requested_url or manifest.source_url,
        effective_url=manifest.source_url,
        snapshot_token=manifest.snapshot_token,
        run_date=manifest.run_date,
        fetched_at=manifest.extracted_at,
        sha256=manifest.sha256,
        bytes=manifest.bytes,
        payload_path=manifest.bronze_path,
        http_status=manifest.status_code,
        content_type=manifest.content_type,
        request_parameters=manifest.request_parameters_json,
        adapter=f"official_data.{provider}",
    )


def legacy_manifest(manifest: ArtifactManifest) -> BronzeManifest:
    return BronzeManifest(
        family=manifest.family,
        dataset=manifest.dataset,
        format=manifest.format,
        source_url=manifest.effective_url or manifest.source_url,
        snapshot_token=manifest.snapshot_token,
        run_date=manifest.run_date,
        extracted_at=str(manifest.fetched_at),
        sha256=manifest.sha256,
        bytes=manifest.bytes,
        bronze_path=manifest.payload_path,
        status_code=manifest.http_status or 200,
        requested_url=manifest.source_url,
        content_type=manifest.content_type,
        request_parameters_json=(
            manifest.request_parameters
            if isinstance(manifest.request_parameters, str)
            else json.dumps(manifest.request_parameters, ensure_ascii=False, sort_keys=True)
            if manifest.request_parameters
            else None
        ),
    )


class _BaseConnector:
    version = "1.0.0"

    def __init__(self, *, output_root: str | Path) -> None:
        self.output_root = Path(output_root)

    def _path(self, manifest: ArtifactManifest) -> Path:
        path = Path(manifest.payload_path)
        return path if path.is_absolute() else self.output_root / path

    def _validate_plan_root(self, plan: OfficialExtractionPlan) -> None:
        if plan.output_root.resolve() != self.output_root.resolve():
            raise ValueError("OfficialExtractionPlan.output_root must match connector output_root")


class IneConnector(_BaseConnector):
    name = "ine"

    def __init__(
        self,
        *,
        output_root: str | Path,
        provider_client: IneApiClient | None = None,
    ) -> None:
        super().__init__(output_root=output_root)
        self.provider_client = provider_client

    def catalog(self) -> Iterator[NativeCatalogRecord]:
        plan = OfficialExtractionPlan(output_root=self.output_root, include_catalog=True)
        manifests = self.extract(plan)
        for manifest in manifests:
            if manifest.dataset == INE_CATALOG_DATASET:
                yield from self._catalog_manifest(manifest)

    def extract(self, plan: OfficialExtractionPlan) -> Iterator[ArtifactManifest]:
        self._validate_plan_root(plan)
        catalog = extract_ine_catalog_resource(
            run_date=plan.run_date,
            output_root=plan.output_root,
            operations=set(plan.operation_ids),
            tables=set(plan.dataset_ids),
            categories=set(plan.category_ids),
            max_tables=plan.max_datasets,
            client=self.provider_client,
        )
        if plan.include_catalog:
            yield public_manifest(self.name, catalog)
        if plan.dataset_ids:
            payload = json.loads((plan.output_root / catalog.bronze_path).read_text("utf-8-sig"))
            for manifest in extract_ine_table_data_batch(
                run_date=plan.run_date,
                output_root=plan.output_root,
                catalog_payload=payload,
                operations=set(plan.operation_ids),
                tables=set(plan.dataset_ids),
                categories=set(plan.category_ids),
                start_date=plan.start_date,
                end_date=plan.end_date,
                nult=plan.latest_periods,
                max_tables=plan.max_datasets,
                skip_existing=plan.resume,
                continue_on_error=plan.continue_on_error,
                report_path=(plan.output_root / "plans" / f"ine-{plan.run_date}.report.json"),
                client=self.provider_client,
            ):
                yield public_manifest(self.name, manifest)

    def _catalog_manifest(self, manifest: ArtifactManifest) -> Iterator[NativeCatalogRecord]:
        payload = json.loads(self._path(manifest).read_text("utf-8-sig"))
        records = ine_catalog_normalized_records(
            payload,
            snapshot_date=manifest.run_date,
            source_file_sha256=manifest.sha256,
        )
        source = manifest.source_ref()
        for row in records["operations"]:
            yield IneOperation.model_validate({**row, "source": source})
        for row in records["tables"]:
            yield IneTable.model_validate({**row, "source": source})
        for row in records["series"]:
            yield IneSeries.model_validate({**row, "source": source})

    def native_observations(
        self, manifests: Iterable[ArtifactManifest]
    ) -> Iterator[IneObservation]:
        for manifest in manifests:
            if manifest.provider != self.name or manifest.dataset != INE_TABLE_DATASET:
                continue
            path = self._path(manifest)
            with path.open("rb") as handle:
                request: dict[str, Any] = next(ijson.items(handle, "request"), {})
            table = request.get("table") or {}
            operation = request.get("operation") or {}
            with path.open("rb") as handle:
                for series in ijson.items(handle, "payload.item"):
                    for row in ine_series_observation_rows(
                        series,
                        table=table,
                        operation=operation,
                        snapshot_date=manifest.run_date,
                        source_file_sha256=manifest.sha256,
                    ):
                        yield IneObservation.model_validate(
                            {**row, "source": manifest.source_ref()}
                        )

    def observations(self, manifests: Iterable[ArtifactManifest]) -> Iterator[Observation]:
        for item in self.native_observations(manifests):
            extra = item.model_extra or {}
            dimensions = {
                "operation_code": extra.get("operation_code"),
                "series_id": item.series_id,
                "period_code": item.period_code,
                "year": extra.get("year"),
                "data_type_code": extra.get("data_type_code"),
            }
            period = item.reference_date.isoformat() if item.reference_date else item.period_code
            yield Observation(
                provider=self.name,
                dataset=item.table_id,
                native_id=item.observation_id,
                dimensions=dimensions,
                period=period,
                value=item.value,
                unit=item.unit_name,
                status=extra.get("data_type_name"),
                source=item.source,
            )


class EurostatConnector(_BaseConnector):
    name = "eurostat"

    def __init__(
        self,
        *,
        output_root: str | Path,
        provider_client: EurostatApiClient | None = None,
    ) -> None:
        super().__init__(output_root=output_root)
        self.provider_client = provider_client

    def catalog(self) -> Iterator[NativeCatalogRecord]:
        plan = OfficialExtractionPlan(output_root=self.output_root, include_catalog=True)
        for manifest in self.extract(plan):
            if manifest.dataset == EUROSTAT_CATALOG_DATASET:
                yield from self._catalog_manifest(manifest)

    def extract(self, plan: OfficialExtractionPlan) -> Iterator[ArtifactManifest]:
        self._validate_plan_root(plan)
        catalog = extract_eurostat_catalog_resource(
            run_date=plan.run_date,
            output_root=plan.output_root,
            client=self.provider_client,
        )
        if plan.include_catalog:
            yield public_manifest(self.name, catalog)
        if plan.dataset_ids:
            content = (plan.output_root / catalog.bronze_path).read_bytes()
            payload = eurostat_catalog_payload_from_content(content)
            for manifest in extract_eurostat_dataset_batch(
                run_date=plan.run_date,
                output_root=plan.output_root,
                catalog_payload=payload,
                datasets=set(plan.dataset_ids),
                max_datasets=plan.max_datasets,
                include_structures=plan.include_structures,
                skip_existing=plan.resume,
                continue_on_error=plan.continue_on_error,
                report_path=(plan.output_root / "plans" / f"eurostat-{plan.run_date}.report.json"),
                client=self.provider_client,
            ):
                yield public_manifest(self.name, manifest)

    def _catalog_manifest(self, manifest: ArtifactManifest) -> Iterator[NativeCatalogRecord]:
        content = self._path(manifest).read_bytes()
        records = eurostat_catalog_normalized_records(
            content,
            snapshot_date=manifest.run_date,
            source_file_sha256=manifest.sha256,
        )["datasets"]
        for row in records:
            yield EurostatCatalogItem.model_validate({**row, "source": manifest.source_ref()})

    def native_observations(
        self, manifests: Iterable[ArtifactManifest]
    ) -> Iterator[EurostatObservation]:
        for manifest in manifests:
            if manifest.provider != self.name or manifest.dataset != EUROSTAT_DATASET_DATA:
                continue
            path = self._path(manifest)
            legacy = legacy_manifest(manifest)
            dataset_code = eurostat_dataset_code_from_manifest(legacy)
            with _open_text(path) as handle:
                for row in csv.DictReader(handle):
                    if not any((value or "").strip() for value in row.values()):
                        continue
                    native = eurostat_observation_normalized_record(
                        row,
                        dataset_code=dataset_code,
                        snapshot_date=manifest.run_date,
                        source_file_sha256=manifest.sha256,
                    )
                    dimensions = json.loads(native.pop("dimensions_json"))
                    native.pop("attributes_json", None)
                    yield EurostatObservation.model_validate(
                        {**native, "dimensions": dimensions, "source": manifest.source_ref()}
                    )

    def observations(self, manifests: Iterable[ArtifactManifest]) -> Iterator[Observation]:
        for item in self.native_observations(manifests):
            yield Observation(
                provider=self.name,
                dataset=item.dataset_code,
                native_id=item.observation_id,
                dimensions=item.dimensions,
                period=item.time_period,
                value=item.obs_value,
                unit=item.unit,
                status=item.obs_status,
                source=item.source,
            )


def _open_text(path: Path) -> IO[str]:
    if path.suffix.casefold() == ".gz":
        return gzip.open(path, "rt", encoding="utf-8-sig", newline="")
    return path.open("r", encoding="utf-8-sig", newline="")


def _register_builtins() -> None:
    if "ine" not in default_registry.names():
        default_registry.register_factory("ine", IneConnector, builtin=True)
    if "eurostat" not in default_registry.names():
        default_registry.register_factory("eurostat", EurostatConnector, builtin=True)


_register_builtins()


class OfficialDataClient:
    def __init__(
        self,
        source: str,
        *,
        output_root: str | Path = "data/lake",
        connector: OfficialSourceConnector | None = None,
        provider_client: Any = None,
    ) -> None:
        self.source = source.strip().casefold()
        self.output_root = Path(output_root)
        self.connector = connector or default_registry.create(
            self.source,
            output_root=self.output_root,
            provider_client=provider_client,
        )

    def catalog(self) -> Iterator[NativeCatalogRecord]:
        yield from self.connector.catalog()

    def extract(self, plan: OfficialExtractionPlan) -> Iterator[ArtifactManifest]:
        if plan.output_root.resolve() != self.output_root.resolve():
            raise ValueError(
                "OfficialExtractionPlan.output_root must match OfficialDataClient.output_root; "
                "construct a client for the requested root"
            )
        yield from self.connector.extract(plan)

    def native_observations(
        self, manifests: Iterable[ArtifactManifest]
    ) -> Iterator[NativeObservation]:
        yield from self.connector.native_observations(manifests)

    def observations(self, manifests: Iterable[ArtifactManifest]) -> Iterator[Observation]:
        yield from self.connector.observations(manifests)
