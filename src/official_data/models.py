"""Native provider contracts and lossless common statistical view."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from datetime import date as Date
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

_SECRET_KEY = re.compile(r"(?:api[_-]?key|token|secret|password|authorization|credential)", re.I)


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): "[REDACTED]" if _SECRET_KEY.search(str(key)) else _redact(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_redact(item) for item in value]
    return value


class PublicModel(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True, populate_by_name=True)


class SourceRef(PublicModel):
    requested_url: str
    effective_url: str | None = None
    sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    parameters: dict[str, Any] = Field(default_factory=dict)
    adapter: str
    adapter_version: str = "1.0.0"
    normalization_version: str = "1.0.0"

    @field_validator("parameters", mode="before")
    @classmethod
    def _redacted_parameters(cls, value: Any) -> Any:
        return _redact(value or {})


class ArtifactManifest(PublicModel):
    schema_version: str = "1.0"
    provider: str
    family: str
    dataset: str
    format: str
    source_url: str
    effective_url: str | None = None
    snapshot_token: str | None = None
    run_date: str
    fetched_at: datetime | str
    sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    bytes: int = Field(ge=0)
    payload_path: str
    http_status: int | None = None
    content_type: str | None = None
    request_parameters: str | dict[str, Any] | None = None
    adapter: str
    adapter_version: str = "1.0.0"
    normalization_version: str = "1.0.0"

    @field_validator("request_parameters", mode="before")
    @classmethod
    def _redacted_parameters(cls, value: Any) -> Any:
        return _redact(value) if isinstance(value, dict) else value

    def source_ref(self) -> SourceRef:
        parameters = (
            self.request_parameters
            if isinstance(self.request_parameters, dict)
            else {"canonical": self.request_parameters}
            if self.request_parameters
            else {}
        )
        return SourceRef(
            requested_url=self.source_url,
            effective_url=self.effective_url or self.source_url,
            sha256=self.sha256,
            fetched_at=self.fetched_at,
            parameters=parameters,
            adapter=self.adapter,
            adapter_version=self.adapter_version,
            normalization_version=self.normalization_version,
        )


class OfficialExtractionPlan(PublicModel):
    dataset_ids: tuple[str, ...] = ()
    operation_ids: tuple[str, ...] = ()
    category_ids: tuple[str, ...] = ()
    output_root: Path
    run_date: str = Field(default_factory=lambda: Date.today().isoformat())
    start_date: str | None = None
    end_date: str | None = None
    latest_periods: int | None = Field(default=None, ge=1)
    include_catalog: bool = True
    include_structures: bool = False
    resume: bool = True
    continue_on_error: bool = False
    max_datasets: int | None = Field(default=None, ge=1)


class ProviderRecord(PublicModel):
    source: SourceRef


class IneOperation(ProviderRecord):
    operation_id: str
    operation_code: str | None = None
    operation_name: str | None = None


class IneTable(ProviderRecord):
    table_id: str
    table_name: str | None = None
    operation_id: str | None = None


class IneSeries(ProviderRecord):
    series_id: str
    series_name: str | None = None
    table_id: str | None = None


class IneDimension(ProviderRecord):
    dimension_id: str
    series_id: str | None = None
    name: str | None = None
    value: str | None = None


class IneObservation(ProviderRecord):
    observation_id: str
    table_id: str
    series_id: str
    reference_date: Date | None = None
    period_code: str | None = None
    value: float | None = None
    unit_name: str | None = None


class EurostatCatalogItem(ProviderRecord):
    dataset_code: str
    title: str | None = None


class EurostatDataset(EurostatCatalogItem):
    last_data_change_at: datetime | str | None = None
    last_structural_change_at: datetime | str | None = None


class EurostatCodelist(ProviderRecord):
    codelist_code: str
    version: str | None = None
    title: str | None = None


class EurostatDimension(ProviderRecord):
    dimension_id: str
    dataset_code: str
    name: str
    role: str | None = None


class EurostatObservation(ProviderRecord):
    observation_id: str
    dataset_code: str
    dimensions: dict[str, str | None]
    time_period: str | None = None
    obs_value: float | None = None
    unit: str | None = None
    obs_status: str | None = None


class Observation(ProviderRecord):
    provider: Literal["ine", "eurostat"] | str
    dataset: str
    native_id: str
    dimensions: dict[str, Any]
    period: str | None = None
    value: float | None = None
    unit: str | None = None
    status: str | None = None


NativeCatalogRecord = IneOperation | IneTable | IneSeries | EurostatCatalogItem | EurostatCodelist
NativeObservation = IneObservation | EurostatObservation
