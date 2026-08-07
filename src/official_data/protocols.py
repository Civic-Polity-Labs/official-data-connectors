"""Contracts for third-party official source connectors."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Protocol, runtime_checkable

from official_data.models import (
    ArtifactManifest,
    NativeCatalogRecord,
    NativeObservation,
    Observation,
    OfficialExtractionPlan,
)


@runtime_checkable
class OfficialSourceConnector(Protocol):
    name: str
    version: str

    def catalog(self) -> Iterator[NativeCatalogRecord]: ...

    def extract(self, plan: OfficialExtractionPlan) -> Iterator[ArtifactManifest]: ...

    def native_observations(
        self, manifests: Iterable[ArtifactManifest]
    ) -> Iterator[NativeObservation]: ...

    def observations(self, manifests: Iterable[ArtifactManifest]) -> Iterator[Observation]: ...
