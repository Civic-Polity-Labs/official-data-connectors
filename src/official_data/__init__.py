"""Public API for extensible official statistical data connectors."""

from official_data.client import OfficialDataClient
from official_data.models import (
    ArtifactManifest,
    EurostatCatalogItem,
    EurostatCodelist,
    EurostatDataset,
    EurostatDimension,
    EurostatObservation,
    IneDimension,
    IneObservation,
    IneOperation,
    IneSeries,
    IneTable,
    Observation,
    OfficialExtractionPlan,
    SourceRef,
)

__all__ = [
    "ArtifactManifest",
    "EurostatCatalogItem",
    "EurostatCodelist",
    "EurostatDataset",
    "EurostatDimension",
    "EurostatObservation",
    "IneDimension",
    "IneObservation",
    "IneOperation",
    "IneSeries",
    "IneTable",
    "Observation",
    "OfficialDataClient",
    "OfficialExtractionPlan",
    "SourceRef",
]

__version__ = "1.0.0"
