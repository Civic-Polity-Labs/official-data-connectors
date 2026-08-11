from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import pytest

from official_data.client import (
    EurostatConnector,
    IneConnector,
    OfficialDataClient,
    legacy_manifest,
    public_manifest,
)
from official_data.eurostat import EUROSTAT_CATALOG_DATASET
from official_data.ine import INE_CATALOG_DATASET
from official_data.models import ArtifactManifest, Observation, OfficialExtractionPlan, SourceRef
from official_data.storage import BronzeManifest


def artifact(*, provider: str, dataset: str, path, content: bytes, format_name: str):
    return ArtifactManifest(
        provider=provider,
        family=provider,
        dataset=dataset,
        format=format_name,
        source_url=f"https://example.test/{provider}",
        run_date="2026-08-07",
        fetched_at=datetime.now(UTC),
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
        payload_path=str(path),
        adapter=f"official_data.{provider}",
    )


def test_manifest_round_trip_preserves_bronze_contract() -> None:
    bronze = BronzeManifest(
        family="ine",
        dataset="catalog",
        format="json",
        source_url="https://example.test/effective",
        requested_url="https://example.test/requested",
        snapshot_token="catalog-es",
        run_date="2026-08-07",
        extracted_at=datetime.now(UTC).isoformat(),
        sha256="a" * 64,
        bytes=10,
        bronze_path="bronze/catalog.json",
        status_code=200,
    )

    public = public_manifest("ine", bronze)
    restored = legacy_manifest(public)

    assert public.source_url == bronze.requested_url
    assert public.effective_url == bronze.source_url
    assert restored.bronze_path == bronze.bronze_path
    assert restored.sha256 == bronze.sha256


def test_native_catalog_models_are_preserved_offline(tmp_path) -> None:
    ine_payload = {
        "payload": {
            "operations": [{"operation_id": "OP1", "operation_code": "OP", "name": "Demo"}],
            "tables": [
                {
                    "operation": {"operation_id": "OP1", "operation_code": "OP"},
                    "table": {"table_id": "T1", "name": "Table"},
                }
            ],
            "series": [
                {
                    "operation": {"operation_id": "OP1"},
                    "table": {"table_id": "T1"},
                    "series": {"series_id": "S1", "Nombre": "Series"},
                }
            ],
        }
    }
    ine_content = json.dumps(ine_payload).encode()
    ine_path = tmp_path / "ine-catalog.json"
    ine_path.write_bytes(ine_content)
    ine_manifest = artifact(
        provider="ine",
        dataset=INE_CATALOG_DATASET,
        path=ine_path,
        content=ine_content,
        format_name="json",
    )

    eurostat_content = b"Code\tType\tLast data change\nDEMO\tdataset\t2026-01-01\n"
    eurostat_path = tmp_path / "eurostat-catalog.tsv"
    eurostat_path.write_bytes(eurostat_content)
    eurostat_manifest = artifact(
        provider="eurostat",
        dataset=EUROSTAT_CATALOG_DATASET,
        path=eurostat_path,
        content=eurostat_content,
        format_name="tsv",
    )

    ine_rows = tuple(IneConnector(output_root=tmp_path)._catalog_manifest(ine_manifest))
    eurostat_rows = tuple(
        EurostatConnector(output_root=tmp_path)._catalog_manifest(eurostat_manifest)
    )

    assert {type(item).__name__ for item in ine_rows} == {"IneOperation", "IneTable", "IneSeries"}
    assert eurostat_rows[0].dataset_code == "DEMO"


class CustomConnector:
    name = "custom"
    version = "1"

    def __init__(self) -> None:
        self.source = SourceRef(
            requested_url="https://example.test/custom",
            sha256="b" * 64,
            adapter="custom",
        )

    def catalog(self):
        return iter(())

    def extract(self, plan):
        return iter(())

    def native_observations(self, manifests):
        return iter(())

    def observations(self, manifests):
        yield Observation(
            provider="custom",
            dataset="demo",
            native_id="one",
            dimensions={},
            value=1,
            source=self.source,
        )


def test_official_client_delegates_to_injected_connector(tmp_path) -> None:
    connector = CustomConnector()
    client = OfficialDataClient("custom", output_root=tmp_path, connector=connector)
    plan = OfficialExtractionPlan(output_root=tmp_path)

    assert tuple(client.catalog()) == ()
    assert tuple(client.extract(plan)) == ()
    assert tuple(client.native_observations(())) == ()
    assert next(client.observations(())).value == 1


def test_official_client_rejects_a_plan_for_another_root(tmp_path) -> None:
    client = OfficialDataClient(
        "custom",
        output_root=tmp_path / "client",
        connector=CustomConnector(),
    )
    with pytest.raises(ValueError, match="must match"):
        tuple(client.extract(OfficialExtractionPlan(output_root=tmp_path / "plan")))
