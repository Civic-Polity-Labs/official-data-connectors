from __future__ import annotations

import gzip
import hashlib
import json
from datetime import UTC, datetime

from official_data.client import EurostatConnector, IneConnector
from official_data.eurostat import EUROSTAT_DATASET_DATA
from official_data.ine import INE_TABLE_DATASET
from official_data.models import ArtifactManifest


def manifest(*, provider: str, dataset: str, path, content: bytes, format_name: str, url: str):
    return ArtifactManifest(
        provider=provider,
        family=provider,
        dataset=dataset,
        format=format_name,
        source_url=url,
        effective_url=url,
        run_date="2026-08-07",
        fetched_at=datetime.now(UTC),
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
        payload_path=str(path),
        http_status=200,
        adapter=f"official_data.{provider}",
    )


def test_ine_observations_stream_one_series_at_a_time(tmp_path) -> None:
    payload = {
        "request": {
            "operation": {"operation_id": "OP1", "operation_code": "OP"},
            "table": {"table_id": "100", "table_name": "Population"},
        },
        "payload": [
            {
                "Id": "SER1",
                "Nombre": "Population",
                "Unidad": {"Nombre": "persons"},
                "Data": [
                    {
                        "Fecha": "2025-01-01T00:00:00.000",
                        "Anyo": 2025,
                        "Periodo": {"Codigo": "A", "Nombre": "Annual"},
                        "TipoDato": {"Codigo": "D", "Nombre": "Definitive"},
                        "Valor": 42,
                    }
                ],
            }
        ],
    }
    content = json.dumps(payload).encode()
    path = tmp_path / "ine.json"
    path.write_bytes(content)
    artifact = manifest(
        provider="ine",
        dataset=INE_TABLE_DATASET,
        path=path,
        content=content,
        format_name="json",
        url="https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/100",
    )
    connector = IneConnector(output_root=tmp_path)

    native = tuple(connector.native_observations((artifact,)))
    common = tuple(connector.observations((artifact,)))

    assert native[0].series_id == "SER1"
    assert common[0].provider == "ine"
    assert common[0].value == 42
    assert common[0].unit == "persons"


def test_eurostat_observations_stream_gzip_rows(tmp_path) -> None:
    text = (
        "DATAFLOW,LAST UPDATE,freq,geo,unit,TIME_PERIOD,OBS_VALUE,OBS_STATUS\n"
        "ESTAT:DEMO,2026-01-01,A,ES,PC,2025,12.5,p\n"
        "ESTAT:DEMO,2026-01-01,A,FR,PC,2025,13.0,\n"
    )
    content = gzip.compress(text.encode())
    path = tmp_path / "eurostat.csv.gz"
    path.write_bytes(content)
    artifact = manifest(
        provider="eurostat",
        dataset=EUROSTAT_DATASET_DATA,
        path=path,
        content=content,
        format_name="csv.gz",
        url="https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/DEMO",
    )
    connector = EurostatConnector(output_root=tmp_path)

    native = tuple(connector.native_observations((artifact,)))
    common = tuple(connector.observations((artifact,)))

    assert len(native) == len(common) == 2
    assert native[0].dataset_code == "DEMO"
    assert common[0].dimensions["geo"] == "ES"
    assert common[0].status == "p"
