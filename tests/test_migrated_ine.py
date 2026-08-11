import json

import official_data.ine as ine_module
from official_data.catalog import DatasetResource
from official_data.http import FetchResult
from official_data.ine import (
    INE_FAMILY,
    INE_TABLE_DATASET,
    build_ine_table_plans,
    extract_ine_table_data_batch,
    ine_catalog_normalized_records,
    ine_table_data_normalized_records,
)
from official_data.storage import persist_bronze


def test_build_ine_table_plans_filters_by_operation_and_table() -> None:
    catalog = {
        "tables": [
            {
                "operation": {
                    "operation_id": "25",
                    "operation_code": "IPC",
                    "ioe_code": "30138",
                    "operation_name": "IPC",
                },
                "table": {"Id": 50902, "Nombre": "Indice nacional", "Codigo": "NAC"},
            },
            {
                "operation": {
                    "operation_id": "99",
                    "operation_code": "EPA",
                    "ioe_code": "30308",
                    "operation_name": "EPA",
                },
                "table": {"Id": 12345, "Nombre": "Otra", "Codigo": "OTR"},
            },
        ]
    }

    plans = build_ine_table_plans(
        catalog_payload=catalog,
        operations={"IPC"},
        tables={"50902"},
    )

    assert len(plans) == 1
    assert plans[0]["operation"]["operation_code"] == "IPC"
    assert plans[0]["table"]["table_id"] == "50902"


def test_discover_ine_catalog_enriches_table_before_max_tables_cutoff(monkeypatch) -> None:
    monkeypatch.setattr(
        ine_module,
        "discover_ine_operations",
        lambda **_: [{"Id": 25, "Codigo": "IPC", "Nombre": "IPC"}],
    )
    monkeypatch.setattr(
        ine_module,
        "discover_ine_tables",
        lambda *_, **__: [{"Id": 50902, "Nombre": "Indices nacionales"}],
    )
    monkeypatch.setattr(
        ine_module,
        "discover_ine_table_groups",
        lambda *_, **__: [{"Id": 1, "Nombre": "Territorio"}],
    )
    monkeypatch.setattr(
        ine_module,
        "discover_ine_table_group_values",
        lambda *_, **__: [{"Id": 10, "Nombre": "Nacional"}],
    )
    monkeypatch.setattr(
        ine_module,
        "discover_ine_table_series",
        lambda *_, **__: [{"COD": "IPC251852", "Nombre": "Nacional"}],
    )

    payload = ine_module.discover_ine_catalog(
        include_table_groups=True,
        include_table_series=True,
        max_tables=1,
        client=object(),
    )

    assert len(payload["tables"]) == 1
    assert len(payload["groups"]) == 1
    assert len(payload["group_values"]) == 1
    assert len(payload["series"]) == 1


def test_extract_ine_batch_reuses_existing_manifest_when_skipping(tmp_path) -> None:
    resource = DatasetResource(
        family=INE_FAMILY,
        dataset=INE_TABLE_DATASET,
        format="json",
        url="https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/50902?nult=1",
        snapshot_token="datos-tabla-50902-nult-1",
    )
    content = json.dumps({"request": {}, "payload": []}).encode()
    existing = persist_bronze(
        root=tmp_path,
        resource=resource,
        run_date="2026-07-05",
        result=FetchResult(url=resource.url, status_code=200, headers={}, content=content),
    )
    catalog = {
        "tables": [
            {
                "operation": {"operation_id": "25", "operation_code": "IPC"},
                "table": {"Id": 50902, "Nombre": "Indices nacionales"},
            }
        ]
    }
    report_path = tmp_path / "audit" / "ine_report.json"

    manifests = extract_ine_table_data_batch(
        run_date="2026-07-05",
        output_root=tmp_path,
        catalog_payload=catalog,
        operations={"IPC"},
        nult=1,
        skip_existing=True,
        report_path=report_path,
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert manifests == [existing]
    assert report["skipped_existing_tables"] == 1
    assert report["failed_tables"] == 0


def test_ine_catalog_rows_normalize_operations_and_tables() -> None:
    payload = {
        "operations": [
            {
                "operation_id": "25",
                "operation_code": "IPC",
                "ioe_code": "30138",
                "operation_name": "Indice de Precios de Consumo",
            }
        ],
        "tables": [
            {
                "operation": {
                    "operation_id": "25",
                    "operation_code": "IPC",
                    "ioe_code": "30138",
                    "operation_name": "Indice de Precios de Consumo",
                },
                "table": {
                    "Id": 50902,
                    "Nombre": "Indices nacionales",
                    "Codigo": "NAC",
                    "Periodicidad": {"Id": 1, "Nombre": "Mensual", "Codigo": "M"},
                    "Publicacion": {
                        "Id": 8,
                        "Nombre": "IPC",
                        "PubFechaAct": {
                            "Fecha": "2026-06-29T09:00:00.000+02:00",
                            "Nombre": "Avance junio",
                        },
                    },
                    "Periodo_ini": {"Codigo": "01", "Nombre_largo": "Enero"},
                    "Anyo_Periodo_ini": "1961",
                    "Ultima_Modificacion": 1781247600000,
                },
            }
        ],
    }

    rows = ine_catalog_normalized_records(
        payload,
        snapshot_date="2026-07-05",
        source_file_sha256="catalog-sha",
    )

    assert rows["operations"][0]["operation_code"] == "IPC"
    assert rows["tables"][0]["table_id"] == "50902"
    assert rows["tables"][0]["periodicity_code"] == "M"
    assert rows["tables"][0]["reference_start_year"] == 1961


def test_ine_catalog_rows_tolerate_out_of_range_epoch_milliseconds() -> None:
    payload = {
        "operations": [
            {
                "operation_id": "777",
                "operation_code": "EPF",
                "operation_name": "Encuesta de Presupuestos Familiares",
            }
        ],
        "tables": [
            {
                "operation": {
                    "operation_id": "777",
                    "operation_code": "EPF",
                    "operation_name": "Encuesta de Presupuestos Familiares",
                },
                "table": {
                    "Id": 75526,
                    "Nombre": "Distribucion segun nivel de ingresos",
                    "Publicacion": {"PubFechaAct": {"Fecha": 1782378000000}},
                    "Ultima_Modificacion": -2208955516000,
                },
            }
        ],
    }

    rows = ine_catalog_normalized_records(
        payload,
        snapshot_date="2026-07-05",
        source_file_sha256="catalog-sha",
    )

    assert rows["tables"][0]["table_id"] == "75526"
    assert rows["tables"][0]["last_modified_at"] is None


def test_ine_table_data_rows_normalize_series_metadata_and_observations() -> None:
    payload = {
        "request": {
            "operation": {
                "operation_id": "25",
                "operation_code": "IPC",
                "ioe_code": "30138",
                "operation_name": "IPC",
            },
            "table": {"table_id": "50902", "table_name": "Indices nacionales"},
        },
        "payload": [
            {
                "COD": "IPC251852",
                "Nombre": "Nacional. Indice general. Indice.",
                "Unidad": {"Nombre": "Indice"},
                "Escala": {"Nombre": " ", "Factor": "1E0"},
                "MetaData": [
                    {
                        "Id": 16473,
                        "Variable": {"Id": 349, "Nombre": "Totales", "Codigo": "100"},
                        "Nombre": "Nacional",
                        "Codigo": "00",
                    }
                ],
                "Data": [
                    {
                        "Fecha": "2025-12-01T00:00:00.000+01:00",
                        "TipoDato": {"Nombre": "Definitivo", "Codigo": "D"},
                        "Periodo": {"Codigo": "12", "Nombre": "M12"},
                        "Anyo": 2025,
                        "Valor": 119.942,
                    }
                ],
            }
        ],
    }

    rows = ine_table_data_normalized_records(
        payload,
        snapshot_date="2026-07-05",
        source_file_sha256="data-sha",
    )

    assert rows["series"][0]["series_id"] == "IPC251852"
    assert rows["series_metadata_values"][0]["value_name"] == "Nacional"
    observation = rows["observations"][0]
    assert observation["table_id"] == "50902"
    assert observation["reference_date"].isoformat() == "2025-12-01"
    assert observation["value"] == 119.942
    assert observation["data_type_code"] == "D"
