import gzip

from official_data.catalog import DatasetResource
from official_data.eurostat import (
    EUROSTAT_CODELIST_DATASET,
    EUROSTAT_DATASET_DATA,
    EUROSTAT_FAMILY,
    build_eurostat_codelist_plans,
    build_eurostat_dataset_plans,
    eurostat_catalog_normalized_records,
    eurostat_codelist_normalized_records,
    eurostat_dataset_data_normalized_records,
    eurostat_toc_normalized_records,
    eurostat_toc_xml_normalized_records,
)
from official_data.http import FetchResult
from official_data.storage import persist_bronze


def test_eurostat_reference_date_supports_semesters() -> None:
    from official_data.eurostat import _reference_date

    assert _reference_date("2025-S1") == "2025-01-01"
    assert _reference_date("2025-S2") == "2025-07-01"


def test_build_eurostat_dataset_plans_filters_changed_datasets() -> None:
    catalog = {
        "datasets": [
            {
                "Code": "DEMO_PJAN",
                "Type": "dataset",
                "Last data change": "11/07/2026 11:00:00",
                "Last structural change": "01/07/2026 11:00:00",
                "Data download url (csv)": "https://example.test/demo_pjan?format=SDMX-CSV",
                "Data structure download url": "https://example.test/demo_pjan/structure",
            },
            {
                "Code": "OLD_DATA",
                "Type": "dataset",
                "Last data change": "01/01/2024 11:00:00",
                "Data download url (csv)": "https://example.test/old?format=SDMX-CSV",
            },
        ]
    }

    plans = build_eurostat_dataset_plans(
        catalog_payload=catalog,
        changed_since="2026-07-10",
    )

    assert [plan["dataset_code"] for plan in plans] == ["DEMO_PJAN"]
    assert plans[0]["data_url"].endswith("format=SDMX-CSV&compressed=true")
    assert plans[0]["structure_url"] == "https://example.test/demo_pjan/structure"


def test_build_eurostat_dataset_plans_compares_mixed_timezone_dates() -> None:
    catalog = {
        "datasets": [
            {
                "Code": "TZ_DATA",
                "Last data change": "2026-07-11T11:00:00+02:00",
                "Data download url (csv)": "https://example.test/tz?format=SDMX-CSV",
            }
        ]
    }

    plans = build_eurostat_dataset_plans(
        catalog_payload=catalog,
        changed_since="2026-07-11",
    )

    assert [plan["dataset_code"] for plan in plans] == ["TZ_DATA"]


def test_build_eurostat_codelist_plans_prefers_specific_version_url() -> None:
    payload = {
        "codelists": [
            {
                "Code": "GEO",
                "Version": "19.0",
                "Specific tsv download url": "https://example.test/GEO/19.0?format=TSV",
                "Latest tsv download url": "https://example.test/GEO?format=TSV",
            }
        ]
    }

    plans = build_eurostat_codelist_plans(codelist_payload=payload, codelists={"GEO"})

    assert plans[0]["url"] == "https://example.test/GEO/19.0?format=TSV"
    assert plans[0]["codelist"]["version"] == "19.0"


def test_eurostat_catalog_rows_match_declared_schema() -> None:
    content = (
        b"Code\tType\tSource dataset\tLast data change\tLast structural change\t"
        b"Data download url (tsv)\tData download url (csv)\tData download url (sdmx)\t"
        b"Data structure download url\tOpen in Data Browser url\n"
        b"DEMO_PJAN\tdataset\tESTAT\t11/07/2026 11:00:00\t10/07/2026 23:00:00\t"
        b"https://example.test/tsv\thttps://example.test/csv\thttps://example.test/sdmx\t"
        b"https://example.test/structure\thttps://example.test/browser\n"
    )

    rows = eurostat_catalog_normalized_records(
        content,
        snapshot_date="2026-07-11",
        source_file_sha256="catalog-sha",
    )

    dataset = rows["datasets"][0]
    assert dataset["dataset_code"] == "DEMO_PJAN"
    assert dataset["last_data_change_at"].startswith("2026-07-11T11:00:00")


def test_eurostat_toc_rows_match_declared_schema() -> None:
    content = (
        b"code\ttitle\ttype\tlast update of data\tlast table structure change\t"
        b"data start\tdata end\tvalues\n"
        b"demo\tPopulation and demography\tfolder\t\t\t\t\t\n"
        b"DEMO_PJAN\t    Population on 1 January\tdataset\t2026-07-11\t"
        b"2026-07-10\t1960\t2025\t48\n"
    )
    rows = eurostat_toc_normalized_records(
        content,
        snapshot_date="2026-07-11",
        source_file_sha256="toc-sha",
    )

    dataset_node = rows["toc"][1]
    assert dataset_node["ordinal"] == 2
    assert dataset_node["code"] == "DEMO_PJAN"
    assert dataset_node["title"] == "Population on 1 January"
    assert dataset_node["depth"] == 1


def test_eurostat_toc_xml_rows_keep_descriptions_links_and_dedupe() -> None:
    content = b"""<?xml version="1.0" encoding="UTF-8"?>
<nt:tree xmlns:nt="urn:eu.europa.ec.eurostat.navtree">
  <nt:branch>
    <nt:leaf type="table">
      <nt:title language="en">Population</nt:title>
      <nt:code>demo_pjan</nt:code>
      <nt:source language="en">Eurostat</nt:source>
      <nt:unit language="en">Persons</nt:unit>
      <nt:shortDescription language="en">Population on 1 January</nt:shortDescription>
      <nt:metadata format="html">https://example.test/metadata</nt:metadata>
      <nt:downloadLink format="tsv">https://example.test/data.tsv</nt:downloadLink>
    </nt:leaf>
    <nt:leaf type="table">
      <nt:title language="en">Population</nt:title>
      <nt:code>DEMO_PJAN</nt:code>
      <nt:metadata format="sdmx">https://example.test/metadata.xml</nt:metadata>
      <nt:downloadLink format="sdmx">https://example.test/data.sdmx</nt:downloadLink>
    </nt:leaf>
  </nt:branch>
</nt:tree>
"""
    rows = eurostat_toc_xml_normalized_records(
        content,
        snapshot_date="2026-07-11",
        source_file_sha256="toc-xml-sha",
    )["toc_metadata"]

    assert len(rows) == 1
    assert rows[0]["code"] == "DEMO_PJAN"
    assert rows[0]["short_description_en"] == "Population on 1 January"
    assert rows[0]["metadata_html_url"] == "https://example.test/metadata"
    assert rows[0]["metadata_sdmx_url"] == "https://example.test/metadata.xml"


def test_eurostat_dataset_data_rows_keep_dimensions_and_attributes(tmp_path) -> None:
    csv_content = gzip.compress(
        b"DATAFLOW,LAST UPDATE,freq,unit,age,sex,geo,TIME_PERIOD,OBS_VALUE,"
        b"OBS_FLAG,CONF_STATUS\n"
        b"ESTAT:DEMO_PJAN(1.0),2026-07-11T11:00:00,A,NR,TOTAL,T,ES,"
        b"2025,48500000,p,C\n"
    )
    manifest = persist_bronze(
        root=tmp_path,
        resource=DatasetResource(
            family=EUROSTAT_FAMILY,
            dataset=EUROSTAT_DATASET_DATA,
            format="csv.gz",
            url="https://example.test/demo_pjan?compressed=true",
            snapshot_token="dataset_data-demo_pjan",
        ),
        run_date="2026-07-11",
        result=FetchResult(
            url="https://example.test/demo_pjan?compressed=true",
            status_code=200,
            headers={},
            content=csv_content,
        ),
    )

    rows = eurostat_dataset_data_normalized_records(
        csv_content,
        manifest=manifest,
        snapshot_date="2026-07-11",
        source_file_sha256="data-sha",
    )

    observation = rows["observations"][0]
    assert observation["dataset_code"] == "DEMO_PJAN"
    assert len(observation["observation_id"]) == 35
    assert observation["observation_id"].startswith("es_")
    assert observation["reference_date"] == "2025-01-01"
    assert observation["obs_value"] == 48500000
    assert '"geo": "ES"' in observation["dimensions_json"]
    assert '"OBS_FLAG": "p"' in observation["attributes_json"]
    assert {row["column_role"] for row in rows["dataset_columns"]} >= {
        "dimension",
        "measure",
        "time",
        "attribute",
    }


def test_eurostat_dataset_code_prefers_source_url_when_token_is_normalized(tmp_path) -> None:
    csv_content = gzip.compress(
        b"DATAFLOW,LAST UPDATE,freq,TIME_PERIOD,OBS_VALUE\n"
        b"ESTAT:STS_COBP_Q$DV_3226(1.0),2026-07-11T11:00:00,Q,2025-Q1,1\n"
    )
    manifest = persist_bronze(
        root=tmp_path,
        resource=DatasetResource(
            family=EUROSTAT_FAMILY,
            dataset=EUROSTAT_DATASET_DATA,
            format="csv.gz",
            url=(
                "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
                "STS_COBP_Q$DV_3226/?format=SDMX-CSV&compressed=true"
            ),
            snapshot_token="dataset_data-sts_cobp_q-dv_3226",
        ),
        run_date="2026-07-11",
        result=FetchResult(
            url="https://example.test/sts.csv.gz",
            status_code=200,
            headers={},
            content=csv_content,
        ),
    )

    rows = eurostat_dataset_data_normalized_records(
        csv_content,
        manifest=manifest,
        snapshot_date="2026-07-11",
        source_file_sha256="data-sha",
    )

    assert rows["observations"][0]["dataset_code"] == "STS_COBP_Q$DV_3226"


def test_eurostat_codelist_values_preserve_version_from_snapshot_token(tmp_path) -> None:
    content = (
        b"CODE\tLabel - English\tLabel - French\tLabel - German\tStandard code\t"
        b"Note (EN only)\tEC_corporate_code\tEC_corporate_uri\n"
        b"ES\tSpain\tEspagne\tSpanien\tES\t\tESP\t"
        b"http://publications.europa.eu/resource/authority/country/ESP\n"
    )
    manifest = _manifest_for_codelist(tmp_path)

    rows = eurostat_codelist_normalized_records(
        content,
        manifest=manifest,
        snapshot_date="2026-07-11",
        source_file_sha256="geo-sha",
    )

    value = rows["codelist_values"][0]
    assert value["codelist_code"] == "GEO"
    assert value["version"] == "19.0"
    assert value["code"] == "ES"


def _manifest_for_codelist(tmp_path):
    return persist_bronze(
        root=tmp_path,
        resource=DatasetResource(
            family=EUROSTAT_FAMILY,
            dataset=EUROSTAT_CODELIST_DATASET,
            format="tsv",
            url="https://example.test/GEO/19.0?format=TSV",
            snapshot_token="codelist-geo-19_0",
        ),
        run_date="2026-07-11",
        result=FetchResult(
            url="https://example.test/GEO/19.0?format=TSV",
            status_code=200,
            headers={},
            content=b"CODE\tLabel - English\nES\tSpain\n",
        ),
    )
