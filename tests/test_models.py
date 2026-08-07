from __future__ import annotations

from datetime import UTC, datetime

from official_data.models import ArtifactManifest, SourceRef


def test_source_ref_and_manifest_redact_parameters() -> None:
    manifest = ArtifactManifest(
        provider="ine",
        family="ine",
        dataset="table",
        format="json",
        source_url="https://example.test/data",
        run_date="2026-08-07",
        fetched_at=datetime.now(UTC),
        sha256="a" * 64,
        bytes=10,
        payload_path="bronze/item.json",
        adapter="official_data.ine",
        request_parameters={"table": "1", "api_key": "secret"},
    )

    assert manifest.request_parameters["api_key"] == "[REDACTED]"
    source = manifest.source_ref()
    assert isinstance(source, SourceRef)
    assert "secret" not in source.model_dump_json()
