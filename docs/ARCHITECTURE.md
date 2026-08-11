# Package architecture

`official-data-connectors` owns provider-neutral acquisition, immutable Bronze
evidence and deterministic normalization for official statistical sources. Version
1.x ships INE and Eurostat, but the public contracts do not depend on either name.

The dependency order is `models/protocols` → provider adapter and transport →
`OfficialDataClient` → CLI/export boundary. `OfficialDataHttpClient` contains only
HTTP concerns; INE and Eurostat parsing stays in their adapters. A source plugin is
registered through `official_data.sources` entry points.

Provider-native rows are authoritative and retained losslessly. `Observation` is an
optional common view, never a replacement for dimensions or provider metadata.
Manifests preserve URL, POST parameters, content metadata, byte count and SHA-256.

Provider adapters return domain-neutral normalized groups such as `operations`,
`series`, `datasets`, `dataset_columns` and `observations`. They never return table
names or layer-prefixed keys. A consumer that owns a warehouse must map those groups
explicitly to its own schema and reject unknown groups, so adding an adapter cannot
silently publish into Silver, Gold or serving layers.

This package must not import Congress-specific endpoints or fallbacks, DuckDB,
Parquet/Iceberg publication, serving tables, scheduler code or quality/promotion
policy. Those belong to `cpl-data-foundry` and `civic-factory-platform`.

All buffered responses are capped; large datasets use atomic streamed downloads.
Format validation scans JSON, delimited text, gzip and XML incrementally. New
connectors must expose explicit time/byte/row limits, resumable state where discovery
can be long, provenance-preserving native records and tests in this repository.
