# Migration lineage

This clean-history repository separates INE and Eurostat connectors from
`Civic-Polity-Labs/cpl-data-foundry` at commit
`1ddde1079604fe1a9088e1fbb29244288a2d4c22`. The 2026-08-07 working-tree bytes are
recorded in the repository artifact `MIGRATION_SOURCE_INVENTORY.sha256`.

- `official_data.ine` originates in `src/congreso_open_data/extractors/ine.py`.
- `official_data.eurostat` originates in `src/congreso_open_data/extractors/eurostat.py`.
- Bounded HTTP, Bronze persistence and normalization helpers originate in the source
  modules with the same base names.
- Public models, registry, `OfficialDataClient`, common `Observation`, exporters and
  CLI are new in the package split.

Lakehouse publication, data-quality orchestration and serving remain in
`cpl-data-foundry` and are not part of this distribution.

## Local package gate

On 2026-08-07: 9 tests passed; Ruff and public-contract mypy passed; measured contract
coverage was 81.93%; wheel and sdist passed `twine check`; the wheel imported and its
CLI ran beside `congreso-open-data` 1.0.0 in a clean environment.
