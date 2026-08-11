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

On 2026-08-08 the package-owned suite contains 51 tests, including migrated INE and
Eurostat regressions plus provider-neutral HTTP and streaming storage contracts.
Coverage is measured over the complete distribution and its migration floor is 65%
with branches (66% measured). Ruff, strict mypy, package build, clean-wheel smoke and
the foundry consumer suite form the release gate.
