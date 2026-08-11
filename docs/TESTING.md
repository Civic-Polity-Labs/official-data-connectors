# Testing and release gate

```powershell
uv sync --locked --extra dev --extra parquet
uv run --no-sync ruff check .
uv run --no-sync mypy
uv run --no-sync pytest --cov=official_data --cov-branch `
  --cov-report=term-missing --cov-fail-under=65
uv build
uv run --no-sync twine check dist/*
```

Coverage is measured over the whole `official_data` package. The 65% migration floor
is an honest full-distribution gate (66% at the 2026-08-08 audit), not the old curated
three-module measurement.

Tests must cover provider discovery/normalization, GET and streamed acquisition,
timeouts/retries/closing, byte limits, hashes, atomic resume, corruption and JSON,
CSV/TSV/gzip, XML, HTML, ZIP and unknown binary contracts. Fixtures stay offline;
bounded live samples are separate change-detection audits. The final integration
gate installs this package beside `congreso-open-data` and runs `cpl-data-foundry`.
