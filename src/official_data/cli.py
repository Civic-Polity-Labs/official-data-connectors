"""CLI for official statistical connectors."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from official_data.client import OfficialDataClient
from official_data.models import OfficialExtractionPlan
from official_data.registry import default_registry

app = typer.Typer(no_args_is_help=True, help="INE, Eurostat and plugin official data connectors")


@app.command("catalog")
def catalog_command(
    source: Annotated[str, typer.Option("--source")],
    output_root: Annotated[Path, typer.Option("--output-root")] = Path("data/lake"),
    limit: Annotated[int | None, typer.Option(min=1)] = None,
) -> None:
    client = OfficialDataClient(source, output_root=output_root)
    for index, record in enumerate(client.catalog(), start=1):
        typer.echo(json.dumps(record.model_dump(mode="json"), ensure_ascii=False))
        if limit is not None and index >= limit:
            break


@app.command("extract")
def extract_command(
    source: Annotated[str, typer.Option("--source")],
    output_root: Annotated[Path, typer.Option("--output-root")],
    dataset: Annotated[list[str] | None, typer.Option("--dataset")] = None,
    operation: Annotated[list[str] | None, typer.Option("--operation")] = None,
    run_date: Annotated[str | None, typer.Option("--run-date")] = None,
    latest_periods: Annotated[int | None, typer.Option("--latest-periods", min=1)] = None,
    max_datasets: Annotated[int | None, typer.Option("--max-datasets", min=1)] = None,
    resume: Annotated[bool, typer.Option("--resume/--no-resume")] = True,
) -> None:
    values: dict[str, object] = {
        "dataset_ids": tuple(dataset or ()),
        "operation_ids": tuple(operation or ()),
        "output_root": output_root,
        "latest_periods": latest_periods,
        "max_datasets": max_datasets,
        "resume": resume,
    }
    if run_date:
        values["run_date"] = run_date
    client = OfficialDataClient(source, output_root=output_root)
    for manifest in client.extract(OfficialExtractionPlan.model_validate(values)):
        typer.echo(json.dumps(manifest.model_dump(mode="json"), ensure_ascii=False))


@app.command("sources")
def sources_command(discover_plugins: bool = True) -> None:
    if discover_plugins:
        default_registry.discover_entry_points()
    for name in default_registry.names():
        typer.echo(name)


if __name__ == "__main__":
    app()
