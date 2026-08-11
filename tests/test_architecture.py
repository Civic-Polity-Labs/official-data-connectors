from __future__ import annotations

import ast
from pathlib import Path


def test_namespace_is_independent_and_has_no_publication_imports() -> None:
    root = Path(__file__).parents[1] / "src" / "official_data"
    forbidden = {"congreso_open_data", "cpl_data_foundry", "materialize", "gold", "serving"}
    violations: list[str] = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.module
                and set(node.module.split(".")) & forbidden
            ):
                violations.append(f"{path.name}:{node.lineno}:{node.module}")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if set(alias.name.split(".")) & forbidden:
                        violations.append(f"{path.name}:{node.lineno}:{alias.name}")
    assert violations == []


def test_provider_neutral_package_does_not_define_congress_transport() -> None:
    root = Path(__file__).parents[1] / "src" / "official_data"
    definitions = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in {
                "CongresoHttpClient",
                "official_diary_pdf_fallback_url",
            }:
                definitions.append(f"{path.name}:{node.lineno}:{node.name}")
    assert definitions == []


def test_normalization_contract_does_not_expose_foundry_table_vocabulary() -> None:
    root = Path(__file__).parents[1] / "src" / "official_data"
    violations = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and any(
                token in node.name.casefold() for token in ("silver", "gold", "materialize")
            ):
                violations.append(f"{path.name}:{node.lineno}:{node.name}")
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and node.value.casefold().startswith(("silver_", "gold_", "serving_"))
            ):
                violations.append(f"{path.name}:{node.lineno}:{node.value}")
    assert violations == []
