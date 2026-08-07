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
    assert violations == []
