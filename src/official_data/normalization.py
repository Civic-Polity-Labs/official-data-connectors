from __future__ import annotations

import csv
import json
import re
import unicodedata
from datetime import date, datetime
from io import StringIO
from typing import Any

DATE_FORMATS = ("%d/%m/%Y", "%d/%m/%y", "%d/%m/%Y %H:%M")


def normalize_text(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def normalize_key(value: Any) -> str:
    if not isinstance(value, str):
        value = str(value)
    value = unicodedata.normalize("NFKD", value.strip().lower())
    value = "".join(char for char in value if not unicodedata.combining(char))
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", value)).strip("_")


def parse_spanish_date(value: Any) -> date | None:
    if not isinstance(value, str) or not value.strip():
        return None
    value = value.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def stable_id(*parts: Any) -> str:
    import hashlib

    normalized = "|".join(str(normalize_text(part) or "") for part in parts)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def load_records(content: bytes, format_name: str) -> list[dict[str, Any]]:
    if format_name == "json":
        data = json.loads(content.decode("utf-8-sig"))
        if isinstance(data, list):
            return [dict(row) for row in data]
        return [dict(data)]
    if format_name == "csv":
        text = content.decode("utf-8-sig")
        return list(csv.DictReader(StringIO(text), delimiter=";"))
    raise ValueError(f"Unsupported tabular format: {format_name}")


def normalize_record_keys(record: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in record.items():
        clean_key = normalize_key(key)
        clean_value = normalize_text(value)
        for alias in _key_aliases(clean_key):
            normalized.setdefault(alias, clean_value)
    return normalized


def _key_aliases(key: str) -> tuple[str, ...]:
    compact = key.replace("_", "")
    parts = key.split("_")
    compact_without_particles = "".join(
        part for part in parts if part not in {"de", "del", "la", "las", "el", "los"}
    )
    aliases = [key, compact, compact_without_particles]
    return tuple(dict.fromkeys(alias for alias in aliases if alias))
