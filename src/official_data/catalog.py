"""Provider-neutral acquisition resource."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DatasetResource:
    family: str
    dataset: str
    format: str
    url: str
    snapshot_token: str | None = None
    legislature: str | None = None
    session: str | None = None
    vote_number: str | None = None
    post_data: dict[str, Any] | None = None
