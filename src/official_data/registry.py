"""Registry/factory for built-in and entry-point official data sources."""

from __future__ import annotations

from collections.abc import Callable
from importlib import metadata
from threading import RLock
from typing import Any, cast

from official_data.protocols import OfficialSourceConnector

ConnectorFactory = Callable[..., OfficialSourceConnector]
BUILTIN_SOURCES = frozenset({"ine", "eurostat"})


class SourceRegistry:
    def __init__(self) -> None:
        self._factories: dict[str, ConnectorFactory] = {}
        self._instances: dict[str, OfficialSourceConnector] = {}
        self._lock = RLock()

    def register_factory(
        self, name: str, factory: ConnectorFactory, *, builtin: bool = False
    ) -> None:
        key = self._key(name)
        with self._lock:
            if key in self._factories or key in self._instances:
                raise ValueError(f"Official source already registered: {key}")
            if key in BUILTIN_SOURCES and not builtin:
                raise ValueError(f"Official source name is reserved: {key}")
            self._factories[key] = factory

    def register_instance(self, name: str, connector: OfficialSourceConnector) -> None:
        key = self._key(name)
        with self._lock:
            if key in BUILTIN_SOURCES:
                raise ValueError(f"Official source name is reserved: {key}")
            if key in self._factories or key in self._instances:
                raise ValueError(f"Official source already registered: {key}")
            self._instances[key] = connector

    def create(self, name: str, **kwargs: Any) -> OfficialSourceConnector:
        key = self._key(name)
        with self._lock:
            instance = self._instances.get(key)
            factory = self._factories.get(key)
        if instance is not None:
            return instance
        if factory is None:
            raise LookupError(
                f"Unknown official source {key!r}. Register it or install an "
                "official_data.sources entry-point plugin."
            )
        return factory(**kwargs)

    def discover_entry_points(self) -> tuple[str, ...]:
        loaded: list[str] = []
        for entry_point in metadata.entry_points(group="official_data.sources"):
            key = self._key(entry_point.name)
            if key in BUILTIN_SOURCES:
                continue
            loaded_object = entry_point.load()
            self.register_factory(key, cast(ConnectorFactory, loaded_object))
            loaded.append(key)
        return tuple(sorted(loaded))

    def names(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(set(self._factories) | set(self._instances)))

    @staticmethod
    def _key(name: str) -> str:
        key = name.strip().casefold()
        if not key:
            raise ValueError("Official source name cannot be empty")
        return key


default_registry = SourceRegistry()
