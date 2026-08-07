from __future__ import annotations

import pytest

from official_data.registry import SourceRegistry


class CustomConnector:
    name = "custom"
    version = "1"

    def catalog(self):
        return iter(())

    def extract(self, plan):
        return iter(())

    def native_observations(self, manifests):
        return iter(())

    def observations(self, manifests):
        return iter(())


def test_custom_source_injection_and_collisions() -> None:
    registry = SourceRegistry()
    connector = CustomConnector()
    registry.register_instance("custom", connector)

    assert registry.create("custom") is connector
    assert registry.names() == ("custom",)
    with pytest.raises(ValueError, match="already registered"):
        registry.register_instance("custom", connector)
    with pytest.raises(ValueError, match="reserved"):
        registry.register_instance("ine", connector)
    with pytest.raises(LookupError, match="Unknown official source"):
        registry.create("absent")


def test_custom_source_factory_and_empty_name_validation() -> None:
    registry = SourceRegistry()
    registry.register_factory("factory", lambda **kwargs: CustomConnector())

    assert isinstance(registry.create("factory", ignored=True), CustomConnector)
    with pytest.raises(ValueError, match="cannot be empty"):
        registry.create(" ")
