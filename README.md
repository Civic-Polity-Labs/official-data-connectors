# official-data-connectors

Extensible Python connectors for official statistical sources. Version 1.0 ships INE
and Eurostat adapters, provider-native records and a lossless common `Observation`
view. Acquisition is synchronous, bounded and resumable; Bronze artifacts retain URL,
request parameters, SHA-256 and adapter provenance.

```bash
pip install official-data-connectors
official-data catalog --source ine
official-data catalog --source eurostat
```

```python
from official_data import OfficialDataClient, OfficialExtractionPlan

client = OfficialDataClient(source="eurostat", output_root="bronze")
plan = OfficialExtractionPlan(dataset_ids=("nama_10_gdp",), output_root="bronze")
manifests = client.extract(plan)
for observation in client.observations(manifests):
    print(observation.period, observation.value, observation.dimensions)
```

## Español

Los registros nativos de INE y Eurostat se conservan. `Observation` es una vista común,
no una sustitución con pérdida. El paquete solo descarga, preserva evidencia, valida y
normaliza; no publica Silver/Gold ni tablas de serving.

## English

Provider-native INE and Eurostat records are retained. `Observation` is a common view,
not a lossy replacement. This package only acquires, preserves, validates and
normalizes official data.

Licensed under MIT.
