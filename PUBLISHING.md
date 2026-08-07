# Publishing 1.0.0

No GitHub or PyPI secret is required. This repository publishes with PyPI Trusted
Publishing (OIDC); do not create `PYPI_API_TOKEN`.

Before creating the first tag, register a pending publisher in PyPI with exactly:

- PyPI project: `official-data-connectors`
- Owner: `Civic-Polity-Labs`
- Repository: `official-data-connectors`
- Workflow: `release.yml`
- Environment: `pypi`

The GitHub `pypi` environment already accepts only `v*` tags and requires approval
from `alejandromorislara`. Once the publisher exists, create and push the annotated
tag `v1.0.0`. The workflow builds one wheel/sdist pair, checks it, retains it as an
artifact and publishes it with attestations.

## Español

No hay que añadir secretos a GitHub. Registra primero el publisher OIDC anterior en
PyPI y solo después publica el tag `v1.0.0`.
