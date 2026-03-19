---
name: openpecha-backend-codebase
description: Master guide for the OpenPecha backend codebase — a Tibetan Buddhist digital library API. Use when working on any code in openpecha-backend: writing routes, database queries, models, annotations, tests, or when the user asks about how this project works, its patterns, or conventions.
---

# OpenPecha Backend Master Guide

## What This Project Does

Flask + Firebase Cloud Functions API for a **Tibetan Buddhist digital library**.
Implements the **FRBR bibliographic model**: `Work → Expression → Manifestation`.

- **Texts (Expressions)**: Multilingual Buddhist works with authors/translators, license, and categories
- **Editions (Manifestations)**: Diplomatic, critical, or collated manuscript versions with full text
- **Annotations**: Scholarly markup — segmentation, pagination, alignment (cross-edition linking), bibliographic metadata, durchen (variant reading) notes
- **Segments**: Character-span units that are cross-aligned across editions and searched semantically
- **Persons, Categories, Languages**: Supporting domain entities

## Tech Stack

| Layer | Technology |
|---|---|
| Framework | Flask 3.1.2 (Blueprints) |
| Hosting | Firebase Cloud Functions |
| Database | Neo4j 2025 (Cypher 25 / GQL) |
| Neo4j driver | `neo4j==6.1.0` |
| File Storage | Firebase Storage (GCS) |
| Validation | Pydantic v2 |
| Auth | SHA-256 hashed API keys in Neo4j |
| Linting | Ruff (all rules), target Python 3.13 |
| Type checking | Pyright 1.1.407 |
| Testing | pytest + testcontainers (real Neo4j via Docker) |

## Project Layout

```
functions/
├── main.py                    # Flask app factory + Firebase entry point
├── models.py                  # All Pydantic domain models
├── request_models.py          # Request/response Pydantic wrappers
├── exceptions.py              # Custom HTTP exception hierarchy
├── identifier.py              # 21-char nanoid-style ID generator
├── storage.py                 # Firebase Storage wrapper
├── firebase_config.py         # Firebase Admin SDK init
├── neo4j_constraints.cypher   # DB constraints + indexes
├── neo4j_schema.yaml          # Full graph schema documentation
├── api/
│   ├── auth.py                # API key validation middleware
│   ├── decorators.py          # @validate_json, @validate_query_params, @require_application
│   ├── texts.py               # /v2/texts routes
│   ├── editions.py            # /v2/editions routes
│   ├── annotations.py         # /v2/annotations routes
│   ├── segments.py            # /v2/segments routes
│   ├── persons.py             # /v2/persons routes
│   ├── categories.py          # /v2/categories routes
│   ├── languages.py           # /v2/languages routes
│   ├── applications.py        # /v2/applications routes
│   └── schema/openapi.yaml    # Full OpenAPI 3.x spec
└── database/
    ├── database.py            # Top-level Database class (context manager)
    ├── data_adapter.py        # Neo4j records → Pydantic models
    ├── database_validator.py  # Cross-entity DB validations
    ├── expression_database.py
    ├── manifestation_database.py
    ├── person_database.py
    ├── segment_database.py
    ├── span_database.py       # Span adjustment on text edits
    ├── nomen_database.py      # Localized text nodes
    └── annotation/
        ├── segmentation_database.py
        ├── alignment_database.py
        ├── pagination_database.py
        ├── bibliographic_database.py
        ├── note_database.py
        └── attribute_database.py
```

## Core Conventions

### 1. Pydantic Input/Output/Patch pattern

Every entity has three model variants in `models.py`:
- `*Input` — creation body (required fields)
- `*Output` — API response shape
- `*Patch` — partial update body (all Optional)

### 2. Database as context manager

```python
with Database() as db:
    result = db.expression.get(expression_id)
```

Never hold a long-lived driver connection. Each `Database` instance opens and closes Neo4j cleanly.

### 3. DataAdapter for Neo4j → Pydantic

Raw Neo4j records are **never** returned directly. `database/data_adapter.py` transforms them into Pydantic model instances. Add new transformations there when adding new node types.

### 4. Decorators for validation

```python
@texts_bp.route("/", methods=["POST"])
@validate_json(ExpressionInput)
def create_text(body: ExpressionInput):
    ...

@texts_bp.route("/", methods=["GET"])
@validate_query_params(TextQueryParams)
def list_texts(params: TextQueryParams):
    ...
```

### 5. Error hierarchy (`exceptions.py`)

Always raise these — they're caught globally in `main.py`:

| Exception | HTTP |
|---|---|
| `DataNotFoundError` | 404 |
| `InvalidRequestError` | 400 |
| `DataConflictError` | 409 |
| `DataValidationError` | 422 |
| `UnauthorizedError` | 401 |

### 6. ID generation

All entity IDs are 21-character random alphanumeric strings from `identifier.py`. Never use `uuid`.

### 7. Cypher 25 / Neo4j 2025 rules

- Use `elementId()` — never `id()` (removed in Neo4j 5+)
- Use `EXISTS {}` subqueries for existence checks
- Use Quantified Path Patterns (QPP) for variable-length paths
- No deprecated syntax; Cypher 25 is enforced

### 8. Multilingual text (Nomen pattern)

All display strings (titles, person names) live in the graph:
```
Nomen -[:HAS_LOCALIZATION]-> LocalizedText -[:HAS_LANGUAGE {bcp47}]-> Language
Nomen -[:ALTERNATIVE_OF]-> Nomen
```
Use `nomen_database.py` when reading/writing localized strings.

### 9. Span tracking

When edition content is edited, `span_database.py` auto-adjusts all character spans stored in Neo4j:
- **Continuous spans** (segments, pages): expand at boundaries
- **Annotation spans** (notes, bibliography, attributes): shift at boundaries, deleted if fully encompassed

### 10. Multi-tenancy

Categories are scoped to `Application` nodes. API keys can be bound to an Application. Callers pass `X-Application` header. Use `@require_application` decorator for application-scoped routes.

## Authentication

- Header: `X-API-Key`
- Keys stored SHA-256 hashed in Neo4j `ApiKey` nodes
- Public routes (no auth): `GET /v2/schema/openapi`, `GET /__/health`
- In tests, auth is bypassed when `app.config["TESTING"] = True`

## API Routes Summary

All routes under `/v2/`. Auth required unless noted.

| Module | Prefix | Key endpoints |
|---|---|---|
| texts | `/v2/texts` | CRUD + `GET /<id>/editions`, `POST /<id>/editions` |
| editions | `/v2/editions` | metadata, content PATCH, DELETE, annotations CRUD, related segments |
| annotations | `/v2/annotations` | segmentation, alignment, pagination, durchen, bibliographic (GET + DELETE per type) |
| segments | `/v2/segments` | `GET /<id>/related`, `GET /<id>/content`, `GET /search` |
| persons | `/v2/persons` | CRUD |
| categories | `/v2/categories` | GET + POST (requires `X-Application`) |
| languages | `/v2/languages` | GET + POST |
| applications | `/v2/applications` | POST |

## Running Locally

```bash
# Start Neo4j
docker compose up -d

# Python env
cd functions && python -m venv venv && source venv/bin/activate
pip install -r requirements.txt -r requirements_dev.txt

# Run via Firebase emulator
firebase emulators:start --only functions

# Run tests (Docker required for testcontainers)
pytest
```

## Environment Variables

| Variable | Purpose |
|---|---|
| `NEO4J_URI` | Neo4j bolt URI |
| `NEO4J_USERNAME` | Neo4j username |
| `NEO4J_PASSWORD` | Neo4j password |
| `NEO4J_DATABASE` | Neo4j database name |
| `FUNCTIONS_EMULATOR` | Set `"true"` for local logging |
| `COMMIT_SHA` | Exposed at `/api/version` |

## Neo4j Graph Schema (Key Node Types)

```
Work, Expression, Manifestation       # FRBR bibliographic hierarchy
Segmentation, Segment, Span           # Annotation layer
Pagination, Volume, Page              # Page structure
BibliographicMetadata, Note, Attribute# Scholarly annotations
Nomen, LocalizedText, Language        # i18n
Person, AI, Contribution, RoleType    # Authorship
Category, Application                 # Multi-tenancy / taxonomy
ApiKey, LicenseType, ManifestationType, AnnotationType  # Auth + enums
```

## External Services

| Service | URL | Purpose |
|---|---|---|
| Search segmenter | `https://sqs-search-segmenter-api.onrender.com` | Triggered async after edition create/delete |
| Vector search | `https://openpecha-search.onrender.com` | Proxied via `GET /v2/segments/search` |

## Additional Resources

- For the full graph schema, see [neo4j_schema.yaml](../../functions/neo4j_schema.yaml)
- For the full API spec, see [openapi.yaml](../../functions/api/schema/openapi.yaml)
- For DB constraints, see [neo4j_constraints.cypher](../../functions/neo4j_constraints.cypher)
