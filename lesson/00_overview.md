# Lesson 00 — System Overview & Architecture

## Learning Objectives

By the end of this lesson you should be able to:

- Describe the full technology stack
- Explain why each technology was chosen
- Draw the high-level data flow from HTTP request to database to response
- Name the three storage concerns and what lives in each

---

## 1. What is OpenPecha API?

OpenPecha API v2 is a backend for managing **Tibetan literary texts** and their scholarly annotations. It supports:

- Multiple editions of the same underlying work (diplomatic, critical, collated)
- Rich multilingual metadata (names/titles in Tibetan, English, etc.)
- Scholarly annotations: segmentations, alignments, paginations, bibliographic markers, notes
- Content editing with automatic span adjustment
- Cross-edition alignment for translations and commentaries

---

## 2. Technology Stack


| Layer          | Technology           | Why                                                    |
| -------------- | -------------------- | ------------------------------------------------------ |
| HTTP framework | **FastAPI** (Python) | Async, OpenAPI auto-docs, Pydantic validation          |
| Graph database | **Neo4j**            | Text/annotation data is fundamentally a property graph |
| Object storage | **AWS S3**           | Large plaintext content lives outside the graph        |
| Observability  | OpenTelemetry        | Tracing + metrics                                      |
| Runtime        | Gunicorn + Uvicorn   | Production async ASGI                                  |


---

## 3. High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         HTTP Clients                             │
│                   (web apps, mobile, scripts)                    │
└─────────────────────────────┬────────────────────────────────────┘
                              │  HTTPS + API Key header
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                        FastAPI App                               │
│                                                                  │
│  Middleware: Auth ─ CORS ─ Request Logger ─ Error Handlers       │
│                                                                  │
│  Routers                                                         │
│  ┌─────────┐ ┌──────────┐ ┌────────────┐ ┌──────────────────┐    │
│  │ /texts  │ │/editions │ │/segments   │ │ /segmentations   │    │
│  │ /persons│ │/languages│ │/categories │ │ /alignments      │    │
│  │ /tags   │ │ /apps    │ │            │ │ /paginations     │    │
│  └────┬────┘ └────┬─────┘ └─────┬──────┘ │ /bibliographic   │    │
│       │           │             │        │ /durchens        │    │
│       │           │             │        └────────┬─────────┘    │
└───────┼───────────┼─────────────┼─────────────────┼──────────────┘
        │           │             │                 │
        ▼           ▼             ▼                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                       Database Layer                             │
│            (TextDatabase, EditionDatabase, ...)                  │
│            Uses: neo4j Python async driver                       │
└──────────────────────────────────┬───────────────────────────────┘
                  ┌────────────────┴──────────────────┐
                  ▼                                   ▼
   ┌──────────────────────────┐         ┌──────────────────────────┐
   │        Neo4j             │         │         AWS S3           │
   │  (graph: texts, editions,│         │  (blob: base text        │
   │  annotations, persons,   │         │   content files)         │
   │  categories, segments,   │         │                          │
   │  spans, nomen, ...)      │         │  Key pattern:            │
   │                          │         │  texts/{text_id}/        │
   │  All metadata +          │         │    editions/{edition_id} │
   │  structural relations    │         │                          │
   └──────────────────────────┘         └──────────────────────────┘
```

### Why the split?

- **Neo4j** holds all *structural* and *relational* data: how texts relate to each other, which segments cover which character positions, what persons contributed to what text.
- **S3** holds the raw **base text** (plaintext, potentially megabytes). Storing large strings in a graph database node wastes index space and slows traversal.

---

## 4. Request Lifecycle

```
1. Client → HTTPS request with X-Api-Key header
2. FastAPI dependency: get_api_key() validates key against Neo4j ApiKey node
3. Router handler called with Database + Storage injected
4. Database layer executes Cypher queries against Neo4j
       (OR)
   Storage layer reads/writes base text on S3
5. Response serialised via Pydantic model → JSON
6. Middleware adds no-cache headers, logs status code
```

---

## 5. Code Layout

```
openpecha-backend/
├── main.py               ← App factory, lifespan, middleware, error handlers
├── config.py             ← Settings from environment variables
├── dependencies.py       ← FastAPI Depends: get_db, get_storage, get_api_key
├── identifier.py         ← ID generation
├── exceptions.py         ← Custom error classes (OpenPechaError hierarchy)
├── observability.py      ← OpenTelemetry setup
├── background_tasks.py   ← Async background jobs (search indexer trigger)
│
├── routers/              ← HTTP layer (one file per domain)
│   ├── texts.py
│   ├── editions.py
│   ├── segments.py
│   ├── persons.py
│   ├── tags.py
│   ├── categories.py
│   ├── languages.py
│   ├── applications.py
│   └── annotation/       ← Annotation-specific routers
│       ├── segmentations.py
│       ├── alignments.py
│       ├── paginations.py
│       ├── bibliographic.py
│       └── durchens.py
│
├── models/               ← Pydantic models (request/response shapes)
│   ├── text.py
│   ├── edition.py
│   ├── annotation.py     ← Segment, Segmentation, Alignment, Pagination, ...
│   ├── enums.py
│   ├── base.py
│   ├── requests.py
│   ├── responses.py
│   └── content_operation.py
│
├── database/             ← Neo4j query layer
│   ├── database.py       ← Database class (owns all sub-databases)
│   ├── text_database.py
│   ├── edition_database.py
│   ├── segment_database.py
│   ├── span_database.py  ← Span adjustment algorithms
│   ├── nomen_database.py
│   ├── data_adapter.py   ← Neo4j record → Pydantic model
│   ├── neo4j_schema.yaml ← Authoritative graph schema
│   └── annotation/
│       ├── segmentation_database.py
│       ├── alignment_database.py
│       ├── pagination_database.py
│       ├── bibliographic_database.py
│       └── note_database.py
│
└── storage/
    └── s3.py             ← S3 base text CRUD
```

---

## 6. Key Design Decisions

### 6.1 Async everywhere

All database calls use `async/await` with the Neo4j async driver. FastAPI and Uvicorn are fully async. This allows high concurrency without blocking threads on I/O.

```python
# Pattern seen throughout the database layer:
async with self.session as session:
    return await session.execute_read(read)
```

### 6.2 Layered validation

Validation happens at two levels:

1. **Pydantic** (model layer) — shape, types, inter-field constraints
2. **DatabaseValidator** (query layer) — existence checks, uniqueness constraints

```python
# Example: creating a text triggers Pydantic + DB validation
await DatabaseValidator.validate_text_title_unique(tx, text.title.root)
await DatabaseValidator.validate_language_code_exists(tx, base_lang_code)
await DatabaseValidator.validate_category_exists(tx, text.category_id)
```

### 6.3 Transactional write operations

Related writes are batched into a single transaction to ensure atomicity. For example, creating an edition can also create its initial segmentation and pagination in one write:

```python
async def transaction_function(tx):
    if text:
        await TextDatabase.create_with_transaction(tx, text, text_id)
    await self.create_with_transaction(tx, edition, text_id, edition_id)
    if segmentation:
        await SegmentationDatabase.add_with_transaction(tx, edition_id, segmentation)
    if pagination:
        await PaginationDatabase.add_with_transaction(tx, edition_id, pagination)

await session.execute_write(transaction_function)
```

---

## 7. Quick Glossary


| Term             | Meaning                                                                                           |
| ---------------- | ------------------------------------------------------------------------------------------------- |
| **Work**         | The abstract intellectual entity (the "thing being talked about")                                 |
| **Text**         | A specific version/language of a Work (root, translation, commentary)                             |
| **Edition**      | A concrete realisation of a Text (diplomatic, critical, collated)                                 |
| **Nomen**        | A multilingual name/title node — maps language codes to strings                                   |
| **Segment**      | A character-range slice of an edition's base text                                                 |
| **Segmentation** | A group of non-overlapping, contiguous segments covering an edition                               |
| **Alignment**    | A mapping between segments of two different editions                                              |
| **Span**         | A `{start, end}` character-position node pointing to a Segment, Page, BibMeta, Note, or Attribute |
| **BDRC**         | Buddhist Digital Resource Center — external identifier system                                     |


---

## Next Lesson

→ [01_graph_data_model.md](01_graph_data_model.md) — The full Neo4j node/relationship schema