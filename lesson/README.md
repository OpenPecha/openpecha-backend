# OpenPecha Backend — Lesson Materials

Engineer-level curriculum for the OpenPecha API v2 backend.
Source code: `openpecha-backend/` | Live docs: `http://13.250.189.160/redoc`

---

## Curriculum Map

| # | File | Topics | Est. Time |
|---|---|---|---|
| 00 | [00_overview.md](00_overview.md) | Stack, architecture, code layout, request lifecycle | 30 min |
| 01 | [01_graph_data_model.md](01_graph_data_model.md) | All Neo4j nodes, relationships, enum nodes, Nomen pattern | 60 min |
| 02 | [02_domain_concepts.md](02_domain_concepts.md) | Work/Text/Edition hierarchy, Pydantic models, validation rules | 45 min |
| 03 | [03_api_reference.md](03_api_reference.md) | All endpoints, request/response shapes, pagination, auth | 60 min |
| 04 | [04_annotation_system.md](04_annotation_system.md) | Segmentation, Alignment, Pagination, BibMeta, Notes | 60 min |
| 05 | [05_flows.md](05_flows.md) | 8 end-to-end operational flows with full traces | 60 min |
| 06 | [06_content_operations.md](06_content_operations.md) | Span adjustment algorithms: INSERT/DELETE/REPLACE | 60 min |
| 07 | [07_alignment_deep_dive.md](07_alignment_deep_dive.md) | Alignment model, bidirectional lookup, transitive traversal | 45 min |
| 08 | [08_exercises.md](08_exercises.md) | 10 hands-on lab exercises with full `curl` commands | 3–4 hrs |

---

## Prerequisites

- Python async/await basics
- Neo4j and Cypher fundamentals (MATCH, CREATE, MERGE, WITH, UNWIND)
- REST API concepts (HTTP methods, status codes, JSON)
- Basic understanding of graph databases (nodes, relationships, properties)

---

## Key Architecture Decisions

```
FastAPI (async) → Database layer (Neo4j async driver) → Neo4j
                → Storage layer (boto3 async)          → AWS S3
```

- **Graph (Neo4j):** All structure and metadata — texts, editions, persons, annotations, segments, spans
- **Object store (S3):** Base text content (the actual Tibetan/English strings)
- **Why split?** Large text strings bloat the graph and slow traversal; structural queries are the graph's strength

---

## The Three Architecture Changes You've Worked On

### 1. Annotation Endpoint Restructuring
Old: `POST /v2/annotations/{instance_id}/annotation` with a `type` field  
New: Type-specific endpoints under `/v2/editions/{edition_id}/segmentations|alignments|pagination|bibliographic|durchens`

See: [03_api_reference.md](03_api_reference.md#5-segmentations), [04_annotation_system.md](04_annotation_system.md)

### 2. Related Segments: Transitive Traversal
Old: One-hop alignment lookup  
New: Multi-hop transitive traversal (default depth 5), still returning flat `PaginatedResponse[SegmentOutput]`. Each item carries `edition_id`, `text_id`, and `segmentation_id` for client-side grouping. The `RelatedSegmentsOutput` model exists in `models/annotation.py` but is not yet returned by any router.

See: [07_alignment_deep_dive.md](07_alignment_deep_dive.md#7-related-segments-transitive-traversal)

### 3. Segmentation Subtypes (`:Display`, `:Aligned`, `:Target`)
Old: All segmentations were equivalent  
New: Explicit Neo4j labels distinguish user-facing display segmentations from internal alignment segmentations

See: [01_graph_data_model.md](01_graph_data_model.md#25-segmentation--segment-nodes), [04_annotation_system.md](04_annotation_system.md#3-segmentation)

---

## Quick Reference: Finding Things in Code

| Question | Where to look |
|---|---|
| What nodes and relationships exist? | `database/neo4j_schema.yaml` |
| How is a request body shaped? | `models/` directory |
| How is an endpoint defined? | `routers/` directory |
| How is a Cypher query executed? | `database/` directory |
| How does span adjustment work? | `database/span_database.py` |
| How is base text stored/retrieved? | `storage/s3.py` |
| How is an API key validated? | `dependencies.py` → `database/api_key_database.py` |
| What happens at startup/shutdown? | `main.py` → `lifespan()` |
| What is the database class structure? | `database/database.py` → `Database` class |
