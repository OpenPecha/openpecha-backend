# Lesson 03 — Complete API Reference

## Learning Objectives

- Navigate all endpoint groups
- Know the request/response shape for each operation
- Understand authentication and scoping headers
- Know which endpoints paginate and how
- Understand which operations are idempotent and which are not

---

## 1. Authentication

All endpoints (except `/__/health`) require an API key.

```
Header: X-Api-Key: <your_api_key>
```

The key is hashed and compared against stored `api_key_hash` values in Neo4j. An invalid or inactive key returns `401 Unauthorized`.

**Optional header:**
```
Header: X-Application: <application_id>
```

When provided, responses filter application-scoped data (tags, categories) to that application.

---

## 2. Pagination

Paginated endpoints accept `limit` and `offset` query parameters.

| Parameter | Default | Max |
|---|---|---|
| `limit` | 20 | — |
| `offset` | 0 | — |

Response envelope:

```json
{
    "items": [...],
    "limit": 20,
    "offset": 0,
    "has_more": true
}
```

`has_more` is computed by fetching `limit + 1` rows and checking if the extra row exists.

**Paginated endpoints:** `GET /v2/texts`, `GET /v2/persons`, `GET /v2/editions/{id}/segments/related`, `GET /v2/segments/{id}/related`

---

## 3. Texts (`/v2/texts`)

### 3.1 List Texts

```
GET /v2/texts
```

| Query Param | Type | Description |
|---|---|---|
| `limit` | int | Page size (default 20) |
| `offset` | int | Skip (default 0) |
| `language` | string | Filter by language code (e.g. `bo`) |
| `title` | string | Substring match on any language title |
| `category_id` | string | Filter by category ID |
| `author_id` | string | Filter by contributor person ID |
| `tag_id` | string | Filter by tag ID |
| `bdrc` | string | Exact match on BDRC ID |
| `wiki` | string | Exact match on Wikidata ID |

**Response:** `PaginatedResponse[TextOutput]`

---

### 3.2 Get Single Text

```
GET /v2/texts/{text_id}
```

**Response:**

```json
{
    "id": "T_001",
    "title": {"bo": "བདེ་གཤེགས།", "en": "Sugata"},
    "alt_titles": [{"en": "The Fortunate One"}],
    "language": "bo",
    "category_id": "CAT_001",
    "license": "public",
    "bdrc": "W12345",
    "wiki": "Q98765",
    "date": "c.12th century",
    "commentary_of": null,
    "translation_of": null,
    "contributions": [
        {
            "person_id": "P_001",
            "role": "author",
            "person_name": {"bo": "མི་ལ་རས་པ།", "en": "Milarepa"}
        }
    ],
    "commentaries": ["T_003", "T_004"],
    "translations": ["T_002"],
    "editions": ["E_001", "E_002"],
    "tag_ids": ["TAG_001"]
}
```

---

### 3.3 Create Text

```
POST /v2/texts
Content-Type: application/json

{
    "title": {"bo": "བདེ་གཤེགས།", "en": "Sugata"},
    "language": "bo",
    "category_id": "CAT_001",
    "license": "public",
    "contributions": [
        {"person_id": "P_001", "role": "author"}
    ]
}
```

**Standalone text** (no translation/commentary relations). Response: `{"id": "T_001"}`

**Translation:**

```json
{
    "title": {"en": "The Fortunate One"},
    "language": "en",
    "translation_of": "T_001",
    "category_id": "CAT_001",
    "license": "cc-by",
    "contributions": [
        {"person_id": "P_002", "role": "translator"}
    ]
}
```

**Commentary:**

```json
{
    "title": {"bo": "འགྲེལ་བ།"},
    "language": "bo",
    "commentary_of": "T_001",
    "category_id": "CAT_001",
    "license": "public",
    "contributions": []
}
```

---

### 3.4 Update Text (Partial)

```
PATCH /v2/texts/{text_id}
Content-Type: application/json

{
    "title": {"bo": "གསར་མཚན་བྱང།"},
    "license": "cc0"
}
```

Only the provided fields are updated. Response: full `TextOutput`.

---

### 3.5 Manage Tags on a Text

```
POST   /v2/texts/{text_id}/tags/{tag_id}   → 204
DELETE /v2/texts/{text_id}/tags/{tag_id}   → 204
```

Tags are stored on the Work node (not the Text node). The code resolves `text_id → work_id` before tagging.

---

### 3.6 List Editions for a Text

```
GET /v2/texts/{text_id}/editions?edition_type=diplomatic
```

| Query Param | Values |
|---|---|
| `edition_type` | `diplomatic`, `critical`, `collated` (optional filter) |

---

### 3.7 Create Edition for a Text

```
POST /v2/texts/{text_id}/editions
Content-Type: application/json

{
    "metadata": {
        "type": "diplomatic",
        "bdrc": "W12345",
        "source": "BDRC scan 2003",
        "colophon": "...",
        "incipit_title": {"bo": "རྒྱལ་བ།"}
    },
    "content": "base text content as a plain string",
    "segmentation": {
        "segments": [
            {"lines": [{"start": 0, "end": 50}]},
            {"lines": [{"start": 50, "end": 100}]}
        ]
    },
    "pagination": {
        "volumes": [
            {
                "pages": [
                    {"reference": "1a", "lines": [{"start": 0, "end": 500}]},
                    {"reference": "1b", "lines": [{"start": 500, "end": 1000}]}
                ]
            }
        ]
    }
}
```

Response: `{"id": "E_001"}`

---

## 4. Editions (`/v2/editions`)

### 4.1 Get Edition Metadata

```
GET /v2/editions/{edition_id}
```

**Response:**

```json
{
    "id": "E_001",
    "text_id": "T_001",
    "type": "diplomatic",
    "bdrc": "W12345",
    "wiki": null,
    "source": "BDRC scan 2003",
    "colophon": "...",
    "incipit_title": {"bo": "རྒྱལ་བ།"},
    "alt_incipit_titles": null
}
```

---

### 4.2 Get Base Text Content

```
GET /v2/editions/{edition_id}/content
GET /v2/editions/{edition_id}/content?span_start=0&span_end=500
```

Returns the raw text string from S3. `span_start`/`span_end` slices the string (Python slice `[start:end]`).

---

### 4.3 Patch Edition Content (Text Operations)

```
PATCH /v2/editions/{edition_id}/content
Content-Type: application/json
```

Three operation types (discriminated by `type` field):

**Insert:**
```json
{"type": "insert", "position": 100, "text": "inserted text "}
```

**Delete:**
```json
{"type": "delete", "start": 100, "end": 200}
```

**Replace:**
```json
{"type": "replace", "start": 100, "end": 200, "text": "replacement"}
```

Response: `204 No Content`

**Side effect:** All span-based annotations (segments, pages, bib metadata, notes, attributes) are automatically adjusted. See Lesson 06 for full span adjustment rules.

---

### 4.4 Delete Edition

```
DELETE /v2/editions/{edition_id}
```

Cascades: deletes all segmentations, alignments, pagination, bibliographic metadata, notes — and the S3 base text file. Response: `204 No Content`.

---

### 4.5 Annotation Sub-Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/v2/editions/{id}/segmentations` | All display segmentations |
| `POST` | `/v2/editions/{id}/segmentations` | Add a display segmentation |
| `GET` | `/v2/editions/{id}/alignments` | All alignments |
| `POST` | `/v2/editions/{id}/alignments` | Add an alignment |
| `GET` | `/v2/editions/{id}/pagination` | The single pagination (or null) |
| `POST` | `/v2/editions/{id}/pagination` | Add pagination |
| `GET` | `/v2/editions/{id}/bibliographic` | All bibliographic metadata |
| `POST` | `/v2/editions/{id}/bibliographic` | Add bibliographic metadata |
| `GET` | `/v2/editions/{id}/durchens` | All durchen (note) annotations |
| `POST` | `/v2/editions/{id}/durchens` | Add a durchen annotation |

---

### 4.6 Related Editions

```
GET /v2/editions/{edition_id}/related
```

Finds editions related via:
1. **Segment alignment** — editions whose segments are linked via `ALIGNED_TO`
2. **Text relationships** — other editions of texts connected by `TRANSLATION_OF` or `COMMENTARY_OF`

---

### 4.7 Related Segments

```
GET /v2/editions/{edition_id}/segments/related?span_start=0&span_end=100&limit=20&offset=0
```

Required params: `span_start`, `span_end`. Optional: `limit` (default 20), `offset` (default 0).

Traverses the alignment graph (up to depth 5) and returns display segments from related editions that overlap the given span. Response is a flat `PaginatedResponse[SegmentOutput]`:

```json
{
    "items": [
        {
            "id": "SEG_010",
            "segmentation_id": "SGN_001",
            "edition_id": "E_002",
            "text_id": "T_002",
            "lines": [{"start": 0, "end": 50}],
            "tag_ids": null
        }
    ],
    "has_more": false,
    "offset": 0,
    "limit": 20
}
```

Each `SegmentOutput` carries `edition_id`, `text_id`, and `segmentation_id` so clients can group by edition/segmentation themselves if needed.

---

## 5. Segmentations (`/v2/segmentations`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/v2/segmentations/{segmentation_id}` | Get a specific segmentation |
| `DELETE` | `/v2/segmentations/{segmentation_id}` | Delete (fails with error if part of alignment — use alignment delete instead) |

> Note: There is no `PUT` endpoint. To replace a segmentation, use the `update()` method directly in code (delete + re-create in one transaction) or call DELETE then POST.

---

## 6. Alignments (`/v2/alignments`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/v2/alignments/{alignment_id}` | Get a specific alignment |
| `DELETE` | `/v2/alignments/{alignment_id}` | Delete alignment + both segmentations |

**Alignment creation request** (via `POST /v2/editions/{id}/alignments`):

```json
{
    "target_edition_id": "E_002",
    "target_segments": [
        {"lines": [{"start": 0, "end": 30}]},
        {"lines": [{"start": 30, "end": 60}]}
    ],
    "aligned_segments": [
        {"lines": [{"start": 0, "end": 25}], "target_indices": [0]},
        {"lines": [{"start": 25, "end": 55}], "target_indices": [0, 1]}
    ]
}
```

`target_indices` is a list of zero-based indexes into `target_segments`. A source segment can map to multiple targets (and vice versa).

---

## 7. Paginations (`/v2/paginations`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/v2/paginations/{pagination_id}` | Get a specific pagination |
| `DELETE` | `/v2/paginations/{pagination_id}` | Delete pagination |

**Pagination creation request:**

```json
{
    "volumes": [
        {
            "pages": [
                {"reference": "1a", "lines": [{"start": 0, "end": 500}]},
                {"reference": "1b", "lines": [{"start": 500, "end": 1000}]}
            ]
        }
    ]
}
```

Rules:
- Single volume: `index` must be `null`
- Multiple volumes: each must have a `index` (1-based, contiguous)
- Pages must be continuous: each page's start = previous page's end

---

## 8. Segments (`/v2/segments`)

There is no `GET /v2/segments` list endpoint and no `GET /v2/segments/{id}` direct fetch. Segments are retrieved through their parent segmentation or via the related/content sub-endpoints.

| Method | Path | Description |
|---|---|---|
| `GET` | `/v2/segments/{segment_id}/content` | Get the raw text for this segment (fetches from S3, slices by span) |
| `GET` | `/v2/segments/{segment_id}/related` | Related segments across aligned editions (transitive traversal). Returns `PaginatedResponse[SegmentOutput]`. |
| `POST` | `/v2/segments/{segment_id}/tags/{tag_id}` | Tag a segment |
| `DELETE` | `/v2/segments/{segment_id}/tags/{tag_id}` | Untag a segment |
| `GET` | `/v2/segments/search?query=...` | Semantic/full-text search via external search API |

**Query params for `/related`:** `limit` (default 20), `offset` (default 0)

**Query params for `/search`:**

| Parameter | Default | Description |
|---|---|---|
| `query` | required | Search query text |
| `search_type` | `"semantic"` | Search algorithm |
| `limit` | 10 | Max results (max 100) |
| `return_text` | `true` | Include text content |
| `title` | null | Filter by title |

---

## 9. Persons (`/v2/persons`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/v2/persons` | Paginated list with optional filters |
| `GET` | `/v2/persons/{person_id}` | Get single person |
| `POST` | `/v2/persons` | Create person |
| `PATCH` | `/v2/persons/{person_id}` | Partial update |

**Create request:**

```json
{
    "name": {"bo": "མི་ལ་རས་པ།", "en": "Milarepa"},
    "alt_names": [{"bo": "མི་ལ་རས་ཆེན།"}],
    "bdrc": "P155",
    "wiki": "Q76648"
}
```

**Query params for list:** `limit`, `offset`, `name` (substring), `bdrc`, `wiki`

---

## 10. Categories (`/v2/categories`)

**Required header on all requests:** `X-Application: <application_id>`

| Method | Path | Description |
|---|---|---|
| `GET` | `/v2/categories` | All categories for the application (optional `parent_id` filter) |
| `GET` | `/v2/categories/{category_id}` | Get a single category by ID |
| `POST` | `/v2/categories` | Create a category |

**Query params for list:**

| Param | Description |
|---|---|
| `parent_id` | Optional — filter to children of this category ID |

**Create request:**

```json
{
    "title": {"en": "Poetry", "bo": "སྙན་ངག"},
    "description": {"en": "Tibetan poetry"},
    "parent_id": "CAT_001"
}
```

Response: `{"id": "CAT_001"}`

**Response (GET list):**

```json
[
    {
        "id": "CAT_001",
        "title": {"en": "Literature"},
        "description": null,
        "parent_id": null,
        "children": ["CAT_002", "CAT_003"]
    }
]
```

---

## 11. Tags (`/v2/tags`)

**Required header for POST:** `X-Application: <application_id>`

| Method | Path | Description |
|---|---|---|
| `GET` | `/v2/tags` | All tags |
| `POST` | `/v2/tags` | Create a tag |
| `GET` | `/v2/tags/{tag_id}` | Get single tag |

---

## 12. Languages (`/v2/languages`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/v2/languages` | All languages |
| `POST` | `/v2/languages` | Create a language |

Language `code` is a BCP-47 base (e.g. `bo`, `en`, `sa`). Text nodes carry the full `bcp47` tag (e.g. `bo-x-ewts`) on the `HAS_LANGUAGE` relationship.

---

## 13. Applications (`/v2/applications`) — Admin

| Method | Path | Description |
|---|---|---|
| `GET` | `/v2/applications` | List all applications |
| `POST` | `/v2/applications` | Create application |

---

## 14. Error Reference

| Status | Description |
|---|---|
| `400` | Bad request / validation failure |
| `401` | Missing or invalid API key |
| `404` | Entity not found |
| `409` | Conflict (duplicate unique field) |
| `422` | Pydantic validation error |
| `500` | Internal server error |
| `501` | Not implemented |

---

## Next Lesson

→ [04_annotation_system.md](04_annotation_system.md) — All annotation types in depth
