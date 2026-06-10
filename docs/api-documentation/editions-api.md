# Editions API Documentation

Editions are concrete versions of a text. The metadata lives in Neo4j and the base text content is stored separately through the storage layer. An edition is always created under a text.

## Concepts

- **Diplomatic edition**: A transcription of a source. Requires `metadata.type = "diplomatic"`, a `bdrc` value, and a `pagination` annotation at creation.
- **Critical edition**: A scholarly edition. Must not include `bdrc`, and requires a `segmentation` annotation at creation.
- **Collated edition**: Accepted by the metadata enum, but the create request does not enforce initial pagination or segmentation for this type.
- **Content operations**: Insert, delete, and replace operations update stored base text and adjust stored spans for annotations on the edition.
- **Related editions**: Discovered from alignment and text relationship data.

## Authentication

Use `X-API-Key` in deployed environments.

```text
X-API-Key: your_api_key
```

## Get Edition Metadata

```http
GET /v2/editions/{edition_id}
```

Returns metadata for one edition.

```json
{
  "id": "ED123",
  "text_id": "TXT123",
  "type": "diplomatic",
  "source": "Derge Kangyur",
  "bdrc": "W22084",
  "wiki": null,
  "colophon": "Colophon text",
  "incipit_title": {
    "bo": "འདི་སྐད་བདག་གིས།"
  },
  "alt_incipit_titles": null
}
```

`404` means the edition does not exist.

## Get Edition Content

```http
GET /v2/editions/{edition_id}/content
```

Returns the stored base text as a JSON string. If both `span_start` and `span_end` are supplied, the returned string is sliced with Python-style half-open indexing.

| Query | Type | Required | Description |
|-------|------|----------|-------------|
| `span_start` | integer | No | Start character offset, inclusive |
| `span_end` | integer | No | End character offset, exclusive |

```bash
curl "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/ED123/content?span_start=0&span_end=100" \
  -H "X-API-Key: your_api_key"
```

## Patch Edition Content

```http
PATCH /v2/editions/{edition_id}/content
```

Applies one text operation and returns `204 No Content`. The request body is the operation object itself.

Insert:

```json
{
  "type": "insert",
  "position": 10,
  "text": "inserted text"
}
```

Delete:

```json
{
  "type": "delete",
  "start": 10,
  "end": 20
}
```

Replace:

```json
{
  "type": "replace",
  "start": 10,
  "end": 20,
  "text": "replacement text"
}
```

Validation rules:

- `start`, `end`, and `position` are character offsets.
- `start` must be less than `end`.
- Insert and replace text must be non-empty.
- Extra fields are rejected.

The database span adjustment is performed before the storage write. If the storage write fails, the code compensates the span adjustment before re-raising.

## Delete Edition

```http
DELETE /v2/editions/{edition_id}
```

Deletes the edition metadata and associated annotation data handled by the database layer. Successful deletion returns `204 No Content`.

Delete behavior:

- Deletes the `Edition` node and its incipit title `Nomen` and `LocalizedText` subgraphs.
- Cascade-deletes segmentations, alignments, pagination, table of contents, bibliographic metadata, durchen notes, spans, segments, pages, volumes, and table of contents sections associated with the edition, including annotations added after edition creation.
- Deletes the edition's `HAS_SOURCE` relationship, but preserves the `Source` node.
- Does not delete the parent `Text`, underlying `Work`, categories, tags, contributors, or lookup/type nodes.
- Does not delete stored base text or other non-database side effects.

Error responses:

- `404 Not Found`: Edition does not exist.
- `401 Unauthorized`: Missing or invalid API key in deployed environments.

## List Editions for a Text

```http
GET /v2/texts/{text_id}/editions
```

Optional query:

| Query | Type | Description |
|-------|------|-------------|
| `edition_type` | `diplomatic`, `critical`, or `collated` | Filters returned editions by type |

```bash
curl "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/TXT123/editions?edition_type=diplomatic" \
  -H "X-API-Key: your_api_key"
```

## Create Edition

```http
POST /v2/texts/{text_id}/editions
```

Creates an edition, stores its content, creates the required initial annotation, and schedules search segmentation in the background.

Diplomatic request:

```json
{
  "content": "Full diplomatic text content",
  "metadata": {
    "type": "diplomatic",
    "bdrc": "W22084",
    "source": "Derge Kangyur"
  },
  "pagination": {
    "volumes": [
      {
        "pages": [
          {
            "reference": "1a",
            "lines": [
              {"start": 0, "end": 100}
            ]
          }
        ]
      }
    ]
  }
}
```

Critical request:

```json
{
  "content": "Full critical text content",
  "metadata": {
    "type": "critical",
    "source": "OpenPecha critical edition",
    "incipit_title": {
      "bo": "འདི་སྐད་བདག་གིས།"
    }
  },
  "segmentation": {
    "segments": [
      {
        "lines": [
          {"start": 0, "end": 50}
        ]
      },
      {
        "lines": [
          {"start": 50, "end": 100}
        ]
      }
    ]
  }
}
```

Response:

```json
{
  "id": "ED123"
}
```

Important validation rules:

- `content` is required and must be non-empty.
- Diplomatic editions require `metadata.bdrc` and `pagination`; they must not include `segmentation`.
- Critical editions must not include `metadata.bdrc`; they require `segmentation` and must not include `pagination`.
- `alt_incipit_titles` can only be set when `incipit_title` is set.
- Pagination pages and segment lines must be sorted and continuous.

## Edition Annotation Collections

Annotations can be listed and created by type under an edition. Creation returns `{ "id": "..." }`.
Segmentation and alignment collection `GET` endpoints return parent annotation resources. Large segment collections are paginated from the annotation-specific `/segments` endpoints.

### Segmentations

```http
GET /v2/editions/{edition_id}/segmentations
POST /v2/editions/{edition_id}/segmentations
```

GET response:

```json
[
  {
    "id": "SGN123",
    "edition_id": "ED123",
    "text_id": "TXT123"
  }
]
```

Use `GET /v2/segmentations/{segmentation_id}/segments?limit=500&offset=0` to fetch paginated segment rows for a segmentation.

Request:

```json
{
  "segments": [
    {
      "lines": [
        {"start": 0, "end": 50}
      ]
    }
  ]
}
```

### Alignments

```http
GET /v2/editions/{edition_id}/alignments
POST /v2/editions/{edition_id}/alignments
```

GET response:

```json
[
  {
    "id": "ALN123",
    "aligned_edition_id": "ED_ALIGNED",
    "aligned_text_id": "TXT_ALIGNED",
    "target_edition_id": "ED_TARGET",
    "target_text_id": "TXT_TARGET",
    "target_segmentation_id": "SGN_TARGET"
  }
]
```

Use `GET /v2/alignments/{alignment_id}/segments?limit=500&offset=0` to fetch paginated aligned segment rows.

The path `edition_id` is the aligned edition when creating an alignment. `target_edition_id` is the edition being aligned to.

```json
{
  "target_edition_id": "ED_TARGET",
  "target_segments": [
    {
      "lines": [
        {"start": 0, "end": 40}
      ]
    }
  ],
  "aligned_segments": [
    {
      "lines": [
        {"start": 0, "end": 35}
      ],
      "target_indices": [0]
    }
  ]
}
```

`target_indices` are zero-based indexes into `target_segments`.

### Pagination

```http
GET /v2/editions/{edition_id}/pagination
POST /v2/editions/{edition_id}/pagination
```

```json
{
  "volumes": [
    {
      "pages": [
        {
          "reference": "1a",
          "lines": [
            {"start": 0, "end": 500}
          ]
        }
      ]
    }
  ]
}
```

A single-volume pagination must omit `index`. Multi-volume pagination must use unique continuous indexes starting at `1`.

### Table of contents

```http
GET /v2/editions/{edition_id}/table-of-contents
POST /v2/editions/{edition_id}/table-of-contents
```

GET response:

```json
[
  {
    "id": "OUT123",
    "edition_id": "ED123",
    "text_id": "TXT123",
    "metadata": {
      "name": "Main sa bcad"
    },
    "sections": [
      {
        "id": "SEC123",
        "title": {
          "bo": "ལེའུ་དང་པོ།",
          "en": "Chapter 1"
        },
        "summary": {
          "en": "Opening topic"
        },
        "span": {"start": 0, "end": 1200},
        "subsections": []
      }
    ]
  }
]
```

Request:

```json
{
  "metadata": {
    "name": "Main sa bcad"
  },
  "sections": [
    {
      "title": {
        "bo": "ལེའུ་དང་པོ།",
        "en": "Chapter 1"
      },
      "summary": {
        "en": "Opening topic"
      },
      "span": {"start": 0, "end": 1200},
      "subsections": [
        {
          "title": {
            "en": "Section 1.1"
          },
          "span": {"start": 0, "end": 350}
        }
      ]
    }
  ]
}
```

Each section has a required localized `title`, optional localized `summary`, required `span`, and optional recursive `subsections`. Each subsection span must be fully contained inside its parent section span. Multiple table of contents can be attached to the same edition. Returned sections and subsections are ordered by their span start/end positions.

### Bibliographic Metadata

```http
GET /v2/editions/{edition_id}/bibliographic
POST /v2/editions/{edition_id}/bibliographic
```

```json
{
  "span": {"start": 5000, "end": 5500},
  "type": "colophon",
  "metadata": {}
}
```

Supported types: `colophon`, `incipit`, `alt_incipit`, `alt_title`, `person`, `title`, and `author`.

### Durchen Notes

```http
GET /v2/editions/{edition_id}/durchens
POST /v2/editions/{edition_id}/durchens
```

```json
{
  "span": {"start": 100, "end": 150},
  "text": "Variant reading from witness B",
  "metadata": {}
}
```

## Related Editions

```http
GET /v2/editions/{edition_id}/related
```

Returns editions related through alignment or text relationships.

## Developer Notes

- Router: `routers/editions.py`.
- Edition creation route: `routers/texts.py`.
- Models: `models/edition.py`, `models/annotation.py`, `models/content_operation.py`, and `models/requests.py`.
- Storage dependency handles base text reads/writes; database dependencies handle metadata, annotations, span adjustment, and related lookups.
