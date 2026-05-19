# Annotations API Documentation

Annotations attach structured information to edition content using character spans. They are created under editions and can also be fetched or deleted directly by annotation ID.

## Concepts

- **Span**: A half-open character range where `start` is inclusive and `end` is exclusive.
- **Lines**: Continuous spans inside a segment or page. Adjacent lines must be sorted and touch each other.
- **Segmentation**: A set of logical content segments for an edition.
- **Alignment**: A mapping between segments in an aligned edition and target segments in another edition.
- **Pagination**: A mapping from character spans to page or folio references.
- **Outline**: A table-of-contents style hierarchy for an edition. Each section has a character span, localized title, optional localized summary, and optional nested subsections.
- **Bibliographic metadata**: Span-level metadata such as colophon, title, incipit, or author.
- **Durchen note**: A span-level critical apparatus note.

## Authentication

Use `X-API-Key` in deployed environments.

```text
X-API-Key: your_api_key
```

## Create and List Annotations on an Edition

Annotations are created and listed by type under an edition. Each `POST` returns `{ "id": "..." }`.

| Type | List | Create |
|------|------|--------|
| Segmentation | `GET /v2/editions/{edition_id}/segmentations` | `POST /v2/editions/{edition_id}/segmentations` |
| Alignment | `GET /v2/editions/{edition_id}/alignments` | `POST /v2/editions/{edition_id}/alignments` |
| Pagination | `GET /v2/editions/{edition_id}/pagination` | `POST /v2/editions/{edition_id}/pagination` |
| Outline | `GET /v2/editions/{edition_id}/outlines` | `POST /v2/editions/{edition_id}/outlines` |
| Bibliographic metadata | `GET /v2/editions/{edition_id}/bibliographic` | `POST /v2/editions/{edition_id}/bibliographic` |
| Durchen notes | `GET /v2/editions/{edition_id}/durchens` | `POST /v2/editions/{edition_id}/durchens` |


### Create Segmentation

```json
{
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
```

Segments must be sorted by their first line's start offset. Lines inside each segment must be continuous.

### Create Alignment

```json
{
  "target_edition_id": "ED_TARGET",
  "target_segments": [
    {
      "lines": [
        {"start": 0, "end": 60}
      ]
    }
  ],
  "aligned_segments": [
    {
      "lines": [
        {"start": 0, "end": 55}
      ],
      "target_indices": [0]
    }
  ]
}
```

The path `edition_id` is the aligned edition. `target_indices` are zero-based indexes into the submitted `target_segments` array.

### Create Pagination

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
  ],
  "metadata": {}
}
```

Single-volume pagination omits `index`. Multi-volume pagination requires unique continuous indexes starting at `1`.

### Create Outline

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
          "summary": {
            "en": "Introductory topic"
          },
          "span": {"start": 0, "end": 350},
          "subsections": []
        }
      ]
    }
  ]
}
```

Each outline can contain one or more root `sections`. Sections can be nested with `subsections` to represent a table of contents or Tibetan `sa bcad` hierarchy. Each section and subsection has the same shape:

- `title`: localized string, required.
- `summary`: localized string, optional.
- `span`: half-open character range for the section.
- `subsections`: nested child sections, optional and defaults to an empty list.

Each subsection span must be fully contained inside its parent section span.
An edition can have multiple outlines. Outline sections are managed as part of the outline annotation; they are not standalone API resources.
Returned sections and subsections are ordered by their span start/end positions.

### Create Bibliographic Metadata

```json
{
  "span": {"start": 5000, "end": 5500},
  "type": "colophon",
  "metadata": {}
}
```

Supported types:

- `colophon`
- `incipit`
- `alt_incipit`
- `alt_title`
- `person`
- `title`
- `author`

### Create Durchen Note

```json
{
  "span": {"start": 100, "end": 150},
  "text": "Variant reading found in manuscript B",
  "metadata": {}
}
```

## Fetch and Delete by ID

### Segmentation

```http
GET /v2/segmentations/{segmentation_id}
GET /v2/segmentations/{segmentation_id}/segments?limit=500&offset=0
DELETE /v2/segmentations/{segmentation_id}
```

`GET /v2/segmentations/{segmentation_id}` response:

```json
{
  "id": "SGN123",
  "edition_id": "ED123",
  "text_id": "TXT123"
}
```

`GET /v2/segmentations/{segmentation_id}/segments` query:

| Query | Type | Required | Default |
|-------|------|----------|---------|
| `limit` | integer, 1-500 | No | 500 |
| `offset` | integer, >= 0 | No | 0 |

Segments response:

```json
{
  "items": [
    {
      "id": "SEG123",
      "lines": [
        {"start": 0, "end": 50}
      ]
    }
  ],
  "has_more": false,
  "offset": 0,
  "limit": 500
}
```

Deleting a standalone segmentation returns `204 No Content`. If the segmentation belongs to an alignment, deletion is rejected for both aligned and target segmentations; delete the alignment instead using the aligned segmentation ID, which is the alignment `id` returned by the API.

### Alignment

```http
GET /v2/alignments/{alignment_id}
GET /v2/alignments/{alignment_id}/segments?limit=500&offset=0
DELETE /v2/alignments/{alignment_id}
```

`GET /v2/alignments/{alignment_id}` response:

```json
{
  "id": "ALN123",
  "aligned_edition_id": "ED_ALIGNED",
  "aligned_text_id": "TXT_ALIGNED",
  "target_edition_id": "ED_TARGET",
  "target_text_id": "TXT_TARGET",
  "target_segmentation_id": "SGN_TARGET"
}
```

`GET /v2/alignments/{alignment_id}/segments` query:

| Query | Type | Required | Default |
|-------|------|----------|---------|
| `limit` | integer, 1-500 | No | 500 |
| `offset` | integer, >= 0 | No | 0 |

Segments response:

```json
{
  "items": [
    {
      "aligned_segment": {
        "id": "SEG_SOURCE",
        "lines": [
          {"start": 0, "end": 55}
        ]
      },
      "target_segments": [
        {
          "id": "SEG_TARGET",
          "segmentation_id": "SGN_TARGET",
          "edition_id": "ED_TARGET",
          "text_id": "TXT_TARGET",
          "lines": [
            {"start": 0, "end": 60}
          ],
          "tag_ids": null
        }
      ]
    }
  ],
  "has_more": false,
  "offset": 0,
  "limit": 500
}
```

Deleting an alignment deletes the alignment and its associated aligned/target segmentations.

### Pagination

```http
GET /v2/paginations/{pagination_id}
DELETE /v2/paginations/{pagination_id}
```

Response:

```json
{
  "id": "PAG123",
  "edition_id": "ED123",
  "text_id": "TXT123",
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
  ],
  "metadata": {}
}
```

### Outline

```http
GET /v2/outlines/{outline_id}
DELETE /v2/outlines/{outline_id}
```

Response:

```json
{
  "id": "OUT123",
  "edition_id": "ED123",
  "text_id": "TXT123",
  "metadata": {
    "name": "Main sa bcad"
  },
  "sections": [
    {
      "id": "SEC_ROOT",
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
          "id": "SEC_CHILD",
          "title": {
            "en": "Section 1.1"
          },
          "summary": {
            "en": "Introductory topic"
          },
          "span": {"start": 0, "end": 350},
          "subsections": []
        }
      ]
    }
  ]
}
```

Deleting an outline deletes its outline sections, section spans, title/summary localized text subgraphs, and metadata.

### Bibliographic Metadata

```http
GET /v2/bibliographic/{bibliographic_id}
DELETE /v2/bibliographic/{bibliographic_id}
```

Response:

```json
{
  "id": "BIB123",
  "edition_id": "ED123",
  "text_id": "TXT123",
  "span": {"start": 5000, "end": 5500},
  "type": "colophon",
  "metadata": {}
}
```

### Durchen Note

```http
GET /v2/durchens/{durchen_id}
DELETE /v2/durchens/{durchen_id}
```

Response:

```json
{
  "id": "DUR123",
  "edition_id": "ED123",
  "text_id": "TXT123",
  "span": {"start": 100, "end": 150},
  "text": "Variant reading found in manuscript B",
  "metadata": {}
}
```

## Example Calls

```bash
curl "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/ED123/segmentations" \
  -H "X-API-Key: your_api_key"

curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/ED123/durchens" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "span": {"start": 100, "end": 150},
    "text": "Variant reading found in manuscript B"
  }'

curl -X DELETE "https://api-l25bgmwqoa-uc.a.run.app/v2/durchens/DUR123" \
  -H "X-API-Key: your_api_key"

curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/ED123/outlines" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {"name": "Main sa bcad"},
    "sections": [
      {
        "title": {"en": "Chapter 1"},
        "span": {"start": 0, "end": 1200}
      }
    ]
  }'
```

## Developer Notes

- Collection routes live in `routers/editions.py`.
- Direct-by-ID routes live in `routers/annotation/`.
- Models live in `models/annotation.py`.
- Content changes through `PATCH /v2/editions/{edition_id}/content` adjust affected spans automatically.
