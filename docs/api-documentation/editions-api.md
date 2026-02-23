# Editions API Documentation

This document provides comprehensive documentation for all Editions-related endpoints in the OpenPecha API v2.

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Edition Endpoints](#edition-endpoints)
   - [Get Edition Metadata](#get-edition-metadata)
   - [Get Edition Content](#get-edition-content)
   - [Modify Edition Content](#modify-edition-content)
   - [Delete Edition](#delete-edition)
4. [Edition Relationships](#edition-relationships)
   - [Get Related Editions](#get-related-editions)
   - [Get Related Segments by Span](#get-related-segments-by-span)
5. [Edition Management](#edition-management)
   - [List Editions for Text](#list-editions-for-text)
   - [Create New Edition](#create-new-edition)
6. [Annotations](#annotations)
   - [Get All Annotations](#get-all-annotations)
   - [Add Annotation](#add-annotation)
7. [Data Models](#data-models)
8. [Error Responses](#error-responses)

---

## Overview

Editions (also known as Manifestations) represent specific physical or digital instantiations of a text. The Editions API provides endpoints for managing edition metadata, content, annotations, and relationships between editions.

### Edition Types

- **Diplomatic**: Represents a faithful transcription of a specific manuscript or print. Requires BDRC identifier.
- **Critical**: Represents a scholarly edition that may incorporate multiple sources. BDRC identifier is forbidden.
- **Collated**: Represents a collation of multiple sources.

### Base URL

```
Development: https://api-l25bgmwqoa-uc.a.run.app
Production: https://api-aq25662yyq-uc.a.run.app
Test: https://api-kwgjscy6gq-uc.a.run.app
Local: http://127.0.0.1:5001/pecha-backend-test-3a4d0/us-central1/api
```

---

## Authentication

All API requests require authentication using an API key.

**Header:**
```
X-API-Key: your_api_key_here
```

---

## Edition Endpoints

### Get Edition Metadata

Retrieve metadata for a specific edition.

**Endpoint:**
```
GET /v2/editions/{edition_id}/metadata
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `edition_id` | string | path | Yes | The ID of the edition |

**Response: 200 OK**

```json
{
  "id": "I12345678",
  "text_id": "E12345678",
  "type": "critical",
  "source": "source-name",
  "bdrc": null,
  "wiki": "Q123456",
  "colophon": "colophon text",
  "incipit_title": {
    "en": "English incipit title",
    "bo": "Tibetan incipit title"
  },
  "alt_incipit_titles": [
    {
      "en": "Alt title 1"
    }
  ]
}
```

**Error Responses:**
- `404 Not Found`: Edition does not exist
- `500 Server Error`: Internal server error

---

### Get Edition Content

Retrieve the base text content for an edition. Optionally specify a character span to retrieve a portion of the content.

**Endpoint:**
```
GET /v2/editions/{edition_id}/content
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `edition_id` | string | path | Yes | The ID of the edition |
| `span_start` | integer | query | No | Start character position (inclusive) |
| `span_end` | integer | query | No | End character position (exclusive) |

**Notes:**
- Both `span_start` and `span_end` must be provided together if retrieving a portion
- If span parameters are omitted, returns the full content

**Response: 200 OK (Full Content)**

```json
"This is the complete base text content of the edition."
```

**Response: 200 OK (Span Content)**

```json
"portion of the text"
```

**Error Responses:**
- `400 Bad Request`: Invalid span parameters
- `404 Not Found`: Edition does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Get full content
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/content" \
  -H "X-API-Key: your_api_key"

# Get content for specific span
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/content?span_start=0&span_end=100" \
  -H "X-API-Key: your_api_key"
```

---

### Modify Edition Content

Apply a text operation (INSERT, DELETE, or REPLACE) to the edition's content. This operation updates the base text in storage and automatically adjusts all affected spans on the same manifestation.

**Endpoint:**
```
PATCH /v2/editions/{edition_id}/content
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `edition_id` | string | path | Yes | The ID of the edition |

**Request Body:**

The request body must contain a text operation object. See [Text Operations Specification](./text-operations-spec.md) for detailed information on span adjustment behavior.

#### Operation Types

**INSERT Operation**

```json
{
  "type": "insert",
  "position": 15,
  "text": "new text"
}
```

**DELETE Operation**

```json
{
  "type": "delete",
  "start": 10,
  "end": 20
}
```

**REPLACE Operation**

```json
{
  "type": "replace",
  "start": 10,
  "end": 20,
  "text": "replacement text"
}
```

**Response: 200 OK**

```json
{
  "message": "Operation applied successfully"
}
```

**Error Responses:**
- `400 Bad Request`: Invalid operation parameters
- `404 Not Found`: Edition does not exist
- `422 Validation Error`: Operation validation failed
- `500 Server Error`: Internal server error

**Span Adjustment:**

When content is modified, all annotations with spans on the same manifestation are automatically adjusted:

- **Segmentation segments**: Adjusted based on continuous span rules
- **Pagination pages**: Adjusted based on continuous span rules
- **Bibliography metadata**: Adjusted based on annotation span rules
- **Durchen notes**: Adjusted based on annotation span rules
- **Alignments**: Source segments are adjusted if they belong to the modified manifestation

See [Text Operations Specification](./text-operations-spec.md) for detailed span adjustment algorithms.

**Example Usage:**

```bash
# Insert text
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/content" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "insert",
    "position": 15,
    "text": "new text"
  }'

# Delete text
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/content" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "delete",
    "start": 10,
    "end": 20
  }'

# Replace text
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/content" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "replace",
    "start": 10,
    "end": 20,
    "text": "replacement text"
  }'
```

---

### Delete Edition

Delete an edition (manifestation) and all its associated data including annotations (segmentation, pagination, bibliography, durchen notes, alignments) and stored content.

**Endpoint:**
```
DELETE /v2/editions/{edition_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `edition_id` | string | path | Yes | The ID of the edition to delete |

**Response: 204 No Content**

Successful deletion returns no content.

**Error Responses:**
- `404 Not Found`: Edition does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X DELETE "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678" \
  -H "X-API-Key: your_api_key"
```

---

## Edition Relationships

### Get Related Editions

Find all manifestations that are related to the given edition through alignment relationships.

**Endpoint:**
```
GET /v2/editions/{edition_id}/related
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `edition_id` | string | path | Yes | The ID of the edition |

**Response: 200 OK**

```json
[
  {
    "id": "I87654321",
    "text_id": "E12345678",
    "type": "critical",
    "source": "source-name",
    "bdrc": null,
    "wiki": null,
    "colophon": "colophon text",
    "incipit_title": {
      "en": "English incipit"
    },
    "alt_incipit_titles": null
  },
  {
    "id": "I87654322",
    "text_id": "E12345679",
    "type": "diplomatic",
    "source": "another-source",
    "bdrc": "W123456",
    "wiki": null,
    "colophon": null,
    "incipit_title": null,
    "alt_incipit_titles": null
  }
]
```

**Error Responses:**
- `404 Not Found`: Edition does not exist
- `500 Server Error`: Internal server error

---

### Get Related Segments by Span

Find segments that overlap with a given character span in the edition, then return all related (aligned) segments from other manifestations.

**Endpoint:**
```
GET /v2/editions/{edition_id}/segments/related
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `edition_id` | string | path | Yes | The ID of the edition |
| `span_start` | integer | query | Yes | Start character position (inclusive) |
| `span_end` | integer | query | Yes | End character position (exclusive) |

**Response: 200 OK**

```json
[
  {
    "id": "SEG001",
    "manifestation_id": "M12345678",
    "text_id": "E12345678",
    "lines": [
      {
        "start": 0,
        "end": 100
      }
    ]
  },
  {
    "id": "SEG002",
    "manifestation_id": "M87654321",
    "text_id": "E87654321",
    "lines": [
      {
        "start": 50,
        "end": 150
      }
    ]
  }
]
```

**Empty Result:**

```json
[]
```

**Error Responses:**
- `400 Bad Request`: Invalid span parameters
- `404 Not Found`: Edition does not exist
- `422 Validation Error`: Validation failed
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/segments/related?span_start=0&span_end=100" \
  -H "X-API-Key: your_api_key"
```

---

## Edition Management

### List Editions for Text

Retrieve all editions associated with a text. Optionally filter by edition type (manifestation type).

**Endpoint:**
```
GET /v2/texts/{text_id}/editions
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `text_id` | string | path | Yes | The ID of the text |
| `edition_type` | string | query | No | Filter by type: `diplomatic`, `critical`, or `all` (default) |

**Response: 200 OK**

```json
[
  {
    "id": "I12345678",
    "text_id": "E12345678",
    "type": "critical",
    "source": "source-name",
    "bdrc": null,
    "wiki": "Q123456",
    "colophon": "colophon text",
    "incipit_title": {
      "en": "English incipit title",
      "bo": "Tibetan incipit title"
    },
    "alt_incipit_titles": [
      {
        "en": "Alt title 1"
      }
    ]
  }
]
```

**Error Responses:**
- `400 Bad Request`: Invalid parameters
- `404 Not Found`: Text does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Get all editions
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/E12345678/editions" \
  -H "X-API-Key: your_api_key"

# Get only diplomatic editions
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/E12345678/editions?edition_type=diplomatic" \
  -H "X-API-Key: your_api_key"

# Get only critical editions
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/E12345678/editions?edition_type=critical" \
  -H "X-API-Key: your_api_key"
```

---

### Create New Edition

Create a new edition with metadata and content for a specific text.

**Endpoint:**
```
POST /v2/texts/{text_id}/editions
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `text_id` | string | path | Yes | The ID of the text |

**Request Body:**

```json
{
  "metadata": {
    "type": "diplomatic | critical",
    "source": "string (optional)",
    "bdrc": "string (required for diplomatic)",
    "wiki": "string (optional)",
    "colophon": "string (optional)",
    "incipit_title": {
      "en": "Opening words",
      "bo": "དབུ་ཚིག"
    },
    "alt_incipit_titles": [
      {
        "en": "Alternative title"
      }
    ]
  },
  "annotation": [...],
  "bibliography_annotation": [...],
  "content": "string (required)"
}
```

#### Metadata Requirements

**For Diplomatic Editions:**
- `type`: Must be `"diplomatic"`
- `bdrc`: **Required** - BDRC identifier for the source
- `source`: Optional source identifier

**For Critical Editions:**
- `type`: Must be `"critical"`
- `bdrc`: Should be `null` or omitted
- `source`: Optional source identifier

#### Annotation Types

**1. Segmentation Annotation (for Critical Editions)**

Array of segment objects with character spans:

```json
"annotation": [
  {
    "span": {
      "start": 0,
      "end": 10
    }
  },
  {
    "span": {
      "start": 10,
      "end": 20
    }
  }
]
```

**2. Pagination Annotation (for Diplomatic Editions)**

Array of page objects with spans and references:

```json
"annotation": [
  {
    "span": {
      "start": 0,
      "end": 10
    },
    "reference": "https://example.com/image1.png"
  },
  {
    "span": {
      "start": 11,
      "end": 20
    },
    "reference": "https://example.com/image2.png"
  }
]
```

**3. Bibliography Annotation**

Can be combined with either segmentation or pagination:

```json
"bibliography_annotation": [
  {
    "span": {
      "start": 5,
      "end": 15
    },
    "type": "colophon"
  },
  {
    "span": {
      "start": 20,
      "end": 30
    },
    "type": "title"
  }
]
```

**Bibliography Types:**
- `colophon`: Colophon text
- `title`: Title text
- `incipit_title`: Opening words/incipit
- `author`: Author attribution

#### Example Requests

**Critical Edition with Segmentation:**

```json
{
  "metadata": {
    "type": "critical",
    "source": "source-name",
    "colophon": "Sample colophon text",
    "incipit_title": {
      "en": "Opening words",
      "bo": "དབུ་ཚིག"
    }
  },
  "annotation": [
    {
      "span": {
        "start": 0,
        "end": 10
      }
    },
    {
      "span": {
        "start": 10,
        "end": 20
      }
    }
  ],
  "content": "This is the text content to be stored"
}
```

**Diplomatic Edition with Pagination:**

```json
{
  "metadata": {
    "type": "diplomatic",
    "source": "source-name",
    "bdrc": "W123456",
    "colophon": "Sample colophon text",
    "incipit_title": {
      "en": "Opening words",
      "bo": "དབུ་ཚིག"
    }
  },
  "annotation": [
    {
      "span": {
        "start": 0,
        "end": 10
      },
      "reference": "ref-001"
    },
    {
      "span": {
        "start": 10,
        "end": 20
      },
      "reference": "ref-002"
    }
  ],
  "content": "This is the text content to be stored"
}
```

**Edition with Bibliography Annotation:**

```json
{
  "metadata": {
    "type": "critical",
    "source": "source-name",
    "colophon": "Sample colophon text",
    "incipit_title": {
      "en": "Opening words",
      "bo": "དབུ་ཚིག"
    }
  },
  "annotation": [
    {
      "span": {
        "start": 0,
        "end": 10
      }
    },
    {
      "span": {
        "start": 10,
        "end": 20
      }
    }
  ],
  "bibliography_annotation": [
    {
      "span": {
        "start": 5,
        "end": 15
      },
      "type": "colophon"
    },
    {
      "span": {
        "start": 20,
        "end": 30
      },
      "type": "title"
    }
  ],
  "content": "This is the text content to be stored"
}
```

**Response: 201 Created**

```json
{
  "message": "Instance created successfully",
  "id": "ABC12345678"
}
```

**Error Responses:**
- `400 Bad Request`: Invalid request parameters
- `422 Validation Error`: Validation failed
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/E12345678/editions" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {
      "type": "critical",
      "source": "My Edition",
      "colophon": "Completed by...",
      "incipit_title": {
        "en": "Beginning of the text"
      }
    },
    "content": "Full text content here..."
  }'
```

---

## Annotations

### Get All Annotations

Retrieve all annotations for a specific edition. Filter by annotation type(s) using the `type` query parameter.

**Endpoint:**
```
GET /v2/editions/{edition_id}/annotations
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `edition_id` | string | path | Yes | The ID of the edition |
| `type` | array[string] | query | No | Filter by annotation type(s) |

**Annotation Types:**
- `segmentation`: Text segmentation
- `alignment`: Cross-edition alignments
- `pagination`: Page/folio references
- `bibliography`: Bibliographic metadata (colophon, title, author)
- `durchen`: Critical apparatus notes

**Note:** The `type` parameter can be repeated to filter multiple types:
```
?type=segmentation&type=pagination
```

**Response: 200 OK (All Annotations)**

```json
{
  "segmentations": [
    {
      "id": "seg_abc123",
      "segments": [
        {
          "id": "segment_001",
          "manifestation_id": "M12345678",
          "text_id": "E12345678",
          "lines": [
            {
              "start": 0,
              "end": 50
            }
          ]
        }
      ]
    }
  ],
  "pagination": {
    "id": "pag_abc123",
    "volume": {
      "index": 1,
      "pages": [
        {
          "reference": "folio_1a",
          "lines": [
            {
              "start": 0,
              "end": 500
            }
          ]
        }
      ]
    }
  },
  "bibliographic_metadata": [
    {
      "id": "bib_abc123",
      "span": {
        "start": 5000,
        "end": 5500
      },
      "type": "colophon"
    }
  ],
  "durchen_notes": [
    {
      "id": "note_abc123",
      "span": {
        "start": 100,
        "end": 150
      },
      "text": "Variant reading in manuscript B"
    }
  ]
}
```

**Error Responses:**
- `404 Not Found`: Edition does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Get all annotations
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations" \
  -H "X-API-Key: your_api_key"

# Get only segmentation annotations
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations?type=segmentation" \
  -H "X-API-Key: your_api_key"

# Get segmentation and pagination annotations
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations?type=segmentation&type=pagination" \
  -H "X-API-Key: your_api_key"
```

---

### Add Annotation

Add an annotation to the specified edition. Exactly one annotation type must be provided per request.

**Endpoint:**
```
POST /v2/editions/{edition_id}/annotations
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `edition_id` | string | path | Yes | The ID of the edition |

**Request Body:**

Exactly one of the following annotation types must be provided:

#### 1. Segmentation Annotation

```json
{
  "segmentation": {
    "segments": [
      {
        "lines": [
          {
            "start": 0,
            "end": 50
          }
        ]
      },
      {
        "lines": [
          {
            "start": 50,
            "end": 100
          }
        ]
      }
    ]
  }
}
```

#### 2. Alignment Annotation

```json
{
  "alignment": {
    "target_id": "M87654321",
    "target_segments": [
      {
        "lines": [
          {
            "start": 0,
            "end": 30
          }
        ]
      },
      {
        "lines": [
          {
            "start": 30,
            "end": 60
          }
        ]
      }
    ],
    "aligned_segments": [
      {
        "lines": [
          {
            "start": 0,
            "end": 25
          }
        ],
        "alignment_indices": [0]
      },
      {
        "lines": [
          {
            "start": 25,
            "end": 50
          }
        ],
        "alignment_indices": [0, 1]
      }
    ]
  }
}
```

**Alignment Explanation:**
- `target_id`: The manifestation being aligned to
- `target_segments`: Segments in the target manifestation
- `aligned_segments`: Segments in the current manifestation (edition_id) with indices indicating which target segments they align to

#### 3. Pagination Annotation

```json
{
  "pagination": {
    "volume": {
      "index": 1,
      "pages": [
        {
          "reference": "folio_1a",
          "lines": [
            {
              "start": 0,
              "end": 500
            }
          ]
        },
        {
          "reference": "folio_1b",
          "lines": [
            {
              "start": 500,
              "end": 1000
            }
          ]
        }
      ]
    }
  }
}
```

#### 4. Bibliographic Metadata

```json
{
  "bibliographic_metadata": [
    {
      "span": {
        "start": 5000,
        "end": 5500
      },
      "type": "colophon"
    },
    {
      "span": {
        "start": 0,
        "end": 100
      },
      "type": "title"
    }
  ]
}
```

**Bibliographic Types:**
- `colophon`: Colophon text
- `title`: Title text
- `incipit`: Incipit/opening words
- `alt_incipit`: Alternative incipit
- `alt_title`: Alternative title
- `person`: Person name mention
- `author`: Author attribution

#### 5. Durchen Notes

```json
{
  "durchen_notes": [
    {
      "span": {
        "start": 100,
        "end": 150
      },
      "text": "Variant reading found in manuscript B: འདི་ནི།"
    }
  ]
}
```

**Response: 201 Created**

```json
{
  "message": "Annotation added successfully"
}
```

**Error Responses:**
- `400 Bad Request`: Invalid request (e.g., multiple annotation types provided)
- `404 Not Found`: Edition does not exist
- `422 Validation Error`: Validation failed
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Add segmentation
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "segmentation": {
      "segments": [
        {
          "lines": [
            {
              "start": 0,
              "end": 50
            }
          ]
        }
      ]
    }
  }'

# Add pagination
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "pagination": {
      "volume": {
        "index": 1,
        "pages": [
          {
            "reference": "folio_1a",
            "lines": [
              {
                "start": 0,
                "end": 500
              }
            ]
          }
        ]
      }
    }
  }'
```

---

## Data Models

### ManifestationOutput

```typescript
{
  id: string;                    // Manifestation ID
  text_id: string;               // Associated text/expression ID
  type: "diplomatic" | "critical" | "collated";
  source: string | null;         // Source identifier
  bdrc: string | null;           // BDRC identifier
  wiki: string | null;           // Wikidata identifier
  colophon: string | null;       // Colophon text
  incipit_title: {               // Localized incipit title
    [language: string]: string;
  } | null;
  alt_incipit_titles: Array<{    // Alternative incipit titles
    [language: string]: string;
  }> | null;
}
```

### SpanModel

```typescript
{
  start: number;   // Start character position (inclusive, >= 0)
  end: number;     // End character position (exclusive, >= 1)
}
```

### SegmentOutput

```typescript
{
  id: string;              // Unique segment identifier
  manifestation_id: string; // ID of the manifestation
  text_id: string;         // ID of the expression (text)
  lines: SpanModel[];      // Character spans (at least 1)
}
```

### TextOperation

```typescript
{
  type: "insert" | "delete" | "replace";
  
  // For INSERT
  position?: number;       // Required for insert (>= 0)
  text?: string;           // Required for insert (non-empty)
  
  // For DELETE
  start?: number;          // Required for delete (>= 0)
  end?: number;            // Required for delete (>= 1, > start)
  
  // For REPLACE
  start?: number;          // Required for replace (>= 0)
  end?: number;            // Required for replace (>= 1, > start)
  text?: string;           // Required for replace (non-empty)
}
```

### SegmentationOutput

```typescript
{
  id: string;                    // Segmentation annotation ID
  segments: SegmentOutput[];     // List of segments
  metadata?: object | null;      // Optional annotation metadata
}
```

### AlignmentOutput

```typescript
{
  id: string;                    // Alignment annotation ID
  target_id: string;             // ID of the target manifestation
  target_segments: SegmentOutput[]; // Segments from target
  aligned_segments: Array<{
    lines: SpanModel[];          // Character spans
    alignment_indices: number[]; // Indices of target segments
  }>;
  metadata?: object | null;      // Optional annotation metadata
}
```

### PaginationOutput

```typescript
{
  id: string;                    // Pagination annotation ID
  volume: {
    index?: number | null;       // Volume index (optional)
    pages: Array<{
      reference: string;         // Page/folio reference
      lines: SpanModel[];        // Character spans (at least 1)
    }>;
    metadata?: object | null;    // Optional volume metadata
  };
  metadata?: object | null;      // Optional annotation metadata
}
```

### BibliographicMetadataOutput

```typescript
{
  id: string;                    // Bibliographic metadata ID
  span: SpanModel;               // Character span
  type: "colophon" | "title" | "incipit" | "alt_incipit" | 
        "alt_title" | "person" | "author";
  metadata?: object | null;      // Optional annotation metadata
}
```

### NoteOutput

```typescript
{
  id: string;                    // Note ID
  span: SpanModel;               // Character span
  text: string;                  // Note content
  metadata?: object | null;      // Optional annotation metadata
}
```

---

## Error Responses

### 400 Bad Request

```json
{
  "error": "There was an error with the request"
}
```

**Common Causes:**
- Invalid span parameters (start >= end, negative values)
- Multiple annotation types provided in single request
- Segmentation is part of alignment and cannot be deleted separately
- Invalid operation parameters for content modification

### 404 Not Found

```json
{
  "error": "Resource was not found"
}
```

**Common Causes:**
- Edition ID does not exist
- Text ID does not exist
- Referenced resources (target manifestation, persons) do not exist

### 422 Validation Error

```json
{
  "error": "Validation error",
  "details": [
    {
      "loc": ["body", "metadata", "type"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

**Common Causes:**
- Missing required fields
- Invalid field types or values
- Diplomatic edition missing BDRC identifier
- Critical edition has BDRC identifier
- Invalid annotation structure

### 500 Server Error

```json
{
  "error": "There was an error on the server"
}
```

---

## Best Practices

### 1. Creating Editions

- Always specify the edition type (`diplomatic` or `critical`)
- For diplomatic editions, include the BDRC identifier
- For critical editions, omit the BDRC identifier
- Provide incipit titles in relevant languages for better discoverability

### 2. Content Modification

- Use atomic operations (INSERT, DELETE, REPLACE) for text changes
- Understand span adjustment behavior before modifying content
- Test operations on development environment first
- See [Text Operations Specification](./text-operations-spec.md) for detailed span behavior

### 3. Annotations

- Add one annotation type per request
- Ensure spans are valid (start < end, within content bounds)
- For alignments, verify both manifestations exist
- Use bibliography annotations to mark structural elements

### 4. Querying

- Filter editions by type when you only need specific types
- Use span-based queries to find related segments efficiently
- Leverage annotation filters to reduce response payload

---

## Workflow Examples

### Creating a Complete Edition with Annotations

1. **Create the text** (if not exists)
```bash
POST /v2/texts
```

2. **Create the edition with content**
```bash
POST /v2/texts/{text_id}/editions
# Include metadata, content, and initial annotations
```

3. **Add additional annotations** (optional)
```bash
POST /v2/editions/{edition_id}/annotations
# Add segmentation, pagination, bibliography, etc.
```

### Finding Related Content

1. **Get related editions**
```bash
GET /v2/editions/{edition_id}/related
```

2. **Find aligned segments for a span**
```bash
GET /v2/editions/{edition_id}/segments/related?span_start=0&span_end=100
```

### Modifying Edition Content

1. **Get current content**
```bash
GET /v2/editions/{edition_id}/content
```

2. **Apply text operation**
```bash
PATCH /v2/editions/{edition_id}/content
# Spans are automatically adjusted
```

3. **Verify annotations**
```bash
GET /v2/editions/{edition_id}/annotations
```

---

## Important Notes

### Removed Endpoints

The following endpoints have been **REMOVED** and are no longer available:
- `POST /v2/editions/{edition_id}/translation`
- `POST /v2/editions/{edition_id}/commentary`

**To create translations or commentaries**, use:
1. `POST /v2/texts` with `translation_of` or `commentary_of` field
2. `POST /v2/texts/{text_id}/editions` to create the edition

### Span Conventions

- All spans use **zero-based indexing**
- Start position is **inclusive**
- End position is **exclusive**
- Valid span: `start < end`
- Span format: `[start, end)`

### Automatic Adjustments

When edition content is modified via `PATCH /v2/editions/{edition_id}/content`, the following are automatically adjusted:
- Segmentation segments
- Pagination pages
- Bibliographic metadata spans
- Durchen note spans
- Alignment segments (if source manifestation)

---

## See Also

- [Text Operations Specification](./text-operations-spec.md) - Detailed span adjustment algorithms
- OpenAPI Specification: `GET /v2/schema/openapi`
