# Annotations API Documentation

This document provides comprehensive documentation for all Annotations-related endpoints in the OpenPecha API v2.

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Annotation Types](#annotation-types)
4. [Annotation Endpoints](#annotation-endpoints)
   - [Segmentation Annotations](#segmentation-annotations)
   - [Alignment Annotations](#alignment-annotations)
   - [Pagination Annotations](#pagination-annotations)
   - [Durchen Notes](#durchen-notes)
   - [Bibliographic Metadata](#bibliographic-metadata)
5. [Edition-Level Annotation Management](#edition-level-annotation-management)
   - [Get All Annotations for Edition](#get-all-annotations-for-edition)
   - [Add Annotation to Edition](#add-annotation-to-edition)
6. [Data Models](#data-models)
7. [Error Responses](#error-responses)
8. [Best Practices](#best-practices)

---

## Overview

Annotations in the OpenPecha API provide a way to add structured metadata and relationships to edition content. Annotations use character span references to mark specific portions of text with semantic information.

### Key Concepts

- **Annotation**: Structured metadata attached to an edition
- **Span**: Character range `[start, end)` where start is inclusive and end is exclusive
- **Segment**: A portion of text defined by one or more spans
- **Alignment**: Mapping between segments in different editions

### Annotation Categories

| Category | Types | Purpose |
|----------|-------|---------|
| **Structural** | Segmentation, Pagination | Divide content into logical units |
| **Relational** | Alignment | Link content across editions |
| **Descriptive** | Bibliography, Durchen | Add metadata and scholarly notes |

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

## Annotation Types

### 1. Segmentation

Divides text into logical segments (e.g., sentences, paragraphs, verses). Used primarily with critical editions.

**Use Cases:**
- Sentence segmentation for translation alignment
- Verse boundaries in poetry
- Paragraph divisions
- Semantic text units

### 2. Alignment

Creates mappings between segments in different editions, enabling cross-edition navigation and parallel viewing.

**Use Cases:**
- Translation alignment (source to target language)
- Edition comparison (diplomatic to critical)
- Multi-version text alignment

### 3. Pagination

Maps character spans to page/folio references in physical manuscripts or printed editions. Used primarily with diplomatic editions.

**Use Cases:**
- Folio references (1a, 1b, 2a, 2b)
- Page numbers
- Image references for digital facsimiles
- Volume and page organization

### 4. Durchen Notes

Critical apparatus notes that document variant readings, textual issues, or scholarly commentary.

**Use Cases:**
- Variant readings from different manuscripts
- Textual corrections
- Editorial notes
- Scholarly observations

### 5. Bibliographic Metadata

Marks spans with bibliographic significance (colophon, title, author attribution, etc.).

**Use Cases:**
- Identifying colophon sections
- Marking title occurrences
- Author attributions
- Incipit markers

---

## Annotation Endpoints

### Segmentation Annotations

#### Get Segmentation by ID

Retrieve a specific segmentation annotation with all its segments.

**Endpoint:**
```
GET /v2/annotations/segmentation/{segmentation_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `segmentation_id` | string | path | Yes | The ID of the segmentation annotation |

**Response: 200 OK**

```json
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
    },
    {
      "id": "segment_002",
      "manifestation_id": "M12345678",
      "text_id": "E12345678",
      "lines": [
        {
          "start": 50,
          "end": 100
        }
      ]
    }
  ]
}
```

**Error Responses:**
- `404 Not Found`: Segmentation does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/segmentation/seg_abc123" \
  -H "X-API-Key: your_api_key"
```

---

#### Delete Segmentation

Permanently delete a standalone segmentation annotation and all its segments.

**Endpoint:**
```
DELETE /v2/annotations/segmentation/{segmentation_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `segmentation_id` | string | path | Yes | The ID of the segmentation annotation to delete |

**Response: 204 No Content**

Successful deletion returns no content.

**Error Responses:**
- `400 Bad Request`: Segmentation is part of an alignment (use DELETE alignment endpoint instead)
- `404 Not Found`: Segmentation does not exist
- `500 Server Error`: Internal server error

**Important:**
If the segmentation is part of an alignment, this endpoint will return `400 Bad Request`. You must delete the alignment using `DELETE /v2/annotations/alignment/{alignment_id}` instead, which will delete both the alignment and its associated segmentations.

**Example Usage:**

```bash
curl -X DELETE "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/segmentation/seg_abc123" \
  -H "X-API-Key: your_api_key"
```

---

### Alignment Annotations

#### Get Alignment by ID

Retrieve a specific alignment annotation with source and target segments.

**Endpoint:**
```
GET /v2/annotations/alignment/{alignment_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `alignment_id` | string | path | Yes | The ID of the alignment annotation |

**Response: 200 OK**

```json
{
  "id": "align_abc123",
  "target_id": "M87654321",
  "target_segments": [
    {
      "id": "target_seg_001",
      "manifestation_id": "M87654321",
      "text_id": "E87654321",
      "lines": [
        {
          "start": 0,
          "end": 30
        }
      ]
    },
    {
      "id": "target_seg_002",
      "manifestation_id": "M87654321",
      "text_id": "E87654321",
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
```

**Alignment Structure:**
- `target_id`: The manifestation being aligned to
- `target_segments`: Segments in the target manifestation
- `aligned_segments`: Segments in the source manifestation with indices indicating which target segments they align to

**Error Responses:**
- `404 Not Found`: Alignment does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/alignment/align_abc123" \
  -H "X-API-Key: your_api_key"
```

---

#### Delete Alignment

Permanently delete an alignment annotation and both its source and target segmentations, including all their segments.

**Endpoint:**
```
DELETE /v2/annotations/alignment/{alignment_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `alignment_id` | string | path | Yes | The ID of the alignment annotation to delete |

**Response: 204 No Content**

Successful deletion returns no content.

**Error Responses:**
- `404 Not Found`: Alignment does not exist
- `500 Server Error`: Internal server error

**Important:**
Deleting an alignment removes:
1. The alignment annotation itself
2. The source segmentation and all its segments
3. The target segmentation and all its segments

**Example Usage:**

```bash
curl -X DELETE "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/alignment/align_abc123" \
  -H "X-API-Key: your_api_key"
```

---

### Pagination Annotations

#### Get Pagination by ID

Retrieve a specific pagination annotation with volume and page information.

**Endpoint:**
```
GET /v2/annotations/pagination/{pagination_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `pagination_id` | string | path | Yes | The ID of the pagination annotation |

**Response: 200 OK**

```json
{
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
```

**Pagination Structure:**
- `volume.index`: Optional volume number
- `volume.pages`: Array of pages with references and spans
- `pages[].reference`: Page/folio identifier (e.g., "folio_1a", "page_5")
- `pages[].lines`: Character spans for the page

**Error Responses:**
- `404 Not Found`: Pagination does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/pagination/pag_abc123" \
  -H "X-API-Key: your_api_key"
```

---

#### Delete Pagination

Permanently delete a pagination annotation and all its pages.

**Endpoint:**
```
DELETE /v2/annotations/pagination/{pagination_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `pagination_id` | string | path | Yes | The ID of the pagination annotation to delete |

**Response: 204 No Content**

Successful deletion returns no content.

**Error Responses:**
- `404 Not Found`: Pagination does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X DELETE "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/pagination/pag_abc123" \
  -H "X-API-Key: your_api_key"
```

---

### Durchen Notes

#### Get Durchen Note by ID

Retrieve a specific durchen (critical apparatus) note.

**Endpoint:**
```
GET /v2/annotations/durchen/{note_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `note_id` | string | path | Yes | The ID of the durchen note |

**Response: 200 OK**

```json
{
  "id": "note_abc123",
  "span": {
    "start": 100,
    "end": 150
  },
  "text": "Variant reading found in manuscript B: འདི་ནི།"
}
```

**Durchen Note Structure:**
- `span`: Character range the note refers to
- `text`: The note content (variant reading, editorial comment, etc.)

**Error Responses:**
- `404 Not Found`: Durchen note does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/durchen/note_abc123" \
  -H "X-API-Key: your_api_key"
```

---

#### Delete Durchen Note

Permanently delete a durchen (critical apparatus) note.

**Endpoint:**
```
DELETE /v2/annotations/durchen/{note_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `note_id` | string | path | Yes | The ID of the durchen note to delete |

**Response: 204 No Content**

Successful deletion returns no content.

**Error Responses:**
- `404 Not Found`: Durchen note does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X DELETE "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/durchen/note_abc123" \
  -H "X-API-Key: your_api_key"
```

---

### Bibliographic Metadata

#### Get Bibliographic Metadata by ID

Retrieve a specific bibliographic metadata annotation (colophon, title, author, etc.).

**Endpoint:**
```
GET /v2/annotations/bibliographic/{bibliographic_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `bibliographic_id` | string | path | Yes | The ID of the bibliographic metadata |

**Response: 200 OK**

```json
{
  "id": "bib_abc123",
  "span": {
    "start": 5000,
    "end": 5500
  },
  "type": "colophon"
}
```

**Bibliographic Types:**

| Type | Description |
|------|-------------|
| `colophon` | Colophon text section |
| `title` | Title occurrence |
| `incipit` | Opening words/incipit |
| `alt_incipit` | Alternative incipit |
| `alt_title` | Alternative title |
| `person` | Person name mention |
| `author` | Author attribution |

**Error Responses:**
- `404 Not Found`: Bibliographic metadata does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/bibliographic/bib_abc123" \
  -H "X-API-Key: your_api_key"
```

---

#### Delete Bibliographic Metadata

Permanently delete a bibliographic metadata annotation.

**Endpoint:**
```
DELETE /v2/annotations/bibliographic/{bibliographic_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `bibliographic_id` | string | path | Yes | The ID of the bibliographic metadata to delete |

**Response: 204 No Content**

Successful deletion returns no content.

**Error Responses:**
- `404 Not Found`: Bibliographic metadata does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X DELETE "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/bibliographic/bib_abc123" \
  -H "X-API-Key: your_api_key"
```

---

## Edition-Level Annotation Management

### Get All Annotations for Edition

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

**Annotation Type Filters:**
- `segmentation`: Text segmentation annotations
- `alignment`: Cross-edition alignment annotations
- `pagination`: Page/folio reference annotations
- `bibliography`: Bibliographic metadata annotations
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
  "alignments": [
    {
      "id": "align_def456",
      "target_id": "M87654321",
      "target_segments": [
        {
          "id": "target_seg_001",
          "manifestation_id": "M87654321",
          "text_id": "E87654321",
          "lines": [
            {
              "start": 0,
              "end": 30
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

**Response Structure:**
- `segmentations`: Array of segmentation annotations (can have multiple)
- `alignments`: Array of alignment annotations (can have multiple)
- `pagination`: Single pagination object (only one per edition)
- `bibliographic_metadata`: Array of bibliographic annotations
- `durchen_notes`: Array of durchen notes

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

# Get only durchen notes
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations?type=durchen" \
  -H "X-API-Key: your_api_key"
```

---

### Add Annotation to Edition

Add an annotation to the specified edition. Exactly one annotation type must be provided per request.

**Endpoint:**
```
POST /v2/editions/{edition_id}/annotations
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `edition_id` | string | path | Yes | The ID of the edition to annotate |

**Request Body:**

Exactly one of the following annotation types must be provided per request.

---

#### 1. Add Segmentation

Divide edition content into segments.

**Request:**
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

**Notes:**
- Each segment must have at least one line (span)
- Segments can have multiple non-contiguous spans
- Spans within a segment should not overlap

**Example:**

```bash
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
  }'
```

---

#### 2. Add Alignment

Create alignment between segments in two editions.

**Request:**
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

- **Source Edition**: The `edition_id` in the URL
- **Target Edition**: The `target_id` in the request body
- **Target Segments**: Segments from the target edition
- **Aligned Segments**: Segments from the source edition
- **Alignment Indices**: Array of target segment indices each source segment aligns to

**Example Mapping:**
```
Source Segment 0 [0-25)   → Target Segment 0 [0-30)
Source Segment 1 [25-50)  → Target Segments 0 [0-30) and 1 [30-60)
```

**Example:**

```bash
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
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
        }
      ]
    }
  }'
```

---

#### 3. Add Pagination

Map edition content to page/folio references.

**Request:**
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

**Notes:**
- Only one pagination annotation per edition
- Volume index is optional
- Each page must have at least one line (span)
- Pages can have multiple non-contiguous spans

**Common Reference Formats:**
- Folio: `"folio_1a"`, `"folio_1b"`, `"folio_2a"`
- Page: `"page_1"`, `"page_2"`
- Image URL: `"https://example.com/image1.png"`

**Example:**

```bash
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

#### 4. Add Bibliographic Metadata

Mark spans with bibliographic significance.

**Request:**
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

| Type | Description | Common Use |
|------|-------------|------------|
| `colophon` | Colophon section | End matter, authorship statements |
| `title` | Title text | Main title occurrences |
| `incipit` | Opening words | Beginning phrases |
| `alt_incipit` | Alternative incipit | Variant opening words |
| `alt_title` | Alternative title | Secondary titles |
| `person` | Person mention | Name references in text |
| `author` | Author attribution | Author names/signatures |

**Example:**

```bash
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
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
  }'
```

---

#### 5. Add Durchen Notes

Add critical apparatus notes documenting variant readings or editorial comments.

**Request:**
```json
{
  "durchen_notes": [
    {
      "span": {
        "start": 100,
        "end": 150
      },
      "text": "Variant reading found in manuscript B: འདི་ནི།"
    },
    {
      "span": {
        "start": 500,
        "end": 520
      },
      "text": "Manuscript C has དེ་ནི། instead"
    }
  ]
}
```

**Example:**

```bash
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "durchen_notes": [
      {
        "span": {
          "start": 100,
          "end": 150
        },
        "text": "Variant reading found in manuscript B: འདི་ནི།"
      }
    ]
  }'
```

---

**Response for All Add Operations: 201 Created**

```json
{
  "message": "Annotation added successfully"
}
```

**Error Responses:**
- `400 Bad Request`: Invalid request (e.g., multiple annotation types provided, or invalid structure)
- `404 Not Found`: Edition does not exist
- `422 Validation Error`: Validation failed (invalid spans, missing required fields)
- `500 Server Error`: Internal server error

---

## Data Models

### SpanModel

Character range in text content.

```typescript
{
  start: number;   // Start position (inclusive, >= 0)
  end: number;     // End position (exclusive, >= 1, must be > start)
}
```

**Conventions:**
- Zero-based indexing
- Start is inclusive, end is exclusive
- Format: `[start, end)`
- Example: `{start: 0, end: 10}` covers characters 0-9

---

### SegmentOutput

A text segment with one or more character spans.

```typescript
{
  id: string;              // Unique segment identifier
  manifestation_id: string; // ID of the manifestation
  text_id: string;         // ID of the expression (text)
  lines: SpanModel[];      // Character spans (at least 1)
}
```

**Example:**

```json
{
  "id": "segment_001",
  "manifestation_id": "M12345678",
  "text_id": "E12345678",
  "lines": [
    {
      "start": 0,
      "end": 50
    },
    {
      "start": 100,
      "end": 150
    }
  ]
}
```

---

### SegmentationOutput

Complete segmentation annotation with all segments.

```typescript
{
  id: string;                    // Segmentation annotation ID
  segments: SegmentOutput[];     // List of segments
  metadata?: object | null;      // Optional annotation metadata
}
```

**Example:**

```json
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
    },
    {
      "id": "segment_002",
      "manifestation_id": "M12345678",
      "text_id": "E12345678",
      "lines": [
        {
          "start": 50,
          "end": 100
        }
      ]
    }
  ]
}
```

---

### AlignedSegment

Segment with alignment mappings to target segments.

```typescript
{
  lines: SpanModel[];          // Character spans for this segment (at least 1)
  alignment_indices: number[]; // Indices of target segments this aligns to (at least 1)
}
```

**Example (One-to-Many Alignment):**

```json
{
  "lines": [
    {
      "start": 25,
      "end": 50
    }
  ],
  "alignment_indices": [0, 1]
}
```

This source segment aligns to both target segment 0 and target segment 1.

---

### AlignmentOutput

Complete alignment annotation with source and target segments.

```typescript
{
  id: string;                    // Alignment annotation ID
  target_id: string;             // ID of the target manifestation
  target_segments: SegmentOutput[]; // Segments from target edition
  aligned_segments: AlignedSegment[]; // Segments from source edition with mappings
  metadata?: object | null;      // Optional annotation metadata
}
```

**Example:**

```json
{
  "id": "align_abc123",
  "target_id": "M87654321",
  "target_segments": [
    {
      "id": "target_seg_001",
      "manifestation_id": "M87654321",
      "text_id": "E87654321",
      "lines": [
        {
          "start": 0,
          "end": 30
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
    }
  ]
}
```

---

### PageModel

Page reference with character spans.

```typescript
{
  reference: string;         // Page/folio identifier
  lines: SpanModel[];        // Character spans (at least 1)
}
```

**Example:**

```json
{
  "reference": "folio_1a",
  "lines": [
    {
      "start": 0,
      "end": 500
    }
  ]
}
```

---

### VolumeModel

Volume containing pages.

```typescript
{
  index?: number | null;     // Volume number (optional)
  pages: PageModel[];        // Pages in volume (at least 1)
  metadata?: object | null;  // Optional volume metadata
}
```

**Example:**

```json
{
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
```

---

### PaginationOutput

Complete pagination annotation.

```typescript
{
  id: string;                    // Pagination annotation ID
  volume: VolumeModel;           // Volume with pages
  metadata?: object | null;      // Optional annotation metadata
}
```

**Example:**

```json
{
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
}
```

---

### BibliographicMetadataOutput

Bibliographic metadata annotation.

```typescript
{
  id: string;                    // Bibliographic metadata ID
  span: SpanModel;               // Character span
  type: "colophon" | "title" | "incipit" | "alt_incipit" | 
        "alt_title" | "person" | "author";
  metadata?: object | null;      // Optional annotation metadata
}
```

**Example:**

```json
{
  "id": "bib_abc123",
  "span": {
    "start": 5000,
    "end": 5500
  },
  "type": "colophon"
}
```

---

### NoteOutput

Durchen (critical apparatus) note.

```typescript
{
  id: string;                    // Note ID
  span: SpanModel;               // Character span
  text: string;                  // Note content
  metadata?: object | null;      // Optional annotation metadata
}
```

**Example:**

```json
{
  "id": "note_abc123",
  "span": {
    "start": 100,
    "end": 150
  },
  "text": "Variant reading found in manuscript B: འདི་ནི།"
}
```

---

### AnnotationRequestOutput

Complete response for edition annotations query.

```typescript
{
  segmentations?: SegmentationOutput[] | null;
  alignments?: AlignmentOutput[] | null;
  pagination?: PaginationOutput | null;
  bibliographic_metadata?: BibliographicMetadataOutput[] | null;
  durchen_notes?: NoteOutput[] | null;
}
```

**Notes:**
- `segmentations`: Can have multiple segmentation annotations
- `alignments`: Can have multiple alignment annotations
- `pagination`: Only one pagination per edition
- `bibliographic_metadata`: Can have multiple bibliographic annotations
- `durchen_notes`: Can have multiple notes

---

## Error Responses

### 400 Bad Request

```json
{
  "error": "There was an error with the request"
}
```

**Common Causes:**
- Multiple annotation types provided in single request
- Segmentation is part of alignment (cannot delete separately)
- Invalid span parameters (start >= end, negative values)
- Invalid alignment indices (referencing non-existent target segments)
- Spans outside edition content bounds

### 404 Not Found

```json
{
  "error": "Resource was not found"
}
```

**Common Causes:**
- Annotation ID does not exist
- Edition ID does not exist
- Target manifestation (for alignment) does not exist

### 422 Validation Error

```json
{
  "error": "Validation error",
  "details": [
    {
      "loc": ["body", "segmentation", "segments"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

**Common Causes:**
- Missing required fields
- Invalid field types or values
- Empty segments array
- Invalid span values (start >= end)
- Missing alignment_indices
- Invalid bibliographic type

### 500 Server Error

```json
{
  "error": "There was an error on the server"
}
```

---

## Best Practices

### 1. Segmentation

**Do:**
- Create non-overlapping segments for clean boundaries
- Use contiguous segments for complete text coverage
- Consider the granularity appropriate for your use case (sentence, verse, paragraph)
- Validate segments cover the intended content

**Don't:**
- Create overlapping segments within the same segmentation
- Leave gaps between segments unless intentional
- Create segments with invalid spans (start >= end)
- Create segments beyond content length

**Example - Sentence Segmentation:**

```json
{
  "segmentation": {
    "segments": [
      {"lines": [{"start": 0, "end": 45}]},      // First sentence
      {"lines": [{"start": 45, "end": 89}]},     // Second sentence
      {"lines": [{"start": 89, "end": 142}]}     // Third sentence
    ]
  }
}
```

---

### 2. Alignment

**Do:**
- Verify both source and target editions exist before creating alignment
- Ensure segment counts match the alignment logic
- Use alignment_indices that reference valid target segment positions
- Test alignment with sample queries

**Don't:**
- Create alignment with non-existent target_id
- Use alignment_indices beyond target_segments array bounds
- Create empty segments or aligned_segments arrays
- Misalign segment counts and indices

**Example - Translation Alignment:**

```json
{
  "alignment": {
    "target_id": "M_ENGLISH",
    "target_segments": [
      {"lines": [{"start": 0, "end": 50}]},      // English segment 1
      {"lines": [{"start": 50, "end": 100}]}     // English segment 2
    ],
    "aligned_segments": [
      {
        "lines": [{"start": 0, "end": 30}],      // Tibetan segment 1
        "alignment_indices": [0]                  // Maps to English segment 1
      },
      {
        "lines": [{"start": 30, "end": 80}],     // Tibetan segment 2
        "alignment_indices": [1]                  // Maps to English segment 2
      }
    ]
  }
}
```

---

### 3. Pagination

**Do:**
- Use consistent reference format throughout
- Ensure pages cover complete content without gaps
- Use descriptive references (folio numbers, page numbers)
- Map pages to their actual character boundaries

**Don't:**
- Create pagination for critical editions (use with diplomatic editions)
- Create multiple pagination annotations for one edition
- Use arbitrary or inconsistent reference formats
- Leave gaps between pages

**Example - Folio Pagination:**

```json
{
  "pagination": {
    "volume": {
      "index": 1,
      "pages": [
        {"reference": "1a", "lines": [{"start": 0, "end": 450}]},
        {"reference": "1b", "lines": [{"start": 450, "end": 900}]},
        {"reference": "2a", "lines": [{"start": 900, "end": 1350}]},
        {"reference": "2b", "lines": [{"start": 1350, "end": 1800}]}
      ]
    }
  }
}
```

---

### 4. Bibliographic Metadata

**Do:**
- Mark all significant bibliographic elements
- Use appropriate types for different elements
- Ensure spans accurately cover the marked content
- Add multiple annotations for different elements

**Don't:**
- Overlap spans with different types unnecessarily
- Use wrong bibliographic types
- Create spans beyond content boundaries

**Example - Multiple Bibliographic Markers:**

```json
{
  "bibliographic_metadata": [
    {
      "span": {"start": 0, "end": 120},
      "type": "title"
    },
    {
      "span": {"start": 120, "end": 250},
      "type": "author"
    },
    {
      "span": {"start": 5000, "end": 5800},
      "type": "colophon"
    }
  ]
}
```

---

### 5. Durchen Notes

**Do:**
- Be specific and concise in note text
- Reference manuscript sources when documenting variants
- Use proper Tibetan Unicode for variant readings
- Link related notes if applicable

**Don't:**
- Create overly long notes (keep focused)
- Use vague references
- Leave out manuscript identifiers
- Overlap note spans unnecessarily

**Example - Variant Reading Notes:**

```json
{
  "durchen_notes": [
    {
      "span": {"start": 234, "end": 242},
      "text": "Derge: དེ་ནི།, Narthang: འདི་ནི།"
    },
    {
      "span": {"start": 567, "end": 578},
      "text": "Manuscript B omits this phrase"
    }
  ]
}
```

---

## Workflow Examples

### Scenario 1: Creating Edition with Complete Annotations

```bash
# Step 1: Create text
TEXT_ID=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {"bo": "དཔེ་མཚོན།"},
    "language": "bo",
    "category_id": "CAT_PHIL",
    "contributions": [{"person_bdrc_id": "P2816", "role": "author"}]
  }' | jq -r '.id')

# Step 2: Create edition with initial segmentation
EDITION_ID=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/$TEXT_ID/editions" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {"type": "critical"},
    "annotation": [
      {"span": {"start": 0, "end": 100}},
      {"span": {"start": 100, "end": 200}}
    ],
    "content": "Full text content here..."
  }' | jq -r '.id')

# Step 3: Add bibliographic metadata
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$EDITION_ID/annotations" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "bibliographic_metadata": [
      {
        "span": {"start": 0, "end": 50},
        "type": "title"
      }
    ]
  }'

# Step 4: Add durchen notes
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$EDITION_ID/annotations" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "durchen_notes": [
      {
        "span": {"start": 150, "end": 160},
        "text": "Variant: འདི་ནི།"
      }
    ]
  }'

# Step 5: Verify all annotations
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$EDITION_ID/annotations" \
  -H "X-API-Key: your_api_key" \
  | jq '.'
```

---

### Scenario 2: Creating Translation Alignment

```bash
# Assume SOURCE_EDITION and TARGET_EDITION exist

# Step 1: Create alignment annotation on source edition
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$SOURCE_EDITION/annotations" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "alignment": {
      "target_id": "'$TARGET_EDITION'",
      "target_segments": [
        {
          "lines": [
            {"start": 0, "end": 100}
          ]
        },
        {
          "lines": [
            {"start": 100, "end": 200}
          ]
        }
      ],
      "aligned_segments": [
        {
          "lines": [
            {"start": 0, "end": 80}
          ],
          "alignment_indices": [0]
        },
        {
          "lines": [
            {"start": 80, "end": 160}
          ],
          "alignment_indices": [1]
        }
      ]
    }
  }'

# Step 2: Query related segments by span
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$SOURCE_EDITION/segments/related?span_start=0&span_end=80" \
  -H "X-API-Key: your_api_key"
```

---

### Scenario 3: Adding Pagination to Diplomatic Edition

```bash
# Create diplomatic edition with pagination
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T12345678/editions" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {
      "type": "diplomatic",
      "bdrc": "W123456",
      "source": "Derge Kangyur"
    },
    "annotation": [
      {
        "span": {"start": 0, "end": 500},
        "reference": "https://www.tbrc.org/images/1a.jpg"
      },
      {
        "span": {"start": 500, "end": 1000},
        "reference": "https://www.tbrc.org/images/1b.jpg"
      }
    ],
    "content": "Full Tibetan text from manuscript..."
  }'
```

---

### Scenario 4: Managing Annotations

```bash
# Get specific annotation
SEGMENTATION=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/segmentation/seg_abc123" \
  -H "X-API-Key: your_api_key")

echo $SEGMENTATION | jq '.'

# Get all annotations for edition
ALL_ANNOTATIONS=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations" \
  -H "X-API-Key: your_api_key")

# Extract specific types
echo $ALL_ANNOTATIONS | jq '.segmentations'
echo $ALL_ANNOTATIONS | jq '.durchen_notes'

# Delete annotation
curl -X DELETE \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/durchen/note_abc123" \
  -H "X-API-Key: your_api_key"
```

---

## Advanced Usage

### Multi-Span Segments

Segments can have multiple non-contiguous spans, useful for handling text with interruptions:

```json
{
  "segmentation": {
    "segments": [
      {
        "lines": [
          {"start": 0, "end": 50},
          {"start": 100, "end": 150}
        ]
      }
    ]
  }
}
```

**Use Cases:**
- Text with insertions or interruptions
- Verses split by commentary
- Non-contiguous semantic units

---

### Complex Alignments

Alignments can be one-to-one, one-to-many, or many-to-one:

**One-to-One Alignment:**
```json
{
  "aligned_segments": [
    {"lines": [{"start": 0, "end": 50}], "alignment_indices": [0]},
    {"lines": [{"start": 50, "end": 100}], "alignment_indices": [1]}
  ]
}
```

**One-to-Many Alignment (one source → multiple targets):**
```json
{
  "aligned_segments": [
    {
      "lines": [{"start": 0, "end": 80}],
      "alignment_indices": [0, 1]  // Maps to target segments 0 AND 1
    }
  ]
}
```

**Many-to-One Alignment (multiple sources → one target):**
```json
{
  "aligned_segments": [
    {
      "lines": [{"start": 0, "end": 30}],
      "alignment_indices": [0]
    },
    {
      "lines": [{"start": 30, "end": 60}],
      "alignment_indices": [0]  // Both map to same target segment
    }
  ]
}
```

---

### Querying Annotations by Type

Filter annotations by specific types to reduce response payload:

```bash
# Get only structural annotations
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations?type=segmentation&type=pagination" \
  -H "X-API-Key: your_api_key"

# Get only descriptive annotations
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations?type=bibliography&type=durchen" \
  -H "X-API-Key: your_api_key"

# Get only alignment annotations
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations?type=alignment" \
  -H "X-API-Key: your_api_key"
```

---

### Span Adjustments on Content Modification

When edition content is modified via `PATCH /v2/editions/{edition_id}/content`, annotations with spans are automatically adjusted.

**Adjustment Behavior:**

| Annotation Type | Span Behavior | Reference |
|-----------------|---------------|-----------|
| Segmentation segments | Continuous span rules | [Text Operations Spec](../text-operations-spec.md) |
| Pagination pages | Continuous span rules | [Text Operations Spec](../text-operations-spec.md) |
| Bibliographic metadata | Annotation span rules | [Text Operations Spec](../text-operations-spec.md) |
| Durchen notes | Annotation span rules | [Text Operations Spec](../text-operations-spec.md) |
| Alignment segments | Adjusted if source manifestation | [Text Operations Spec](../text-operations-spec.md) |

**Example:**

```bash
# Original segmentation: [0-50), [50-100)
# Insert "NEW " at position 10 (adds 4 characters)
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/content" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "type": "insert",
    "position": 10,
    "text": "NEW "
  }'

# Resulting segmentation: [0-54), [54-104)
# All spans automatically adjusted
```

See [Text Operations Specification](../text-operations-spec.md) for complete adjustment algorithms.

---

## Common Scenarios

### Scenario 1: Critical Edition with Scholarly Apparatus

```bash
# Create critical edition with segmentation and notes
EDITION_ID=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T_ROOT/editions" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "metadata": {
      "type": "critical",
      "source": "Scholarly Edition 2024"
    },
    "annotation": [
      {"span": {"start": 0, "end": 150}},
      {"span": {"start": 150, "end": 300}}
    ],
    "content": "Critical text with established reading..."
  }' | jq -r '.id')

# Add bibliographic markers
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$EDITION_ID/annotations" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "bibliographic_metadata": [
      {"span": {"start": 0, "end": 80}, "type": "title"},
      {"span": {"start": 80, "end": 120}, "type": "author"}
    ]
  }'

# Add critical apparatus notes
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$EDITION_ID/annotations" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "durchen_notes": [
      {
        "span": {"start": 175, "end": 185},
        "text": "Derge: དེ་ནི། | Narthang: འདི་ནི། | Cone: དེ་ནི།"
      },
      {
        "span": {"start": 290, "end": 295},
        "text": "This passage appears only in Derge and Cone editions"
      }
    ]
  }'
```

---

### Scenario 2: Diplomatic Edition with Image References

```bash
# Create diplomatic edition with pagination
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T_ROOT/editions" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {
      "type": "diplomatic",
      "bdrc": "W23703",
      "source": "Derge Kangyur, Volume 1"
    },
    "annotation": [
      {
        "span": {"start": 0, "end": 450},
        "reference": "https://library.bdrc.io/W23703-001-1a"
      },
      {
        "span": {"start": 450, "end": 900},
        "reference": "https://library.bdrc.io/W23703-001-1b"
      },
      {
        "span": {"start": 900, "end": 1350},
        "reference": "https://library.bdrc.io/W23703-001-2a"
      }
    ],
    "content": "Diplomatic transcription of manuscript..."
  }'
```

---

### Scenario 3: Parallel Translation with Alignment

```bash
# Assume we have:
# - SOURCE_EDITION (Tibetan text)
# - TRANSLATION_EDITION (English translation)

# Create segmentation for source (if not exists)
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$SOURCE_EDITION/annotations" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "segmentation": {
      "segments": [
        {"lines": [{"start": 0, "end": 100}]},
        {"lines": [{"start": 100, "end": 200}]},
        {"lines": [{"start": 200, "end": 300}]}
      ]
    }
  }'

# Create alignment between source and translation
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$SOURCE_EDITION/annotations" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "alignment": {
      "target_id": "'$TRANSLATION_EDITION'",
      "target_segments": [
        {"lines": [{"start": 0, "end": 120}]},
        {"lines": [{"start": 120, "end": 240}]},
        {"lines": [{"start": 240, "end": 360}]}
      ],
      "aligned_segments": [
        {"lines": [{"start": 0, "end": 100}], "alignment_indices": [0]},
        {"lines": [{"start": 100, "end": 200}], "alignment_indices": [1]},
        {"lines": [{"start": 200, "end": 300}], "alignment_indices": [2]}
      ]
    }
  }'

# Query related segments
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$SOURCE_EDITION/segments/related?span_start=0&span_end=100" \
  -H "X-API-Key: your_api_key"
```

---

### Scenario 4: Updating Annotations

Annotations are immutable once created. To update an annotation:

```bash
# Step 1: Get current annotation
CURRENT=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/segmentation/seg_abc123" \
  -H "X-API-Key: your_api_key")

# Step 2: Delete existing annotation
curl -X DELETE \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/segmentation/seg_abc123" \
  -H "X-API-Key: your_api_key"

# Step 3: Create new annotation with updated data
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "segmentation": {
      "segments": [
        {"lines": [{"start": 0, "end": 60}]},
        {"lines": [{"start": 60, "end": 120}]}
      ]
    }
  }'
```

**Note:** For annotations that are automatically adjusted by content operations, you don't need to update them manually.

---

## Integration with Other APIs

### Annotations and Segments

Query individual segments to get their content:

```
1. GET /v2/editions/{edition_id}/annotations?type=segmentation
2. Extract segment IDs from response
3. GET /v2/segments/{segment_id}/content → Get segment text
4. GET /v2/segments/{segment_id}/related → Find aligned segments
```

### Annotations and Content Modification

When modifying edition content, annotations are automatically adjusted:

```
1. GET /v2/editions/{edition_id}/annotations → Get current state
2. PATCH /v2/editions/{edition_id}/content → Modify content
3. GET /v2/editions/{edition_id}/annotations → Verify adjustments
```

### Annotations and Edition Creation

Add annotations during edition creation:

```
POST /v2/texts/{text_id}/editions
{
  "metadata": {...},
  "annotation": [...]           // Segmentation or pagination
  "bibliography_annotation": [...] // Optional bibliography
  "content": "..."
}
```

---

## Validation Rules

### Segmentation

| Rule | Validation |
|------|------------|
| Segments array | Required, at least 1 segment |
| Segment lines | Required, at least 1 span per segment |
| Span start | >= 0 |
| Span end | >= 1, must be > start |
| Span bounds | Must be within edition content length |

### Alignment

| Rule | Validation |
|------|------------|
| target_id | Required, must reference existing manifestation |
| target_segments | Required, at least 1 segment |
| aligned_segments | Required, at least 1 segment |
| alignment_indices | Required, at least 1 index per segment |
| alignment_indices values | Must be valid indices into target_segments array |

### Pagination

| Rule | Validation |
|------|------------|
| volume | Required |
| pages | Required, at least 1 page |
| page reference | Required, non-empty string |
| page lines | Required, at least 1 span per page |
| volume index | Optional, integer if provided |

### Bibliographic Metadata

| Rule | Validation |
|------|------------|
| Array | Required, at least 1 item |
| span | Required per item |
| type | Required per item, must be valid type |
| Span bounds | Must be within edition content length |

### Durchen Notes

| Rule | Validation |
|------|------------|
| Array | Required, at least 1 item |
| span | Required per item |
| text | Required per item, non-empty string |
| Span bounds | Must be within edition content length |

---

## Annotation Lifecycle

### Creation

1. **During Edition Creation**
   - Add segmentation/pagination via `annotation` field
   - Add bibliography via `bibliography_annotation` field
   - Content and initial annotations created atomically

2. **After Edition Creation**
   - Add any annotation type via `POST /v2/editions/{edition_id}/annotations`
   - One annotation type per request
   - Can add multiple annotations of same type separately

### Retrieval

1. **Individual Annotation**
   - Use type-specific GET endpoints
   - Requires annotation ID

2. **All Annotations for Edition**
   - Use `GET /v2/editions/{edition_id}/annotations`
   - Filter by type(s) to reduce payload

### Modification

Annotations are **immutable**. To modify:
1. Delete existing annotation
2. Create new annotation with updated data

**Exception:** Spans are automatically adjusted during content operations.

### Deletion

1. **Individual Annotation**
   - Use type-specific DELETE endpoints
   - Requires annotation ID

2. **Cascade Deletion**
   - Deleting edition deletes all its annotations
   - Deleting alignment deletes both source and target segmentations
   - Deleting standalone segmentation that's in alignment fails (must delete alignment)

---

## Troubleshooting

### Common Issues

**Issue: "Segmentation is part of alignment" when deleting segmentation**

**Solution:**
- The segmentation is linked to an alignment
- Delete the alignment instead: `DELETE /v2/annotations/alignment/{alignment_id}`
- This will delete both the alignment and associated segmentations

```bash
# Step 1: Get alignments for edition
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations?type=alignment" \
  -H "X-API-Key: your_api_key"

# Step 2: Delete alignment (not segmentation)
curl -X DELETE "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/alignment/align_abc123" \
  -H "X-API-Key: your_api_key"
```

---

**Issue: "Validation error" on alignment_indices**

**Solution:**
- Ensure all indices reference valid positions in target_segments array
- Array indices are 0-based
- If you have 2 target segments, valid indices are 0 and 1

```json
// ✗ Wrong - index 2 doesn't exist
{
  "target_segments": [
    {"lines": [{"start": 0, "end": 50}]},
    {"lines": [{"start": 50, "end": 100}]}
  ],
  "aligned_segments": [
    {"lines": [{"start": 0, "end": 40}], "alignment_indices": [2]}
  ]
}

// ✓ Correct
{
  "target_segments": [
    {"lines": [{"start": 0, "end": 50}]},
    {"lines": [{"start": 50, "end": 100}]}
  ],
  "aligned_segments": [
    {"lines": [{"start": 0, "end": 40}], "alignment_indices": [0, 1]}
  ]
}
```

---

**Issue: "Multiple annotation types provided"**

**Solution:**
- Each POST request must contain exactly one annotation type
- Create multiple annotations via separate requests

```bash
# ✗ Wrong - multiple types in one request
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations" \
  -d '{
    "segmentation": {...},
    "bibliographic_metadata": [...]
  }'

# ✓ Correct - separate requests
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations" \
  -d '{"segmentation": {...}}'

curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/annotations" \
  -d '{"bibliographic_metadata": [...]}'
```

---

**Issue: Span exceeds content length**

**Solution:**
- Verify edition content length before creating annotations
- Ensure all spans are within bounds `[0, content_length)`

```bash
# Step 1: Get content to determine length
CONTENT=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I12345678/content" \
  -H "X-API-Key: your_api_key")

CONTENT_LENGTH=$(echo -n "$CONTENT" | wc -c)
echo "Content length: $CONTENT_LENGTH"

# Step 2: Create annotations with valid spans
# Ensure all end positions <= CONTENT_LENGTH
```

---

**Issue: Cannot delete pagination/segmentation after content modification**

**Solution:**
- Annotations are preserved and adjusted during content operations
- If you need to remove an annotation, delete it explicitly
- Content operations don't delete annotations, only adjust spans

---

**Issue: Empty segments array or pages array**

**Solution:**
- Arrays must contain at least one item
- Each segment must have at least one line
- Each page must have at least one line

```json
// ✗ Wrong - empty arrays
{
  "segmentation": {
    "segments": []
  }
}

// ✓ Correct
{
  "segmentation": {
    "segments": [
      {"lines": [{"start": 0, "end": 100}]}
    ]
  }
}
```

---

## Performance Considerations

### Query Optimization

**Do:**
- Filter by annotation type when you only need specific types
- Use type-specific endpoints when you have annotation IDs
- Paginate results when querying editions with many annotations

**Don't:**
- Fetch all annotations if you only need one type
- Repeatedly query individual annotations (batch requests)
- Query annotations unnecessarily

### Annotation Density

**Guidance:**
- **Segmentation**: Typical density is 1 segment per 50-200 characters (sentence level)
- **Pagination**: Depends on manuscript (typically 400-600 characters per folio side)
- **Bibliographic**: Sparse, only marking significant elements
- **Durchen**: Sparse, only where textual issues exist

---

## Complete TypeScript Types

```typescript
// Span model
interface SpanModel {
  start: number;   // >= 0
  end: number;     // >= 1, > start
}

// Segment models
interface SegmentOutput {
  id: string;
  manifestation_id: string;
  text_id: string;
  lines: SpanModel[];  // At least 1
}

interface SegmentationOutput {
  id: string;
  segments: SegmentOutput[];
  metadata?: object | null;
}

// Alignment models
interface AlignedSegment {
  lines: SpanModel[];          // At least 1
  alignment_indices: number[]; // At least 1
}

interface AlignmentOutput {
  id: string;
  target_id: string;
  target_segments: SegmentOutput[];
  aligned_segments: AlignedSegment[];
  metadata?: object | null;
}

// Pagination models
interface PageModel {
  reference: string;
  lines: SpanModel[];  // At least 1
}

interface VolumeModel {
  index?: number | null;
  pages: PageModel[];  // At least 1
  metadata?: object | null;
}

interface PaginationOutput {
  id: string;
  volume: VolumeModel;
  metadata?: object | null;
}

// Bibliographic metadata
type BibliographicType = 
  | "colophon"
  | "title"
  | "incipit"
  | "alt_incipit"
  | "alt_title"
  | "person"
  | "author";

interface BibliographicMetadataOutput {
  id: string;
  span: SpanModel;
  type: BibliographicType;
  metadata?: object | null;
}

// Durchen notes
interface NoteOutput {
  id: string;
  span: SpanModel;
  text: string;
  metadata?: object | null;
}

// Annotation request/response
interface AnnotationRequestOutput {
  segmentations?: SegmentationOutput[] | null;
  alignments?: AlignmentOutput[] | null;
  pagination?: PaginationOutput | null;
  bibliographic_metadata?: BibliographicMetadataOutput[] | null;
  durchen_notes?: NoteOutput[] | null;
}

// Input types for creating annotations
interface SegmentationInput {
  segments: Array<{
    lines: SpanModel[];  // At least 1
  }>;
  metadata?: object | null;
}

interface AlignmentInput {
  target_id: string;
  target_segments: Array<{
    lines: SpanModel[];  // At least 1
  }>;
  aligned_segments: AlignedSegment[];
  metadata?: object | null;
}

interface PaginationInput {
  volume: VolumeModel;
  metadata?: object | null;
}

interface BibliographicMetadataInput {
  span: SpanModel;
  type: BibliographicType;
  metadata?: object | null;
}

interface NoteInput {
  span: SpanModel;
  text: string;
  metadata?: object | null;
}

interface AnnotationRequestInput {
  segmentation?: SegmentationInput;
  alignment?: AlignmentInput;
  pagination?: PaginationInput;
  bibliographic_metadata?: BibliographicMetadataInput[];
  durchen_notes?: NoteInput[];
}
```

---

## Quick Reference

### Annotation Types Summary

| Type | Endpoint Pattern | Can Delete Standalone | Multiple per Edition |
|------|------------------|----------------------|---------------------|
| Segmentation | `/annotations/segmentation/{id}` | Yes (if not in alignment) | Yes |
| Alignment | `/annotations/alignment/{id}` | Yes | Yes |
| Pagination | `/annotations/pagination/{id}` | Yes | No (only one) |
| Bibliographic | `/annotations/bibliographic/{id}` | Yes | Yes |
| Durchen | `/annotations/durchen/{id}` | Yes | Yes |

### HTTP Methods Summary

| Method | Endpoint Pattern | Purpose |
|--------|-----------------|---------|
| GET | `/v2/annotations/{type}/{id}` | Get specific annotation |
| DELETE | `/v2/annotations/{type}/{id}` | Delete specific annotation |
| GET | `/v2/editions/{edition_id}/annotations` | Get all annotations for edition |
| POST | `/v2/editions/{edition_id}/annotations` | Add annotation to edition |

### Status Codes

| Status | Meaning | Common Use |
|--------|---------|------------|
| 200 | OK | Successful GET |
| 201 | Created | Successful POST |
| 204 | No Content | Successful DELETE |
| 400 | Bad Request | Invalid parameters, multiple types, alignment constraint |
| 404 | Not Found | Resource doesn't exist |
| 422 | Validation Error | Invalid structure, missing fields |
| 500 | Server Error | Internal error |

### Span Conventions

| Convention | Value |
|------------|-------|
| Indexing | Zero-based |
| Start | Inclusive |
| End | Exclusive |
| Format | `[start, end)` |
| Valid span | `0 <= start < end <= content_length` |

---

## Real-World Examples

### Example 1: Heart Sutra with Complete Annotations

```bash
# Create critical edition with sentence segmentation
EDITION_ID=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T_HEART_SUTRA/editions" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "metadata": {
      "type": "critical",
      "source": "2024 Critical Edition"
    },
    "annotation": [
      {"span": {"start": 0, "end": 89}},      // Sentence 1
      {"span": {"start": 89, "end": 156}},    // Sentence 2
      {"span": {"start": 156, "end": 234}}    // Sentence 3
    ],
    "bibliography_annotation": [
      {"span": {"start": 0, "end": 35}, "type": "title"},
      {"span": {"start": 200, "end": 234}, "type": "colophon"}
    ],
    "content": "སྟོན་པ་བཅོམ་ལྡན་འདས་རྒྱལ་པོའི་ཁབ་ན་བྱ་རྒོད་ཀྱི་ཕུང་པོའི་རི་ལ་..."
  }' | jq -r '.id')

# Add durchen notes for variant readings
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$EDITION_ID/annotations" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "durchen_notes": [
      {
        "span": {"start": 45, "end": 52},
        "text": "Derge: བཅོམ་ལྡན། | Narthang: བཅོམ་ལྡན་འདས།"
      }
    ]
  }'
```

---

### Example 2: Multi-Volume Manuscript with Pagination

```bash
# Create diplomatic edition for volume 1
VOL1_EDITION=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T_KANGYUR/editions" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "metadata": {
      "type": "diplomatic",
      "bdrc": "W22084",
      "source": "Derge Kangyur Volume 1"
    },
    "annotation": [
      {"span": {"start": 0, "end": 450}, "reference": "1a"},
      {"span": {"start": 450, "end": 900}, "reference": "1b"},
      {"span": {"start": 900, "end": 1350}, "reference": "2a"},
      {"span": {"start": 1350, "end": 1800}, "reference": "2b"}
    ],
    "content": "Full diplomatic transcription..."
  }' | jq -r '.id')
```

---

### Example 3: Parallel Tibetan-English Translation

```bash
# Assume Tibetan and English editions exist

# Create alignment between Tibetan source and English translation
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$TIBETAN_EDITION/annotations" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "alignment": {
      "target_id": "'$ENGLISH_EDITION'",
      "target_segments": [
        {"lines": [{"start": 0, "end": 150}]},
        {"lines": [{"start": 150, "end": 300}]}
      ],
      "aligned_segments": [
        {
          "lines": [{"start": 0, "end": 120}],
          "alignment_indices": [0]
        },
        {
          "lines": [{"start": 120, "end": 240}],
          "alignment_indices": [1]
        }
      ]
    }
  }'

# Now users can navigate between aligned segments
# Get Tibetan segment 0 → finds English segment 0
# Get English segment 0 → finds Tibetan segment 0
```

---

## Testing Annotations

### Test Segmentation Creation

```bash
# Create test segmentation
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I_TEST/annotations" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "segmentation": {
      "segments": [
        {"lines": [{"start": 0, "end": 10}]},
        {"lines": [{"start": 10, "end": 20}]}
      ]
    }
  }' -w "\nStatus: %{http_code}\n"
```

### Test Annotation Retrieval

```bash
# Get all annotations
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/I_TEST/annotations" \
  -H "X-API-Key: your_api_key" \
  | jq '.'

# Get specific segmentation
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/annotations/segmentation/seg_test123" \
  -H "X-API-Key: your_api_key" \
  | jq '.'
```

### Test Span Adjustment

```bash
# Step 1: Create edition with segmentation
EDITION=$(curl -s -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T_TEST/editions" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "metadata": {"type": "critical"},
    "annotation": [
      {"span": {"start": 0, "end": 10}},
      {"span": {"start": 10, "end": 20}}
    ],
    "content": "0123456789ABCDEFGHIJ"
  }' | jq -r '.id')

# Step 2: Get annotations before modification
echo "Before modification:"
curl -s -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$EDITION/annotations" \
  -H "X-API-Key: your_api_key" \
  | jq '.segmentations[0].segments[].lines'

# Step 3: Insert text
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$EDITION/content" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "type": "insert",
    "position": 5,
    "text": "XXX"
  }'

# Step 4: Verify spans adjusted
echo "After modification:"
curl -s -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/$EDITION/annotations" \
  -H "X-API-Key: your_api_key" \
  | jq '.segmentations[0].segments[].lines'

# Expected: segments adjusted to [0-13), [13-23)
```

---

## Important Notes

### Annotation Mutability

- **Immutable**: Annotation content cannot be updated directly
- **To modify**: Delete and recreate annotation
- **Exception**: Spans automatically adjust during content operations

### Cascade Deletion Rules

| Delete Operation | What Gets Deleted |
|------------------|-------------------|
| Delete Edition | All annotations on that edition |
| Delete Alignment | Alignment + both source and target segmentations |
| Delete Segmentation | Segmentation + all segments (if standalone) |
| Delete Pagination | Pagination + all pages |
| Delete Bibliographic | Single bibliographic metadata item |
| Delete Durchen | Single durchen note |

### Multiple Annotations

| Type | Multiple Allowed? | Notes |
|------|-------------------|-------|
| Segmentation | Yes | Can have multiple different segmentations |
| Alignment | Yes | Can align to multiple editions |
| Pagination | No | Only one pagination per edition |
| Bibliographic | Yes | Multiple metadata items allowed |
| Durchen | Yes | Multiple notes allowed |

### Span Overlaps

| Within Same Annotation Type | Allowed? | Notes |
|----------------------------|----------|-------|
| Segmentation segments | No | Should be non-overlapping |
| Pagination pages | No | Should be non-overlapping |
| Bibliographic metadata | Yes | Can overlap (e.g., title within colophon) |
| Durchen notes | Yes | Can overlap (multiple notes on same text) |

---

## Use Case Patterns

### Pattern 1: Sentence-Level Translation Alignment

```
1. Create source edition with sentence segmentation
2. Create translation edition with sentence segmentation
3. Create alignment mapping source sentences to translation sentences
4. Users can navigate sentence-by-sentence between translations
```

### Pattern 2: Scholarly Critical Edition

```
1. Create critical edition with paragraph segmentation
2. Add bibliographic metadata (title, author, colophon)
3. Add durchen notes documenting variant readings
4. Users can read edition with critical apparatus
```

### Pattern 3: Digital Facsimile

```
1. Create diplomatic edition from manuscript
2. Add pagination with folio references and image URLs
3. Add bibliographic metadata marking structural elements
4. Users can navigate page-by-page with images
```

### Pattern 4: Comparative Study

```
1. Create multiple editions of same text
2. Add segmentation to each edition
3. Create alignments between editions
4. Users can compare different versions side-by-side
```

---

## See Also

- [Editions API Documentation](./editions-api.md) - Edition management and content operations
- [Texts API Documentation](./texts-api.md) - Text-level metadata
- [Text Operations Specification](../text-operations-spec.md) - Span adjustment algorithms
- [Segments API](#segments-api) - Working with individual segments
- OpenAPI Specification: `GET /v2/schema/openapi`
