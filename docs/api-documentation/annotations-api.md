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
