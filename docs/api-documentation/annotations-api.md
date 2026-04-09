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
---

## Annotation Design

[Design Link](https://excalidraw.com/#json=FOKFHw3wBx0Yf5fm-6vrU,M_x0GiMa5bYEjeiwnLhQMQ)

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
      "edition_id": "M12345678",
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
      "edition_id": "M12345678",
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
      "edition_id": "M87654321",
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
      "edition_id": "M87654321",
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
- `target_id`: The edition being aligned to
- `target_segments`: Segments in the target edition
- `aligned_segments`: Segments in the source edition with indices indicating which target segments they align to

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