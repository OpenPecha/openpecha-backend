# API Migration Guide

This guide documents the changes made to the OpenPecha API and provides
migration instructions for application builders.

## Required Headers

### X-Application Header

All API endpoints that interact with application-scoped data now require the
`X-Application` header. This header identifies which application context the
request belongs to.

**Required Header:**

```
X-Application: your-application-id
```

**Affected Endpoints:**

- `GET /v2/categories` - Returns categories for the specified application
- `POST /v2/categories` - Creates a category within the specified application

**Error Response (400 Bad Request):**

If the header is missing:

```json
{
    "error": "Missing required header: X-Application"
}
```

**Error Response (404 Not Found):**

If the application ID doesn't exist:

```json
{
    "error": "Application 'invalid-app-id' not found"
}
```

**Example Request:**

```bash
curl -X GET "https://api.openpecha.org/v2/categories" \
  -H "X-Application: webuddhist"
```

---

## Annotations Endpoint Migration

### Summary of Changes

The annotations API has been significantly refactored. The main changes are:

1. **Generic GET endpoint replaced with type-specific endpoints**
2. **POST/GET annotations moved from `/v2/annotations/` to `/v2/editions/`**
3. **PUT endpoint for annotations removed** (no longer exists)
4. **Response structure updated** to match actual Pydantic models

---

### Breaking Changes

#### 1. Retrieving Annotations by ID

**Old API (REMOVED):**

```
GET /v2/annotations/{annotation_id}
```

This generic endpoint that returned any annotation type by ID no longer exists.

**New API:** Use the type-specific endpoints:

| Annotation Type | New Endpoint                                           |
| --------------- | ------------------------------------------------------ |
| Segmentation    | `GET /v2/annotations/segmentation/{segmentation_id}`   |
| Alignment       | `GET /v2/annotations/alignment/{alignment_id}`         |
| Pagination      | `GET /v2/annotations/pagination/{pagination_id}`       |
| Durchen (Notes) | `GET /v2/annotations/durchen/{note_id}`                |
| Bibliographic   | `GET /v2/annotations/bibliographic/{bibliographic_id}` |

**Migration Steps:**

1. Determine the annotation type you're retrieving
2. Use the corresponding type-specific endpoint
3. Update your response parsing to match the new response structure (see below)

---

#### 2. Response Structure Changes

The response structure for each annotation type now matches the actual Pydantic
models:

**Segmentation Response:**

```json
{
    "id": "seg_abc123",
    "segments": [
        {
            "id": "segment_001",
            "manifestation_id": "M12345678",
            "text_id": "E12345678",
            "lines": [
                { "start": 0, "end": 50 }
            ]
        }
    ],
    "metadata": null
}
```

**Alignment Response:**

```json
{
    "id": "align_abc123",
    "target_id": "M87654321",
    "target_segments": [
        {
            "id": "target_seg_001",
            "manifestation_id": "M87654321",
            "text_id": "E87654321",
            "lines": [{ "start": 0, "end": 30 }]
        }
    ],
    "aligned_segments": [
        {
            "lines": [{ "start": 0, "end": 25 }],
            "alignment_indices": [0]
        }
    ],
    "metadata": null
}
```

**Pagination Response:**

```json
{
    "id": "pag_abc123",
    "volume": {
        "index": 1,
        "pages": [
            {
                "reference": "folio_1a",
                "lines": [{ "start": 0, "end": 500 }]
            }
        ],
        "metadata": null
    },
    "metadata": null
}
```

**Durchen (Note) Response:**

```json
{
    "id": "note_abc123",
    "span": { "start": 100, "end": 150 },
    "text": "Variant reading found in manuscript B",
    "metadata": null
}
```

**Bibliographic Metadata Response:**

```json
{
    "id": "bib_abc123",
    "span": { "start": 5000, "end": 5500 },
    "type": "colophon",
    "metadata": null
}
```

---

#### 3. Adding Annotations

**Old API (REMOVED):**

```
POST /v2/annotations/{instance_id}/annotation
```

With request body containing a `type` field to specify annotation type.

**New API:**

```
POST /v2/editions/{edition_id}/annotations
```

**Key Changes:**

- Path changed from `/v2/annotations/` to `/v2/editions/`
- Parameter renamed from `instance_id` to `edition_id`
- Request body structure changed: instead of a `type` field, provide the
  annotation data directly under the annotation type key

**Old Request Body:**

```json
{
    "type": "segmentation",
    "annotation": [
        { "span": { "start": 0, "end": 50 } },
        { "span": { "start": 50, "end": 100 } }
    ]
}
```

**New Request Body:**

```json
{
    "segmentation": {
        "segments": [
            { "lines": [{ "start": 0, "end": 50 }] },
            { "lines": [{ "start": 50, "end": 100 }] }
        ]
    }
}
```

**Annotation Type Request Bodies:**

_Segmentation:_

```json
{
    "segmentation": {
        "segments": [
            { "lines": [{ "start": 0, "end": 50 }] }
        ]
    }
}
```

_Alignment:_

```json
{
    "alignment": {
        "target_id": "M87654321",
        "target_segments": [
            { "lines": [{ "start": 0, "end": 30 }] }
        ],
        "aligned_segments": [
            { "lines": [{ "start": 0, "end": 25 }], "alignment_indices": [0] }
        ]
    }
}
```

_Pagination:_

```json
{
    "pagination": {
        "volume": {
            "index": 1,
            "pages": [
                {
                    "reference": "folio_1a",
                    "lines": [{ "start": 0, "end": 500 }]
                }
            ]
        }
    }
}
```

_Bibliographic Metadata:_

```json
{
    "bibliographic_metadata": [
        { "span": { "start": 5000, "end": 5500 }, "type": "colophon" }
    ]
}
```

_Durchen Notes:_

```json
{
    "durchen_notes": [
        { "span": { "start": 100, "end": 150 }, "text": "Variant reading" }
    ]
}
```

---

#### 4. Getting All Annotations for an Edition

**New API:**

```
GET /v2/editions/{edition_id}/annotations
GET /v2/editions/{edition_id}/annotations?type=segmentation&type=pagination
```

**Query Parameters:**

- `type` (optional, repeatable): Filter by annotation type(s)
  - Valid values: `segmentation`, `alignment`, `pagination`, `bibliography`,
    `durchen`
  - If not specified, returns all annotation types

**Response:**

```json
{
  "segmentations": [...],
  "alignments": [...],
  "pagination": {...},
  "bibliographic_metadata": [...],
  "durchen_notes": [...]
}
```

Fields are `null` if no annotations of that type exist or if filtered out.

---

#### 5. Deleting Annotations (NEW)

**New API:** Type-specific DELETE endpoints are now available:

| Annotation Type | DELETE Endpoint                                           |
| --------------- | --------------------------------------------------------- |
| Segmentation    | `DELETE /v2/annotations/segmentation/{segmentation_id}`   |
| Alignment       | `DELETE /v2/annotations/alignment/{alignment_id}`         |
| Pagination      | `DELETE /v2/annotations/pagination/{pagination_id}`       |
| Durchen (Notes) | `DELETE /v2/annotations/durchen/{note_id}`                |
| Bibliographic   | `DELETE /v2/annotations/bibliographic/{bibliographic_id}` |

**Response (200 OK):**

```json
{
    "message": "Segmentation deleted successfully"
}
```

**Note:** The message varies based on annotation type.

---

#### 6. Updating Annotations (REMOVED)

**Old API (REMOVED):**

```
PUT /v2/annotations/{annotation_id}/annotation
```

This endpoint no longer exists. To update annotations:

1. Delete the existing annotation using the type-specific DELETE endpoint
2. Create a new annotation with the updated data

---

### Terminology Changes

| Old Term         | New Term        |
| ---------------- | --------------- |
| `instance`       | `edition`       |
| `instance_id`    | `edition_id`    |
| `/v2/instances/` | `/v2/editions/` |

---

### Removed Annotation Types

The following annotation types mentioned in the old OpenAPI spec are **not
currently implemented** as individual GET endpoints:

- `table_of_contents`
- `search_segmentation`

---

### Quick Reference

| Action               | Old Endpoint                                    | New Endpoint                                 |
| -------------------- | ----------------------------------------------- | -------------------------------------------- |
| Get segmentation     | `GET /v2/annotations/{id}`                      | `GET /v2/annotations/segmentation/{id}`      |
| Get alignment        | `GET /v2/annotations/{id}`                      | `GET /v2/annotations/alignment/{id}`         |
| Get pagination       | `GET /v2/annotations/{id}`                      | `GET /v2/annotations/pagination/{id}`        |
| Get durchen          | `GET /v2/annotations/{id}`                      | `GET /v2/annotations/durchen/{id}`           |
| Get bibliographic    | `GET /v2/annotations/{id}`                      | `GET /v2/annotations/bibliographic/{id}`     |
| Delete segmentation  | N/A                                             | `DELETE /v2/annotations/segmentation/{id}`   |
| Delete alignment     | N/A                                             | `DELETE /v2/annotations/alignment/{id}`      |
| Delete pagination    | N/A                                             | `DELETE /v2/annotations/pagination/{id}`     |
| Delete durchen       | N/A                                             | `DELETE /v2/annotations/durchen/{id}`        |
| Delete bibliographic | N/A                                             | `DELETE /v2/annotations/bibliographic/{id}`  |
| Add annotation       | `POST /v2/annotations/{instance_id}/annotation` | `POST /v2/editions/{edition_id}/annotations` |
| Get all annotations  | N/A                                             | `GET /v2/editions/{edition_id}/annotations`  |
| Update annotation    | `PUT /v2/annotations/{id}/annotation`           | **REMOVED** (use delete + add)               |

---

## Categories Endpoint Migration

### Summary of Changes

The categories API has been updated for consistency with the rest of the API.

1. **Field naming standardized** to use `parent_id` instead of `parent`
2. **Response structure updated** to match actual Pydantic models
3. **`language` query parameter removed** from GET endpoint

---

### Breaking Changes

#### 1. GET /v2/categories Response Structure

**Old Response:**

```json
[
    {
        "id": "CAT12345678",
        "parent": null,
        "title": "Literature",
        "has_child": true
    }
]
```

**New Response:**

```json
[
    {
        "id": "CAT12345678",
        "parent_id": null,
        "title": {
            "en": "Literature",
            "bo": "རྩོམ་རིག"
        },
        "has_children": true
    }
]
```

**Key Changes:**

- `parent` → `parent_id`
- `has_child` → `has_children`
- `title` is now a localized object `{lang: text}` instead of a single string
- `application` field is **only in request**, not returned in response

---

#### 2. POST /v2/categories Request Body

**Old Request:**

```json
{
    "application": "webuddhist",
    "title": {
        "en": "Poetry",
        "bo": "སྙན་ངག"
    },
    "parent": "CAT12345678"
}
```

**New Request:**

```json
{
    "application": "webuddhist",
    "title": {
        "en": "Poetry",
        "bo": "སྙན་ངག"
    },
    "parent_id": "CAT12345678"
}
```

**Key Change:** `parent` → `parent_id`

---

#### 3. POST /v2/categories Response

**Old Response:**

```json
{
    "id": "CAT87654321",
    "application": "webuddhist",
    "title": {
        "en": "Poetry",
        "bo": "སྙན་ངག"
    },
    "parent": "CAT12345678"
}
```

**New Response:**

```json
{
    "id": "CAT87654321"
}
```

The response now only returns the created category ID.

---

#### 4. Removed Query Parameter

The `language` query parameter has been removed from `GET /v2/categories`. All
localized titles are now returned in the response as a localized object.

---

### Quick Reference

| Action          | Old Field/Param | New Field/Param |
| --------------- | --------------- | --------------- |
| Parent ID       | `parent`        | `parent_id`     |
| Has children    | `has_child`     | `has_children`  |
| Title format    | `string`        | `{lang: text}`  |
| Language filter | `?language=bo`  | **REMOVED**     |

---

## Texts Endpoint Migration

### Summary of Changes

The texts API has been updated for consistency and to match actual Pydantic
models.

1. **Query parameters updated** - removed `type` and `author`; `category_id` is
   now available
2. **Request/Response structure updated** - uses
   `commentary_of`/`translation_of` instead of `type`/`target`
3. **Response includes related IDs** - `commentaries`, `translations`,
   `editions` arrays
4. **Copyright/License values use snake_case** - e.g., `public_domain` instead
   of `Public domain`

---

### Breaking Changes

#### 1. GET /v2/texts Query Parameters

**Removed Parameters:**

- `type` - No longer supported for filtering
- `author` - No longer supported for filtering

**Added Parameters:**

- `category_id` - Filter by category ID

**Current Parameters:**

| Parameter     | Type    | Description                             |
| ------------- | ------- | --------------------------------------- |
| `limit`       | integer | Number of results per page (default 20) |
| `offset`      | integer | Number of results to skip (default 0)   |
| `language`    | string  | Filter by language code                 |
| `title`       | string  | Filter by title (case-insensitive)      |
| `category_id` | string  | Filter by category ID                   |

---

#### 2. POST /v2/texts Request Body

**Old Request:**

```json
{
    "type": "translation",
    "title": { "en": "English Translation" },
    "language": "en",
    "target": "T12345678",
    "contributions": [{ "person_id": "P123", "role": "translator" }],
    "category_id": "CAT123",
    "copyright": "Public domain",
    "license": "CC0"
}
```

**New Request:**

```json
{
    "title": { "en": "English Translation" },
    "language": "en",
    "translation_of": "T12345678",
    "contributions": [{ "person_id": "P123", "role": "translator" }],
    "category_id": "CAT123",
    "license": "cc0"
}
```

**Key Changes:**

- `type` field **REMOVED** - type is inferred from `commentary_of` or
  `translation_of`
- `target` → `commentary_of` or `translation_of` (depending on relationship
  type)
- `copyright` field **REMOVED** - use `license` instead
- `license` values: `"cc0"`, `"public"`, `"cc-by"`, `"cc-by-sa"`,
  `"copyrighted"`, `"unknown"`, etc.

---

#### 3. POST /v2/texts Response

**Old Response:**

```json
{
    "message": "Text created successfully",
    "id": "T12345678"
}
```

**New Response:**

```json
{
    "id": "T12345678"
}
```

Response now only returns the created ID.

---

#### 4. GET Response Structure

**Old Response:**

```json
{
    "id": "T12345678",
    "type": "root",
    "title": { "en": "Sample Text" },
    "target": null,
    "copyright": "Public domain",
    "license": "CC0"
}
```

**New Response:**

```json
{
    "id": "T12345678",
    "title": { "en": "Sample Text" },
    "language": "bo",
    "category_id": "CAT123",
    "commentary_of": null,
    "translation_of": null,
    "license": "public",
    "commentaries": ["C123"],
    "translations": ["TR123"],
    "editions": ["M123"],
    "contributions": [...]
}
```

**Key Changes:**

- `type` field **REMOVED** - determine type from
  `commentary_of`/`translation_of` presence
- `target` → `commentary_of` or `translation_of`
- `copyright` field **REMOVED** - use `license` instead
- Added `commentaries`, `translations`, `editions` arrays with related IDs

---

### Quick Reference

| Action               | Old Field/Value  | New Field/Value                     |
| -------------------- | ---------------- | ----------------------------------- |
| Text type            | `type: "root"`   | `commentary_of` or `translation_of` |
| Commentary relation  | `target: "T123"` | `commentary_of: "T123"`             |
| Translation relation | `target: "T123"` | `translation_of: "T123"`            |
| Copyright field      | `copyright`      | **REMOVED** (use `license`)         |
| License type         | `"CC0"`          | `"cc0"`                             |
| Filter by type       | `?type=root`     | **REMOVED**                         |
| Filter by author     | `?author=name`   | **REMOVED**                         |
| Filter by category   | N/A              | `?category_id=CAT123`               |

---

## Editions Endpoint Migration

### Summary of Changes

The editions API (formerly "instances") has been significantly restructured:

1. **URL path renamed** - `/v2/instances/...` → `/v2/editions/...`
2. **Parameter renamed** - `instance_id` → `edition_id`
3. **Tag renamed** - `Instances` → `Editions`
4. **Type field renamed** - `instance_type` → `edition_type`
5. **Translation/Commentary endpoints REMOVED** - use separate text + edition
   creation instead
6. **New endpoints added** - `/metadata`, `/content`

---

### Breaking Changes

#### 1. Translation/Commentary Endpoints REMOVED

The old endpoints that created both text and edition in one call have been
removed:

| Old Endpoint (REMOVED)                | New Workflow                                    |
| ------------------------------------- | ----------------------------------------------- |
| `POST /v2/instances/{id}/translation` | 1. `POST /v2/texts` with `translation_of` field |
|                                       | 2. `POST /v2/texts/{text_id}/editions`          |
| `POST /v2/instances/{id}/commentary`  | 1. `POST /v2/texts` with `commentary_of` field  |
|                                       | 2. `POST /v2/texts/{text_id}/editions`          |

**Old Workflow (single call):**

```json
POST /v2/instances/{instance_id}/translation
{
    "language": "en",
    "content": "Translated text...",
    "title": "English Translation",
    "copyright": "Public domain",
    "license": "CC0",
    "segmentation": [...],
    "author": { "person_id": "P123" }
}
```

**New Workflow (two calls):**

```json
// Step 1: Create the text with translation_of relationship
POST /v2/texts
{
    "title": { "en": "English Translation" },
    "language": "en",
    "translation_of": "T12345678",
    "contributions": [{ "person_id": "P123", "role": "translator" }],
    "category_id": "CAT123",
    "license": "cc0"
}
// Response: { "id": "T87654321" }

// Step 2: Create the edition for that text
POST /v2/texts/T87654321/editions
{
    "metadata": { "type": "critical" },
    "content": "Translated text...",
    "segmentation": [...]
}
```

---

#### 2. Endpoint Changes

| Old Path                                           | New Path                                         |
| -------------------------------------------------- | ------------------------------------------------ |
| `GET /v2/instances/{instance_id}`                  | `GET /v2/editions/{edition_id}/metadata`         |
| N/A                                                | `GET /v2/editions/{edition_id}/content`          |
| `GET /v2/instances/{instance_id}/annotations`      | `GET /v2/editions/{edition_id}/annotations`      |
| `POST /v2/instances/{instance_id}/annotations`     | `POST /v2/editions/{edition_id}/annotations`     |
| `GET /v2/instances/{instance_id}/related`          | `GET /v2/editions/{edition_id}/related`          |
| `GET /v2/instances/{instance_id}/segments/related` | `GET /v2/editions/{edition_id}/segments/related` |
| N/A                                                | `DELETE /v2/editions/{edition_id}`               |

---

#### 4. Parameter and Field Naming

| Old Value       | New Value      |
| --------------- | -------------- |
| `instance_id`   | `edition_id`   |
| `instance_type` | `edition_type` |

---

### Quick Reference

| Action             | Old                                | New                                         |
| ------------------ | ---------------------------------- | ------------------------------------------- |
| Create translation | `POST /instances/{id}/translation` | `POST /texts` + `POST /texts/{id}/editions` |
| Create commentary  | `POST /instances/{id}/commentary`  | `POST /texts` + `POST /texts/{id}/editions` |
| Get metadata       | `GET /instances/{id}`              | `GET /editions/{id}/metadata`               |
| Get content        | N/A                                | `GET /editions/{id}/content`                |
| Delete edition     | N/A                                | `DELETE /editions/{id}`                     |
| Path parameter     | `instance_id`                      | `edition_id`                                |
| Type field         | `instance_type`                    | `edition_type`                              |
| API tag            | `Instances`                        | `Editions`                                  |

---

## Persons Endpoint Migration

### Summary of Changes

The persons endpoint has minor updates to response format consistency.

1. **POST response simplified** - now returns only `{"id": "..."}` instead of
   `{"message": "...", "_id": "..."}`
2. **Schema definitions added** - `PersonInput` and `PersonOutput` schemas now
   defined

---

### Breaking Changes

#### 1. POST /v2/persons Response

**Old Response:**

```json
{
    "message": "Person created successfully",
    "_id": "P12345678"
}
```

**New Response:**

```json
{
    "id": "P12345678"
}
```

---

### Quick Reference

| Action          | Old                                 | New              |
| --------------- | ----------------------------------- | ---------------- |
| Create response | `{"message": "...", "_id": "P123"}` | `{"id": "P123"}` |

---

## Segments Endpoint Migration

### Summary of Changes

The segments endpoint has been updated with simplified response formats.

1. **`/related` response simplified** - now returns flat array of
   `SegmentOutput` instead of `{targets: [], sources: []}`
2. **`/content` response simplified** - now returns string directly instead of
   `{content: "..."}`
3. **`/batch-overlapping` endpoint REMOVED** - no longer available

---

### Breaking Changes

#### 1. GET /v2/segments/{segment_id}/related Response

**Old Response:**

```json
{
    "targets": [
        {
            "instance": { "id": "M123", "type": "critical" },
            "text": { "id": "E123", "title": {...} },
            "segments": [{ "id": "SEG001", "span": { "start": 0, "end": 100 } }]
        }
    ],
    "sources": []
}
```

**New Response:**

```json
[
    {
        "id": "SEG001",
        "manifestation_id": "M12345678",
        "text_id": "E12345678",
        "lines": [{ "start": 0, "end": 100 }]
    },
    {
        "id": "SEG002",
        "manifestation_id": "M12345678",
        "text_id": "E12345678",
        "lines": [{ "start": 100, "end": 200 }]
    }
]
```

---

#### 2. GET /v2/segments/{segment_id}/content Response

**Old Response:**

```json
{
    "content": "This is the text content of the segment."
}
```

**New Response:**

```json
"This is the text content of the segment."
```

---

#### 3. POST /v2/segments/batch-overlapping REMOVED

This endpoint has been removed and is no longer available.

---

### Quick Reference

| Action            | Old                                | New                       |
| ----------------- | ---------------------------------- | ------------------------- |
| Get related       | `{targets: [], sources: []}`       | `[SegmentOutput, ...]`    |
| Get content       | `{content: "..."}`                 | `"..."` (string directly) |
| Batch overlapping | `POST /segments/batch-overlapping` | **REMOVED**               |

---
