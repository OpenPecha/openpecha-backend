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
| Segmentation    | `GET /v2/segmentations/{segmentation_id}`               |
| Segmentation segments | `GET /v2/segmentations/{segmentation_id}/segments` |
| Text-pair alignment | `GET /v2/texts/{source_text_id}/alignments/{target_text_id}` |
| Pagination      | `GET /v2/paginations/{pagination_id}`                   |
| Durchen (Notes) | `GET /v2/durchens/{note_id}`                           |
| Bibliographic   | `GET /v2/bibliographic/{bibliographic_id}`             |

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
    "edition_id": "M12345678",
    "text_id": "E12345678"
}
```

Segmentation segments are paginated separately:

```json
// GET /v2/segmentations/{segmentation_id}/segments?limit=500&offset=0
{
    "items": [
        {
            "id": "segment_001",
            "lines": [
                { "start": 0, "end": 50 }
            ]
        }
    ],
    "has_more": false,
    "offset": 0,
    "limit": 500
}
```

**Text-Pair Alignment Response:**

```json
{
    // GET /v2/texts/{source_text_id}/alignments/{target_text_id}?limit=500&offset=0
    "items": [
        {
            "source_segment": {
                "id": "source_seg_001",
                "segmentation_id": "source_sgn_abc123",
                "edition_id": "M12345678",
                "text_id": "E12345678",
                "lines": [{ "start": 0, "end": 25 }],
                "tag_ids": null
            },
            "target_segment": {
                "id": "target_seg_001",
                "segmentation_id": "target_sgn_abc123",
                "edition_id": "M87654321",
                "text_id": "E87654321",
                "lines": [{ "start": 0, "end": 30 }],
                "tag_ids": null
            }
        }
    ],
    "has_more": false,
    "offset": 0,
    "limit": 500
}
```

**Pagination Response:**

```json
{
    "id": "pag_abc123",
    "edition_id": "M12345678",
    "text_id": "E12345678",
    "volumes": [{
        "pages": [
            {
                "reference": "folio_1a",
                "lines": [{ "start": 0, "end": 500 }]
            }
        ],
        "metadata": null
    }],
    "metadata": null
}
```

**Durchen (Note) Response:**

```json
{
    "id": "note_abc123",
    "edition_id": "M12345678",
    "text_id": "E12345678",
    "span": { "start": 100, "end": 150 },
    "text": "Variant reading found in manuscript B",
    "metadata": null
}
```

**Bibliographic Metadata Response:**

```json
{
    "id": "bib_abc123",
    "edition_id": "M12345678",
    "text_id": "E12345678",
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

**New API:** Use type-specific endpoints for each annotation type:

| Annotation Type | New Endpoint                                           |
| --------------- | ------------------------------------------------------ |
| Segmentation    | `POST /v2/editions/{edition_id}/segmentations`        |
| Text-pair alignment | `PUT /v2/texts/{source_text_id}/alignments/{target_text_id}` |
| Pagination      | `POST /v2/editions/{edition_id}/pagination`            |
| Durchen (Notes) | `POST /v2/editions/{edition_id}/durchens`             |
| Bibliographic   | `POST /v2/editions/{edition_id}/bibliographic`         |

**Key Changes:**

- Path changed from `/v2/annotations/` to `/v2/editions/`
- Parameter renamed from `instance_id` to `edition_id`
- Each annotation type now has its own dedicated endpoint
- Request body contains the annotation data directly (no wrapper object)
- Response returns `{"id": "..."}` with single ID instead of array

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

**New Request Bodies:**

_Segmentation:_

```json
POST /v2/editions/{edition_id}/segmentations
{
    "segments": [
        { "lines": [{ "start": 0, "end": 50 }] },
        { "lines": [{ "start": 50, "end": 100 }] }
    ]
}
```

_Text-pair alignment:_

```json
PUT /v2/texts/{source_text_id}/alignments/{target_text_id}
{
    "alignments": [
        {
            "source_segment_id": "source_seg_001",
            "target_segment_id": "target_seg_001"
        }
    ]
}
```

_Pagination:_

```json
POST /v2/editions/{edition_id}/pagination
{
    "volumes": [{
        "pages": [
            {
                "reference": "folio_1a",
                "lines": [{ "start": 0, "end": 500 }]
            }
        ]
    }]
}
```

_Bibliographic Metadata:_

```json
POST /v2/editions/{edition_id}/bibliographic
{
    "span": { "start": 5000, "end": 5500 },
    "type": "colophon"
}
```

_Durchen Notes:_

```json
POST /v2/editions/{edition_id}/durchens
{
    "span": { "start": 100, "end": 150 },
    "text": "Variant reading"
}
```

**Response (201 Created):**

```json
{
    "id": "ann_abc123"
}
```

---

#### 4. Getting All Annotations for an Edition

**New API:** Use type-specific endpoints for each annotation type:

| Annotation Type | New Endpoint                                           |
| --------------- | ------------------------------------------------------ |
| Segmentation    | `GET /v2/editions/{edition_id}/segmentations`          |
| Pagination      | `GET /v2/editions/{edition_id}/pagination`             |
| Durchen (Notes) | `GET /v2/editions/{edition_id}/durchens`               |
| Bibliographic   | `GET /v2/editions/{edition_id}/bibliographic`          |

**Response:** Each endpoint returns an array of that annotation type:

```json
// GET /v2/editions/{edition_id}/segmentations
[
    {
        "id": "seg_abc123",
        "edition_id": "M12345678",
        "text_id": "E12345678"
    }
]
// GET /v2/editions/{edition_id}/pagination
{
    "id": "pag_abc123",
    "edition_id": "M12345678",
    "text_id": "E12345678",
    "volumes": [{
        "pages": [...]
    }],
    "metadata": null
}

// GET /v2/editions/{edition_id}/bibliographic
[
    {
        "id": "bib_abc123",
        "edition_id": "M12345678",
        "text_id": "E12345678",
        "span": { "start": 5000, "end": 5500 },
        "type": "colophon",
        "metadata": null
    }
]

// GET /v2/editions/{edition_id}/durchens
[
    {
        "id": "note_abc123",
        "edition_id": "M12345678",
        "text_id": "E12345678",
        "span": { "start": 100, "end": 150 },
        "text": "Variant reading",
        "metadata": null
    }
]
```

**Key Changes:**

- Each annotation type now has its own dedicated endpoint
- No more generic endpoint that returns all types in one response
- Pagination returns a single object (not array) since only one pagination per edition
- Bibliographic and Durchen return arrays (multiple can exist per edition)

---

#### 5. Deleting Annotations (NEW)

**New API:** Type-specific DELETE endpoints are now available:

| Annotation Type | DELETE Endpoint                                           |
| --------------- | --------------------------------------------------------- |
| Segmentation    | `DELETE /v2/segmentations/{segmentation_id}`               |
| Text-pair alignment | `DELETE /v2/texts/{source_text_id}/alignments/{target_text_id}` |
| Pagination      | `DELETE /v2/paginations/{pagination_id}`                   |
| Durchen (Notes) | `DELETE /v2/durchens/{note_id}`                           |
| Bibliographic   | `DELETE /v2/bibliographic/{bibliographic_id}`             |

**Response:** `204 No Content` (empty body)

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
| Get segmentation     | `GET /v2/annotations/{id}`                      | `GET /v2/segmentations/{id}`                 |
| Get segmentation segments | N/A                                        | `GET /v2/segmentations/{id}/segments`        |
| Get text-pair alignments | N/A                                      | `GET /v2/texts/{source_text_id}/alignments/{target_text_id}` |
| Get pagination       | `GET /v2/annotations/{id}`                      | `GET /v2/paginations/{id}`                   |
| Get durchen          | `GET /v2/annotations/{id}`                      | `GET /v2/durchens/{id}`                      |
| Get bibliographic    | `GET /v2/annotations/{id}`                      | `GET /v2/bibliographic/{id}`                 |
| Delete segmentation  | N/A                                             | `DELETE /v2/segmentations/{id}`               |
| Delete text-pair alignments | N/A                                      | `DELETE /v2/texts/{source_text_id}/alignments/{target_text_id}` |
| Delete pagination    | N/A                                             | `DELETE /v2/paginations/{id}`                 |
| Delete durchen       | N/A                                             | `DELETE /v2/durchens/{id}`                    |
| Delete bibliographic | N/A                                             | `DELETE /v2/bibliographic/{id}`               |
| Add segmentation     | `POST /v2/annotations/{instance_id}/annotation` | `POST /v2/editions/{edition_id}/segmentations`|
| Replace text-pair alignments | N/A                                      | `PUT /v2/texts/{source_text_id}/alignments/{target_text_id}` |
| Add pagination       | `POST /v2/annotations/{instance_id}/annotation` | `POST /v2/editions/{edition_id}/pagination`   |
| Add durchen          | `POST /v2/annotations/{instance_id}/annotation` | `POST /v2/editions/{edition_id}/durchens`     |
| Add bibliographic    | `POST /v2/annotations/{instance_id}/annotation` | `POST /v2/editions/{edition_id}/bibliographic` |
| Get all segmentations| N/A                                             | `GET /v2/editions/{edition_id}/segmentations` |
| Get all pagination   | N/A                                             | `GET /v2/editions/{edition_id}/pagination`    |
| Get all durchen      | N/A                                             | `GET /v2/editions/{edition_id}/durchens`      |
| Get all bibliographic| N/A                                             | `GET /v2/editions/{edition_id}/bibliographic`  |
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
        "children": ["CAT456", "CAT789"]
    }
]
```

**Key Changes:**

- `parent` → `parent_id`
- `has_child` → `children` (list of child category IDs)
- `title` is now a localized object `{lang: text}` instead of a single string
- `application` field moved from request body to `X-Application` header
- `description` field added (optional, localized object)

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
    "title": {
        "en": "Poetry",
        "bo": "སྙན་ངག"
    },
    "description": {
        "en": "Tibetan poetry collection"
    },
    "parent_id": "CAT12345678"
}
```

The `X-Application` header is required (application is no longer in the body).

**Key Changes:**

- `parent` → `parent_id`
- `application` moved from request body to `X-Application` header
- `description` field added (optional, localized object)

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
| Has children    | `has_child`     | `children` (list) |
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
| `tag_id`      | string  | Filter by tag ID                        |
| `author_id`   | string  | Filter by author (person) ID            |
| `bdrc`        | string  | Filter by BDRC ID                       |
| `wiki`        | string  | Filter by Wikidata ID                   |

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
    "contributions": [...],
    "tag_ids": ["TAG123"]
}
```

**Key Changes:**

- `type` field **REMOVED** - determine type from
  `commentary_of`/`translation_of` presence
- `target` → `commentary_of` or `translation_of`
- `copyright` field **REMOVED** - use `license` instead
- Added `commentaries`, `translations`, `editions` arrays with related IDs
- Added `tag_ids` array with associated tag IDs

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
| Filter by BDRC ID    | N/A              | `?bdrc=W123456`                     |
| Filter by Wiki ID    | N/A              | `?wiki=Q123456`                     |

---

### GET /v2/texts/{text_id} - BDRC ID Lookup Removed

The `GET /v2/texts/{text_id}` endpoint no longer accepts BDRC IDs as the path
parameter. Previously, you could pass either an expression ID or a BDRC ID:

**Old Behavior (REMOVED):**

```
GET /v2/texts/W123456  # Would find text by BDRC ID
```

**New Behavior:**

To find a text by BDRC ID, use the list endpoint with the `bdrc` filter:

```
GET /v2/texts?bdrc=W123456
```

This returns an array of matching texts (typically one result).

**Migration Steps:**

1. If you were using `GET /v2/texts/{bdrc_id}`, change to
   `GET /v2/texts?bdrc={bdrc_id}`
2. Update your response handling - the new endpoint returns an array, not a
   single object
3. Access the first element of the array: `response[0]`

---

### PATCH /v2/texts/{text_id} (NEW)

A new PATCH endpoint has been added for partial updates to texts. This replaces
the separate `PUT /v2/texts/{text_id}/title` and
`PUT /v2/texts/{text_id}/license` endpoints which have been **REMOVED**.

**Request Body:**

```json
{
    "title": { "en": "Updated Title", "bo": "གསར་བསྒྱུར་མཚན་བྱང་།" },
    "alt_titles": [{ "en": "Alternative Title" }],
    "bdrc": "W654321",
    "wiki": "Q654321",
    "date": "2025-01-01",
    "language": "bo",
    "category_id": "CAT456",
    "license": "cc0",
    "tag_ids": ["TAG123", "TAG456"]
}
```

All fields are optional, but at least one must be provided. Only provided fields
will be updated; omitted fields retain their current values.

**Response:** Returns the full updated text object (same as GET response).

**Removed Endpoints:**

| Old Endpoint                 | New Approach                                |
| ---------------------------- | ------------------------------------------- |
| `PUT /v2/texts/{id}/title`   | `PATCH /v2/texts/{id}` with `title` field   |
| `PUT /v2/texts/{id}/license` | `PATCH /v2/texts/{id}` with `license` field |

---

### Text Tags (NEW)

Tag management endpoints for texts have been added.

**New Endpoints:**

| Method | Endpoint                                 | Description           |
| ------ | ---------------------------------------- | --------------------- |
| POST   | `/v2/texts/{text_id}/tags/{tag_id}`      | Add tag to text       |
| DELETE | `/v2/texts/{text_id}/tags/{tag_id}`      | Remove tag from text  |

**Response:** `204 No Content` (empty body)

The `tag_ids` field is included in the GET `/v2/texts/{text_id}` response.

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
6. **New endpoints added**

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
| `GET /v2/instances/{instance_id}`                  | `GET /v2/editions/{edition_id}`         |
| N/A                                                | `GET /v2/editions/{edition_id}/content`          |
|                                                    | Optional: `?span_start=0&span_end=100`           |
| `GET /v2/instances/{instance_id}/annotations`      | `GET /v2/editions/{edition_id}/annotations`      |
| `POST /v2/instances/{instance_id}/annotations`     | `POST /v2/editions/{edition_id}/annotations`     |
| `GET /v2/instances/{instance_id}/related`          | `GET /v2/editions/{edition_id}/related`          |
| N/A                                                | `DELETE /v2/editions/{edition_id}`               |

---

#### 3. Patch Edition Content (NEW)

A new PATCH endpoint has been added for applying text operations to edition content.

**New Endpoint:**

```
PATCH /v2/editions/{edition_id}/content
```

**Request Body (discriminated by `type`):**

_Insert:_

```json
{
    "type": "insert",
    "position": 100,
    "text": "inserted text"
}
```

_Delete:_

```json
{
    "type": "delete",
    "start": 100,
    "end": 200
}
```

_Replace:_

```json
{
    "type": "replace",
    "start": 100,
    "end": 200,
    "text": "replacement text"
}
```

Operations supported:
- `insert` - Insert text at a position
- `delete` - Delete text from start to end position
- `replace` - Replace text from start to end with new text

**Response:** `204 No Content` (empty body)

Text operations automatically adjust annotation spans to maintain consistency.

---

#### 4. Parameter and Field Naming

| Old Value       | New Value      |
| --------------- | -------------- |
| `instance_id`   | `edition_id`   |
| `instance_type` | `edition_type` |

---

### Quick Reference

| Action             | Old                                | New                                              |
| ------------------ | ---------------------------------- | ------------------------------------------------ |
| Create translation | `POST /instances/{id}/translation` | `POST /texts` + `POST /texts/{text_id}/editions` |
| Create commentary  | `POST /instances/{id}/commentary`  | `POST /texts` + `POST /texts/{text_id}/editions` |
| Get metadata       | `GET /instances/{id}`              | `GET /editions/{edition_id}`            |
| Get content        | N/A                                | `GET /editions/{edition_id}/content`             |
| Patch edition content | N/A                                              | `PATCH /v2/editions/{edition_id}/content`    |
| Delete edition     | N/A                                | `DELETE /v2/editions/{edition_id}`               |
| Path parameter     | `instance_id`                      | `edition_id`                                     |
| Type field         | `instance_type`                    | `edition_type`                                   |
| API tag            | `Instances`                        | `Editions`                                       |

---

## Persons Endpoint Migration

### Summary of Changes

The persons endpoint has been updated with new endpoints and response format
changes.

1. **POST response simplified** - now returns only `{"id": "..."}` instead of
   `{"message": "...", "_id": "..."}`
2. **Schema definitions added** - `PersonInput` and `PersonOutput` schemas now
   defined
3. **GET endpoints added** - retrieve single person or paginated list
4. **PATCH endpoint added** - partial update of person data

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

### New Endpoints

#### GET /v2/persons/{person_id}

Retrieve a single person by ID.

**Response:**

```json
{
    "id": "P12345678",
    "bdrc": "P123",
    "wiki": "Q123",
    "name": { "bo": "རིན་ཆེན་སྡེ།", "en": "Rinchen De" },
    "alt_names": [{ "bo": "རིན་ཆེན་སྡེ་བ།" }]
}
```

---

#### GET /v2/persons

Retrieve a paginated list of persons with optional filters.

**Query Parameters:**

| Parameter | Type    | Description                             |
| --------- | ------- | --------------------------------------- |
| `limit`   | integer | Number of results per page (default 20) |
| `offset`  | integer | Number of results to skip (default 0)   |
| `name`    | string  | Filter by person name                   |
| `bdrc`    | string  | Filter by BDRC ID                       |
| `wiki`    | string  | Filter by Wikidata ID                   |

**Response:** Array of `PersonOutput` objects (same structure as GET by ID).

---

#### PATCH /v2/persons/{person_id}

Partially update a person. All fields are optional, but at least one must be
provided.

**Request Body:**

```json
{
    "name": { "bo": "Updated Name" },
    "alt_names": [{ "en": "New Alt Name" }],
    "bdrc": "P456",
    "wiki": "Q456"
}
```

**Response:** Returns the full updated person object.

---

### Quick Reference

| Action          | Old                                 | New                               |
| --------------- | ----------------------------------- | --------------------------------- |
| Create response | `{"message": "...", "_id": "P123"}` | `{"id": "P123"}`                 |
| Get by ID       | N/A                                 | `GET /v2/persons/{person_id}`     |
| List all        | N/A                                 | `GET /v2/persons`                 |
| Update          | N/A                                 | `PATCH /v2/persons/{person_id}`   |

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
        "edition_id": "M12345678",
        "text_id": "E12345678",
        "lines": [{ "start": 0, "end": 100 }],
        "tag_ids": []
    },
    {
        "id": "SEG002",
        "edition_id": "M12345678",
        "text_id": "E12345678",
        "lines": [{ "start": 100, "end": 200 }],
        "tag_ids": ["TAG123"]
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

### Segment Tags (NEW)

Tag management endpoints for segments have been added.

**New Endpoints:**

| Method | Endpoint                                      | Description             |
| ------ | --------------------------------------------- | ----------------------- |
| POST   | `/v2/segments/{segment_id}/tags/{tag_id}`     | Add tag to segment      |
| DELETE | `/v2/segments/{segment_id}/tags/{tag_id}`     | Remove tag from segment |

**Response:** `204 No Content` (empty body)

The `tag_ids` field is included in `SegmentOutput` responses.

---

### Segment Search (NEW)

A search endpoint for segments has been added, proxying to an external search
API.

**Endpoint:**

```
GET /v2/segments/search?query=your+search+text
```

**Query Parameters:**

| Parameter     | Type    | Description                              |
| ------------- | ------- | ---------------------------------------- |
| `query`       | string  | Search query (required)                  |
| `search_type` | string  | Type of search (default `"semantic"`)    |
| `limit`       | integer | Max results (default 10, max 100)        |
| `return_text` | boolean | Include text content (default `true`)    |
| `title`       | string  | Filter by title                          |

**Response:**

```json
{
    "query": "search text",
    "results": [
        {
            "id": "SEG001",
            "distance": 0.85,
            "entity": {},
            "segmentation_ids": ["SEGMENTATION_001"]
        }
    ],
    "count": 1
}
```

---

### Quick Reference

| Action            | Old                                | New                       |
| ----------------- | ---------------------------------- | ------------------------- |
| Get related       | `{targets: [], sources: []}`       | `[RelatedSegmentsOutput, ...]` (see below) |
| Get content       | `{content: "..."}`                 | `"..."` (string directly) |
| Batch overlapping | `POST /segments/batch-overlapping` | **REMOVED**               |
| Add tag to segment  | N/A                                | `POST /v2/segments/{id}/tags/{tag_id}`   |
| Remove tag from segment | N/A                            | `DELETE /v2/segments/{id}/tags/{tag_id}` |
| Add tag to text     | N/A                                | `POST /v2/texts/{id}/tags/{tag_id}`      |
| Remove tag from text | N/A                               | `DELETE /v2/texts/{id}/tags/{tag_id}`    |
| Search segments     | N/A                                | `GET /v2/segments/search?query=...`      |

---

## Related Segments API Redesign

### Summary of Changes

The related segments endpoint has been redesigned to support **transitive
traversal** across the full alignment tree. Previously, this endpoint only
returned directly aligned segments (one hop). Now it traverses multiple hops
in both directions (up toward root texts, down toward translations/commentaries),
bridging between alignment and display segmentations via character span overlap.

**Affected endpoint:**

- `GET /v2/segments/{segment_id}/related`

---

### Breaking Changes

#### 1. Response Structure Changed

The endpoint now returns `list[RelatedSegmentsOutput]` instead of
`list[SegmentOutput]`.

Results are grouped by edition, and within each edition by segmentation
(since an edition can have multiple display segmentations).

**Old Response:**

```json
[
    {
        "id": "SEG001",
        "edition_id": "M12345678",
        "text_id": "E12345678",
        "lines": [{ "start": 0, "end": 100 }],
        "tag_ids": []
    },
    {
        "id": "SEG002",
        "edition_id": "M12345678",
        "text_id": "E12345678",
        "lines": [{ "start": 100, "end": 200 }],
        "tag_ids": ["TAG123"]
    }
]
```

**New Response:**

```json
[
    {
        "edition_id": "M12345678",
        "text_id": "E12345678",
        "segmentations": [
            {
                "segmentation_id": "SGN_ABC",
                "segments": [
                    {
                        "id": "SEG001",
                        "lines": [{ "start": 0, "end": 100 }],
                        "tag_ids": null
                    },
                    {
                        "id": "SEG002",
                        "lines": [{ "start": 100, "end": 200 }],
                        "tag_ids": ["TAG123"]
                    }
                ]
            }
        ]
    },
    {
        "edition_id": "M87654321",
        "text_id": "E87654321",
        "segmentations": [
            {
                "segmentation_id": "SGN_DEF",
                "segments": [
                    {
                        "id": "SEG003",
                        "lines": [{ "start": 0, "end": 50 }]
                    }
                ]
            }
        ]
    }
]
```

**Key structural changes:**

- `edition_id` and `text_id` moved from individual segments to the top-level
  group (no longer repeated per segment)
- New `segmentations` array groups segments by their display segmentation
- Each segmentation group includes a `segmentation_id`
- `tag_ids` is `null` (omitted) when there are no tags, instead of `[]`

---

#### 2. Transitive Traversal (New Behavior)

The endpoint now follows alignment chains transitively. For example, given
texts A ← B ← C (B is a translation of A, C is a commentary on B):

- Querying related segments from **A** now returns display segments from
  both **B** and **C** (C is found transitively via B)
- Previously, only B would have been returned

This works for arbitrary tree structures, diamond/convergent structures, and
chains of any depth (default max depth: 5).

---

#### 3. `SegmentOutput` No Longer Contains `edition_id` / `text_id`

The `SegmentOutput` model used in responses (segmentation listings, related
segments, etc.) no longer includes `edition_id` or `text_id` fields. These
fields are now provided at the grouping level in `RelatedSegmentsOutput`.

**Old `SegmentOutput`:**

```json
{
    "id": "SEG001",
    "edition_id": "M123",
    "text_id": "E123",
    "lines": [{ "start": 0, "end": 100 }],
    "tag_ids": []
}
```

**New `SegmentOutput`:**

```json
{
    "id": "SEG001",
    "lines": [{ "start": 0, "end": 100 }],
    "tag_ids": null
}
```

---

#### 4. `GET /editions/{id}/segmentations` Returns Plain Segmentations

Segmentation subtype labels have been removed. This endpoint returns the
segmentations created for the edition; alignments no longer create internal
segmentation nodes.

---

### Migration Steps

1. **Update response parsing** — the response is now grouped by edition and
   segmentation. To get a flat list of all related segment IDs:

   ```python
   segment_ids = [
       seg["id"]
       for group in response
       for sgn in group["segmentations"]
       for seg in sgn["segments"]
   ]
   ```

2. **Use `edition_id` from the group level** — instead of reading
   `segment.edition_id`, read `group["edition_id"]`

3. **Handle `tag_ids: null`** — the field may be `null` instead of `[]` when
   no tags are present

---
