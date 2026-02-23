# Texts API Documentation

This document provides comprehensive documentation for all Texts-related endpoints in the OpenPecha API v2.

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Text Endpoints](#text-endpoints)
   - [List All Texts](#list-all-texts)
   - [Get Text by ID](#get-text-by-id)
   - [Create New Text](#create-new-text)
   - [Update Text](#update-text)
4. [Text Relationships](#text-relationships)
   - [Translations](#translations)
   - [Commentaries](#commentaries)
5. [Data Models](#data-models)
6. [Error Responses](#error-responses)

---

## Overview

Texts (also known as Expressions in FRBR terminology) represent the intellectual content of a work independent of any specific physical manifestation. A text can have multiple editions (manifestations), translations, and commentaries.

### Key Concepts

- **Text (Expression)**: The abstract intellectual content of a work
- **Edition (Manifestation)**: A specific physical or digital instantiation of a text
- **Translation**: A text translated into another language
- **Commentary**: A text that comments on or explains another text
- **Contribution**: Attribution of a person or AI to a text (author, translator, reviser, scholar)

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

## Text Endpoints

### List All Texts

Retrieve all texts with optional filtering and pagination.

**Endpoint:**
```
GET /v2/texts
```

**Parameters:**

| Name | Type | Location | Required | Default | Description |
|------|------|----------|----------|---------|-------------|
| `limit` | integer | query | No | 20 | Number of results per page (1-100) |
| `offset` | integer | query | No | 0 | Number of results to skip |
| `language` | string | query | No | - | Filter by language code |
| `title` | string | query | No | - | Filter by title (case-insensitive substring match, searches both primary title and alternative titles) |
| `category_id` | string | query | No | - | Filter by category ID |
| `bdrc` | string | query | No | - | Filter by BDRC identifier |
| `wiki` | string | query | No | - | Filter by Wikidata identifier |

**Response: 200 OK**

```json
[
  {
    "id": "ABC12345678",
    "title": {
      "en": "Sample Expression",
      "bo": "དཔེ་མཚོན་ཚིག་སྒྲུབ།"
    },
    "language": "bo",
    "category_id": "CAT12345678",
    "contributions": [
      {
        "person_id": "P12345678",
        "role": "author"
      }
    ],
    "license": "public",
    "commentaries": [],
    "translations": ["DEF87654321"],
    "editions": ["M12345678"]
  },
  {
    "id": "DEF87654321",
    "title": {
      "en": "AI Generated Translation"
    },
    "language": "en",
    "category_id": "CAT12345678",
    "translation_of": "ABC12345678",
    "contributions": [
      {
        "ai_id": "gpt-4",
        "role": "translator"
      }
    ],
    "license": "cc0",
    "commentaries": [],
    "translations": [],
    "editions": []
  }
]
```

**Error Responses:**
- `400 Bad Request`: Invalid query parameters
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Get all texts (default pagination)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key"

# Get texts with pagination
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?limit=50&offset=100" \
  -H "X-API-Key: your_api_key"

# Filter by language
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?language=bo" \
  -H "X-API-Key: your_api_key"

# Filter by title (substring match)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?title=Heart%20Sutra" \
  -H "X-API-Key: your_api_key"

# Filter by category
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?category_id=CAT12345678" \
  -H "X-API-Key: your_api_key"

# Filter by BDRC identifier
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?bdrc=W123456" \
  -H "X-API-Key: your_api_key"

# Combine multiple filters
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?language=bo&category_id=CAT12345678&limit=10" \
  -H "X-API-Key: your_api_key"
```

---

### Get Text by ID

Fetch a specific text by its text ID.

**Endpoint:**
```
GET /v2/texts/{text_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `text_id` | string | path | Yes | The ID of the text to retrieve |

**Response: 200 OK (Root Text)**

```json
{
  "id": "T12345678",
  "title": {
    "en": "Sample Expression",
    "bo": "དཔེ་མཚོན་ཚིག་སྒྲུབ།"
  },
  "language": "bo",
  "category_id": "CAT12345678",
  "contributions": [
    {
      "person_id": "P12345678",
      "role": "author"
    }
  ],
  "bdrc": "W123456",
  "license": "public",
  "commentaries": ["C12345678"],
  "translations": ["TR12345678"],
  "editions": ["M12345678"]
}
```

**Response: 200 OK (Translation Text)**

```json
{
  "id": "TR12345678",
  "title": {
    "en": "English Translation"
  },
  "language": "en",
  "category_id": "CAT12345678",
  "translation_of": "T12345678",
  "contributions": [
    {
      "person_id": "P87654321",
      "role": "translator"
    }
  ],
  "license": "cc0",
  "commentaries": [],
  "translations": [],
  "editions": []
}
```

**Error Responses:**
- `404 Not Found`: Text does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T12345678" \
  -H "X-API-Key: your_api_key"
```

---

### Create New Text

Create a new text record with metadata and contributions.

**Endpoint:**
```
POST /v2/texts
```

**Request Body:**

```json
{
  "title": {
    "en": "English Title",
    "bo": "Tibetan Title"
  },
  "language": "bo",
  "category_id": "CAT12345678",
  "contributions": [
    {
      "person_id": "P12345678",
      "role": "author"
    }
  ],
  "bdrc": "W123456",
  "wiki": "Q123456",
  "date": "1200",
  "alt_titles": [
    {
      "en": "Alternative Title",
      "bo": "གཞན་མིང་།"
    }
  ],
  "license": "public"
}
```

**Required Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `title` | object | Localized title (language code → text mapping) |
| `language` | string | Primary language code (e.g., "bo", "en") |
| `category_id` | string | Category ID this text belongs to |
| `contributions` | array | At least one contribution (person or AI) |

**Optional Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `bdrc` | string | BDRC identifier |
| `wiki` | string | Wikidata identifier |
| `date` | string | Date of composition |
| `alt_titles` | array | Alternative localized titles |
| `translation_of` | string | Text ID this is a translation of |
| `commentary_of` | string | Text ID this is a commentary of |
| `license` | string | License type (see [License Types](#license-types)) |

#### Contributions

Each contribution must specify a role and either a person or AI identifier:

**Human Contribution:**
```json
{
  "person_id": "P12345678",
  "role": "author"
}
```

or

```json
{
  "person_bdrc_id": "P87654321",
  "role": "translator"
}
```

**AI Contribution:**
```json
{
  "ai_id": "gpt-4",
  "role": "translator"
}
```

**Contribution Roles:**
- `author`: Original author
- `translator`: Translator
- `reviser`: Editor/reviser
- `scholar`: Scholar

#### Creating Related Texts

**Translation:**

To create a translation, include the `translation_of` field with the source text ID:

```json
{
  "title": {
    "en": "English Translation"
  },
  "language": "en",
  "category_id": "CAT12345678",
  "translation_of": "ABC12345678",
  "contributions": [
    {
      "person_bdrc_id": "P87654321",
      "role": "translator"
    }
  ],
  "license": "cc0"
}
```

**Commentary:**

To create a commentary, include the `commentary_of` field:

```json
{
  "title": {
    "bo": "འགྲེལ་པ།"
  },
  "language": "bo",
  "category_id": "CAT12345678",
  "commentary_of": "ABC12345678",
  "contributions": [
    {
      "person_id": "P12345678",
      "role": "author"
    }
  ]
}
```

**AI Translation:**

For AI-generated translations:

```json
{
  "title": {
    "en": "AI English Translation"
  },
  "language": "en",
  "category_id": "CAT12345678",
  "translation_of": "ABC12345678",
  "contributions": [
    {
      "ai_id": "gpt-4",
      "role": "translator"
    }
  ],
  "license": "cc0"
}
```

**Response: 201 Created**

```json
{
  "id": "T12345678"
}
```

**Error Responses:**
- `400 Bad Request`: Invalid request parameters
- `422 Validation Error`: Validation failed
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Create a root text
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "Heart Sutra",
      "bo": "སྙིང་པོའི་མདོ།"
    },
    "language": "bo",
    "category_id": "CAT12345678",
    "contributions": [
      {
        "person_id": "P12345678",
        "role": "author"
      }
    ],
    "bdrc": "W123456",
    "license": "public"
  }'

# Create a translation
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "Heart Sutra - English Translation"
    },
    "language": "en",
    "category_id": "CAT12345678",
    "translation_of": "T12345678",
    "contributions": [
      {
        "person_bdrc_id": "P87654321",
        "role": "translator"
      }
    ],
    "license": "cc-by"
  }'
```

---

### Update Text

Partially update a text record. Only provided fields will be updated; omitted fields retain their current values.

**Endpoint:**
```
PATCH /v2/texts/{text_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `text_id` | string | path | Yes | The ID of the text to update |

**Request Body:**

All fields are optional. Only include fields you want to update.

```json
{
  "title": {
    "en": "Updated Title",
    "bo": "གསར་བསྒྱུར་མཚན་བྱང་།"
  },
  "bdrc": "W654321",
  "wiki": "Q999999",
  "date": "1250",
  "language": "bo",
  "category_id": "CAT87654321",
  "alt_titles": [
    {
      "en": "Alternative Title",
      "bo": "མཚན་བྱང་གཞན།"
    }
  ],
  "license": "cc0"
}
```

**Updatable Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `title` | object | Localized title (language code → text) |
| `alt_titles` | array | Alternative localized titles |
| `language` | string | Primary language code |
| `category_id` | string | Category ID |
| `bdrc` | string | BDRC identifier |
| `wiki` | string | Wikidata identifier |
| `date` | string | Date of composition |
| `license` | string | License type |

**Note:** You cannot update `translation_of`, `commentary_of`, or `contributions` via PATCH. These are set during creation.

**Response: 200 OK**

```json
{
  "id": "T12345678",
  "title": {
    "en": "Updated Title",
    "bo": "གསར་བསྒྱུར་མཚན་བྱང་།"
  },
  "language": "bo",
  "category_id": "CAT12345678",
  "contributions": [
    {
      "person_id": "P12345678",
      "role": "author"
    }
  ],
  "bdrc": "W654321",
  "wiki": "Q123456",
  "license": "cc0",
  "commentaries": [],
  "translations": [],
  "editions": []
}
```

**Error Responses:**
- `400 Bad Request`: Invalid request parameters
- `404 Not Found`: Text does not exist
- `422 Validation Error`: Validation failed
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Update BDRC ID only
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T12345678" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "bdrc": "W654321"
  }'

# Update title only
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T12345678" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "New Title",
      "bo": "མཚན་བྱང་གསར་པ།"
    }
  }'

# Update multiple fields
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T12345678" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "New Title"
    },
    "bdrc": "W999999",
    "wiki": "Q999999",
    "license": "cc-by-sa"
  }'
```

---

## Text Relationships

### Translations

Texts can be translations of other texts. The relationship is bidirectional:

- **Source Text**: Contains `translations` array with IDs of translation texts
- **Translation Text**: Contains `translation_of` field with source text ID

**Example:**

Root text (Tibetan):
```json
{
  "id": "T12345678",
  "title": { "bo": "རྩ་བའི་གཞུང་།" },
  "language": "bo",
  "translations": ["T87654321", "T11111111"]
}
```

Translation text (English):
```json
{
  "id": "T87654321",
  "title": { "en": "Root Text - English" },
  "language": "en",
  "translation_of": "T12345678"
}
```

**Creating a Translation:**

1. Create the translation text with `translation_of` field
2. Create an edition for the translation with content
3. Optionally create alignment annotations between source and translation editions

---

### Commentaries

Texts can be commentaries on other texts. The relationship is bidirectional:

- **Root Text**: Contains `commentaries` array with IDs of commentary texts
- **Commentary Text**: Contains `commentary_of` field with root text ID

**Example:**

Root text:
```json
{
  "id": "T12345678",
  "title": { "bo": "རྩ་བའི་གཞུང་།" },
  "commentaries": ["C12345678", "C87654321"]
}
```

Commentary text:
```json
{
  "id": "C12345678",
  "title": { "bo": "འགྲེལ་པ།" },
  "commentary_of": "T12345678"
}
```

**Creating a Commentary:**

1. Create the commentary text with `commentary_of` field
2. Create an edition for the commentary with content

---

## Data Models

### ExpressionInput

Schema for creating a new text:

```typescript
{
  title: {                       // Required
    [language: string]: string;  // Localized title
  };
  language: string;              // Required: Primary language code
  category_id: string;           // Required: Category ID
  contributions: Array<{         // Required: At least one
    person_id?: string;          // Person ID (human)
    person_bdrc_id?: string;     // Person BDRC ID (human)
    ai_id?: string;              // AI model identifier
    role: "author" | "translator" | "reviser" | "scholar";
  }>;
  bdrc?: string | null;          // Optional: BDRC identifier
  wiki?: string | null;          // Optional: Wikidata identifier
  date?: string | null;          // Optional: Date of composition
  alt_titles?: Array<{           // Optional: Alternative titles
    [language: string]: string;
  }> | null;
  translation_of?: string | null; // Optional: Source text ID for translations
  commentary_of?: string | null;  // Optional: Root text ID for commentaries
  license?: LicenseType;         // Optional: License type
}
```

### ExpressionPatch

Schema for updating a text (all fields optional):

```typescript
{
  title?: {
    [language: string]: string;
  };
  language?: string | null;
  category_id?: string | null;
  bdrc?: string | null;
  wiki?: string | null;
  date?: string | null;
  alt_titles?: Array<{
    [language: string]: string;
  }> | null;
  license?: LicenseType;
}
```

**Note:** `translation_of`, `commentary_of`, and `contributions` cannot be updated via PATCH.

### ExpressionOutput

Schema for text response data:

```typescript
{
  id: string;                    // Expression ID
  title: {                       // Localized title
    [language: string]: string;
  };
  language: string;              // Primary language code
  category_id: string;           // Category ID
  contributions: Array<{         // Attribution list
    person_id?: string | null;
    person_bdrc_id?: string | null;
    person_name?: {
      [language: string]: string;
    } | null;
    ai_id?: string | null;
    role: "author" | "translator" | "reviser" | "scholar";
  }>;
  license: LicenseType;          // License type
  commentaries: string[];        // IDs of commentaries on this text
  translations: string[];        // IDs of translations of this text
  editions: string[];            // IDs of editions (manifestations)
  bdrc?: string | null;          // BDRC identifier
  wiki?: string | null;          // Wikidata identifier
  date?: string | null;          // Date of composition
  alt_titles?: Array<{           // Alternative titles
    [language: string]: string;
  }> | null;
  translation_of?: string | null; // Source text ID (for translations)
  commentary_of?: string | null;  // Root text ID (for commentaries)
}
```

### ContributionInput

Schema for contribution attribution:

```typescript
{
  role: "author" | "translator" | "reviser" | "scholar"; // Required
  
  // For human contributions (provide ONE of):
  person_id?: string;           // Person ID
  person_bdrc_id?: string;      // Person BDRC ID
  
  // For AI contributions:
  ai_id?: string;               // AI model identifier (e.g., "gpt-4")
}
```

**Validation Rules:**
- Must provide exactly one of: `person_id`, `person_bdrc_id`, or `ai_id`
- Cannot mix person and AI identifiers
- At least one contribution is required when creating a text

### ContributionOutput

Schema for contribution in response data:

```typescript
{
  person_id?: string | null;
  person_bdrc_id?: string | null;
  person_name?: {                // Resolved person name
    [language: string]: string;
  } | null;
  ai_id?: string | null;
  role: "author" | "translator" | "reviser" | "scholar";
}
```

### License Types

Valid license values:

| License | Description |
|---------|-------------|
| `cc0` | CC0 - Public Domain Dedication |
| `public` | Public Domain |
| `cc-by` | Creative Commons Attribution |
| `cc-by-sa` | Creative Commons Attribution-ShareAlike |
| `cc-by-nd` | Creative Commons Attribution-NoDerivatives |
| `cc-by-nc` | Creative Commons Attribution-NonCommercial |
| `cc-by-nc-sa` | Creative Commons Attribution-NonCommercial-ShareAlike |
| `cc-by-nc-nd` | Creative Commons Attribution-NonCommercial-NoDerivatives |
| `copyrighted` | Copyrighted |
| `unknown` | Unknown License |

---

## Error Responses

### 400 Bad Request

```json
{
  "error": "There was an error with the request"
}
```

**Common Causes:**
- Invalid query parameters
- Invalid filter values
- Malformed request body

### 404 Not Found

```json
{
  "error": "Resource was not found"
}
```

**Common Causes:**
- Text ID does not exist
- Referenced category does not exist
- Referenced person does not exist
- Referenced source text (for translation_of or commentary_of) does not exist

### 422 Validation Error

```json
{
  "error": "Validation error",
  "details": [
    {
      "loc": ["body", "title"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

**Common Causes:**
- Missing required fields (title, language, category_id, contributions)
- Invalid field types or values
- Empty contributions array
- Invalid contribution (missing person_id/person_bdrc_id/ai_id)
- Invalid license type
- Invalid language code

### 500 Server Error

```json
{
  "error": "There was an error on the server"
}
```

---

## Best Practices

### 1. Creating Texts

**Do:**
- Provide localized titles in relevant languages (especially primary language)
- Include BDRC identifiers when available for better integration
- Specify appropriate license for all texts
- Include date of composition when known
- Use person BDRC IDs when available for better linking

**Don't:**
- Create texts without proper attribution (contributions)
- Use invalid language codes
- Mix person and AI identifiers in the same contribution
- Leave category_id empty or use non-existent categories

### 2. Updating Texts

**Do:**
- Update only the fields that need to change
- Verify text exists before updating
- Use proper validation for identifiers

**Don't:**
- Try to update `translation_of` or `commentary_of` (set at creation)
- Try to update `contributions` via PATCH
- Update to invalid license types

### 3. Querying Texts

**Do:**
- Use pagination for large result sets (default limit is 20)
- Filter by category for better organization
- Use title search for case-insensitive substring matching
- Combine filters to narrow results

**Don't:**
- Request limit > 100 (will be rejected)
- Assume exact match for title filter (it's substring match)

### 4. Managing Relationships

**Do:**
- Set `translation_of` when creating translation texts
- Set `commentary_of` when creating commentary texts
- Verify referenced texts exist before creating relationships
- Use the bidirectional arrays (translations, commentaries) for navigation

**Don't:**
- Try to create circular relationships (A translates B, B translates A)
- Reference non-existent texts in relationship fields

---

## Workflow Examples

### Creating a Complete Text with Editions

**Step 1: Create the Text**

```bash
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "The Precious Garland",
      "bo": "རིན་ཆེན་ཕྲེང་བ།"
    },
    "language": "bo",
    "category_id": "CAT_PHILOSOPHY",
    "contributions": [
      {
        "person_bdrc_id": "P2816",
        "role": "author"
      }
    ],
    "bdrc": "W23703",
    "license": "public"
  }'
```

Response:
```json
{
  "id": "T_NEW_12345678"
}
```

**Step 2: Create Edition for the Text**

```bash
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T_NEW_12345678/editions" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {
      "type": "critical",
      "source": "Derge Edition",
      "incipit_title": {
        "bo": "བཙུན་པ་ཀླུ་སྒྲུབ་ཀྱིས་མཛད་པའི་རིན་ཆེན་ཕྲེང་བ།"
      }
    },
    "content": "Full Tibetan text content here..."
  }'
```

**Step 3: Query the Complete Text**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T_NEW_12345678" \
  -H "X-API-Key: your_api_key"
```

---

### Creating a Translation Workflow

**Step 1: Find the Source Text**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?title=Heart%20Sutra&language=bo" \
  -H "X-API-Key: your_api_key"
```

**Step 2: Create Translation Text**

```bash
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "Heart Sutra - English Translation by John Doe"
    },
    "language": "en",
    "category_id": "CAT_SUTRAS",
    "translation_of": "T_SOURCE_12345678",
    "contributions": [
      {
        "person_id": "P_TRANSLATOR",
        "role": "translator"
      }
    ],
    "license": "cc-by"
  }'
```

**Step 3: Create Edition with Translation Content**

```bash
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T_TRANSLATION_ID/editions" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {
      "type": "critical",
      "source": "Modern Translation 2024"
    },
    "content": "English translation text here..."
  }'
```

**Step 4: Create Alignment (Optional)**

```bash
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/editions/EDITION_SOURCE/annotations" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "alignment": {
      "target_id": "EDITION_TRANSLATION",
      "target_segments": [...],
      "aligned_segments": [...]
    }
  }'
```

---

### Creating AI-Generated Translation

**Step 1: Create AI Translation Text**

```bash
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "en": "AI English Translation"
    },
    "language": "en",
    "category_id": "CAT12345678",
    "translation_of": "T_SOURCE_12345678",
    "contributions": [
      {
        "ai_id": "gpt-4",
        "role": "translator"
      }
    ],
    "license": "cc0"
  }'
```

---

### Searching and Filtering

**Find all texts in a category:**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?category_id=CAT_SUTRAS&limit=50" \
  -H "X-API-Key: your_api_key"
```

**Find texts by author (via person):**

First get person ID, then filter texts that reference that person in contributions (requires application-level filtering after retrieval).

**Find all translations of a text:**

```bash
# Get the source text, which includes translations array
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T12345678" \
  -H "X-API-Key: your_api_key"
```

**Find all commentaries on a text:**

```bash
# Get the source text, which includes commentaries array
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T12345678" \
  -H "X-API-Key: your_api_key"
```

**Search by title substring:**

```bash
# Finds all texts with "Heart" in any title
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?title=Heart" \
  -H "X-API-Key: your_api_key"
```

---

## Advanced Usage

### Working with Multilingual Titles

Texts support localized titles in multiple languages:

```json
{
  "title": {
    "en": "Heart Sutra",
    "bo": "སྙིང་པོའི་མདོ།",
    "zh": "心经",
    "sa": "प्रज्ञापारमिताहृदय"
  },
  "alt_titles": [
    {
      "en": "Perfection of Wisdom Heart Sutra",
      "bo": "ཤེས་རབ་སྙིང་པོའི་མདོ།"
    },
    {
      "en": "Heart of the Perfection of Wisdom"
    }
  ]
}
```

**Benefits:**
- Better discoverability across languages
- Support for multilingual applications
- Alternative titles captured for search

### Managing Contributions

**Multiple Authors:**

```json
{
  "contributions": [
    {
      "person_id": "P_AUTHOR_1",
      "role": "author"
    },
    {
      "person_id": "P_AUTHOR_2",
      "role": "author"
    }
  ]
}
```

**Translator and Reviser:**

```json
{
  "contributions": [
    {
      "person_bdrc_id": "P123",
      "role": "translator"
    },
    {
      "person_bdrc_id": "P456",
      "role": "reviser"
    }
  ]
}
```

**AI with Human Review:**

For AI-generated translations that have been reviewed by a scholar:

```json
{
  "contributions": [
    {
      "ai_id": "gpt-4",
      "role": "translator"
    },
    {
      "person_id": "P_SCHOLAR",
      "role": "reviser"
    }
  ]
}
```

### Pagination Strategies

**Basic Pagination:**

```bash
# Page 1 (first 20 items)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?limit=20&offset=0"

# Page 2 (next 20 items)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?limit=20&offset=20"

# Page 3 (next 20 items)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?limit=20&offset=40"
```

**Large Result Sets:**

```bash
# Fetch 100 items at a time (maximum)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?limit=100&offset=0"
```

### Combining Filters

**Find Tibetan texts in specific category:**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?language=bo&category_id=CAT_SUTRAS"
```

**Find texts with BDRC identifier:**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?bdrc=W23703"
```

**Search with title and language:**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?title=Garland&language=bo"
```

---

## Integration with Other APIs

### Texts and Editions

After creating a text, create editions to hold the actual content:

```
1. POST /v2/texts → Get text_id
2. POST /v2/texts/{text_id}/editions → Create edition with content
3. GET /v2/editions/{edition_id}/content → Retrieve content
```

### Texts and Categories

Texts must belong to a category:

```
1. GET /v2/categories → Find or create category
2. POST /v2/texts with category_id
```

### Texts and Persons

Contributions link texts to persons:

```
1. GET /v2/persons?name=Author → Find person
   OR POST /v2/persons → Create person
2. POST /v2/texts with person_id in contributions
```

### Texts and Relations

Query text relationships programmatically:

```
1. GET /v2/texts/{text_id} → Get text with translations/commentaries arrays
2. GET /v2/relations/expressions/{expression_id} → Get detailed relationship graph
```

---

## Common Scenarios

### Scenario 1: Import Text from BDRC

```bash
# Step 1: Create person if not exists
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "name": {"bo": "ཀླུ་སྒྲུབ།", "en": "Nagarjuna"},
    "bdrc": "P2816"
  }'

# Step 2: Create text
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"bo": "རིན་ཆེན་ཕྲེང་བ།", "en": "Precious Garland"},
    "language": "bo",
    "category_id": "CAT_BUDDHIST_PHILOSOPHY",
    "bdrc": "W23703",
    "contributions": [{"person_bdrc_id": "P2816", "role": "author"}],
    "license": "public"
  }'

# Step 3: Create diplomatic edition
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T_NEW_ID/editions" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "metadata": {
      "type": "diplomatic",
      "bdrc": "W23703",
      "source": "Derge Kangyur"
    },
    "content": "Full text content from BDRC..."
  }'
```

### Scenario 2: Create Modern Translation

```bash
# Step 1: Find source text
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?bdrc=W23703"

# Step 2: Create translation text
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"en": "The Precious Garland - Modern English Translation"},
    "language": "en",
    "category_id": "CAT_BUDDHIST_PHILOSOPHY",
    "translation_of": "T_SOURCE_ID",
    "contributions": [
      {"person_id": "P_TRANSLATOR", "role": "translator"}
    ],
    "license": "cc-by-sa"
  }'

# Step 3: Create critical edition with translation
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T_TRANSLATION_ID/editions" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "metadata": {
      "type": "critical",
      "source": "2024 Translation"
    },
    "content": "English translation content..."
  }'
```

### Scenario 3: Update Text Metadata

```bash
# Add Wikidata identifier
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T12345678" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "wiki": "Q749743"
  }'

# Update license
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T12345678" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "license": "cc-by-sa"
  }'

# Add alternative titles
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T12345678" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "alt_titles": [
      {"en": "Alternative English Title"},
      {"bo": "གཞན་མིང་།"}
    ]
  }'
```

### Scenario 4: Browse Texts by Category

```bash
# Step 1: Get category ID
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"

# Step 2: Get all texts in that category
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts?category_id=CAT_FOUND_ID&limit=50" \
  -H "X-API-Key: your_api_key"

# Step 3: For each text, get editions if needed
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T12345678/editions" \
  -H "X-API-Key: your_api_key"
```

---

## Validation Rules

### Text Creation

| Field | Validation |
|-------|------------|
| `title` | Required, must be object with at least one language |
| `language` | Required, must be valid language code |
| `category_id` | Required, must reference existing category |
| `contributions` | Required, at least one contribution |
| `contributions[].role` | Required, must be: author, translator, reviser, or scholar |
| `contributions[].person_id` or `person_bdrc_id` or `ai_id` | Exactly one must be provided |
| `bdrc` | Optional, must be unique if provided |
| `wiki` | Optional, must be valid Wikidata ID format |
| `translation_of` | Optional, must reference existing text |
| `commentary_of` | Optional, must reference existing text |
| `license` | Optional, must be valid license type |

### Text Update

| Field | Validation |
|-------|------------|
| All fields | Optional |
| `title` | If provided, must be object with at least one language |
| `language` | If provided, must be valid language code |
| `category_id` | If provided, must reference existing category |
| `bdrc` | If provided, must be unique |
| `license` | If provided, must be valid license type |

---

## API Response Fields Explained

### Arrays in Response

**`commentaries`**: Array of text IDs that are commentaries on this text
```json
"commentaries": ["C12345678", "C87654321"]
```

**`translations`**: Array of text IDs that are translations of this text
```json
"translations": ["TR12345678", "TR87654321"]
```

**`editions`**: Array of edition (manifestation) IDs for this text
```json
"editions": ["M12345678", "M87654321"]
```

### Relationship Fields

**`translation_of`**: Only present on translation texts, contains source text ID
```json
"translation_of": "T_SOURCE_12345678"
```

**`commentary_of`**: Only present on commentary texts, contains root text ID
```json
"commentary_of": "T_ROOT_12345678"
```

### Contribution Details

Contributions in responses may include resolved person names:

```json
{
  "person_id": "P12345678",
  "person_bdrc_id": "P2816",
  "person_name": {
    "bo": "ཀླུ་སྒྲུབ།",
    "en": "Nagarjuna"
  },
  "ai_id": null,
  "role": "author"
}
```

---

## Important Notes

### Text vs Edition

- **Text (Expression)**: Abstract intellectual content
  - Has title, language, author, category
  - Can have multiple editions
  - Managed via `/v2/texts` endpoints

- **Edition (Manifestation)**: Concrete instantiation
  - Has actual text content
  - Has specific source/BDRC reference
  - Managed via `/v2/editions` endpoints

### Person References

When creating contributions, you can use:
- `person_id`: Internal system person ID
- `person_bdrc_id`: BDRC person identifier (automatically resolves to person_id)

The API will resolve BDRC IDs to internal person IDs automatically. If the person doesn't exist, you may need to create it first via `POST /v2/persons`.

### Language Codes

Use standard BCP47 language codes:
- `bo`: Tibetan
- `en`: English
- `zh`: Chinese
- `sa`: Sanskrit
- `pi`: Pali

Verify language exists in system via `GET /v2/languages` before use.

### Category References

Categories must exist before creating texts. Use `GET /v2/categories` with appropriate `X-Application` header to find or create categories.

---

## Migration Notes

### Creating Translations and Commentaries

The old endpoints `/v2/editions/{edition_id}/translation` and `/v2/editions/{edition_id}/commentary` have been removed.

**Old Way (Deprecated):**
```
POST /v2/editions/{edition_id}/translation
```

**New Way:**
```
1. POST /v2/texts with translation_of field
2. POST /v2/texts/{text_id}/editions to create edition
```

This change provides better separation between the abstract text level (Expression) and the concrete edition level (Manifestation).

---

## Related Resources

To get editions for a text, use the Editions API:

**List Editions:**
```
GET /v2/texts/{text_id}/editions
```

**Create Edition:**
```
POST /v2/texts/{text_id}/editions
```

See [Editions API Documentation](./editions-api.md) for complete edition management documentation.

---

## Complete TypeScript Types

```typescript
// Input types
interface ExpressionInput {
  title: { [language: string]: string };
  language: string;
  category_id: string;
  contributions: ContributionInput[];
  bdrc?: string | null;
  wiki?: string | null;
  date?: string | null;
  alt_titles?: Array<{ [language: string]: string }> | null;
  translation_of?: string | null;
  commentary_of?: string | null;
  license?: LicenseType;
}

interface ContributionInput {
  person_id?: string;
  person_bdrc_id?: string;
  ai_id?: string;
  role: "author" | "translator" | "reviser" | "scholar";
}

interface ExpressionPatch {
  title?: { [language: string]: string };
  language?: string | null;
  category_id?: string | null;
  bdrc?: string | null;
  wiki?: string | null;
  date?: string | null;
  alt_titles?: Array<{ [language: string]: string }> | null;
  license?: LicenseType;
}

// Output types
interface ExpressionOutput {
  id: string;
  title: { [language: string]: string };
  language: string;
  category_id: string;
  contributions: ContributionOutput[];
  license: LicenseType;
  commentaries: string[];
  translations: string[];
  editions: string[];
  bdrc?: string | null;
  wiki?: string | null;
  date?: string | null;
  alt_titles?: Array<{ [language: string]: string }> | null;
  translation_of?: string | null;
  commentary_of?: string | null;
}

interface ContributionOutput {
  person_id?: string | null;
  person_bdrc_id?: string | null;
  person_name?: { [language: string]: string } | null;
  ai_id?: string | null;
  role: "author" | "translator" | "reviser" | "scholar";
}

type LicenseType = 
  | "cc0"
  | "public"
  | "cc-by"
  | "cc-by-sa"
  | "cc-by-nd"
  | "cc-by-nc"
  | "cc-by-nc-sa"
  | "cc-by-nc-nd"
  | "copyrighted"
  | "unknown";
```

---

## Quick Reference

### Creating Different Text Types

| Text Type | Required Fields | Special Fields |
|-----------|----------------|----------------|
| **Root Text** | title, language, category_id, contributions | bdrc (optional) |
| **Translation** | title, language, category_id, contributions, translation_of | - |
| **Commentary** | title, language, category_id, contributions, commentary_of | - |
| **AI Translation** | title, language, category_id, contributions (with ai_id), translation_of | - |

### Query Parameters Summary

| Endpoint | Filtering | Pagination | Sorting |
|----------|-----------|------------|---------|
| `GET /v2/texts` | language, title, category_id, bdrc, wiki | limit, offset | Not specified |
| `GET /v2/texts/{text_id}` | N/A | N/A | N/A |

### HTTP Methods Summary

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/v2/texts` | List all texts with filtering |
| GET | `/v2/texts/{text_id}` | Get single text by ID |
| POST | `/v2/texts` | Create new text |
| PATCH | `/v2/texts/{text_id}` | Partially update text |

---

## Troubleshooting

### Common Issues

**Issue: "Category not found"**
- Verify category exists via `GET /v2/categories`
- Ensure correct `X-Application` header for category context

**Issue: "Person not found"**
- Check person exists via `GET /v2/persons`
- Create person first if using `person_id`
- Use `person_bdrc_id` for automatic resolution

**Issue: "Validation error: field required"**
- Ensure all required fields are present: title, language, category_id, contributions
- Check contributions array is not empty
- Verify each contribution has required role and identifier

**Issue: "Translation source text not found"**
- Verify `translation_of` text ID exists
- Check text ID is correct (no typos)

**Issue: "Invalid license type"**
- Use one of the valid license values from [License Types](#license-types)
- Check spelling matches exactly

### Debugging Tips

1. **Test with GET first**: Verify text exists before updating
2. **Check related resources**: Ensure categories, persons exist before referencing
3. **Use validation errors**: Error details show exact field causing issue
4. **Start simple**: Create minimal text first, then update with additional fields
5. **Verify IDs**: Double-check all referenced IDs (category_id, person_id, etc.)

---

## See Also

- [Editions API Documentation](./editions-api.md) - Managing editions and content
- [Text Operations Specification](../text-operations-spec.md) - Content modification behavior
- OpenAPI Specification: `GET /v2/schema/openapi`
- Relations API: `GET /v2/relations/expressions/{expression_id}`
- Persons API: `/v2/persons` endpoints
- Categories API: `/v2/categories` endpoints
