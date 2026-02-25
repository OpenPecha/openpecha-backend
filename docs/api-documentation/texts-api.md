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


| Name          | Type    | Location | Required | Default | Description                                                                                            |
| ------------- | ------- | -------- | -------- | ------- | ------------------------------------------------------------------------------------------------------ |
| `limit`       | integer | query    | No       | 20      | Number of results per page (1-100)                                                                     |
| `offset`      | integer | query    | No       | 0       | Number of results to skip                                                                              |
| `language`    | string  | query    | No       | -       | Filter by language code                                                                                |
| `title`       | string  | query    | No       | -       | Filter by title (case-insensitive substring match, searches both primary title and alternative titles) |
| `category_id` | string  | query    | No       | -       | Filter by category ID                                                                                  |
| `bdrc`        | string  | query    | No       | -       | Filter by BDRC identifier                                                                              |
| `wiki`        | string  | query    | No       | -       | Filter by Wikidata identifier                                                                          |


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


| Name      | Type   | Location | Required | Description                    |
| --------- | ------ | -------- | -------- | ------------------------------ |
| `text_id` | string | path     | Yes      | The ID of the text to retrieve |


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


| Field           | Type   | Description                                    |
| --------------- | ------ | ---------------------------------------------- |
| `title`         | object | Localized title (language code → text mapping) |
| `language`      | string | Primary language code (e.g., "bo", "en")       |
| `category_id`   | string | Category ID this text belongs to               |
| `contributions` | array  | At least one contribution (person or AI)       |


**Optional Fields:**


| Field            | Type   | Description                                        |
| ---------------- | ------ | -------------------------------------------------- |
| `bdrc`           | string | BDRC identifier                                    |
| `wiki`           | string | Wikidata identifier                                |
| `date`           | string | Date of composition                                |
| `alt_titles`     | array  | Alternative localized titles                       |
| `translation_of` | string | Text ID this is a translation of                   |
| `commentary_of`  | string | Text ID this is a commentary of                    |
| `license`        | string | License type (see [License Types](#license-types)) |


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


| Name      | Type   | Location | Required | Description                  |
| --------- | ------ | -------- | -------- | ---------------------------- |
| `text_id` | string | path     | Yes      | The ID of the text to update |


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


| Field         | Type   | Description                            |
| ------------- | ------ | -------------------------------------- |
| `title`       | object | Localized title (language code → text) |
| `alt_titles`  | array  | Alternative localized titles           |
| `language`    | string | Primary language code                  |
| `category_id` | string | Category ID                            |
| `bdrc`        | string | BDRC identifier                        |
| `wiki`        | string | Wikidata identifier                    |
| `date`        | string | Date of composition                    |
| `license`     | string | License type                           |


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


