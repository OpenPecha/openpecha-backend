# Persons API Documentation

This document provides comprehensive documentation for all Persons-related endpoints in the OpenPecha API v2.

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Person Endpoints](#person-endpoints)
   - [List All Persons](#list-all-persons)
   - [Get Person by ID](#get-person-by-id)
   - [Create New Person](#create-new-person)
   - [Update Person](#update-person)
4. [Person Integration](#person-integration)
   - [Linking Persons to Texts](#linking-persons-to-texts)
   - [Person Contributions](#person-contributions)
5. [Data Models](#data-models)
6. [Error Responses](#error-responses)

---

## Overview

Persons represent individuals who have contributed to texts as authors, translators, revisers, or scholars. The Persons API provides endpoints for managing person records with localized names and external identifiers (BDRC, Wikidata).

### Key Concepts

- **Person**: An individual who has contributed to one or more texts
- **Contributions**: Roles a person plays in relation to texts (author, translator, reviser, scholar)
- **Localized Names**: Support for names in multiple languages and scripts
- **External Identifiers**: Integration with BDRC and Wikidata

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

## Person Endpoints

### List All Persons

Retrieve all persons from the database with optional pagination and filtering.

**Endpoint:**
```
GET /v2/persons
```

**Parameters:**

| Name | Type | Location | Required | Default | Description |
|------|------|----------|----------|---------|-------------|
| `limit` | integer | query | No | 20 | Number of results per page (1-100) |
| `offset` | integer | query | No | 0 | Number of results to skip |
| `name` | string | query | No | - | Filter by name (case-insensitive substring match, searches both primary name and alternative names) |
| `bdrc` | string | query | No | - | Filter by BDRC ID (exact match) |
| `wiki` | string | query | No | - | Filter by Wikidata ID (exact match) |

**Response: 200 OK**

```json
[
  {
    "id": "P12345678",
    "name": {
      "en": "John Doe",
      "bo": "ཇོན་དོ།"
    },
    "alt_names": [
      {
        "en": "J. Doe",
        "bo": "ཇོན།"
      }
    ],
    "bdrc": "P123456",
    "wiki": "Q123456"
  },
  {
    "id": "P87654321",
    "name": {
      "bo": "ཀླུ་སྒྲུབ།",
      "en": "Nagarjuna",
      "sa": "नागार्जुन"
    },
    "alt_names": [
      {
        "bo": "སློབ་དཔོན་ཀླུ་སྒྲུབ།",
        "en": "Acharya Nagarjuna"
      }
    ],
    "bdrc": "P2816",
    "wiki": "Q182485"
  }
]
```

**Error Responses:**
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Get all persons (default pagination)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key"

# Get persons with pagination
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?limit=50&offset=100" \
  -H "X-API-Key: your_api_key"

# Filter by name (substring match)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?name=Nagarjuna" \
  -H "X-API-Key: your_api_key"

# Filter by BDRC ID (exact match)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?bdrc=P2816" \
  -H "X-API-Key: your_api_key"

# Filter by Wikidata ID (exact match)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?wiki=Q182485" \
  -H "X-API-Key: your_api_key"

# Combine filters with pagination
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?name=Rinpoche&limit=30" \
  -H "X-API-Key: your_api_key"
```

---

### Get Person by ID

Fetch a specific person by their ID.

**Endpoint:**
```
GET /v2/persons/{person_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `person_id` | string | path | Yes | The ID of the person to retrieve |

**Response: 200 OK**

```json
{
  "id": "P12345678",
  "name": {
    "en": "John Doe",
    "bo": "ཇོན་དོ།"
  },
  "alt_names": [
    {
      "en": "J. Doe",
      "bo": "ཇོན།"
    },
    {
      "en": "John D."
    }
  ],
  "bdrc": "P123456",
  "wiki": "Q123456"
}
```

**Error Responses:**
- `404 Not Found`: Person does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key"
```

---

### Create New Person

Create a new person record with localized names and external identifiers.

**Endpoint:**
```
POST /v2/persons
```

**Request Body:**

```json
{
  "name": {
    "en": "John Doe",
    "bo": "ཇོན་དོ།"
  },
  "alt_names": [
    {
      "en": "J. Doe",
      "bo": "ཇོན།"
    }
  ],
  "bdrc": "P123456",
  "wiki": "Q123456"
}
```

**Required Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | object | Localized name (language code → name mapping, at least one language required) |

**Optional Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `alt_names` | array | Alternative localized names |
| `bdrc` | string | BDRC person identifier (must be unique) |
| `wiki` | string | Wikidata identifier (must be unique) |

**Response: 201 Created**

```json
{
  "id": "P12345678"
}
```

**Error Responses:**
- `400 Bad Request`: Invalid request parameters
- `409 Conflict`: Person with BDRC ID already exists
- `422 Validation Error`: Validation failed
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Create person with basic info
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "en": "Nagarjuna",
      "bo": "ཀླུ་སྒྲུབ།",
      "sa": "नागार्जुन"
    },
    "bdrc": "P2816",
    "wiki": "Q182485"
  }'

# Create person with alternative names
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "en": "Tsongkhapa",
      "bo": "ཙོང་ཁ་པ།"
    },
    "alt_names": [
      {
        "en": "Je Tsongkhapa",
        "bo": "རྗེ་ཙོང་ཁ་པ།"
      },
      {
        "en": "Lobsang Drakpa",
        "bo": "བློ་བཟང་གྲགས་པ།"
      }
    ],
    "bdrc": "P64",
    "wiki": "Q234330"
  }'

# Create person with minimal info
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "en": "Unknown Translator"
    }
  }'
```

---

### Update Person

Partially update a person record. Only provided fields will be updated; omitted fields retain their current values.

**Endpoint:**
```
PATCH /v2/persons/{person_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `person_id` | string | path | Yes | The ID of the person to update |

**Request Body:**

All fields are optional. Only include fields you want to update.

```json
{
  "name": {
    "en": "Updated Name",
    "bo": "གསར་བསྒྱུར་མིང་།"
  },
  "alt_names": [
    {
      "en": "Alternative Name",
      "bo": "གཞན་མིང་།"
    }
  ],
  "bdrc": "P654321",
  "wiki": "Q999999"
}
```

**Updatable Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | object | Localized name (language code → name) |
| `alt_names` | array | Alternative localized names |
| `bdrc` | string | BDRC identifier (must be unique) |
| `wiki` | string | Wikidata identifier (must be unique) |

**Response: 200 OK**

```json
{
  "id": "P12345678",
  "name": {
    "en": "Updated Name",
    "bo": "གསར་བསྒྱུར་མིང་།"
  },
  "alt_names": [
    {
      "en": "Alternative Name",
      "bo": "གཞན་མིང་།"
    }
  ],
  "bdrc": "P654321",
  "wiki": "Q123456"
}
```

**Error Responses:**
- `400 Bad Request`: Invalid request parameters
- `404 Not Found`: Person does not exist
- `409 Conflict`: BDRC ID or Wiki ID already exists for another person
- `422 Validation Error`: Validation failed
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Update BDRC ID only
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "bdrc": "P654321"
  }'

# Update name only
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "en": "Updated Name",
      "bo": "གསར་བསྒྱུར་མིང་།"
    }
  }'

# Update multiple fields
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "en": "New Name"
    },
    "bdrc": "P999999",
    "wiki": "Q999999",
    "alt_names": [
      {
        "en": "Alternative Name",
        "bo": "གཞན་མིང་།"
      }
    ]
  }'

# Add Wikidata identifier to existing person
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "wiki": "Q182485"
  }'
```

---

## Person Integration

### Linking Persons to Texts

Persons are linked to texts through contributions. When creating or referencing a text, you can specify person contributions using:

1. **Internal Person ID** (`person_id`)
2. **BDRC Person ID** (`person_bdrc_id`)

**Example in Text Creation:**

```json
{
  "title": { "en": "Sample Text" },
  "language": "bo",
  "category_id": "CAT12345678",
  "contributions": [
    {
      "person_id": "P12345678",
      "role": "author"
    },
    {
      "person_bdrc_id": "P2816",
      "role": "translator"
    }
  ]
}
```

The API automatically resolves BDRC IDs to internal person IDs.

---

### Person Contributions

Persons can contribute to texts in various roles:

| Role | Description | Common Use Cases |
|------|-------------|------------------|
| `author` | Original author/composer | Root texts, commentaries |
| `translator` | Translator of text | Translations |
| `reviser` | Editor or reviser | Critical editions, modern translations |
| `scholar` | Scholar who compiled or edited | Anthologies, critical editions |

**Example Contribution Workflow:**

```bash
# Step 1: Create person
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "name": {"en": "Jane Smith", "bo": "སྨིཐ།"},
    "bdrc": "P999999"
  }'
# Returns: {"id": "P_NEW_ID"}

# Step 2: Create text with person as contributor
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"en": "New Translation"},
    "language": "en",
    "category_id": "CAT12345678",
    "translation_of": "T_SOURCE",
    "contributions": [
      {
        "person_id": "P_NEW_ID",
        "role": "translator"
      }
    ]
  }'
```

---

## Data Models

### PersonInput

Schema for creating a new person:

```typescript
{
  name: {                        // Required
    [language: string]: string;  // At least one language required
  };
  alt_names?: Array<{            // Optional
    [language: string]: string;
  }> | null;
  bdrc?: string | null;          // Optional: BDRC identifier (must be unique)
  wiki?: string | null;          // Optional: Wikidata identifier (must be unique)
}
```

**Examples:**

Minimal person:
```json
{
  "name": {
    "en": "Unknown Author"
  }
}
```

Complete person with multiple languages:
```json
{
  "name": {
    "bo": "ཀླུ་སྒྲུབ།",
    "en": "Nagarjuna",
    "sa": "नागार्जुन",
    "zh": "龙树"
  },
  "alt_names": [
    {
      "bo": "སློབ་དཔོན་ཀླུ་སྒྲུབ།",
      "en": "Acharya Nagarjuna"
    },
    {
      "en": "Nāgārjuna"
    }
  ],
  "bdrc": "P2816",
  "wiki": "Q182485"
}
```

### PersonPatch

Schema for updating a person (all fields optional):

```typescript
{
  name?: {
    [language: string]: string;
  };
  alt_names?: Array<{
    [language: string]: string;
  }> | null;
  bdrc?: string | null;
  wiki?: string | null;
}
```

**Note:** At least one field must be provided in a PATCH request.

### PersonOutput

Schema for person response data:

```typescript
{
  id: string;                    // Person ID
  name: {                        // Localized name
    [language: string]: string;
  };
  alt_names?: Array<{            // Alternative names (optional)
    [language: string]: string;
  }> | null;
  bdrc?: string | null;          // BDRC identifier
  wiki?: string | null;          // Wikidata identifier
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
- Invalid query parameters
- Malformed request body
- Invalid JSON structure

### 404 Not Found

```json
{
  "error": "Resource was not found"
}
```

**Common Causes:**
- Person ID does not exist
- Invalid person_id in path parameter

### 409 Conflict

```json
{
  "error": "Person with BDRC ID 'P123456' already exists"
}
```

**Common Causes:**
- Attempting to create person with duplicate BDRC ID
- Attempting to update person with BDRC ID that belongs to another person
- Attempting to update person with Wiki ID that belongs to another person

### 422 Validation Error

```json
{
  "error": "Validation error",
  "details": [
    {
      "loc": ["body", "name"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

**Common Causes:**
- Missing required `name` field
- Empty name object
- Invalid field types
- Name object without any language entries

### 500 Server Error

```json
{
  "error": "There was an error on the server"
}
```

---

## Best Practices

### 1. Creating Persons

**Do:**
- Provide names in multiple languages when available
- Include BDRC identifiers for better integration with Buddhist Digital Resource Center
- Include Wikidata identifiers for better linking to external knowledge bases
- Use alternative names to capture different name forms, titles, or appellations
- Standardize name formats within each language

**Don't:**
- Create duplicate persons (check via BDRC ID or name search first)
- Use abbreviations in primary name field (use alt_names instead)
- Leave name object empty
- Duplicate BDRC or Wiki IDs across persons

### 2. Updating Persons

**Do:**
- Update only the fields that need to change
- Verify person exists before updating
- Check for conflicts before changing BDRC or Wiki IDs
- Add alternative names incrementally as you discover them

**Don't:**
- Replace entire name object if only updating one language
- Overwrite alt_names array if only adding one name
- Change BDRC ID unless correcting an error

### 3. Searching Persons

**Do:**
- Use name filter for substring matching (searches all names)
- Use BDRC filter for exact identifier lookup
- Use pagination for large result sets
- Search by alternative names (included in name search)

**Don't:**
- Assume name search is exact match (it's case-insensitive substring)
- Request limit > 100 (will be rejected)
- Mix BDRC/Wiki filters with name filters unnecessarily

### 4. Multilingual Names

**Do:**
- Provide Tibetan names in Tibetan script when available
- Include Sanskrit names for Indian Buddhist masters
- Use proper Unicode for all scripts
- Include English transliterations for accessibility

**Don't:**
- Mix scripts within a single language entry
- Use ASCII approximations when proper Unicode is available
- Leave primary languages empty

---

## Workflow Examples

### Scenario 1: Import Person from BDRC

```bash
# Step 1: Check if person already exists
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?bdrc=P2816" \
  -H "X-API-Key: your_api_key"

# If not exists, create person
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "bo": "ཀླུ་སྒྲུབ།",
      "en": "Nagarjuna",
      "sa": "नागार्जुन"
    },
    "alt_names": [
      {
        "bo": "སློབ་དཔོན་ཀླུ་སྒྲུབ།",
        "en": "Acharya Nagarjuna"
      }
    ],
    "bdrc": "P2816",
    "wiki": "Q182485"
  }'

# Step 2: Use person in text creation
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"bo": "རིན་ཆེན་ཕྲེང་བ།"},
    "language": "bo",
    "category_id": "CAT_PHILOSOPHY",
    "contributions": [
      {
        "person_bdrc_id": "P2816",
        "role": "author"
      }
    ]
  }'
```

### Scenario 2: Add Alternative Name

```bash
# Step 1: Get current person data
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key"

# Step 2: Update with new alternative name (replaces alt_names array)
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "alt_names": [
      {
        "en": "Existing Alternative 1"
      },
      {
        "en": "New Alternative Name",
        "bo": "མིང་གསར།"
      }
    ]
  }'
```

**Important:** PATCH replaces the entire alt_names array. To add a name, include all existing names plus the new one.

### Scenario 3: Link to External Resources

```bash
# Add Wikidata identifier to existing person
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "wiki": "Q182485"
  }'

# Update BDRC identifier (e.g., if correcting an error)
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "bdrc": "P2816"
  }'
```

### Scenario 4: Find Person and Use in Text

```bash
# Step 1: Search for person by name
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?name=Nagarjuna" \
  -H "X-API-Key: your_api_key"

# Response includes person_id: "P12345678"

# Step 2: Create text with that person as author
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"bo": "རིན་ཆེན་ཕྲེང་བ།"},
    "language": "bo",
    "category_id": "CAT_PHILOSOPHY",
    "contributions": [
      {
        "person_id": "P12345678",
        "role": "author"
      }
    ]
  }'
```

---

## Advanced Usage

### Working with Multilingual Names

Persons in the OpenPecha system often have names in multiple languages and scripts. The API supports rich multilingual name data:

**Example: Buddhist Master with Multiple Names**

```json
{
  "name": {
    "bo": "རྗེ་ཙོང་ཁ་པ་བློ་བཟང་གྲགས་པ།",
    "en": "Je Tsongkhapa Lobsang Drakpa",
    "sa": "सुमति कीर्ति",
    "zh": "宗喀巴大师"
  },
  "alt_names": [
    {
      "bo": "ཙོང་ཁ་པ།",
      "en": "Tsongkhapa"
    },
    {
      "bo": "རྗེ་རིན་པོ་ཆེ།",
      "en": "Je Rinpoche"
    },
    {
      "bo": "བློ་བཟང་གྲགས་པ།",
      "en": "Lobsang Drakpa"
    },
    {
      "en": "Tsong-kha-pa"
    },
    {
      "sa": "सुमतिकीर्ति",
      "en": "Sumati Kirti"
    }
  ],
  "bdrc": "P64",
  "wiki": "Q234330"
}
```

**Benefits:**
- Comprehensive name coverage for search
- Support for different transliteration systems
- Multiple names/titles for the same person
- Language-specific name forms

### Search Strategies

**By Primary or Alternative Name:**

The `name` filter searches both the primary name and all alternative names:

```bash
# Finds person if "Rinpoche" appears in any name
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?name=Rinpoche" \
  -H "X-API-Key: your_api_key"
```

**By Exact BDRC ID:**

```bash
# Exact match on BDRC identifier
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?bdrc=P2816" \
  -H "X-API-Key: your_api_key"
```

**By Wikidata ID:**

```bash
# Exact match on Wikidata identifier
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?wiki=Q182485" \
  -H "X-API-Key: your_api_key"
```

### Handling Duplicate Prevention

Before creating a person, check if they already exist:

```bash
# Check by BDRC ID (most reliable)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?bdrc=P2816" \
  -H "X-API-Key: your_api_key"

# Check by name (less reliable, returns substring matches)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?name=Nagarjuna" \
  -H "X-API-Key: your_api_key"
```

If person exists, use their ID. If not, create new person.

### Pagination Strategies

**Basic Pagination:**

```bash
# Page 1 (first 20 persons)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?limit=20&offset=0"

# Page 2 (next 20 persons)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?limit=20&offset=20"
```

**Large Result Sets:**

```bash
# Fetch 100 persons at a time (maximum)
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?limit=100&offset=0"
```

**Filtered Pagination:**

```bash
# First page of persons named "Rinpoche"
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?name=Rinpoche&limit=20&offset=0"
```

---

## Common Scenarios

### Scenario 1: Importing Historical Buddhist Masters

```bash
# Create person with full metadata
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "bo": "འཇམ་དབྱངས་མཁྱེན་བརྩེའི་དབང་པོ།",
      "en": "Jamyang Khyentse Wangpo",
      "sa": "मञ्जुश्रीज्ञानवशवर्तिन्"
    },
    "alt_names": [
      {
        "bo": "མཁྱེན་བརྩེ་དབང་པོ།",
        "en": "Khyentse Wangpo"
      },
      {
        "bo": "འཇམ་དབྱངས་མཁྱེན་བརྩེ།",
        "en": "Jamyang Khyentse"
      }
    ],
    "bdrc": "P258",
    "wiki": "Q983614"
  }'
```

### Scenario 2: Modern Translator

```bash
# Create modern translator
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "en": "Jeffrey Hopkins"
    },
    "alt_names": [
      {
        "en": "Prof. Jeffrey Hopkins"
      }
    ],
    "wiki": "Q6175666"
  }'
```

### Scenario 3: Anonymous or Unknown Authors

```bash
# Create placeholder for unknown author
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "en": "Unknown Author",
      "bo": "རྩོམ་པ་པོ་མི་ཤེས།"
    }
  }'
```

### Scenario 4: Correcting Person Information

```bash
# Step 1: Get current person data
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key"

# Step 2: Correct BDRC ID
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "bdrc": "P2816"
  }'

# Step 3: Verify update
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key"
```

---

## Validation Rules

### Person Creation

| Field | Validation |
|-------|------------|
| `name` | Required, must be object with at least one language entry |
| `name[language]` | Non-empty string |
| `alt_names` | Optional, must be array of objects |
| `alt_names[]` | Each must be object with at least one language entry |
| `bdrc` | Optional, must be unique across all persons |
| `wiki` | Optional, must be unique across all persons |

### Person Update

| Field | Validation |
|-------|------------|
| All fields | Optional, at least one must be provided |
| `name` | If provided, must be object with at least one language entry |
| `alt_names` | If provided, must be array of objects |
| `bdrc` | If provided, must be unique (409 Conflict if duplicate) |
| `wiki` | If provided, must be unique (409 Conflict if duplicate) |

---

## Integration Examples

### Person → Text → Edition Flow

Complete workflow from person creation to edition with content:

```bash
# 1. Create person
PERSON_RESPONSE=$(curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "name": {"bo": "ཀླུ་སྒྲུབ།", "en": "Nagarjuna"},
    "bdrc": "P2816"
  }')
PERSON_ID=$(echo $PERSON_RESPONSE | jq -r '.id')

# 2. Create text with person as author
TEXT_RESPONSE=$(curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"bo": "རིན་ཆེན་ཕྲེང་བ།"},
    "language": "bo",
    "category_id": "CAT_PHIL",
    "contributions": [{"person_id": "'$PERSON_ID'", "role": "author"}]
  }')
TEXT_ID=$(echo $TEXT_RESPONSE | jq -r '.id')

# 3. Create edition
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/$TEXT_ID/editions" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "metadata": {"type": "critical"},
    "content": "Full text content..."
  }'
```

### Using BDRC ID for Direct Linking

When you have a BDRC person ID, you can use it directly without knowing the internal person ID:

```bash
# Create text using person_bdrc_id
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"bo": "གཞུང་ལུགས།"},
    "language": "bo",
    "category_id": "CAT_PHIL",
    "contributions": [
      {
        "person_bdrc_id": "P2816",
        "role": "author"
      }
    ]
  }'
```

The API will automatically resolve `P2816` to the internal person ID. If the person doesn't exist yet, you'll get a 404 error and need to create the person first.

---

## Search and Discovery

### Finding Persons by Name Pattern

**Tibetan Script Search:**

```bash
# Search Tibetan names
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?name=ཀླུ་སྒྲུབ" \
  -H "X-API-Key: your_api_key"
```

**English Transliteration Search:**

```bash
# Search English names
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?name=Nagarjuna" \
  -H "X-API-Key: your_api_key"
```

**Title Search:**

```bash
# Search for persons with "Rinpoche" in their name
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?name=Rinpoche" \
  -H "X-API-Key: your_api_key"
```

**Partial Name Search:**

```bash
# Search for persons with "Khyentse" anywhere in name
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?name=Khyentse" \
  -H "X-API-Key: your_api_key"
```

### Building Person Directory

```bash
# Get all persons in batches of 100
#!/bin/bash

OFFSET=0
LIMIT=100

while true; do
  RESPONSE=$(curl -s -X GET \
    "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?limit=$LIMIT&offset=$OFFSET" \
    -H "X-API-Key: your_api_key")
  
  COUNT=$(echo $RESPONSE | jq '. | length')
  
  if [ $COUNT -eq 0 ]; then
    break
  fi
  
  echo "Fetched $COUNT persons at offset $OFFSET"
  echo $RESPONSE | jq '.'
  
  OFFSET=$((OFFSET + LIMIT))
done
```

---

## Name Formatting Guidelines

### Tibetan Names

**Do:**
- Use Tibetan Unicode (U+0F00–U+0FFF)
- Include full formal names in primary name field
- Use abbreviated forms in alt_names
- Include title prefixes (རྗེ་, སློབ་དཔོན་, etc.) in alt_names

**Example:**
```json
{
  "name": {
    "bo": "རྗེ་ཙོང་ཁ་པ་བློ་བཟང་གྲགས་པ།"
  },
  "alt_names": [
    {"bo": "ཙོང་ཁ་པ།"},
    {"bo": "རྗེ་རིན་པོ་ཆེ།"},
    {"bo": "བློ་བཟང་གྲགས་པ།"}
  ]
}
```

### English Transliterations

**Do:**
- Use standard academic transliteration in primary name
- Include common alternate spellings in alt_names
- Include titles separately in alt_names

**Example:**
```json
{
  "name": {
    "en": "Nagarjuna"
  },
  "alt_names": [
    {"en": "Nāgārjuna"},
    {"en": "Acharya Nagarjuna"},
    {"en": "Nagārjuna"}
  ]
}
```

### Sanskrit Names

**Do:**
- Use Devanagari script for Sanskrit names
- Include romanized versions in alt_names
- Preserve diacritical marks in romanization

**Example:**
```json
{
  "name": {
    "sa": "नागार्जुन"
  },
  "alt_names": [
    {"en": "Nāgārjuna"},
    {"sa": "आचार्य नागार्जुन"}
  ]
}
```

---

## Querying Related Data

### Find Texts by Person

While there's no direct endpoint to find texts by person, you can:

1. **Get person ID** (via search or known ID)
2. **Query texts** and filter client-side

```bash
# Step 1: Find person
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?name=Nagarjuna" \
  -H "X-API-Key: your_api_key"
# Returns: person_id "P12345678"

# Step 2: Get all texts and filter by person_id in contributions
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" | \
  jq '[.[] | select(.contributions[].person_id == "P12345678")]'
```

### Find Person from Text

```bash
# Step 1: Get text
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/T12345678" \
  -H "X-API-Key: your_api_key"
# Returns contributions with person_id

# Step 2: Get person details
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key"
```

**Note:** Text responses include resolved person names in contributions for convenience:

```json
{
  "contributions": [
    {
      "person_id": "P12345678",
      "person_bdrc_id": "P2816",
      "person_name": {
        "bo": "ཀླུ་སྒྲུབ།",
        "en": "Nagarjuna"
      },
      "role": "author"
    }
  ]
}
```

---

## Troubleshooting

### Common Issues

**Issue: "Person with BDRC ID 'P123456' already exists" (409 Conflict)**

**Solution:**
- Check if person already exists using `GET /v2/persons?bdrc=P123456`
- Use existing person ID instead of creating duplicate
- If updating, ensure new BDRC ID is unique

**Issue: "Validation error: field required" on name**

**Solution:**
- Ensure `name` field is provided
- Verify `name` is an object (not a string)
- Include at least one language entry in name object

```json
// ✗ Wrong
{
  "name": "John Doe"
}

// ✓ Correct
{
  "name": {
    "en": "John Doe"
  }
}
```

**Issue: "Resource was not found" when using person_bdrc_id in text creation**

**Solution:**
- Person with that BDRC ID doesn't exist yet
- Create person first using `POST /v2/persons`
- Then use person_id or person_bdrc_id in text contributions

**Issue: Name search returns too many results**

**Solution:**
- Name filter uses substring matching (case-insensitive)
- Be more specific in search query
- Use BDRC ID for exact match
- Use pagination to handle large result sets

**Issue: Cannot update alternative names without losing existing ones**

**Solution:**
- PATCH replaces entire alt_names array
- Fetch current person data first
- Merge new alt_names with existing ones
- Send complete alt_names array in PATCH

```bash
# Step 1: Get current data
CURRENT=$(curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key")

# Step 2: Merge alt_names (do this in your application code)
# Add new name to existing alt_names array

# Step 3: Update with complete array
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "alt_names": [
      {"en": "Existing Name 1"},
      {"en": "Existing Name 2"},
      {"en": "New Name"}
    ]
  }'
```

---

## Data Quality Guidelines

### BDRC Identifiers

- Format: `P` followed by digits (e.g., `P2816`, `P64`)
- Validate format before submission
- Check BDRC database for correct IDs
- BDRC IDs are unique and immutable

### Wikidata Identifiers

- Format: `Q` followed by digits (e.g., `Q182485`, `Q234330`)
- Validate against Wikidata before submission
- Link to Wikidata for additional biographical information
- Wikidata IDs are unique and immutable

### Name Entry

**Primary Name:**
- Use most formal or complete name form
- Include all parts of compound names
- Prefer traditional names over modern romanizations

**Alternative Names:**
- Include common short forms
- Add honorific titles separately
- Include different transliteration systems
- Add names from different traditions

### Character Encoding

- Use UTF-8 encoding for all text
- Use proper Unicode for Tibetan (not Wylie)
- Use Devanagari for Sanskrit (not IAST unless in English field)
- Preserve diacritical marks in romanizations

---

## API Response Details

### Name Object Structure

The name object uses language codes as keys and name strings as values:

```json
{
  "name": {
    "bo": "ཀླུ་སྒྲུབ།",
    "en": "Nagarjuna",
    "sa": "नागार्जुन",
    "zh": "龙树"
  }
}
```

**Supported Languages:**
- `bo`: Tibetan (Tibetan script)
- `en`: English (Latin script)
- `sa`: Sanskrit (Devanagari script)
- `zh`: Chinese (Chinese characters)
- `pi`: Pali
- Any other valid language code

### Alternative Names Array

Alternative names follow the same structure as the primary name:

```json
{
  "alt_names": [
    {
      "bo": "སློབ་དཔོན་ཀླུ་སྒྲུབ།",
      "en": "Acharya Nagarjuna"
    },
    {
      "en": "Nāgārjuna"
    },
    {
      "bo": "ཀླུ་གྲུབ།"
    }
  ]
}
```

**Each entry:**
- Can have one or multiple languages
- Is independent of other entries
- Contributes to name search

---

## Quick Reference

### Creating Different Person Types

| Person Type | Example Name Structure | Common Fields |
|-------------|----------------------|---------------|
| **Buddhist Master** | Tibetan + English + Sanskrit | bdrc, wiki, multiple alt_names |
| **Modern Scholar** | English (+ Tibetan if available) | wiki, professional titles in alt_names |
| **Translator** | Primary language + English | bdrc (if applicable) |
| **Unknown/Anonymous** | "Unknown" in relevant languages | No external identifiers |

### Query Parameters Summary

| Endpoint | Filtering | Pagination | Matching |
|----------|-----------|------------|----------|
| `GET /v2/persons` | name, bdrc, wiki | limit, offset | name: substring, bdrc/wiki: exact |
| `GET /v2/persons/{person_id}` | N/A | N/A | N/A |

### HTTP Methods Summary

| Method | Endpoint | Purpose | Auth Required |
|--------|----------|---------|---------------|
| GET | `/v2/persons` | List all persons with filtering | Yes |
| GET | `/v2/persons/{person_id}` | Get single person by ID | Yes |
| POST | `/v2/persons` | Create new person | Yes |
| PATCH | `/v2/persons/{person_id}` | Partially update person | Yes |

### Status Codes Summary

| Status | Meaning | Common Causes |
|--------|---------|---------------|
| 200 | OK | Successful GET/PATCH |
| 201 | Created | Successful POST |
| 400 | Bad Request | Invalid parameters |
| 404 | Not Found | Person ID doesn't exist |
| 409 | Conflict | Duplicate BDRC/Wiki ID |
| 422 | Validation Error | Missing required fields, invalid format |
| 500 | Server Error | Internal error |

---

## Testing and Development

### Test Person Creation

```bash
# Create a test person
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "en": "Test Person",
      "bo": "དཔེ་མཚོན་མི་སྣ།"
    }
  }'
```

### Test Person Retrieval

```bash
# Get person by ID
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key" \
  -w "\nHTTP Status: %{http_code}\n"
```

### Test Person Search

```bash
# Test name search
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?name=Test" \
  -H "X-API-Key: your_api_key" \
  | jq '.'

# Test BDRC search
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?bdrc=P2816" \
  -H "X-API-Key: your_api_key" \
  | jq '.'
```

### Test Person Update

```bash
# Update and verify
curl -X PATCH "https://api-l25bgmwqoa-uc.a.run.app/v2/persons/P12345678" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "wiki": "Q999999"
  }' | jq '.'
```

---

## Migration and Data Import

### Importing from BDRC

When importing persons from Buddhist Digital Resource Center:

```bash
#!/bin/bash

# Example: Import Nagarjuna
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "bo": "ཀླུ་སྒྲུབ།",
      "en": "Nagarjuna",
      "sa": "नागार्जुन"
    },
    "alt_names": [
      {
        "bo": "སློབ་དཔོན་ཀླུ་སྒྲུབ།",
        "en": "Acharya Nagarjuna"
      },
      {
        "bo": "ཀླུ་གྲུབ།"
      }
    ],
    "bdrc": "P2816",
    "wiki": "Q182485"
  }'
```

**Best Practices:**
1. Check if person exists first (by BDRC ID)
2. Import primary Tibetan name from BDRC
3. Add English transliteration
4. Include Sanskrit name if applicable
5. Add alternative names and titles
6. Link to Wikidata when available

### Bulk Import

```python
import requests
import json

API_KEY = "your_api_key"
BASE_URL = "https://api-l25bgmwqoa-uc.a.run.app"
headers = {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
}

persons_data = [
    {
        "name": {"bo": "ཀླུ་སྒྲུབ།", "en": "Nagarjuna"},
        "bdrc": "P2816",
        "wiki": "Q182485"
    },
    {
        "name": {"bo": "ཙོང་ཁ་པ།", "en": "Tsongkhapa"},
        "bdrc": "P64",
        "wiki": "Q234330"
    }
]

for person_data in persons_data:
    # Check if exists
    bdrc = person_data.get("bdrc")
    if bdrc:
        check_response = requests.get(
            f"{BASE_URL}/v2/persons?bdrc={bdrc}",
            headers=headers
        )
        if check_response.json():
            print(f"Person {bdrc} already exists, skipping")
            continue
    
    # Create person
    response = requests.post(
        f"{BASE_URL}/v2/persons",
        headers=headers,
        json=person_data
    )
    
    if response.status_code == 201:
        person_id = response.json()["id"]
        print(f"Created person: {person_id}")
    else:
        print(f"Error: {response.status_code} - {response.text}")
```

---

## Complete TypeScript Types

```typescript
// Input types
interface PersonInput {
  name: {                        // Required
    [language: string]: string;  // At least one language required
  };
  alt_names?: Array<{            // Optional
    [language: string]: string;
  }> | null;
  bdrc?: string | null;          // Optional: BDRC identifier
  wiki?: string | null;          // Optional: Wikidata identifier
}

interface PersonPatch {
  name?: {                       // Optional
    [language: string]: string;
  };
  alt_names?: Array<{            // Optional
    [language: string]: string;
  }> | null;
  bdrc?: string | null;          // Optional
  wiki?: string | null;          // Optional
}

// Output types
interface PersonOutput {
  id: string;                    // Person ID
  name: {                        // Localized name
    [language: string]: string;
  };
  alt_names?: Array<{            // Alternative names (optional)
    [language: string]: string;
  }> | null;
  bdrc?: string | null;          // BDRC identifier
  wiki?: string | null;          // Wikidata identifier
}

// Used in text contributions
interface ContributionOutput {
  person_id?: string | null;
  person_bdrc_id?: string | null;
  person_name?: {                // Resolved from person record
    [language: string]: string;
  } | null;
  ai_id?: string | null;
  role: "author" | "translator" | "reviser" | "scholar";
}
```

---

## Real-World Examples

### Example 1: Historical Buddhist Master (Complete)

```bash
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "bo": "འཇམ་མགོན་ཀོང་སྤྲུལ་བློ་གྲོས་མཐའ་ཡས།",
      "en": "Jamgön Kongtrül Lodrö Thaye",
      "sa": "मञ्जुनाथ"
    },
    "alt_names": [
      {
        "bo": "ཀོང་སྤྲུལ་བློ་གྲོས་མཐའ་ཡས།",
        "en": "Kongtrül Lodrö Thaye"
      },
      {
        "bo": "འཇམ་མགོན་ཀོང་སྤྲུལ།",
        "en": "Jamgön Kongtrül"
      },
      {
        "bo": "ཡོན་ཏན་རྒྱ་མཚོ།",
        "en": "Yönten Gyatso"
      }
    ],
    "bdrc": "P293",
    "wiki": "Q980080"
  }'
```

### Example 2: Modern Scholar (Minimal)

```bash
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "en": "Robert Thurman"
    },
    "alt_names": [
      {
        "en": "Robert A. F. Thurman"
      },
      {
        "en": "Prof. Robert Thurman"
      }
    ],
    "wiki": "Q371427"
  }'
```

### Example 3: Tibetan Scholar with Multiple Names

```bash
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "bo": "མི་ཕམ་རྒྱ་མཚོ།",
      "en": "Mipham Gyatso"
    },
    "alt_names": [
      {
        "bo": "འཇམ་དཔལ་དགྱེས་པའི་རྡོ་རྗེ།",
        "en": "Jampal Gyepe Dorje"
      },
      {
        "bo": "མི་ཕམ་འཇམ་དབྱངས་རྣམ་རྒྱལ་རྒྱ་མཚོ།",
        "en": "Mipham Jamyang Namgyal Gyatso"
      },
      {
        "en": "Ju Mipham"
      }
    ],
    "bdrc": "P252",
    "wiki": "Q708206"
  }'
```

### Example 4: Anonymous Author Placeholder

```bash
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": {
      "en": "Anonymous",
      "bo": "མིང་མེད།"
    }
  }'
```

---

## Integration with Texts API

### Using Persons in Text Contributions

When creating texts, reference persons in contributions:

**Method 1: Using Internal Person ID**

```bash
# If you know the person_id
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"bo": "རིན་ཆེན་ཕྲེང་བ།"},
    "language": "bo",
    "category_id": "CAT_PHIL",
    "contributions": [
      {
        "person_id": "P12345678",
        "role": "author"
      }
    ]
  }'
```

**Method 2: Using BDRC Person ID**

```bash
# If you have BDRC ID (API resolves automatically)
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"bo": "རིན་ཆེན་ཕྲེང་བ།"},
    "language": "bo",
    "category_id": "CAT_PHIL",
    "contributions": [
      {
        "person_bdrc_id": "P2816",
        "role": "author"
      }
    ]
  }'
```

### Multiple Contributors

```bash
# Text with author and translator
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "title": {"en": "English Translation of Root Text"},
    "language": "en",
    "category_id": "CAT_PHIL",
    "translation_of": "T_SOURCE",
    "contributions": [
      {
        "person_bdrc_id": "P2816",
        "role": "author"
      },
      {
        "person_id": "P_TRANSLATOR",
        "role": "translator"
      }
    ]
  }'
```

---

## Complete Workflow Example

### Creating Text with New Person

```bash
# Step 1: Check if person exists
SEARCH_RESULT=$(curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?bdrc=P2816" \
  -H "X-API-Key: your_api_key")

if [ "$(echo $SEARCH_RESULT | jq '. | length')" -eq 0 ]; then
  # Person doesn't exist, create it
  echo "Creating person..."
  
  PERSON_RESPONSE=$(curl -s -X POST \
    "https://api-l25bgmwqoa-uc.a.run.app/v2/persons" \
    -H "X-API-Key: your_api_key" \
    -H "Content-Type: application/json" \
    -d '{
      "name": {
        "bo": "ཀླུ་སྒྲུབ།",
        "en": "Nagarjuna"
      },
      "bdrc": "P2816",
      "wiki": "Q182485"
    }')
  
  PERSON_ID=$(echo $PERSON_RESPONSE | jq -r '.id')
  echo "Created person: $PERSON_ID"
else
  # Person exists, get ID
  PERSON_ID=$(echo $SEARCH_RESULT | jq -r '.[0].id')
  echo "Found existing person: $PERSON_ID"
fi

# Step 2: Create text with person as author
TEXT_RESPONSE=$(curl -s -X POST \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/texts" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "title": {
      "bo": "རིན་ཆེན་ཕྲེང་བ།",
      "en": "The Precious Garland"
    },
    "language": "bo",
    "category_id": "CAT_PHILOSOPHY",
    "contributions": [
      {
        "person_id": "'$PERSON_ID'",
        "role": "author"
      }
    ],
    "bdrc": "W23703",
    "license": "public"
  }')

TEXT_ID=$(echo $TEXT_RESPONSE | jq -r '.id')
echo "Created text: $TEXT_ID"

# Step 3: Verify text shows person information
curl -s -X GET \
  "https://api-l25bgmwqoa-uc.a.run.app/v2/texts/$TEXT_ID" \
  -H "X-API-Key: your_api_key" \
  | jq '.contributions'
```

---

## Frequently Asked Questions

### Q: What's the difference between person_id and person_bdrc_id?

**A:** 
- `person_id`: Internal system identifier (e.g., `P12345678`)
- `person_bdrc_id`: External BDRC identifier (e.g., `P2816`)

When creating texts, you can use either:
- If you have the internal ID, use `person_id`
- If you have the BDRC ID, use `person_bdrc_id` (API resolves automatically)

### Q: Can I search for a person by partial name?

**A:** Yes, the `name` filter performs case-insensitive substring matching across both primary names and alternative names.

```bash
# Finds any person with "Khyentse" anywhere in their names
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/persons?name=Khyentse" \
  -H "X-API-Key: your_api_key"
```

### Q: What happens if I try to create a person with a duplicate BDRC ID?

**A:** The API returns a `409 Conflict` error. BDRC identifiers must be unique across all persons.

### Q: Can I update only one language in the name object?

**A:** No, PATCH replaces the entire field. To update one language:
1. Get current person data
2. Merge your changes with existing name object
3. Send complete name object in PATCH

### Q: How do I delete a person?

**A:** The API currently does not support person deletion. This is intentional to maintain referential integrity with texts that reference persons in contributions.

### Q: Can I query all texts by a specific person?

**A:** Not directly. You need to:
1. Get person ID
2. Query all texts
3. Filter client-side for texts with that person_id in contributions

Alternatively, if you know the person's BDRC ID, you can reference it directly when creating texts using `person_bdrc_id`.

---

## See Also

- [Texts API Documentation](./texts-api.md) - Using persons in text contributions
- [Editions API Documentation](./editions-api.md) - Edition management
- OpenAPI Specification: `GET /v2/schema/openapi`
- BDRC Person Database: https://www.tbrc.org/
- Wikidata: https://www.wikidata.org/
