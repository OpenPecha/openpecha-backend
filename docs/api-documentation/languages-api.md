# Languages API Documentation

This document provides documentation for all Languages-related endpoints in the OpenPecha API v2.

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Language Endpoints](#language-endpoints)
   - [List All Languages](#list-all-languages)
   - [Create New Language](#create-new-language)

---

## Overview

The Languages API provides endpoints to manage language codes and names in the OpenPecha system. Languages are used to tag texts, categories, and other content with their respective language information.

### Key Concepts

- **Language Code**: Short identifier for a language (e.g., "bo", "en", "sa", "zh")
- **Language Name**: Human-readable name for the language (e.g., "tibetan", "english")

### Common Language Codes

| Code | Language |
|------|----------|
| `bo` | Tibetan |
| `en` | English |
| `sa` | Sanskrit |
| `zh` | Chinese |
| `hi` | Hindi |
| `ne` | Nepali |

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

## Language Endpoints

### List All Languages

Retrieve all available language codes from the database.

**Endpoint:**
```
GET /v2/languages
```

**Parameters:** None

**Response: 200 OK**

```json
[
  {
    "code": "bo",
    "name": "tibetan"
  },
  {
    "code": "en",
    "name": "english"
  },
  {
    "code": "sa",
    "name": "sanskrit"
  },
  {
    "code": "zh",
    "name": "chinese"
  }
]
```

**Response Structure:**
- Returns array of language objects
- Each object contains `code` and `name`
- Empty array `[]` if no languages exist

**Error Responses:**
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Get all languages
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/languages" \
  -H "X-API-Key: your_api_key"
```

**Use Cases:**
- Populate language selector dropdowns
- Validate language codes
- Display available languages to users
- Build language filters

---

### Create New Language

Create a new language entry in the database.

**Endpoint:**
```
POST /v2/languages
```

**Request Body:**

```json
{
  "code": "bo",
  "name": "tibetan"
}
```

**Request Schema:**

| Field | Type | Required | Constraints | Description |
|-------|------|----------|-------------|-------------|
| `code` | string | Yes | minLength: 1 | Language code (e.g., "bo", "en") |
| `name` | string | Yes | minLength: 1 | Language name |

**Response: 201 Created**

```json
{
  "code": "bo",
  "name": "tibetan"
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `code` | string | Language code |
| `name` | string | Language name |

**Error Responses:**
- `400 Bad Request`: Invalid request (e.g., missing fields, invalid JSON)
- `422 Validation Error`: Validation failed (e.g., duplicate code, empty fields)
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Create Tibetan language
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/languages" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "code": "bo",
    "name": "tibetan"
  }'

# Create English language
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/languages" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "code": "en",
    "name": "english"
  }'

# Create Sanskrit language
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/languages" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "code": "sa",
    "name": "sanskrit"
  }'
```

**Use Cases:**
- Initialize language list
- Add support for new languages
- Set up system languages

---
