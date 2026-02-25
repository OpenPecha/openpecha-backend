# Relations API Documentation

This document provides comprehensive documentation for all Relations-related endpoints in the OpenPecha API v2.

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Relations Endpoints](#relations-endpoints)
   - [Get Relations for Expression](#get-relations-for-expression)
4. [Data Models](#data-models)
5. [Error Responses](#error-responses)
6. [Best Practices](#best-practices)
7. [Workflow Examples](#workflow-examples)

---

## Overview

The Relations API provides a unified view of all relationships for a given expression (text). It returns connections between texts such as translations, commentaries, and other text relationships.

### Key Concepts

- **Relation**: A directional connection between two expressions
- **Expression**: A text in the OpenPecha system (referenced by expression_id or text_id)
- **Relation Type**: The nature of the relationship (TRANSLATION_OF, COMMENTARY_OF)
- **Direction**: Whether the relation is incoming (`in`) or outgoing (`out`) from the query expression
- **Relation Graph**: The complete network of related expressions

### Relation Types

| Type | Description | Example |
|------|-------------|---------|
| `TRANSLATION_OF` | One text is a translation of another | English translation of Tibetan text |
| `COMMENTARY_OF` | One text is a commentary on another | Scholarly commentary on a sutra |

### Directions

| Direction | Meaning | Example |
|-----------|---------|---------|
| `in` | Incoming relation (other → this) | English text is translation of Tibetan text (Tibetan's perspective: incoming) |
| `out` | Outgoing relation (this → other) | Tibetan text has English translation (Tibetan's perspective: outgoing) |

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

## Relations Endpoints

### Get Relations for Expression

Retrieve all relationships for a given expression, including relation type, direction, and related expression IDs.

**Endpoint:**
```
GET /v2/relations/expressions/{expression_id}
```

**Parameters:**

| Name | Type | Location | Required | Description |
|------|------|----------|----------|-------------|
| `expression_id` | string | path | Yes | The ID of the expression to retrieve relations for |

**Response: 200 OK**

The response is an object where:
- **Keys**: Expression IDs (including the query expression and related expressions)
- **Values**: Arrays of relations for that expression

```json
{
  "13mxFPmsmIqktVTYVV8AL": [
    {
      "type": "TRANSLATION_OF",
      "direction": "in",
      "otherId": "RQKWt16H22fyxodz1Hpu8"
    },
    {
      "type": "TRANSLATION_OF",
      "direction": "in",
      "otherId": "zRsb99ocU5fM8wurAUfMQ"
    },
    {
      "type": "COMMENTARY_OF",
      "direction": "out",
      "otherId": "t0CzlMC69QUySNEBzSL4e"
    }
  ]
}
```

**Response Structure:**

The response contains the complete relation graph for the expression:
1. Primary key is the query expression ID
2. Relations array contains all direct relationships
3. Additional keys may be present for related expressions and their relationships

**Understanding the Response:**

For expression `13mxFPmsmIqktVTYVV8AL`:
- Has 2 incoming TRANSLATION_OF relations (this expression is a translation of 2 other texts)
- Has 1 outgoing COMMENTARY_OF relation (this expression is a commentary on another text)

**Empty Result:**

```json
{
  "13mxFPmsmIqktVTYVV8AL": []
}
```

**Error Responses:**
- `404 Not Found`: Expression does not exist
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Get all relations for an expression
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/relations/expressions/13mxFPmsmIqktVTYVV8AL" \
  -H "X-API-Key: your_api_key"

# Pretty print with jq
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/relations/expressions/13mxFPmsmIqktVTYVV8AL" \
  -H "X-API-Key: your_api_key" \
  | jq '.'
```

**Use Cases:**
- Display all translations of a text
- Show commentaries on a text
- Build navigation between related texts
- Construct relation graphs
- Find source texts for translations
- Discover related content

---

## Data Models

### Relation

Schema for individual relation:

```typescript
{
  type: "TRANSLATION_OF" | "COMMENTARY_OF";  // Relationship type
  direction: "in" | "out";                    // Direction relative to expression
  otherId: string;                            // Related expression ID
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Type of relationship |
| `direction` | string | Direction: `in` (incoming) or `out` (outgoing) |
| `otherId` | string | ID of the related expression |

**Example:**

```json
{
  "type": "TRANSLATION_OF",
  "direction": "in",
  "otherId": "SOURCE_TEXT_ID"
}
```

**Interpretation:** This expression is a translation of `SOURCE_TEXT_ID`

---

### RelationsResponse

Complete response schema:

```typescript
{
  [expression_id: string]: Relation[]
}
```

**Structure:**
- Keys are expression IDs
- Values are arrays of relations for that expression
- Primary key is always the query expression
- May include additional keys for related expressions

**Example:**

```json
{
  "QUERY_EXPRESSION_ID": [
    {
      "type": "TRANSLATION_OF",
      "direction": "in",
      "otherId": "SOURCE_TEXT_ID"
    },
    {
      "type": "COMMENTARY_OF",
      "direction": "out",
      "otherId": "COMMENTED_TEXT_ID"
    }
  ]
}
```

---

## Understanding Directions

### TRANSLATION_OF Relations

**Outgoing (`out`)**: This expression has been translated into other languages

```json
{
  "TIBETAN_TEXT_ID": [
    {
      "type": "TRANSLATION_OF",
      "direction": "out",
      "otherId": "ENGLISH_TEXT_ID"
    },
    {
      "type": "TRANSLATION_OF",
      "direction": "out",
      "otherId": "CHINESE_TEXT_ID"
    }
  ]
}
```

**Interpretation:** Tibetan text has English and Chinese translations

---

**Incoming (`in`)**: This expression is a translation of another expression

```json
{
  "ENGLISH_TEXT_ID": [
    {
      "type": "TRANSLATION_OF",
      "direction": "in",
      "otherId": "TIBETAN_TEXT_ID"
    }
  ]
}
```

**Interpretation:** English text is a translation of Tibetan text

---

### COMMENTARY_OF Relations

**Outgoing (`out`)**: This expression is a commentary on another expression

```json
{
  "COMMENTARY_TEXT_ID": [
    {
      "type": "COMMENTARY_OF",
      "direction": "out",
      "otherId": "ROOT_TEXT_ID"
    }
  ]
}
```

**Interpretation:** This text is a commentary on the root text

---

**Incoming (`in`)**: This expression has commentaries written about it

```json
{
  "ROOT_TEXT_ID": [
    {
      "type": "COMMENTARY_OF",
      "direction": "in",
      "otherId": "COMMENTARY_TEXT_ID_1"
    },
    {
      "type": "COMMENTARY_OF",
      "direction": "in",
      "otherId": "COMMENTARY_TEXT_ID_2"
    }
  ]
}
```

**Interpretation:** Root text has 2 commentaries

---

## Error Responses

### 404 Not Found

```json
{
  "error": "Resource was not found"
}
```

**Common Causes:**
- Expression ID does not exist
- Expression was deleted
- Invalid expression ID format

---

### 500 Server Error

```json
{
  "error": "There was an error on the server"
}
```

**Common Causes:**
- Database connection issues
- Internal server error
- Graph query timeout

---

