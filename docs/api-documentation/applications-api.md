# Applications API Documentation

This document provides comprehensive documentation for all Applications-related endpoints in the OpenPecha API v2.

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Application Endpoints](#application-endpoints)
   - [Create New Application](#create-new-application)
4. [Application Context](#application-context)
5. [Data Models](#data-models)
6. [Error Responses](#error-responses)
7. [Best Practices](#best-practices)
8. [Workflow Examples](#workflow-examples)

---

### Why Applications?

Applications provide:
1. **Content Isolation**: Separate category taxonomies for different platforms
2. **Multi-Tenancy**: Support multiple projects/services in one system
3. **Organized Namespaces**: Clear boundaries between different use cases


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

## Application Endpoints

### Create New Application

Create a new application entry in the database. Input is normalized to lowercase for both id and name.

**Endpoint:**
```
POST /v2/applications
```

**Request Body:**

```json
{
  "name": "MyApp"
}
```

**Request Schema:**

| Field | Type | Required | Constraints | Description |
|-------|------|----------|-------------|-------------|
| `name` | string | Yes | minLength: 1 | Application name (will be lowercased) |

**Response: 201 Created**

```json
{
  "id": "myapp",
  "name": "myapp"
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Application identifier (lowercase) |
| `name` | string | Application name (lowercase) |

**Key Behavior:**
- Input name is trimmed and converted to lowercase
- Both `id` and `name` are set to the normalized value
- Application IDs must be unique

**Error Responses:**
- `400 Bad Request`: Invalid request (e.g., missing name, invalid JSON)
- `422 Validation Error`: Application already exists or validation failed
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Create application
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/applications" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "MyApp"
  }'

# Response: {"id": "342424sdr", "name": "webuddhist"}
```

**Use Cases:**
- Initialize new platform or project
- Set up content namespace
- Create isolated category taxonomy
- Enable multi-tenant architecture

---

## Application Context

Applications are primarily used to scope category operations via the `X-Application` header.

### X-Application Header

Categories belong to specific applications. When querying or creating categories, you must specify which application context to use:

```bash
# Get categories for "webuddhist" application
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: webuddhist"

# Get categories for "monlam" application
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/categories" \
  -H "X-API-Key: your_api_key" \
  -H "X-Application: monlam"
```

---

## Data Models

### ApplicationCreateRequest

Input schema for creating application:

```typescript
{
  name: string;  // Application name (min 1 character, will be lowercased)
}
```

**Example:**

```json
{
  "name": "WebBuddhist"
}
```

### ApplicationOutput

Output schema for created application:

```typescript
{
  id: string;    // Application identifier (lowercase)
  name: string;  // Application name (lowercase)
}
```

**Example:**

```json
{
  "id": "webbuddhist",
  "name": "webbuddhist"
}
```

**Characteristics:**
- `id` and `name` are always identical
- Both are lowercase
- Unique across system

---

## Error Responses

### 400 Bad Request

```json
{
  "error": "There was an error with the request"
}
```

**Common Causes:**
- Missing request body
- Invalid JSON format
- Missing `name` field
- Empty `name` field

**Example:**

```bash
# Missing name field
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/applications" \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{}'

# Response: 400 Bad Request
```

---

### 422 Validation Error

```json
{
  "error": "Application with id 'myapp' already exists"
}
```

**Common Causes:**
- Application name already exists
- Duplicate application creation
- Name conflicts after normalization

**Example:**

```bash
# First call succeeds
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/applications" \
  -H "X-API-Key: your_api_key" \
  -d '{"name": "MyApp"}'
# Response: 201 Created

# Second call with same name (case-insensitive) fails
curl -X POST "https://api-l25bgmwqoa-uc.a.run.app/v2/applications" \
  -H "X-API-Key: your_api_key" \
  -d '{"name": "myapp"}'
# Response: 422 Validation Error
```

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
- System failure

---

