# Applications API Documentation

This document provides comprehensive documentation for all Applications-related endpoints in the OpenPecha API v2.

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Application Endpoints](#application-endpoints)
   - [Create New Application](#create-new-application)

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
