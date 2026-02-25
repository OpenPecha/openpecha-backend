# Schema API Documentation

This document provides documentation for the Schema endpoint in the OpenPecha API v2.

---

## Table of Contents

1. [Overview](#overview)
2. [Schema Endpoints](#schema-endpoints)
   - [Get OpenAPI Specification](#get-openapi-specification)
3. [Use Cases](#use-cases)

---

## Overview

The Schema API provides access to the OpenAPI specification file that describes all available endpoints, request/response schemas, and data models for the OpenPecha API.

### Key Concepts

- **OpenAPI Specification**: A standard format (formerly Swagger) for describing REST APIs
- **YAML Format**: The specification is returned in YAML format
- **Machine-Readable**: Can be used by tools to generate client libraries, documentation, and more

### Base URL

```
Development: https://api-l25bgmwqoa-uc.a.run.app
Production: https://api-aq25662yyq-uc.a.run.app
Test: https://api-kwgjscy6gq-uc.a.run.app
Local: http://127.0.0.1:5001/pecha-backend-test-3a4d0/us-central1/api
```

---

## Schema Endpoints

### Get OpenAPI Specification

Retrieve the complete OpenAPI specification file for the API.

**Endpoint:**
```
GET /v2/schema/openapi
```

**Parameters:** None

**Authentication:** Not required (public endpoint)

**Response: 200 OK**

Returns the OpenAPI specification file in YAML format.

**Content-Type:** `application/x-yaml`

**Response Structure:**

The response is a complete OpenAPI 3.0 specification document containing:
- API metadata (version, title, description)
- Server URLs
- All available endpoints
- Request/response schemas
- Data models and components
- Authentication requirements
- Error response formats

**Example Response (partial):**

```yaml
openapi: 3.0.0
info:
  title: OpenPecha API
  version: 2.0.0
  description: API for managing Buddhist texts and related resources
servers:
  - url: https://api-l25bgmwqoa-uc.a.run.app
paths:
  /v2/texts:
    get:
      summary: List all texts
      parameters:
        - name: limit
          in: query
          schema:
            type: integer
      responses:
        '200':
          description: Success
          content:
            application/json:
              schema:
                type: array
  # ... many more endpoints
components:
  schemas:
    # ... data models
  responses:
    # ... reusable responses
```

**Error Responses:**
- `500 Server Error`: Internal server error

**Example Usage:**

```bash
# Download OpenAPI specification
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/schema/openapi" \
  -o openapi.yaml

# View specification
curl -X GET "https://api-l25bgmwqoa-uc.a.run.app/v2/schema/openapi"

# Download with wget
wget https://api-l25bgmwqoa-uc.a.run.app/v2/schema/openapi -O openapi.yaml
```

---
