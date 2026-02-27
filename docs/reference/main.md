# Application entry point (main)

This document describes how the Flask app and Firebase HTTP entrypoint are wired in [functions/main.py](functions/main.py).

---

## Table of Contents

1. [Overview](#overview)
2. [Firebase and logging](#firebase-and-logging)
3. [create_app](#create_app)
4. [Blueprint registration](#blueprint-registration)
5. [Middleware and hooks](#middleware-and-hooks)
6. [Routes defined in main](#routes-defined-in-main)
7. [Error handling](#error-handling)
8. [Firebase function](#firebase-function)

---

## Overview

[functions/main.py](functions/main.py) serves two roles:

- **App factory** – `create_app(testing=False)` builds the Flask application, registers blueprints, and configures before/after request hooks and error handlers.
- **HTTP entrypoint** – The `api(req)` Firebase function receives each request and dispatches it through the Flask app.

---

## Firebase and logging

- **\_init_firebase()** – Ensures Firebase Admin is initialized (using `ApplicationDefault()` credentials if not already initialized). Idempotent.
- **Logging** – If `FUNCTIONS_EMULATOR` is not `"true"`, Google Cloud Logging is set up; otherwise basic logging is used with format `%(levelname)s - %(name)s - %(message)s`.

---

## create_app

**Signature:** `create_app(*, testing: bool = False) -> Flask`

**Config:**

| Key | Value |
|-----|--------|
| `TESTING` | Passed-through `testing` flag |
| `SEND_FILE_MAX_AGE_DEFAULT` | 0 |
| `JSON_AS_ASCII` | False |
| `JSON_SORT_KEYS` | False |

When `testing=True`, the `before_request` authentication step is skipped.

---

## Blueprint registration

| URL prefix | Blueprint | Module |
|------------|-----------|--------|
| `/v2/texts` | texts_bp | api.texts |
| `/api` | api_bp | api.api |
| `/v2/editions` | editions_bp | api.editions |
| `/v2/persons` | persons_bp | api.persons |
| `/v2/segments` | segments_bp | api.segments |
| `/v2/schema` | schema_bp | api.schema |
| `/v2/annotations` | annotations_bp | api.annotations |
| `/v2/applications` | applications_bp | api.applications |
| `/v2/categories` | categories_bp | api.categories |
| `/v2/languages` | languages_bp | api.languages |

---

## Middleware and hooks

### before_request: authenticate_request

- **Purpose:** Validate API key for all requests except public paths.
- **When testing:** No-op (returns without calling `validate_api_key()`).
- **Public paths (no auth):** `/v2/schema/openapi`, `/__/health`.
- **Other paths:** Calls `validate_api_key()` from [api.auth](functions/api/auth.py). On failure, `UnauthorizedError` is raised and handled by the exception handler.

### after_request (order matters)

1. **add_no_cache_headers** – Sets `Cache-Control: no-cache, no-store, must-revalidate`, `Pragma: no-cache`, `Expires: 0`.
2. **log_response** – Logs request method, path, request body, response body, and status code at INFO level. Response body is normalized (JSON decoded, text decoded, or `"<binary data>"`).

---

## Routes defined in main

| Method | Path | Auth | Response |
|--------|------|------|----------|
| GET | `/__/health` | No (public) | `{"status": "healthy"}`, 200 |

Used for Firebase / load balancer health checks.

---

## Error handling

**Handler:** `@app.errorhandler(Exception)` – handles all uncaught exceptions.

**Order of checks:**

1. **Pydantic ValidationError** – Response: `{"error": "<first validation message>"}`, status **422**.
2. **NotImplementedError** – Response: `{"error": str(e)}`, status **501**.
3. **OpenPechaError** (and subclasses) – Response: `e.to_dict()` (typically `{"error": "<message>"}`), status **e.status_code** (400, 401, 404, 409, 422, or 500).
4. **Any other Exception** – Full traceback is logged; response: `{"error": str(e)}`, status **500**.

---

## Firebase function

**Function name:** `api`

**Decorator:** `@https_fn.on_request(...)`

**Options:**

- **CORS** – Origins `["*"]`; methods `["GET", "POST", "OPTIONS", "PUT"]`.
- **max_instances** – 1.
- **timeout_sec** – 540 (9 minutes).
- **secrets** – `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, `NEO4J_DATABASE`.
- **memory** – 512 MB.

**Behavior:** Calls `_init_firebase()`, creates the app with `create_app()`, then runs the request in a Flask request context via `app.full_dispatch_request()`.
