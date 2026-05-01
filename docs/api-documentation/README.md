# OpenPecha API v2

This folder documents the public FastAPI routes exposed by the backend. All endpoint paths below are relative to the API host.

## Base URLs

```text
Development: https://api-l25bgmwqoa-uc.a.run.app
Production: https://api-aq25662yyq-uc.a.run.app
Test: https://api-kwgjscy6gq-uc.a.run.app
Local Firebase emulator: http://127.0.0.1:5001/pecha-backend-test-3a4d0/us-central1/api
Local FastAPI app: http://127.0.0.1:8000
```

## Authentication

Most routes require `X-API-Key`. In local test/dev mode the dependency accepts requests without a real key, but deployed environments validate the header.

Application-scoped routes also require or accept `X-Application`:

- Required for `categories` and `tags`.
- Optional for `texts` and `segments`; when present, application-owned tags are filtered to that application.
- App-bound API keys must use the same `X-Application` value as the key's bound application.

Common errors:

- `401` for missing or invalid API keys in deployed environments.
- `404` when a requested resource or application does not exist.
- `409` for conflicts such as duplicate unique identifiers.
- `422` for Pydantic validation errors, missing required headers, invalid ranges, null values in PATCH requests, extra fields, or business validation errors.

## Common Data Shapes

Localized strings are objects keyed by language code:

```json
{
  "en": "Heart Sutra",
  "bo": "ཤེས་རབ་སྙིང་པོ།"
}
```

Character spans use half-open ranges: `start` is inclusive and `end` is exclusive. A segment contains one or more continuous `lines`; multiple lines inside one segment must be sorted and adjacent.

Paginated list endpoints return:

```json
{
  "items": [],
  "has_more": false,
  "offset": 0,
  "limit": 20
}
```

## Active Route Inventory

Applications:

- `POST /v2/applications`

Languages:

- `GET /v2/languages`
- `POST /v2/languages`

Categories:

- `GET /v2/categories`
- `GET /v2/categories/{category_id}`
- `POST /v2/categories`

Tags:

- `GET /v2/tags`
- `POST /v2/tags`
- `DELETE /v2/tags/{tag_id}`
- `POST /v2/texts/{text_id}/tags/{tag_id}`
- `DELETE /v2/texts/{text_id}/tags/{tag_id}`
- `POST /v2/segments/{segment_id}/tags/{tag_id}`
- `DELETE /v2/segments/{segment_id}/tags/{tag_id}`

Persons:

- `GET /v2/persons`
- `GET /v2/persons/{person_id}`
- `POST /v2/persons`
- `PATCH /v2/persons/{person_id}`

Texts:

- `GET /v2/texts`
- `GET /v2/texts/{text_id}`
- `POST /v2/texts`
- `PATCH /v2/texts/{text_id}`
- `GET /v2/texts/{text_id}/editions`
- `POST /v2/texts/{text_id}/editions`

Editions:

- `GET /v2/editions/{edition_id}`
- `GET /v2/editions/{edition_id}/content`
- `PATCH /v2/editions/{edition_id}/content`
- `DELETE /v2/editions/{edition_id}`
- `GET /v2/editions/{edition_id}/related`
- `GET /v2/editions/{edition_id}/segments/related`
- `GET /v2/editions/{edition_id}/segmentations`
- `POST /v2/editions/{edition_id}/segmentations`
- `GET /v2/editions/{edition_id}/alignments`
- `POST /v2/editions/{edition_id}/alignments`
- `GET /v2/editions/{edition_id}/pagination`
- `POST /v2/editions/{edition_id}/pagination`
- `GET /v2/editions/{edition_id}/bibliographic`
- `POST /v2/editions/{edition_id}/bibliographic`
- `GET /v2/editions/{edition_id}/durchens`
- `POST /v2/editions/{edition_id}/durchens`

Annotation objects by ID:

- `GET /v2/segmentations/{segmentation_id}`
- `DELETE /v2/segmentations/{segmentation_id}`
- `GET /v2/alignments/{alignment_id}`
- `DELETE /v2/alignments/{alignment_id}`
- `GET /v2/paginations/{pagination_id}`
- `DELETE /v2/paginations/{pagination_id}`
- `GET /v2/bibliographic/{bibliographic_id}`
- `DELETE /v2/bibliographic/{bibliographic_id}`
- `GET /v2/durchens/{durchen_id}`
- `DELETE /v2/durchens/{durchen_id}`

Segments:

- `GET /v2/segments/{segment_id}/content`
- `GET /v2/segments/{segment_id}/related`
- `GET /v2/segments/search`

Operational:

- `GET /__/health`
- `GET /openapi.json`
- `GET /docs`
- `GET /redoc`

There is currently no registered `/v2/relations/...` route and no registered `/v2/schema/openapi` route. Text relations are exposed on text records (`translation_of`, `commentary_of`, `translations`, `commentaries`) and related edition lookup is exposed through `GET /v2/editions/{edition_id}/related`.
