# Change Documentation — Verse Segments

This document describes the **verse segment** feature: what changed, in which files, and
the effects on every existing segmentation type. It is intended as a review/impact
reference for the change set on this branch.

## 1. Summary

A segment in a **Display** segmentation can now optionally be marked as a *verse* by
providing `"type": "verse"` together with a `verse_index` of the form `chapter.verse`
(e.g. `1.1`, `1.10`, `2.1`). At the graph level a verse segment receives an extra `:Verse`
label and a `verse_index` property. Verse marking applies **only to Display
segmentations** — alignment (Aligned/Target) segmentations are unaffected.

The change is **additive and backward compatible**: both fields are optional, plain
segments behave exactly as before, and no data migration is required.

## 2. Files changed

| File | Layer | Change |
|------|-------|--------|
| `models/enums.py` | Model | Added `SegmentType` enum with a single member `VERSE = "verse"`. |
| `models/annotation.py` | Model | Added `DisplaySegmentInput` (with `type` / `verse_index` + validation), added `type`/`verse_index` to `SegmentOutput`, switched `SegmentationInput.segments` to `list[DisplaySegmentInput]`, added a per-segmentation verse-index uniqueness validator, and the `VERSE_INDEX_RE` pattern. |
| `database/annotation/segmentation_database.py` | DB | Create path tags verse segments with `:Verse` + `verse_index` (via `FOREACH`); read queries return `is_verse` / `verse_index`; output mapping populates `type`/`verse_index`. |
| `database/segment_database.py` | DB | Single-segment and related-segment queries return `is_verse` / `verse_index`; output mappings populate `type`/`verse_index`. |
| `database/neo4j_schema.yaml` | Schema (reference) | Documented the `verse_index` property and the `Verse` subtype under `Segment`. |
| `docs/api-documentation/annotations-api.md` | Docs | Documented verse request/response, validation rules, and omission semantics. |
| `docs/api-documentation/segments-api.md` | Docs | Documented verse fields on `GET /v2/segments/{id}` and the related endpoint. |
| `tests/test_segments.py` | Tests | Added `TestVerseSegments` suite (round-trip, single GET, related endpoint, all validation failure modes, multi-verse persistence). |
| `tests/test_annotations.py` | Tests | Migrated `SegmentationInput(segments=[...])` construction from `SegmentInput` to `DisplaySegmentInput` (target/alignment segments left as `SegmentInput`). |

## 3. Data model changes (`models/annotation.py`, `models/enums.py`)

- **`SegmentType`** — new `StrEnum` with `VERSE = "verse"`. Currently the only segment subtype.
- **`DisplaySegmentInput(LinesModel)`** — new input model for Display-segmentation segments:
  - `type: SegmentType | None = None`
  - `verse_index: str | None = None`
  - `validate_verse` enforces:
    - `type == verse` ⇒ `verse_index` is **required** and must match `^[1-9][0-9]*\.[1-9][0-9]*$` (both parts integers ≥ 1, no leading zeros).
    - `verse_index` present without `type == verse` ⇒ rejected.
- **`SegmentationInput`**:
  - `segments` changed from `list[SegmentInput]` → `list[DisplaySegmentInput]`.
  - New `validate_verse_indices_unique` validator: verse indices must be **unique within a
    single segmentation**.
- **`SegmentOutput(LinesModel)`** — gained `type: SegmentType | None = None` and
  `verse_index: str | None = None`. All output models that extend it
  (`SegmentWithContextOutput`, and via composition `AlignmentSegmentOutput.aligned_segment`,
  alignment `target_segments`) inherit these fields.

`verse_index` is intentionally a **string**, not a number — `1.10` is a distinct index that
sorts after `1.9`, which a float could not represent.

## 4. Database changes

### Create (`segmentation_database.py`)
The Display-segmentation create query conditionally marks verse segments:

```cypher
CREATE (segment:Segment {id: segment_data.id})-[:SEGMENT_OF]->(segmentation)
FOREACH (_ IN CASE WHEN segment_data.type = 'verse' THEN [1] ELSE [] END |
    SET segment:Verse, segment.verse_index = segment_data.verse_index)
```

Only segments with `type == 'verse'` get the `:Verse` label and `verse_index` property.

### Read (`segmentation_database.py`, `segment_database.py`)
All segment-returning queries now also project:

```cypher
segment:Verse AS is_verse, segment.verse_index AS verse_index
```

Output mapping converts `is_verse` → `type = SegmentType.VERSE` (or `None`) and passes
`verse_index` through. Because the relevant endpoints use
`response_model_exclude_none=True`, both fields are **omitted** from JSON when the segment
is not a verse.

## 5. Schema (`database/neo4j_schema.yaml`)

Added under the `Segment` node:
- `verse_index` property (`type: string`, `required: false`).
- `Verse` subtype, consistent with the existing `subtypes` convention used by the
  `Segmentation` node (`Display` / `Aligned` / `Target`).

> Note: `neo4j_schema.yaml` is a **reference document** — it is not loaded or applied at
> runtime (only referenced in a comment in `neo4j_triggers.py`). No new constraint or index
> is required: `verse_index` is not globally unique, only unique per segmentation (enforced
> in the model layer).

## 6. API changes

- **`POST /v2/editions/{edition_id}/segmentations`** — each segment object may now include
  optional `type` and `verse_index`. Plain payloads are unchanged.
- **`GET /v2/segmentations/{segmentation_id}/segments`** and
  **`GET /v2/segments/{segment_id}`** — include `type` and `verse_index` for verse
  segments; both are **omitted** otherwise (these routes set
  `response_model_exclude_none=True`).
- **`GET /v2/segments/{segment_id}/related`** and
  **`GET /v2/editions/{edition_id}/segments/related`** — include `type` and `verse_index`
  for verse segments, but for non-verse segments they return `type` and `verse_index` as
  **`null`** (these routes do **not** set `response_model_exclude_none`). See §10.

No endpoint paths, status codes, or existing field shapes changed.

## 7. Effects on existing segmentation types

The system has three segmentation subtypes. Their behavior after this change:

### Display segmentations (`:Segmentation:Display`)
- **This is the only subtype that gains verse capability.**
- Created via `POST /v2/editions/{id}/segmentations` using `SegmentationInput` →
  `DisplaySegmentInput`.
- **New validation now applies to Display creation** (only when verse fields are used):
  required/format `verse_index`, `verse_index` only with `type == verse`, and uniqueness of
  verse indices within the segmentation. A request using only `lines` is unaffected.
- **Existing Display segmentations** (created before this change) have no `:Verse` label, so
  reads return `is_verse = false` and the fields are omitted — identical to prior behavior.

### Aligned segmentations (`:Segmentation:Aligned` — source side of an alignment)
- **No behavioral change.** Aligned segments are created by the alignment create query
  (`alignment_database.py`), which does **not** include the verse `FOREACH`, so aligned
  segments never receive a `:Verse` label.
- Their inputs come from `AlignmentInput.aligned_segments: list[AlignedSegmentInput]`, which
  was **not** changed and does not accept `type`/`verse_index`.
- On read, aligned segment outputs inherit the new `SegmentOutput` fields but they are always
  `None` → omitted.

### Target segmentations (`:Segmentation:Target` — target side of an alignment)
- **No behavioral change** to created data — target segments are likewise created without the
  verse `FOREACH`.
- **Input note:** `AlignmentInput.target_segments` remains `list[SegmentInput]` (not
  `DisplaySegmentInput`). Because `OpenPechaModel` uses `extra="forbid"`, sending `type` or
  `verse_index` inside an alignment's `target_segments` is **rejected with 422**. Verse data
  must be attached through a Display segmentation, not through an alignment.
- On read, target segment outputs omit the verse fields (always `None`).

### Cross-cutting (all subtypes)
- The shared read queries in `segment_database.py` and the `get_segments` query in
  `segmentation_database.py` project `is_verse`/`verse_index` for **every** segment regardless
  of subtype. For non-verse segments (which is all aligned/target segments and all plain
  Display segments) the values resolve to `false`/`null`. On the omit-null endpoints they are
  dropped from the response; on the related endpoints they appear as `null` (see §10).
- `SegmentInput` is now used **only** for alignment `target_segments`; `DisplaySegmentInput`
  is used for Display segmentation segments. They are otherwise structurally compatible
  (`DisplaySegmentInput` is a superset adding two optional fields).

## 8. Backward compatibility & migration

- **No migration required.** Pre-existing segments without a `:Verse` label read back exactly
  as before (verse fields omitted).
- **No breaking change for existing clients.** Both new fields are optional; previously valid
  request and response payloads remain valid. The `SegmentationInput.segments` model widened
  from `SegmentInput` to `DisplaySegmentInput` (a superset), so existing plain payloads still
  validate.

## 9. Test coverage

- New `TestVerseSegments` suite in `tests/test_segments.py` covers: verse round-trip,
  single-segment GET, the related endpoint, and every validation failure mode
  (`verse` without index, index without `verse`, malformed indices, unknown `type`, duplicate
  indices), plus multi-verse persistence with distinct indices (`1.1`, `1.10`, `2.1`).
- Measured branch coverage of the changed modules confirms **100% line and branch coverage of
  all new verse code**; the only uncovered lines in those modules are pre-existing, non-verse
  paths.
- Full suite: **616 passed, 1 skipped** (exit 0).

## 10. Known inconsistency — null vs. omitted on related endpoints (pre-existing)

Segment-returning endpoints are not uniform in how they serialize null-valued optional
fields:

| Endpoint | `response_model_exclude_none` | Non-verse / untagged segment |
|----------|-------------------------------|------------------------------|
| `GET /v2/segmentations/{id}/segments` | yes | `type`/`verse_index`/`tag_ids` **omitted** |
| `GET /v2/segments/{id}` | yes | `type`/`verse_index`/`tag_ids` **omitted** |
| `GET /v2/segments/{id}/related` | **no** | `type`/`verse_index`/`tag_ids` returned as **`null`** |
| `GET /v2/editions/{id}/segments/related` | **no** | `type`/`verse_index`/`tag_ids` returned as **`null`** |

This inconsistency **pre-dates the verse change** (it already affected `tag_ids` on the
related endpoints); the verse change merely added two more optional fields (`type`,
`verse_index`) that exhibit the same null-vs-omitted split.

**Suggested fix (not applied):** add `response_model_exclude_none=True` to the two related
route decorators (`routers/segments.py::get_related`,
`routers/editions.py::get_segment_related`) so all four endpoints behave identically. This
would also change `tag_ids` on the related endpoints from `null` to omitted, so it is a small
but observable change to an existing response shape and is left as a decision for the
maintainers.
