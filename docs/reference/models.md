# Models reference

This document describes the Pydantic models and enums in [functions/models.py](functions/models.py) used by the API and database layer for request/response validation and domain types.

---

## Table of Contents

1. [Overview](#overview)
2. [Conventions](#conventions)
3. [Enums](#enums)
4. [Person](#person)
5. [Contributions](#contributions)
6. [Annotation and span](#annotation-and-span)
7. [Segments](#segments)
8. [Pagination, bibliography, notes, attributes](#pagination-bibliography-notes-attributes)
9. [Expression](#expression)
10. [Manifestation (edition)](#manifestation-edition)
11. [Category](#category)
12. [Search](#search)
13. [Text operations](#text-operations)

---

## Overview

`functions/models.py` provides:

- **Request/response models** – Input (e.g. `PersonInput`), output (e.g. `PersonOutput`), and patch (e.g. `PersonPatch`) variants for API payloads.
- **Domain models** – Shared types for expressions, manifestations, annotations, segments, etc.
- **Validation** – Pydantic validators enforce business rules (e.g. at most one parent for expressions, continuous spans).

See the source file for full validator implementations.

---

## Conventions

- **OpenPechaModel** – Base for all domain models. Config: `extra="forbid"`, `str_strip_whitespace=True`.
- **NonEmptyStr** – Type alias: non-empty string, stripped; used for IDs and required text fields.
- **LocalizedString** – `RootModel[dict[str, NonEmptyStr]]`: locale-keyed strings (e.g. `{"bo": "title in Tibetan", "en": "title in English"}`). Must have at least one entry.

---

## Enums

| Enum | Values |
|------|--------|
| **TextType** | `root`, `commentary`, `translation`, `translation_source`, `none` |
| **ContributorRole** | `translator`, `reviser`, `author`, `scholar` |
| **AnnotationType** | `segmentation`, `alignment`, `pagination`, `version`, `bibliography`, `table_of_contents`, `durchen`, `search_segmentation` |
| **ManifestationType** | `diplomatic`, `critical`, `collated` |
| **LicenseType** | `cc0`, `public`, `cc-by`, `cc-by-sa`, `cc-by-nd`, `cc-by-nc`, `cc-by-nc-sa`, `cc-by-nc-nd`, `copyrighted`, `unknown` |
| **NoteType** | `durchen` |
| **BibliographyType** | `colophon`, `incipit`, `alt_incipit`, `alt_title`, `person`, `title`, `author` |
| **AttributeType** | `ocr_confidence` |

---

## Person

- **PersonBase** – `bdrc`, `wiki`, `name` (LocalizedString), `alt_names` (optional). Validator: dedupe `alt_names` and exclude primary `name`.
- **PersonInput** – Same as PersonBase; used for create.
- **PersonPatch** – All fields optional. Validators: at least one field must be provided; dedupe `alt_names` when present.
- **PersonOutput** – PersonBase + `id`.

---

## Contributions

- **AIContributionModel** – `ai_id`, `role` (ContributorRole); for AI-generated contributions.
- **ContributionBase** – `person_id` or `person_bdrc_id` (optional), `role`.
- **ContributionInput** – Exactly one of `person_id` or `person_bdrc_id` must be set (validator).
- **ContributionOutput** – ContributionBase + optional `person_name` (LocalizedString).

---

## Annotation and span

- **AnnotationModel** – `id`, `type` (AnnotationType), `aligned_to` (optional). Validator: `aligned_to` may only be set when `type` is `ALIGNMENT`.
- **SpanModel** – `start` (≥0, inclusive), `end` (≥1, exclusive). Validator: `start` < `end`.
- **AnnotationMetadata** – Empty extensible model for annotation metadata.

---

## Segments

- **SegmentBase** – `lines` (list of SpanModel, min length 1). Lines must be continuous and sorted (each line’s `start` equals previous line’s `end`). Property `span` returns combined span of first and last line.
- **SegmentInput** / **SegmentOutput** – SegmentOutput adds `id`, `manifestation_id`, `text_id`.
- **AlignedSegment** – `lines`, `alignment_indices` (min length 1). Same line continuity rule.
- **SegmentationBase** – Generic over segment type. `segments`, optional `metadata`. Validator: segments sorted by span start.
- **SegmentationInput** / **SegmentationOutput** – SegmentationOutput adds `id`.
- **AlignmentBase** – `target_id`, `target_segments`, `aligned_segments`, optional `metadata`. Validators: both segment lists sorted by span start.
- **AlignmentInput** / **AlignmentOutput** – AlignmentOutput adds `id`.

---

## Pagination, bibliography, notes, attributes

- **PageModel** – `lines` (SpanModel list), `reference`. Lines must be continuous and sorted.
- **VolumeModel** – Optional `index`, `pages` (min length 1), optional `metadata`. Validator: pages continuous (end of one = start of next).
- **PaginationBase** – `volume`, optional `metadata`. **PaginationInput** / **PaginationOutput** (output adds `id`).
- **BibliographicMetadataBase** – `span`, `type` (BibliographyType), optional `metadata`. **BibliographicMetadataInput** / **BibliographicMetadataOutput** (output adds `id`).
- **NoteBase** – `span`, `text`, optional `metadata`. **NoteInput** / **NoteOutput** (output adds `id`).
- **AttributeBase** – `span`, `type` (AttributeType), `value` (Any), optional `metadata`. **AttributeInput** / **AttributeOutput** (output adds `id`).

---

## Expression

- **ExpressionBase** – `bdrc`, `wiki`, `date`, `title` (LocalizedString), `alt_titles`, `language`, `commentary_of`, `translation_of`, `category_id`, `license` (default PUBLIC_DOMAIN_MARK). Validators: at most one of `commentary_of` / `translation_of`; title must include an entry for the expression’s (base) language; dedupe `alt_titles`.
- **ExpressionInput** – ExpressionBase + `contributions` (list of ContributionInput or AIContributionModel).
- **ExpressionPatch** – All fields optional. Validators: at least one field provided; dedupe `alt_titles` when both title and alt_titles present.
- **ExpressionOutput** – ExpressionBase + `id`, `contributions` (ContributionOutput or AIContributionModel), `commentaries`, `translations`, `editions` (list of IDs).

---

## Manifestation (edition)

- **ManifestationBase** – `bdrc`, `wiki`, `type` (ManifestationType), `source`, `colophon`, `incipit_title`, `alt_incipit_titles`. Validators: when `type` is `diplomatic`, `bdrc` required; when `type` is `critical`, `bdrc` must not be set; `alt_incipit_titles` only when `incipit_title` is set; dedupe `alt_incipit_titles`.
- **ManifestationInput** / **ManifestationOutput** – ManifestationOutput adds `id`, `text_id`.

---

## Category

- **CategoryBase** – `title` (LocalizedString), optional `description`, optional `parent_id`.
- **CategoryInput** – Same as CategoryBase.
- **CategoryOutput** – CategoryBase + `id`, `children` (list of IDs, default empty).

---

## Search

- **SearchFilterModel** – Optional `title`.
- **SearchResultModel** – `id`, `distance`, `entity` (dict), `segmentation_ids` (default empty list).
- **SearchResponseModel** – `query`, `search_type`, `results`, `count`.

---

## Text operations

Used for PATCH content (insert/delete/replace spans of text). Discriminated union on `type`.

- **InsertOperation** – `type: "insert"`, `position` (≥0), `text` (min length 1).
- **DeleteOperation** – `type: "delete"`, `start` (≥0), `end` (≥1). Validator: `start` < `end`.
- **ReplaceOperation** – `type: "replace"`, `start`, `end`, `text`. Validator: `start` < `end`.
- **TextOperation** – RootModel wrapping one of the above; `.operation` returns the concrete operation.

See [functions/models.py](functions/models.py) for validators and field details.
