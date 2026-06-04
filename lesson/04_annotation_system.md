 # Lesson 04 — Annotation System

## Learning Objectives

- Identify all six annotation types and their graph structure
- Understand how `Span` nodes work and what they point to
- Know the difference between continuous (segment/page) and point-in-text (bib/note/attr) annotations
- Read and write each annotation type via the API
- Understand the `AnnotationMetadata` placeholder
- Know the constraint rules for each annotation type (one pagination per edition, etc.)

---

## 1. Overview

An **annotation** is structured data that **points at a character range** within an edition's base text. The base text is a flat string stored in S3. Annotations index into it using `{start, end}` positions (0-based, exclusive end: `[start, end)`).

```
Base text (S3):
  "འཕགས་པ་སྤྱན་རས་གཟིགས་དབང་ཕྱུག"
   0      5    10   15   20   25   30
   
A Segment or Annotation saying start=5, end=15 points to: "སྤྱན་རས་གཟིགས"
```

All six annotation types attach to an `Edition` node in the graph.

---

## 2. Annotation Type Matrix

| Type | Graph node | Endpoint | Multiplicity | Span structure |
|---|---|---|---|---|
| **Segmentation** | `Segmentation:Display` → `Segment` → `Span` | `/editions/{id}/segmentations` | Many per edition | Multi-line segments (contiguous) |
| **Alignment** | `Segmentation:Aligned` + `Segmentation:Target` + `ALIGNED_TO` | `/editions/{id}/alignments` | Many per edition | Multi-line segments (may overlap) |
| **Pagination** | `Pagination` → `Volume` → `Page` → `Span` | `/editions/{id}/pagination` | **One** per edition | Pages contiguous within volume |
| **Table of contents** | `TableOfContents` → `TableOfContentsSection` ← `Span` | `/editions/{id}/table-of-contents` | Many per edition | Nested sections, each a single span |
| **Bibliographic** | `BibliographicMetadata` ← `Span` | `/editions/{id}/bibliographic` | Many per edition | Single span |
| **Durchen (Note)** | `Note` ← `Span` | `/editions/{id}/durchens` | Many per edition | Single span |

*(An `Attribute` node also exists for OCR confidence scores, but there is no public router endpoint for it yet.)*

---

## 3. Segmentation

### 3.1 Graph Structure

```
Edition
  ◄──[SEGMENTATION_OF]── Segmentation:Display {id}
                              ◄──[SEGMENT_OF]── Segment {id}
                                                  ◄──[SPAN_OF]── Span {start, end}  ← line 1
                                                  ◄──[SPAN_OF]── Span {start, end}  ← line 2
```

### 3.2 Invariants

- Segments must be **sorted by span start** in the request
- Within a segment, **lines must be continuous**: `lines[i].end == lines[i+1].start`
- A segmentation's segments are typically contiguous (cover the full text without gaps), though the system doesn't enforce this at write time

### 3.3 Request Body

```json
POST /v2/editions/{edition_id}/segmentations
{
    "segments": [
        {"lines": [{"start": 0, "end": 50}]},
        {"lines": [
            {"start": 50, "end": 75},
            {"start": 75, "end": 100}
        ]},
        {"lines": [{"start": 100, "end": 150}]}
    ],
    "metadata": null
}
```

A segment with two lines represents a segment that spans a line break. The span of the whole segment is `[lines[0].start, lines[-1].end]`.

### 3.4 Response Body

`SegmentationOutput` is now a **lightweight header** — it no longer embeds the segment list. Fetch segments via the paginated `GET /v2/segmentations/{id}/segments` endpoint.

```json
// GET /v2/segmentations/SGN_001
{
    "id": "SGN_001",
    "edition_id": "E_001",
    "text_id": "T_001",
    "metadata": null
}
```

```json
// GET /v2/segmentations/SGN_001/segments?limit=500&offset=0
// → PaginatedResponse[SegmentOutput] (minimal segment shape: id + lines only)
{
    "items": [
        {"id": "SEG_001", "lines": [{"start": 0, "end": 50}]},
        {"id": "SEG_002", "lines": [{"start": 50, "end": 100}]}
    ],
    "has_more": false,
    "offset": 0,
    "limit": 500
}
```

> Model note: `SegmentOutput` is now minimal (`id` + `lines`). The context-rich fields (`segmentation_id`, `edition_id`, `text_id`, `tag_ids`) live on a separate model, `SegmentWithContextOutput`, returned by `GET /v2/segments/{id}` and the related-segments endpoints. See Lesson 07 §7.5.

### 3.5 Deletion Rules

- A display segmentation can always be deleted
- A segmentation labelled `:Aligned` or `:Target` **cannot** be deleted directly — you must delete the parent alignment

---

## 4. Alignment

### 4.1 Concept

An alignment maps segments from one edition ("source/aligned edition") to segments in another edition ("target edition"). It creates:

- One `Segmentation:Aligned` attached to the **source edition**
- One `Segmentation:Target` attached to the **target edition**
- `ALIGNED_TO` relationships from source segments to target segments

```
Source Edition                    Target Edition
  Segmentation:Aligned              Segmentation:Target
    Segment A ──[ALIGNED_TO]──────► Segment X
    Segment A ──[ALIGNED_TO]──────► Segment Y
    Segment B ──[ALIGNED_TO]──────► Segment Y
```

This supports **many-to-many** alignment.

### 4.2 Graph Structure

```
Edition_Source ◄──[SEGMENTATION_OF]── Segmentation:Aligned
                                          ◄──[SEGMENT_OF]── Segment_A
                                                 ──[ALIGNED_TO]──►
                                                    Segment_X ──[SEGMENT_OF]──►
                                                                  Segmentation:Target
                                                                    ──[SEGMENTATION_OF]──►
                                                                       Edition_Target
```

### 4.3 Request Body

```json
POST /v2/editions/{source_edition_id}/alignments
{
    "target_edition_id": "E_002",
    "target_segments": [
        {"lines": [{"start": 0, "end": 30}]},
        {"lines": [{"start": 30, "end": 60}]}
    ],
    "aligned_segments": [
        {
            "lines": [{"start": 0, "end": 25}],
            "target_indices": [0]
        },
        {
            "lines": [{"start": 25, "end": 55}],
            "target_indices": [0, 1]
        }
    ]
}
```

`target_indices` is a list of zero-based indexes into `target_segments`. Here:
- Source segment 0 (`[0,25)`) maps to target segment 0 (`[0,30)`)
- Source segment 1 (`[25,55)`) maps to both target segment 0 and 1

### 4.4 Response Body

Like `SegmentationOutput`, `AlignmentOutput` is now a **header** that no longer embeds the aligned/target segment rows. It identifies the two editions/segmentations involved:

```json
// GET /v2/alignments/ALIGN_001
{
    "id": "ALIGN_001",
    "aligned_edition_id": "E_001",
    "aligned_text_id": "T_001",
    "target_edition_id": "E_002",
    "target_text_id": "T_002",
    "target_segmentation_id": "SGN_TARGET_001",
    "metadata": null
}
```

The actual aligned segment rows are fetched via the paginated `GET /v2/alignments/{id}/segments` endpoint, which returns `PaginatedResponse[AlignmentSegmentOutput]`:

```json
// GET /v2/alignments/ALIGN_001/segments?limit=500&offset=0
{
    "items": [
        {
            "aligned_segment": {
                "id": "S_SEG_001",
                "lines": [{"start": 0, "end": 25}]
            },
            "target_segments": [
                {
                    "id": "T_SEG_001",
                    "segmentation_id": "SGN_TARGET_001",
                    "edition_id": "E_002",
                    "text_id": "T_002",
                    "lines": [{"start": 0, "end": 30}],
                    "tag_ids": null
                }
            ]
        }
    ],
    "has_more": false,
    "offset": 0,
    "limit": 500
}
```

Note: `aligned_segment` is a minimal `SegmentOutput` (`id` + `lines`), while each entry in `target_segments` is a `SegmentWithContextOutput` carrying edition/text/segmentation context.

### 4.5 Retrieving Alignments

```
GET /v2/editions/{edition_id}/alignments
```

Returns alignments where the edition is **either** the source or the target (the Cypher UNION query handles both directions):

```cypher
-- source side:
MATCH (edition)<-[:SEGMENTATION_OF]-(source_segmentation:Segmentation:Aligned)

UNION

-- target side (follow backwards to find the Aligned segmentation):
MATCH (edition)<-[:SEGMENTATION_OF]-(:Segmentation:Target)<-[:SEGMENT_OF]-(:Segment)
      <-[:ALIGNED_TO]-(:Segment)-[:SEGMENT_OF]->(source_segmentation:Segmentation:Aligned)
```

---

## 5. Pagination

### 5.1 Graph Structure

```
Edition ◄──[PAGINATION_OF]── Pagination {id}
                                 ◄──[VOLUME_OF]── Volume {id, index?}
                                                     ◄──[PAGE_OF]── Page {id, reference}
                                                                        ◄──[SPAN_OF]── Span {start, end}
```

### 5.2 Volume Rules

| Situation | Volume `index` value |
|---|---|
| Single volume | `null` |
| Multiple volumes | 1, 2, 3, ... (contiguous) |

### 5.3 Page Continuity Rule

Pages within a volume must be continuous — no gaps:

```
page_1: lines [0, 500)
page_2: lines [500, 1000)    ← start must equal previous page's end
page_3: lines [1000, 1500)
```

### 5.4 Request Body

**Single volume, single page:**
```json
{
    "volumes": [
        {
            "pages": [
                {"reference": "1a", "lines": [{"start": 0, "end": 500}]}
            ]
        }
    ]
}
```

**Multi-volume:**
```json
{
    "volumes": [
        {
            "index": 1,
            "pages": [
                {"reference": "v1_1a", "lines": [{"start": 0, "end": 500}]},
                {"reference": "v1_1b", "lines": [{"start": 500, "end": 1000}]}
            ]
        },
        {
            "index": 2,
            "pages": [
                {"reference": "v2_1a", "lines": [{"start": 1000, "end": 1500}]}
            ]
        }
    ]
}
```

### 5.5 Constraint

Only **one** pagination per edition is allowed. Attempting to add a second returns an error.

---

## 6. Table of Contents

> Previously called "outline". Renamed to "table of contents" during the dev sync (`TableOfContents` node, `/table-of-contents` endpoints).

### 6.1 Concept

A table of contents represents a section hierarchy over an edition (e.g. chapters and sub-chapters, or a Tibetan `sa bcad`). Each section has a localized `title`, an optional localized `summary`, a character `span`, and optional nested `subsections`. An edition can have **multiple** table-of-contents annotations.

### 6.2 Graph Structure

```
Edition ◄──[TOC_OF]── TableOfContents {id}
                          ◄──[SECTION_OF]── TableOfContentsSection {id}   ← root section
                                                ──[HAS_TITLE]──►   Nomen   (required)
                                                ──[HAS_SUMMARY]──► Nomen   (optional)
                                                ◄──[SPAN_OF]──     Span {start, end}
                                                ◄──[SUBSECTION_OF]── TableOfContentsSection  ← nested child
```

### 6.3 Invariants

- A table of contents must have at least one root section (`sections` has `min_length=1`).
- Each subsection's span must be **fully contained** within its parent section's span (Pydantic `TableOfContentsSectionInput.validate_subsection_spans_contained`).
- Returned sections/subsections are ordered by their span start/end.
- Section spans are `Span` nodes, so they are **adjusted automatically** when edition content is patched (treated as annotation spans — they shift, they don't expand; see Lesson 06).

### 6.4 Request Body

```json
POST /v2/editions/{edition_id}/table-of-contents
{
    "metadata": {"name": "Main sa bcad"},
    "sections": [
        {
            "title": {"bo": "ལེའུ་དང་པོ།", "en": "Chapter 1"},
            "summary": {"en": "Opening topic"},
            "span": {"start": 0, "end": 1200},
            "subsections": [
                {
                    "title": {"en": "Section 1.1"},
                    "span": {"start": 0, "end": 350},
                    "subsections": []
                }
            ]
        }
    ]
}
```

Response: `{"id": "TOC_001"}`

### 6.5 Response Body (by ID)

```json
// GET /v2/table-of-contents/TOC_001
{
    "id": "TOC_001",
    "edition_id": "E_001",
    "text_id": "T_001",
    "sections": [
        {
            "id": "SEC_ROOT",
            "title": {"bo": "ལེའུ་དང་པོ།", "en": "Chapter 1"},
            "summary": {"en": "Opening topic"},
            "span": {"start": 0, "end": 1200},
            "subsections": [
                {
                    "id": "SEC_CHILD",
                    "title": {"en": "Section 1.1"},
                    "summary": null,
                    "span": {"start": 0, "end": 350},
                    "subsections": []
                }
            ]
        }
    ],
    "metadata": {"name": "Main sa bcad"}
}
```

Deleting a table of contents (`DELETE /v2/table-of-contents/{toc_id}`) removes its sections, section spans, title/summary Nomen subgraphs, and metadata. Sections are **not** standalone API resources — they are managed only as part of the parent table-of-contents annotation.

---

## 7. Bibliographic Metadata

### 7.1 Concept

Marks a character span as having a specific bibliographic role (colophon text, incipit, authorship statement, etc.).

### 7.2 Graph Structure

```
BibliographicMetadata {id}
    ──[BIBLIOGRAPHY_OF]──► Edition
    ──[HAS_TYPE]──────────► BibliographyType
    ◄──[SPAN_OF]────────── Span {start, end}
```

### 7.3 BibliographyType Values

| Value | Meaning |
|---|---|
| `colophon` | End text stating who copied the text |
| `incipit` | Opening passage |
| `alt_incipit` | Alternative opening |
| `alt_title` | An alternative title embedded in the text |
| `person` | A person mentioned in the text |
| `title` | The title as it appears in the text |
| `author` | Author attribution statement in the text |

### 7.4 Request/Response

```json
POST /v2/editions/{edition_id}/bibliographic
{
    "span": {"start": 5000, "end": 5200},
    "type": "colophon",
    "metadata": null
}
```

Response: `{"id": "BIB_001"}`

---

## 8. Durchen (Note)

### 8.1 Concept

"Durchen" (གདུར་ཅན།) is a Tibetan textual criticism notation marking variant readings. In this system it is modelled as a `Note` with `type: "durchen"`.

### 8.2 Graph Structure

```
Note {id, text: string}
    ──[NOTE_OF]──► Edition
    ──[HAS_TYPE]──► NoteType {name: "durchen"}
    ◄──[SPAN_OF]── Span {start, end}
```

### 8.3 Request/Response

```json
POST /v2/editions/{edition_id}/durchens
{
    "span": {"start": 100, "end": 150},
    "text": "Variant in MS Lhasa reads: 'བཀྲ་ཤིས།'",
    "metadata": null
}
```

Response: `{"id": "NOTE_001"}`

---

## 9. AnnotationMetadata

All annotation types have an optional `metadata` field. In the current schema it carries an optional `name`:

```python
class AnnotationMetadata(OpenPechaModel):
    name: str | None = None
```

It is present in the graph as an `AnnotationMetadata` node attached via `HAS_METADATA` and is largely a **placeholder** reserved for future expansion (e.g. provenance, revision history, confidence scores). The table-of-contents request, for example, uses `metadata.name` to label the table of contents (`{"name": "Main sa bcad"}`).

In most requests you can pass `"metadata": null` or omit it.

---

## 10. Span Pointing: What Can a Span Point To?

```cypher
// The SPAN_OF relationship target can be:
Span ──[SPAN_OF]──► Segment
Span ──[SPAN_OF]──► Page
Span ──[SPAN_OF]──► BibliographicMetadata
Span ──[SPAN_OF]──► Note
Span ──[SPAN_OF]──► Attribute
Span ──[SPAN_OF]──► TableOfContentsSection
```

The `BATCH_UPDATE_SPANS_QUERY` in `span_database.py` updates all of these in one pass using the multi-label match:

```cypher
MATCH (span:Span)-[:SPAN_OF]->(entity:Segment|Page|BibliographicMetadata|Note|Attribute|TableOfContentsSection {id: u.entity_id})
SET span.start = u.new_start, span.end = u.new_end
```

---

## 11. Annotation Deletion Cascade

When `DELETE /v2/editions/{edition_id}` is called, the delete order is:

```python
# edition_database.py
await AlignmentDatabase.delete_all_with_transaction(tx, edition_id)
await SegmentationDatabase.delete_all_with_transaction(tx, edition_id)
await PaginationDatabase.delete_all_with_transaction(tx, edition_id)
await TableOfContentsDatabase.delete_all_with_transaction(tx, edition_id)
await BibliographicDatabase.delete_all_with_transaction(tx, edition_id)
await NoteDatabase.delete_all_with_transaction(tx, edition_id)
await tx.run(EditionDatabase.DELETE_QUERY, edition_id=edition_id)
```

Alignments are deleted first because their associated segmentations include both `:Aligned` and `:Target` labels. If we deleted plain segmentations first, alignment segmentations would remain orphaned.

---

## 12. Annotation Endpoint Matrix (Full)

| Action | Endpoint | Method | Returns |
|---|---|---|---|
| Add segmentation | `/v2/editions/{id}/segmentations` | POST | `{"id": "..."}` |
| List segmentations | `/v2/editions/{id}/segmentations` | GET | `[SegmentationOutput]` |
| Get segmentation | `/v2/segmentations/{id}` | GET | `SegmentationOutput` |
| Get segmentation segments | `/v2/segmentations/{id}/segments` | GET | `PaginatedResponse[SegmentOutput]` |
| Delete segmentation | `/v2/segmentations/{id}` | DELETE | 204 |
| Add alignment | `/v2/editions/{id}/alignments` | POST | `{"id": "..."}` |
| List alignments | `/v2/editions/{id}/alignments` | GET | `[AlignmentOutput]` |
| Get alignment | `/v2/alignments/{id}` | GET | `AlignmentOutput` |
| Get alignment segments | `/v2/alignments/{id}/segments` | GET | `PaginatedResponse[AlignmentSegmentOutput]` |
| Delete alignment | `/v2/alignments/{id}` | DELETE | 204 |
| Add pagination | `/v2/editions/{id}/pagination` | POST | `{"id": "..."}` |
| Get pagination | `/v2/editions/{id}/pagination` | GET | `PaginationOutput \| null` |
| Get pagination by ID | `/v2/paginations/{id}` | GET | `PaginationOutput` |
| Delete pagination | `/v2/paginations/{id}` | DELETE | 204 |
| Add table of contents | `/v2/editions/{id}/table-of-contents` | POST | `{"id": "..."}` |
| List tables of contents | `/v2/editions/{id}/table-of-contents` | GET | `[TableOfContentsOutput]` |
| Get table of contents | `/v2/table-of-contents/{id}` | GET | `TableOfContentsOutput` |
| Delete table of contents | `/v2/table-of-contents/{id}` | DELETE | 204 |
| Add bibliographic | `/v2/editions/{id}/bibliographic` | POST | `{"id": "..."}` |
| List bibliographic | `/v2/editions/{id}/bibliographic` | GET | `[BibliographicMetadataOutput]` |
| Get bibliographic | `/v2/bibliographic/{id}` | GET | `BibliographicMetadataOutput` |
| Delete bibliographic | `/v2/bibliographic/{id}` | DELETE | 204 |
| Add durchen | `/v2/editions/{id}/durchens` | POST | `{"id": "..."}` |
| List durchens | `/v2/editions/{id}/durchens` | GET | `[NoteOutput]` |
| Get durchen | `/v2/durchens/{id}` | GET | `NoteOutput` |
| Delete durchen | `/v2/durchens/{id}` | DELETE | 204 |

---

## Practice Questions

1. You want to mark that characters 200-400 in edition E_001 are the colophon. Which endpoint and body do you use?
2. A segmentation has 3 segments. You delete the middle span via `DELETE /v2/editions/{id}/content`. What happens to the segmentation?
3. Can you have both a segmentation and a pagination on the same edition? Why would you want both?
4. What does `target_indices: [0, 1]` mean in an alignment request?
5. Why are alignments deleted before segmentations during edition deletion?

---

## Next Lesson

→ [05_flows.md](05_flows.md) — End-to-end operational flows
