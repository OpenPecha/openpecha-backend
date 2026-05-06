# Lesson 04 — Annotation System

## Learning Objectives

- Identify all five annotation types and their graph structure
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

All five annotation types attach to an `Edition` node in the graph.

---

## 2. Annotation Type Matrix

| Type | Graph node | Endpoint | Multiplicity | Span structure |
|---|---|---|---|---|
| **Segmentation** | `Segmentation:Display` → `Segment` → `Span` | `/editions/{id}/segmentations` | Many per edition | Multi-line segments (contiguous) |
| **Alignment** | `Segmentation:Aligned` + `Segmentation:Target` + `ALIGNED_TO` | `/editions/{id}/alignments` | Many per edition | Multi-line segments (may overlap) |
| **Pagination** | `Pagination` → `Volume` → `Page` → `Span` | `/editions/{id}/pagination` | **One** per edition | Pages contiguous within volume |
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

```json
{
    "id": "SGN_001",
    "edition_id": "E_001",
    "text_id": "T_001",
    "segments": [
        {
            "id": "SEG_001",
            "segmentation_id": "SGN_001",
            "edition_id": "E_001",
            "text_id": "T_001",
            "lines": [{"start": 0, "end": 50}],
            "tag_ids": null
        }
    ],
    "metadata": null
}
```

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

```json
{
    "id": "ALIGN_001",
    "aligned_edition_id": "E_001",
    "target_edition_id": "E_002",
    "target_segments": [
        {
            "id": "T_SEG_001",
            "segmentation_id": "SGN_TARGET_001",
            "edition_id": "E_002",
            "text_id": "T_002",
            "lines": [{"start": 0, "end": 30}],
            "tag_ids": null
        }
    ],
    "aligned_segments": [
        {
            "id": "S_SEG_001",
            "segmentation_id": "ALIGN_001",
            "edition_id": "E_001",
            "text_id": "T_001",
            "lines": [{"start": 0, "end": 25}],
            "target_indices": [0]
        }
    ],
    "metadata": null
}
```

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

## 6. Bibliographic Metadata

### 6.1 Concept

Marks a character span as having a specific bibliographic role (colophon text, incipit, authorship statement, etc.).

### 6.2 Graph Structure

```
BibliographicMetadata {id}
    ──[BIBLIOGRAPHY_OF]──► Edition
    ──[HAS_TYPE]──────────► BibliographyType
    ◄──[SPAN_OF]────────── Span {start, end}
```

### 6.3 BibliographyType Values

| Value | Meaning |
|---|---|
| `colophon` | End text stating who copied the text |
| `incipit` | Opening passage |
| `alt_incipit` | Alternative opening |
| `alt_title` | An alternative title embedded in the text |
| `person` | A person mentioned in the text |
| `title` | The title as it appears in the text |
| `author` | Author attribution statement in the text |

### 6.4 Request/Response

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

## 7. Durchen (Note)

### 7.1 Concept

"Durchen" (གདུར་ཅན།) is a Tibetan textual criticism notation marking variant readings. In this system it is modelled as a `Note` with `type: "durchen"`.

### 7.2 Graph Structure

```
Note {id, text: string}
    ──[NOTE_OF]──► Edition
    ──[HAS_TYPE]──► NoteType {name: "durchen"}
    ◄──[SPAN_OF]── Span {start, end}
```

### 7.3 Request/Response

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

## 8. AnnotationMetadata

All annotation types have an optional `metadata` field. In the current schema:

```python
class AnnotationMetadata(OpenPechaModel):
    pass
```

It is a **placeholder** — currently empty but present in the graph as an `AnnotationMetadata` node attached via `HAS_METADATA`. This is reserved for future expansion (e.g. provenance, revision history, confidence scores).

In all current requests, pass `"metadata": null` or omit it.

---

## 9. Span Pointing: What Can a Span Point To?

```cypher
// The SPAN_OF relationship target can be:
Span ──[SPAN_OF]──► Segment
Span ──[SPAN_OF]──► Page
Span ──[SPAN_OF]──► BibliographicMetadata
Span ──[SPAN_OF]──► Note
Span ──[SPAN_OF]──► Attribute
```

The `BATCH_UPDATE_SPANS_QUERY` in `span_database.py` updates all of these in one pass using the multi-label match:

```cypher
MATCH (span:Span)-[:SPAN_OF]->(entity:Segment|Page|BibliographicMetadata|Note|Attribute {id: u.entity_id})
SET span.start = u.new_start, span.end = u.new_end
```

---

## 10. Annotation Deletion Cascade

When `DELETE /v2/editions/{edition_id}` is called, the delete order is:

```python
# edition_database.py
await AlignmentDatabase.delete_all_with_transaction(tx, edition_id)
await SegmentationDatabase.delete_all_with_transaction(tx, edition_id)
await PaginationDatabase.delete_all_with_transaction(tx, edition_id)
await BibliographicDatabase.delete_all_with_transaction(tx, edition_id)
await NoteDatabase.delete_all_with_transaction(tx, edition_id)
await tx.run(EditionDatabase.DELETE_QUERY, edition_id=edition_id)
```

Alignments are deleted first because their associated segmentations include both `:Aligned` and `:Target` labels. If we deleted plain segmentations first, alignment segmentations would remain orphaned.

---

## 11. Annotation Endpoint Matrix (Full)

| Action | Endpoint | Method | Returns |
|---|---|---|---|
| Add segmentation | `/v2/editions/{id}/segmentations` | POST | `{"id": "..."}` |
| List segmentations | `/v2/editions/{id}/segmentations` | GET | `[SegmentationOutput]` |
| Get segmentation | `/v2/segmentations/{id}` | GET | `SegmentationOutput` |
| Replace segmentation | `/v2/segmentations/{id}` | PUT | `{"id": "..."}` |
| Delete segmentation | `/v2/segmentations/{id}` | DELETE | 204 |
| Add alignment | `/v2/editions/{id}/alignments` | POST | `{"id": "..."}` |
| List alignments | `/v2/editions/{id}/alignments` | GET | `[AlignmentOutput]` |
| Get alignment | `/v2/alignments/{id}` | GET | `AlignmentOutput` |
| Delete alignment | `/v2/alignments/{id}` | DELETE | 204 |
| Add pagination | `/v2/editions/{id}/pagination` | POST | `{"id": "..."}` |
| Get pagination | `/v2/editions/{id}/pagination` | GET | `PaginationOutput \| null` |
| Get pagination by ID | `/v2/paginations/{id}` | GET | `PaginationOutput` |
| Delete pagination | `/v2/paginations/{id}` | DELETE | 204 |
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
