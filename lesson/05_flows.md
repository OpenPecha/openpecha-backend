# Lesson 05 — End-to-End Operational Flows

## Learning Objectives

- Trace a complete request from HTTP to Neo4j to response
- Know exactly which nodes and relationships are created for each workflow
- Understand the atomic transaction boundaries
- Debug a "Text not found" or "Critical edition already present" error by tracing through the flow

---

## Flow 1: Create a New Standalone Text with Edition

This is the most common "add new content" flow.

### Step-by-step

```
Client                     FastAPI                   Neo4j                S3
  │                           │                        │                   │
  │── POST /v2/texts ────────►│                        │                   │
  │   {title, language,       │                        │                   │
  │    contributions, ...}    │                        │                   │
  │                          │── validate Pydantic ───►│                   │
  │                          │── DatabaseValidator:    │                   │
  │                          │   title unique?  ───────►                   │
  │                          │   language exists? ─────►                   │
  │                          │   category exists? ─────►                   │
  │                          │   persons exist? ───────►                   │
  │                          │                         │                   │
  │                          │── WRITE transaction ───►│                   │
  │                          │   CREATE Work           │                   │
  │                          │   CREATE Text           │                   │
  │                          │   CREATE Nomen + LocalizedText(s)           │
  │                          │   MERGE Work-[:TEXT_OF]-Text                │
  │                          │   MERGE Text-[:HAS_LANGUAGE]-Language       │
  │                          │   MERGE Text-[:HAS_TITLE]-Nomen             │
  │                          │   MERGE Text-[:HAS_LICENSE]-LicenseType     │
  │                          │   CREATE Contribution nodes                 │
  │                          │   MERGE Work-[:HAS_CATEGORY]-Category       │
  │                          │◄── text_id returned ───│                   │
  │◄── 201 {"id": "T_001"} ──│                        │                   │
  │                          │                        │                   │
  │── POST /v2/texts/T_001/editions ─────────────────►│                   │
  │   {metadata, content,    │                        │                   │
  │    pagination}           │  (diplomatic requires  │                   │
  │                          │   pagination, no seg)  │                   │
  │                          │── generate edition_id  │                   │
  │                          │                        │── store_base_text →│
  │                          │                        │      (S3 write)    │
  │                          │                        │                    │
  │                          │── WRITE transaction ───►│                   │
  │                          │   MATCH Text(T_001)     │                   │
  │                          │   CREATE Edition        │                   │
  │                          │   MERGE Edition-[:EDITION_OF]-Text          │
  │                          │   MERGE Edition-[:HAS_TYPE]-EditionType     │
  │                          │   [IF incipit_title]                        │
  │                          │     CREATE Nomen + LocalizedText(s)         │
  │                          │     CREATE Edition-[:HAS_INCIPIT_TITLE]-Nomen│
  │                          │   [IF source]                               │
  │                          │     MERGE Source node                       │
  │                          │     CREATE Edition-[:HAS_SOURCE]-Source     │
  │                          │   [IF segmentation provided]                │
  │                          │     CREATE Segmentation:Display             │
  │                          │     CREATE Segment × N                      │
  │                          │     CREATE Span × M                         │
  │                          │   [IF pagination provided]                  │
  │                          │     CREATE Pagination → Volume(s) → Page(s) │
  │                          │     CREATE Span × P                         │
  │◄── 201 {"id": "E_001"} ──│                         │                   │
  │                          │                         │                   │
  │                          │── background task ─────────────────────────►
  │                          │   trigger_search_segmenter(E_001)           │
```

### Graph state after this flow

```
(Work {id:"W_001"})
  ← [:TEXT_OF {original:true}] ── (Text {id:"T_001"})
    → [:HAS_LANGUAGE] → (Language {code:"bo"})
    → [:HAS_TITLE] → (Nomen) → (LocalizedText) → (Language)
    → [:HAS_LICENSE] → (LicenseType {name:"public"})
    → [:HAS_CONTRIBUTION] → (Contribution) → [:BY] → (Person {id:"P_001"})
                                             → [:WITH_ROLE] → (RoleType {name:"author"})
    ← [:EDITION_OF] ── (Edition {id:"E_001"})
      → [:HAS_TYPE] → (EditionType {name:"diplomatic"})
      ← [:SEGMENTATION_OF] ── (Segmentation:Display {id:"SGN_001"})
        ← [:SEGMENT_OF] ── (Segment {id:"SEG_001"}) ← [:SPAN_OF] ── (Span {0,50})
        ← [:SEGMENT_OF] ── (Segment {id:"SEG_002"}) ← [:SPAN_OF] ── (Span {50,100})
      ← [:PAGINATION_OF] ── (Pagination {id:"PAG_001"})
        ← [:VOLUME_OF] ── (Volume {index:null})
          ← [:PAGE_OF] ── (Page {ref:"1a"}) ← [:SPAN_OF] ── (Span {0,500})
```

S3 key: `texts/T_001/editions/E_001` → `"base text content"`

---

## Flow 2: Create a Translation

```
Prerequisites: Text T_001 (Tibetan) exists, Person P_002 (translator) exists

1. POST /v2/texts
   {
       "title": {"en": "English Translation"},
       "language": "en",
       "translation_of": "T_001",
       "category_id": "CAT_001",
       "license": "cc-by",
       "contributions": [{"person_id": "P_002", "role": "translator"}]
   }

   Validation:
   ✓ T_001 exists
   ✓ T_001.language ≠ "en" (different language required)
   ✓ P_002 exists

   Graph created:
   (Text {id:"T_002"}) → [:TRANSLATION_OF] → (Text {id:"T_001"})
   (Text {id:"T_002"}) → [:TEXT_OF {original:false}] → (Work {id:"W_001"})  ← same Work!

2. POST /v2/texts/T_002/editions
   {
       "metadata": {"type": "critical"},
       "content": "English translation text...",
       "segmentation": {
           "segments": [{"lines": [{"start": 0, "end": 50}]}, ...]
       }
   }

   IMPORTANT: critical editions MUST include segmentation — they cannot have
   pagination. Collated editions have no such constraints.
   Note: only ONE critical edition per text is allowed.
```

---

## Flow 3: Add an Annotation to an Existing Edition

All annotation additions follow the same pattern:

```
Client                    FastAPI                   Neo4j
  │                          │                        │
  │── POST /v2/editions/E_001/segmentations ─────────►│
  │   {segments: [...]}      │                        │
  │                          │── validate edition exists ──►│
  │                          │── WRITE transaction ───►│
  │                          │   MATCH Edition(E_001)  │
  │                          │   CREATE Segmentation:Display {id: SGN_002}
  │                          │   CREATE Segment × N    │
  │                          │   CREATE Span × M       │
  │◄── 201 {"id": "SGN_002"}─│                        │
```

Multiple display segmentations can coexist on one edition (e.g., one for sentence boundaries, one for page/paragraph display).

---

## Flow 4: Create an Alignment Between Two Editions

```
Prerequisites: Edition E_001 (Tibetan), Edition E_002 (English) both exist

POST /v2/editions/E_001/alignments
{
    "target_edition_id": "E_002",
    "target_segments": [
        {"lines": [{"start": 0, "end": 30}]},   ← from E_002's text
        {"lines": [{"start": 30, "end": 60}]}
    ],
    "aligned_segments": [
        {"lines": [{"start": 0, "end": 25}], "target_indices": [0]},
        {"lines": [{"start": 25, "end": 55}], "target_indices": [0, 1]}
    ]
}
```

**Transaction creates:**

```
(Edition E_001) ← [:SEGMENTATION_OF] ── (Segmentation:Aligned {id:"ALIGN_001"})
  ← [:SEGMENT_OF] ── (Segment A1) ← [:SPAN_OF] ── (Span {0,25})
                      ── [:ALIGNED_TO] ──► (Segment T1)
  ← [:SEGMENT_OF] ── (Segment A2) ← [:SPAN_OF] ── (Span {25,55})
                      ── [:ALIGNED_TO] ──► (Segment T1)
                      ── [:ALIGNED_TO] ──► (Segment T2)

(Edition E_002) ← [:SEGMENTATION_OF] ── (Segmentation:Target {id:"TARG_001"})
  ← [:SEGMENT_OF] ── (Segment T1) ← [:SPAN_OF] ── (Span {0,30})
  ← [:SEGMENT_OF] ── (Segment T2) ← [:SPAN_OF] ── (Span {30,60})
```

**Key invariant:** The alignment is identified by the `:Aligned` segmentation ID. The `:Target` segmentation ID is internal.

---

## Flow 5: Update Text Metadata

```
PATCH /v2/texts/T_001
{
    "title": {"bo": "གསར་མཚན།", "en": "New Title"},
    "license": "cc0"
}
```

**Transaction:**

1. Read current state of T_001 (to merge with patch)
2. DELETE old Nomen + LocalizedText nodes (title)
3. CREATE new Nomen + LocalizedText nodes
4. MERGE new HAS_TITLE relationship
5. UPDATE license relationship (delete old, create new)

**What is NOT changed:**
- `language` (not in patch)
- `category_id` (not in patch)
- Contributions (not exposed in patch)
- Editions (separate concern)

---

## Flow 6: Content Patch with Span Adjustment

This flow is the most complex. See also Lesson 06 for the full algorithm.

```
Initial state:
  Text: "Hello world"   (11 chars, [0,11))
  Segment S1: Span [0,6)   → "Hello "
  Segment S2: Span [6,11)  → "world"
  Bib metadata B1: Span [0,5) → "Hello"

PATCH /v2/editions/E_001/content
{"type": "insert", "position": 6, "text": "beautiful "}
```

**Flow:**

```
1. GET edition metadata (to find text_id)
2. adjust_spans_for_insert(edition_id, position=6, length=10)
   ┌─────────────────────────────────────────────────────┐
   │ WRITE transaction                                    │
   │                                                      │
   │ Fetch continuous spans (Segments, Pages):            │
   │   S1 [0,6) → insert at boundary 6 → expand → [0,16) │
   │   S2 [6,11) → insert at start → shift → [16,21)      │
   │                                                      │
   │ Fetch annotation spans (Bib, Note, Attr):            │
   │   B1 [0,5) → insert at 6 > end 5 → no change        │
   │                                                      │
   │ BATCH UPDATE: S1→[0,16), S2→[16,21)                 │
   └─────────────────────────────────────────────────────┘

3. storage.apply_insert(text_id, edition_id, position=6, text="beautiful ")

Final state:
  Text: "Hello beautiful world"   (21 chars)
  S1: [0,16) → "Hello beautiful "
  S2: [16,21) → "world"
  B1: [0,5) → "Hello"  (unchanged)
```

**Compensation on S3 failure:**

If the S3 write fails after the span adjustment in Neo4j, the code compensates by applying the inverse operation:

```python
try:
    await storage.apply_insert(...)
except Exception:
    await db.span.adjust_spans_for_delete(
        edition_id=edition_id,
        start=op.position,
        end=op.position + len(op.text),
    )
    raise
```

This is a **best-effort compensation**, not a two-phase commit. In rare cases of concurrent writes between compensation and client retry, spans could become inconsistent.

---

## Flow 7: Get Related Segments (Transitive Alignment)

```
Given this text tree:
  T_A (Tibetan original)
    T_B = translation of T_A (English)
    T_C = commentary on T_A (Tibetan)

Editions:
  E_A for T_A — aligned to E_B
  E_B for T_B — aligned to E_A
  E_C for T_C — aligned to E_A

GET /v2/segments/SEG_from_E_A/related
```

**Traversal:**

```
SEG_from_E_A
  → via ALIGNED_TO → SEG_in_E_B (direct hop)
  → via ALIGNED_TO → SEG_in_E_C (direct hop)

No further hops needed here. But with deeper chains:
  E_D = translation of E_B
  GET /v2/segments/SEG_from_E_A/related
    → E_B (hop 1)
    → E_D (hop 2, via E_B's alignment to E_D)
```

The algorithm traverses **up to depth 5 by default**. It collects all reachable editions via alignment links, then resolves display segments that overlap the queried span from each of those editions.

Response is a flat `PaginatedResponse[SegmentWithContextOutput]`. Each `SegmentWithContextOutput` includes `edition_id`, `text_id`, and `segmentation_id` so clients can group client-side if needed. See Lesson 03 §4.7 for the full response shape.

---

## Flow 8: Delete an Edition

```
DELETE /v2/editions/E_001
```

**Order of operations (all in one transaction):**

```
1. AlignmentDatabase.delete_all_with_transaction(tx, E_001)
   For each alignment (source or target side):
     a. find the matching :Aligned segmentation ID
     b. delete_with_transaction(aligned_seg_id, include_aligned=True)
     c. delete_with_transaction(target_seg_id, include_aligned=True)
     Each deletes: Segmentation + all Segment nodes + all Span nodes

2. SegmentationDatabase.delete_all_with_transaction(tx, E_001)
   Only :Display segmentations remain at this point
   Deletes: Segmentation + Segments + Spans

3. PaginationDatabase.delete_all_with_transaction(tx, E_001)
   Deletes: Pagination + Volumes + Pages + Spans

4. TableOfContentsDatabase.delete_all_with_transaction(tx, E_001)
   Deletes: TableOfContents + TableOfContentsSections + Spans
            + each section's title/summary Nomen + LocalizedText

5. BibliographicDatabase.delete_all_with_transaction(tx, E_001)
   DETACH DELETE BibliographicMetadata + Span

6. NoteDatabase.delete_all_with_transaction(tx, E_001)
   DETACH DELETE Note + Span

7. EditionDatabase.DELETE_QUERY
   DETACH DELETE Edition
   Also cleans up: Source (if no other editions reference it)
   Also cleans up: Nomen + LocalizedText for incipit_title

After transaction:
8. storage.delete_base_text(text_id, edition_id)  ← S3 delete
```

---

## Key Invariants to Know

| Invariant | Enforced where |
|---|---|
| Text title uniqueness | `DatabaseValidator.validate_text_title_unique` |
| Only one critical edition per Text | `EditionDatabase._validate_no_critical_exists` |
| Diplomatic edition requires BDRC | Pydantic `EditionBase.validate_edition` |
| Translation must differ in language from source | `TextDatabase._validate_translation_language` |
| Not both commentary and translation | Pydantic `TextBase.validate_text` |
| Title must include text's own language | Pydantic `TextBase.validate_text` |
| Critical edition must include segmentation (no pagination) | Pydantic `EditionRequestModel.validate_annotation` |
| Diplomatic edition must include pagination (no segmentation) | Pydantic `EditionRequestModel.validate_annotation` |
| Segmentation lines must be continuous | Pydantic `LinesModel.validate_lines` |
| Segments must be sorted by span start | Pydantic `SegmentationBase.validate_segments_sorted` |
| Single volume must have index=null | Pydantic `PaginationBase.validate_volume_indexes` |
| Multi-volume must have unique, contiguous indexes | Pydantic `PaginationBase.validate_volume_indexes` |
| Pages must be continuous within a volume | Pydantic `Volume.validate_pages_continuous` |
| Cannot directly delete aligned/target segmentation | `SegmentationDatabase.delete_with_transaction` |

---

## Next Lesson

→ [06_content_operations.md](06_content_operations.md) — Span adjustment algorithms in detail
