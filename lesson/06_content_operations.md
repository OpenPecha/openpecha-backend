# Lesson 06 — Content Operations & Span Adjustment Algorithms

## Learning Objectives

- Explain the three content operations: INSERT, DELETE, REPLACE
- Distinguish "continuous" spans (Segment, Page) from "annotation" spans (Note, BibMeta, Attribute, TableOfContentsSection)
- Apply the adjustment algorithm by hand for any operation
- Understand the edge cases: insert at 0, boundary inserts, encompassed spans
- Read the Python code in `database/span_database.py` and predict the outcome

---

## 1. Why Span Adjustment?

The base text is stored in S3 as a plain string. Annotations and segments store their positions as `{start, end}` character offsets into that string. When the base text changes, **all stored offsets must be updated** to remain correct.

```
Before edit:
  Base text: "Hello world"  (positions 0-10)
  Segment:   [0, 6)  → "Hello "
  Note:      [0, 5)  → "Hello"

INSERT "beautiful " at position 6:
  Base text: "Hello beautiful world"  (positions 0-20)
  Segment:   must now be [0, 16)
  Note:      must remain [0, 5) (insert was after its end)
```

Without adjustment, the segment would still claim to cover `[0, 6)` which no longer corresponds to "Hello ".

---

## 2. Two Categories of Spans

The adjustment algorithm treats spans differently depending on their entity type:

| Category | Entity types | Behavior |
|---|---|---|
| **Continuous** | `Segment`, `Page` | Cover contiguous text with no gaps; expand at boundaries |
| **Annotation** | `Note`, `BibliographicMetadata`, `Attribute`, `TableOfContentsSection` | Point at a specific text region; shift at boundaries |

The key difference is at **insert at boundary**:
- A `Segment` covering `[0, 10)` with an insert at position 10 **expands** to `[0, 10+len)`
- A `Note` covering `[0, 10)` with an insert at position 10 **stays** at `[0, 10)` (insert is outside)

---

## 3. INSERT Operation

### 3.1 Continuous Span Adjustment (`_adjust_continuous_for_insert`)

```
Parameters: start, end, insert_pos, insert_len

Cases:
  1. insert_pos == 0 AND start == 0:  EXPAND → (0, end + insert_len)
     Rationale: First span must absorb content inserted at the very beginning
  
  2. insert_pos <= start:             SHIFT  → (start + insert_len, end + insert_len)
  
  3. insert_pos <= end:               EXPAND → (start, end + insert_len)
     (includes insert at exact end boundary)
  
  4. insert_pos > end:                UNCHANGED → (start, end)
```

### 3.2 Annotation Span Adjustment (`_adjust_annotation_for_insert`)

```
Parameters: start, end, insert_pos, insert_len

Cases:
  1. insert_pos <= start:    SHIFT  → (start + insert_len, end + insert_len)
  
  2. insert_pos < end:       EXPAND → (start, end + insert_len)
     (strictly inside, not at end boundary)
  
  3. insert_pos >= end:      UNCHANGED → (start, end)
```

### 3.3 Visual Comparison

```
Span:  [====start====end====]
             ↑ insert here

Continuous:  before start → shift both
             at start     → shift both
             after start, before end → expand
             at end       → expand (absorbs trailing insert)
             after end    → no change

Annotation:  before start → shift both
             at start     → shift both
             strictly inside (not at end) → expand
             at end       → NO change (insert is outside)
             after end    → no change
```

### 3.4 Table: INSERT Behavior by Position

| Insert at | Continuous `[5, 15)` | Annotation `[5, 15)` |
|---|---|---|
| 0 (and span starts at 0) | `[0, 20)` (expand) | `[10, 20)` (shift) |
| 3 (before span) | `[10, 20)` (shift) | `[10, 20)` (shift) |
| 5 (at start) | `[10, 20)` (shift) | `[10, 20)` (shift) |
| 10 (inside) | `[5, 20)` (expand) | `[5, 20)` (expand) |
| 15 (at end) | `[5, 20)` (expand) | `[5, 15)` (unchanged) |
| 20 (after) | `[5, 15)` (unchanged) | `[5, 15)` (unchanged) |

*(Assuming insert_len = 5)*

---

## 4. DELETE Operation

DELETE shares the same logic for both continuous and annotation spans. Spans that fall entirely within the deleted range are **deleted** (not just adjusted).

### 4.1 `_adjust_span_for_delete`

Returns `None` if the span should be deleted; otherwise returns `(new_start, new_end)`.

```
Parameters: start, end, del_start, del_end

del_len = del_end - del_start

Cases:
  1. del_end <= start:                         SHIFT  → (start - del_len, end - del_len)
  
  2. del_start >= end:                         UNCHANGED → (start, end)
  
  3. del_start <= start AND del_end >= end:    DELETE → None
  
  4. del_start <= start < del_end < end:       TRIM START → (del_start, end - del_len)
  
  5. start < del_start < end <= del_end:       TRIM END → (start, del_start)
  
  6. start < del_start AND del_end < end:      SHRINK → (start, end - del_len)
```

### 4.2 DELETE Behavior Matrix

```
Span:  [====start====end====]

Delete zone:
  Before span:        [del_start..del_end]  →  shift span left
  After span:                                →  unchanged
  Encompasses span:   [────────────────────] → DELETE span
  Overlaps start:     [──────────────]──────  → trim start (start moves to del_start)
  Overlaps end:              ──────────────── → trim end (end moves to del_start)
  Inside span:            [──────]            → shrink span
```

### 4.3 Example

```
Segments: S1=[0,10), S2=[10,20), S3=[20,30)
DELETE [10, 20) (delete S2's exact range)

S1=[0,10):  del_start=10 >= end=10 → UNCHANGED [0,10)
S2=[10,20): del_start<=start AND del_end>=end → DELETE
S3=[20,30): del_end=20 <= start=20 → SHIFT by -10 → [10,20)

Result: S1=[0,10), S3=[10,20)  (gap closed, S2 removed)
```

---

## 5. REPLACE Operation

REPLACE is the most complex. It differs between continuous and annotation spans, and handles the "keep first encompassed" logic.

### 5.1 `_adjust_annotation_for_replace`

```
Parameters: start, end, replace_start, replace_end, new_len

delta = new_len - (replace_end - replace_start)

Cases:
  1. replace_start >= end:             UNCHANGED
  2. replace_end <= start:             SHIFT by delta
  3. replace_start <= start AND replace_end >= end:   DELETE → None
  4. start < replace_start AND replace_end < end:     SHRINK → (start, end + delta)
  5. replace_start <= start < replace_end < end:      TRIM START → (replace_start + new_len, end + delta)
  6. start < replace_start < end <= replace_end:      TRIM END → (start, replace_start + new_len)
```

### 5.2 `_adjust_continuous_for_replace`

Same as annotation BUT:
- **Exact match** (`start==replace_start AND end==replace_end`) → `(start, start + new_len)` (preserves, resizes)
- **Encompasses** → keeps first encompassed span, deletes the rest

```
Parameters: (same as above) + is_first_encompassed: bool

Additional cases before the shared ones:
  exact match:           → (start, start + new_len)
  encompasses (first):   → (replace_start, replace_start + new_len)
  encompasses (others):  → DELETE → None
```

### 5.3 "First Encompassed" Logic

When a REPLACE operation encompasses multiple segments, you can't keep all of them (they'd all collapse to the same position). The code keeps only the **first** one (by span_start order) and deletes the rest.

```
Segments: S1=[0,10), S2=[10,20), S3=[20,30)
REPLACE [5, 25) with "XX" (new_len=2, delta = 2 - 20 = -18)

S1=[0,10): overlaps end → TRIM END → (0, 5+2) = [0, 7)
S2=[10,20): encompasses, first → (5, 5+2) = [5, 7)
S3=[20,30): encompasses, not first → DELETE

Result: S1=[0,7), S2=[5,7)
```

Wait — that creates an overlap. Let me trace through more carefully using the actual code:

```python
# For S1=[0,10), replace_start=5, replace_end=25, new_len=2:
# Case: start < replace_start < end <= replace_end
# → (start, replace_start + new_len) = (0, 5+2) = (0, 7)

# For S2=[10,20), encompasses and first:
# → (replace_start, replace_start + new_len) = (5, 7)

# For S3=[20,30), encompasses, not first → None (DELETE)
```

S1=[0,7), S2=[5,7) — note the overlap between S1 and S2. This is an expected trade-off; the text was replaced destructively. The "first encompassed" logic at least preserves one segment covering the replaced region.

---

## 6. Special Case: Insert at Position 0

```
Segments: S1=[0,10), S2=[10,20)
INSERT "HELLO " at position 0 (6 chars)

Continuous S1=[0,10): insert_pos==0 AND start==0 → EXPAND → [0, 16)
Continuous S2=[10,20): insert_pos=0 <= start=10 → SHIFT → [16, 26)

Result: S1=[0,16), S2=[16,26)  ← S1 absorbed the new content
```

Without this special case, S1 would shift to `[6,16)` leaving a gap `[0,6)` uncovered by any segment.

---

## 7. The Full Span Adjustment Flow in Code

```python
# span_database.py

async def adjust_spans_for_insert(self, edition_id, position, length):
    async def write(tx):
        updates = []

        # Continuous spans: Segments, Pages
        result = await tx.run(FIND_CONTINUOUS_SPANS_QUERY, edition_id=edition_id)
        for record in await result.data():
            adjusted = _adjust_continuous_for_insert(
                record["span_start"], record["span_end"], position, length
            )
            if adjusted != (record["span_start"], record["span_end"]):
                updates.append({"entity_id": record["entity_id"],
                                 "new_start": adjusted[0],
                                 "new_end": adjusted[1]})

        # Annotation spans: Notes, BibMeta, Attributes, TableOfContentsSections
        result = await tx.run(FIND_ANNOTATION_SPANS_QUERY, edition_id=edition_id)
        for record in await result.data():
            adjusted = _adjust_annotation_for_insert(...)
            ...

        # Batch write all updates
        await _flush_batch(tx, updates, [], BATCH_UPDATE_SPANS_QUERY, BATCH_DELETE_ENTITIES_QUERY)

    async with self._db.get_session() as session:
        await session.execute_write(write)
```

The Cypher batch update:
```cypher
UNWIND $updates AS u
MATCH (span:Span)-[:SPAN_OF]->(entity:Segment|Page|BibliographicMetadata|Note|Attribute|TableOfContentsSection {id: u.entity_id})
SET span.start = u.new_start, span.end = u.new_end
```

The batch delete (note: for `TableOfContentsSection` it also removes the section's title/summary Nomen subgraph):
```cypher
UNWIND $entity_ids AS eid
MATCH (entity:Segment|Page|BibliographicMetadata|Note|Attribute|TableOfContentsSection {id: eid})
OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(entity)
OPTIONAL MATCH (entity)-[:HAS_TITLE|HAS_SUMMARY]->(nomen:Nomen)
OPTIONAL MATCH (nomen)-[:HAS_LOCALIZATION]->(localized:LocalizedText)
DETACH DELETE span, localized, nomen, entity
```

---

## 8. Summary Table: All Operation × Category Combinations

| Operation | Position/Overlap | Continuous (Seg/Page) | Annotation (Note/Bib/Attr/TOC Section) |
|---|---|---|---|
| **INSERT** | pos == 0, span starts at 0 | Expand | Shift |
| **INSERT** | pos ≤ start | Shift | Shift |
| **INSERT** | start < pos < end | Expand | Expand |
| **INSERT** | pos == end | Expand | Unchanged |
| **INSERT** | pos > end | Unchanged | Unchanged |
| **DELETE** | del_end ≤ start | Shift | Shift |
| **DELETE** | del_start ≥ end | Unchanged | Unchanged |
| **DELETE** | encompasses | **Delete entity** | **Delete entity** |
| **DELETE** | overlaps start | Trim start | Trim start |
| **DELETE** | overlaps end | Trim end | Trim end |
| **DELETE** | inside span | Shrink | Shrink |
| **REPLACE** | rep_end ≤ start | Shift by delta | Shift by delta |
| **REPLACE** | rep_start ≥ end | Unchanged | Unchanged |
| **REPLACE** | exact match | Resize (preserve) | **Delete entity** |
| **REPLACE** | encompasses (first) | Resize (preserve) | **Delete entity** |
| **REPLACE** | encompasses (other) | **Delete entity** | **Delete entity** |
| **REPLACE** | overlaps start | Trim start + shift | Trim start + shift |
| **REPLACE** | overlaps end | Trim end | Trim end |
| **REPLACE** | inside span | Shrink by delta | Shrink by delta |

---

## 9. Practice Problems

Solve by hand, then verify with the code:

**Problem 1:**
```
Segments: S1=[0,50), S2=[50,100), S3=[100,150)
Annotation: A1=[20,80)
INSERT "XYZ" at position 50 (len=3)

What are the new positions?
```

**Problem 2:**
```
Segments: S1=[0,30), S2=[30,60), S3=[60,90)
DELETE [25, 65)

Which segments are deleted? What do the survivors become?
```

**Problem 3:**
```
Segments: S1=[0,20), S2=[20,40), S3=[40,60)
Note: N1=[15,45)
REPLACE [10,50) with "ABCDE" (len=5)

What happens to S1, S2, S3, N1?
```

<details>
<summary>Answers</summary>

**Problem 1:**
- S1=[0,50): insert at end boundary (pos=50 == end=50) → EXPAND → [0,53)
- S2=[50,100): insert at start (pos=50 == start=50) → SHIFT → [53,103)
- S3=[100,150): no change → [100,150) ... wait, should shift: pos=50 < start=100 → SHIFT → [103, 153)
- A1=[20,80): insert strictly inside (50 > 20, 50 < 80) → EXPAND → [20, 83)

**Problem 2:**
- del_start=25, del_end=65, del_len=40
- S1=[0,30): overlaps end: start < del_start < end <= del_end → TRIM END → (0, 25)
- S2=[30,60): encompasses (25<=30 AND 65>=60) → DELETE
- S3=[60,90): overlaps start (del_start=25 <= start=60 < del_end=65 < end=90) → TRIM START → (25, 50)

**Problem 3:**
- delta = 5 - (50-10) = 5 - 40 = -35
- S1=[0,20): overlaps end: start < replace_start < end <= replace_end → TRIM END → (0, 10+5) = (0, 15)
- S2=[20,40): encompasses, first → (replace_start, replace_start+new_len) = (10, 15)
- S3=[40,60): encompasses, not first → DELETE
- N1=[15,45): encompasses (10<=15 AND 50>=45) → DELETE

</details>

---

## Next Lesson

→ [07_alignment_deep_dive.md](07_alignment_deep_dive.md) — Alignment system and related segments traversal
