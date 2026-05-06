# Lesson 07 — Alignment System & Related Segments Traversal

## Learning Objectives

- Explain the two-segmentation model (`:Aligned` + `:Target`)
- Construct a valid alignment request for a many-to-many case
- Trace the `GET_BY_EDITION_ID_QUERY` Cypher to understand bidirectional lookup
- Explain transitive related-segment traversal and its depth limit
- Understand the difference between `GET /editions/{id}/segmentations` and `GET /editions/{id}/alignments`

---

## 1. Motivation

Editions of the same text in different languages can be **aligned at the segment level**, enabling parallel reading, cross-reference navigation, and search result aggregation. A Tibetan original and its English translation may have:

- One Tibetan segment → one English segment (1:1)
- One Tibetan segment → two English segments (1:N — the translator split a verse)
- Two Tibetan segments → one English segment (N:1 — the translator merged two clauses)

---

## 2. The Two-Segmentation Model

Each alignment creates **two** Segmentation nodes (not one):

```
Edition_Source (Tibetan)           Edition_Target (English)
  ←[SEGMENTATION_OF]─                ←[SEGMENTATION_OF]─
  Segmentation:Aligned               Segmentation:Target
    ←[SEGMENT_OF]─                     ←[SEGMENT_OF]─
    Segment_A ──[ALIGNED_TO]──────────► Segment_X
    Segment_B ──[ALIGNED_TO]──────────► Segment_X
    Segment_B ──[ALIGNED_TO]──────────► Segment_Y
```

**Why two segmentations?**
- Each edition needs its own segmentation with its own character-offset Spans
- The `:Aligned` label on the source segmentation indicates "this segmentation exists for alignment purposes"
- The `:Target` label on the target segmentation means "segments here are the target of ALIGNED_TO edges"

---

## 3. Request Structure in Detail

```json
POST /v2/editions/{source_edition_id}/alignments
{
    "target_edition_id": "E_002",
    "target_segments": [
        {"lines": [{"start": 0, "end": 30}]},    // index 0
        {"lines": [{"start": 30, "end": 60}]}     // index 1
    ],
    "aligned_segments": [
        {
            "lines": [{"start": 0, "end": 25}],
            "target_indices": [0]          // maps to target_segments[0]
        },
        {
            "lines": [{"start": 25, "end": 55}],
            "target_indices": [0, 1]       // maps to target_segments[0] AND [1]
        }
    ]
}
```

**Interpretation:**
```
Source text: "AAAAAAAAAAAAAAAAAAAAAAAAA" (25 chars covered by seg A)
             "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" (30 chars covered by seg B)

Target text: "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX" (30 chars = index 0)
             "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYY" (30 chars = index 1)

Alignment:
  Source seg A [0,25) ←→ Target seg X [0,30)
  Source seg B [25,55) ←→ Target seg X [0,30) AND Target seg Y [30,60)
```

---

## 4. How Alignment IDs Work

The response `id` for an alignment is the **`:Aligned` segmentation's ID**, not a separate node:

```json
{"id": "ALIGN_SGN_001"}
```

- `GET /v2/alignments/ALIGN_SGN_001` fetches the `:Aligned` segmentation and its connected `:Target`
- `DELETE /v2/alignments/ALIGN_SGN_001` deletes both the `:Aligned` and `:Target` segmentations

This means: the alignment ID *is* the aligned segmentation ID. There is no separate "Alignment" node in the graph.

---

## 5. Bidirectional Lookup (GET /editions/{id}/alignments)

A key feature: you can get all alignments for an edition whether it is the **source or target** of the alignment.

The Cypher uses `UNION`:

```cypher
MATCH (edition:Edition {id: $edition_id})
CALL {
    -- Case 1: This edition is the SOURCE (Aligned side)
    WITH edition
    MATCH (edition)<-[:SEGMENTATION_OF]-(source_segmentation:Segmentation:Aligned)
    RETURN source_segmentation

    UNION

    -- Case 2: This edition is the TARGET side
    WITH edition
    MATCH (edition)<-[:SEGMENTATION_OF]-(:Segmentation:Target)
          <-[:SEGMENT_OF]-(:Segment)
          <-[:ALIGNED_TO]-(:Segment)
          -[:SEGMENT_OF]->(source_segmentation:Segmentation:Aligned)
    RETURN DISTINCT source_segmentation
}
WITH DISTINCT source_segmentation
...
```

In case 2, the traversal reads:
> "Find any `:Target` segmentation attached to this edition, find segments in it, walk backwards via `ALIGNED_TO`, find the source segment, find its parent `Segmentation:Aligned`"

---

## 6. Parsing the Alignment Record

The `AlignmentDatabase._parse_record` method deserializes the Cypher result into `AlignmentOutput`. The challenge is reconstructing `target_indices`:

```python
target_min_start_to_segment: dict[int, SegmentOutput] = {}
target_min_starts_ordered: list[int] = []

for source_seg in record["segments"]:
    for target_data in source_seg["aligned_targets"]:
        target_min_start = target_data["min_start"]
        if target_min_start not in target_min_start_to_segment:
            # Record unique target segments
            target_min_start_to_segment[target_min_start] = SegmentOutput(...)
            target_min_starts_ordered.append(target_min_start)

    # Convert min_start → index in target list
    aligned_to_min_starts = [t["min_start"] for t in source_seg["aligned_targets"]]
    indices = [target_min_starts_ordered.index(ms) for ms in aligned_to_min_starts]
    aligned_segments.append(AlignedSegmentOutput(..., target_indices=indices))
```

Key insight: `target_indices` is **position-based**, not ID-based. It's the index into the `target_segments` list in the response.

---

## 7. Related Segments: Transitive Traversal

### 7.1 What "related" means

Given a segment (or a span within an edition), "related segments" are segments in other editions that cover the same textual content — found via the alignment graph.

### 7.2 Single-hop (old behavior)

Old behavior: one `ALIGNED_TO` hop in each direction.

```
E_A ─ aligned ─► E_B
Query segment in E_A → find aligned segment in E_B
```

### 7.3 Multi-hop (current behavior)

Current behavior: traverse the full alignment tree, default depth 5.

```
E_A ─ aligned ─► E_B ─ aligned ─► E_C

Query segment in E_A:
  Hop 1: find aligned segments in E_B
  Hop 2: from segments found in E_B, find aligned segments in E_C
  ...
```

The system also uses **text-level relationships** (`TRANSLATION_OF`, `COMMENTARY_OF`) to bridge to editions that don't have a direct alignment. In `edition_database.py`:

```cypher
-- Related via segment alignment (bidirectional)
MATCH (source:Edition {id: $edition_id})
      <-[:SEGMENTATION_OF]-(s1 WHERE s1:Aligned OR s1:Target)
      <-[:SEGMENT_OF]-(:Segment)
      -[:ALIGNED_TO]-(:Segment)
      -[:SEGMENT_OF]->(s2 WHERE s2:Aligned OR s2:Target)
      -[:SEGMENTATION_OF]->(m:Edition)
      -[:EDITION_OF]->(e:Text)
WHERE m.id <> $edition_id

UNION

-- Related via text relationships (translation/commentary chains)
MATCH (source:Edition {id: $edition_id})-[:EDITION_OF]->(:Text)
      -[:TRANSLATION_OF|:COMMENTARY_OF]-(e:Text)<-[:EDITION_OF]-(m:Edition)
WHERE m.id <> $edition_id
```

### 7.4 Response Format

Results are returned as a flat `PaginatedResponse[SegmentOutput]`. Each item carries `edition_id`, `text_id`, and `segmentation_id` directly, so clients can group by edition or segmentation if needed:

```json
{
    "items": [
        {
            "id": "SEG_010",
            "segmentation_id": "SGN_DISPLAY_A",
            "edition_id": "E_002",
            "text_id": "T_002",
            "lines": [{"start": 0, "end": 50}],
            "tag_ids": null
        },
        {
            "id": "SEG_011",
            "segmentation_id": "SGN_DISPLAY_A",
            "edition_id": "E_002",
            "text_id": "T_002",
            "lines": [{"start": 50, "end": 100}],
            "tag_ids": null
        }
    ],
    "has_more": false,
    "offset": 0,
    "limit": 20
}
```

The `RelatedSegmentsOutput` / `RelatedSegmentationOutput` models exist in `models/annotation.py` but are not currently returned by any router — they are available for future use or client-side grouping utilities.

---

## 7.5 SegmentOutput Field Note

The migration guide mentioned removing `edition_id`/`text_id` from `SegmentOutput`. **This did not happen.** The current `SegmentOutput` model includes:

```python
class SegmentOutput(SegmentBase):
    id: NonEmptyStr
    segmentation_id: NonEmptyStr
    edition_id: NonEmptyStr      # ← still present
    text_id: NonEmptyStr         # ← still present
    tag_ids: list[str] | None = None
```

These fields are populated whenever a segment is fetched — including in related-segment results.

---

## 8. Display vs. Alignment Segmentations

`GET /v2/editions/{id}/segmentations` only returns **`:Display`** segmentations:

```cypher
MATCH (edition:Edition {id: $edition_id})<-[:SEGMENTATION_OF]-(segmentation:Segmentation:Display)
```

**`:Aligned`** and **`:Target`** segmentations are excluded from this endpoint. They are only accessible via the alignment endpoints.

| Endpoint | Returns |
|---|---|
| `GET /editions/{id}/segmentations` | `:Display` only |
| `GET /editions/{id}/alignments` | `:Aligned` + `:Target` (via UNION) |
| `GET /segmentations/{id}` | Any segmentation by ID |

---

## 9. Alignment Deletion

```
DELETE /v2/alignments/{segmentation_id}
```

1. Validates that `segmentation_id` is an `:Aligned` segmentation
2. Finds the connected `:Target` segmentation ID (via `ALIGNED_TO` path)
3. Calls `SegmentationDatabase.delete_with_transaction` with `include_aligned=True` on both

The `include_aligned=True` flag bypasses the safety check that normally prevents deletion of aligned/target segmentations.

---

## 10. Visual Summary: Alignment Graph

```
TIBETAN EDITION (E_TIB)                  ENGLISH EDITION (E_EN)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[text]: "ཀུན་གཞི།    བར་ཆད།..."            [text]: "Foundation...  Obstacles..."

Segmentation:Aligned (id=SA)             Segmentation:Target (id=ST)
  Segment A1 ─────────────────────────────► Segment T1
  [0,30)       ALIGNED_TO                   [0,40)
                                              "Foundation..."
  Segment A2 ─────────────────────────────► Segment T1
  [30,55)      ALIGNED_TO                   [0,40)
  (same target)
  
  Segment A3 ─────────────────────────────► Segment T2
  [55,80)      ALIGNED_TO                   [40,80)
                                              "Obstacles..."

  Segment A3 ─────────────────────────────► Segment T3
               ALIGNED_TO                   [80,120)
                                              "..."

Segmentation:Display (id=SD)
  Segment D1 [0,55) (merged segments A1+A2 for display)
  Segment D2 [55,80)
  ...
```

Note: The `:Display` segmentation is entirely separate from the `:Aligned` segmentation. An edition typically has:
- 0 or 1 display segmentations (for user-facing display)
- 0 or more aligned segmentations (one per text it's aligned with)

---

## 11. Common Pitfalls

| Pitfall | Explanation |
|---|---|
| Trying to delete a `:Display` segmentation that doesn't exist as `:Aligned` | Will raise `InvalidRequestError` if you call `DELETE /segmentations/{id}` on an `:Aligned` segmentation without deleting the parent alignment first |
| Forgetting that alignment IDs are segmentation IDs | The `id` in `AlignmentOutput` is the `:Aligned` segmentation's ID |
| Assuming `target_indices` are stable | They are index-positions in the current `target_segments` list, not permanent IDs |
| Mixing up source/target edition direction | The `aligned_edition_id` in `AlignmentOutput` is the **source** (the edition whose segments have `ALIGNED_TO` outgoing edges) |

---

## Practice Questions

1. You have Tibetan edition E_A and English edition E_B. You want to align them so that:
   - Tibetan segment [0,100) aligns to English segment [0,80)
   - Tibetan segment [100,200) aligns to English segments [80,160) AND [160,220)
   
   Write the full request body.

2. After creating this alignment, you call `GET /v2/editions/E_B/alignments`. Will you get a result? Why or why not?

3. You call `DELETE /v2/segmentations/{aligned_seg_id}` directly (not via the alignment endpoint). What error do you get?

4. Edition E_A has one `:Display` segmentation and one `:Aligned` segmentation. How many segmentation nodes are in the graph for E_A?

---

## Next Lesson

→ [08_exercises.md](08_exercises.md) — Hands-on lab exercises
