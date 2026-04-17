"""
Performance benchmark for the related segments traversal algorithm.

NOT included in the default test suite — run explicitly:
    pytest tests/bench_segments_related.py -m benchmark -v -s

Builds a large graph (9 editions, thousands of segments and alignments)
and measures the time to resolve related segments from various positions
in the tree.

Graph topology:

    Root
    ├── T1 (translation)
    │   ├── C1 (commentary)
    │   │   ├── SubC (sub-commentary)
    │   │   │   └── SubSubC (sub-sub-commentary)
    │   │   └── T3 (translation of commentary)
    │   └── C2 (commentary)
    │       └── Diamond ← also reachable from T2
    └── T2 (translation)
        └── Diamond (commentary, convergent)

Requires Docker for Neo4j testcontainer.
"""

import json
import statistics
import time
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------
SEGMENTS_PER_EDITION = 500
LINES_PER_SEGMENT = 2
CHARS_PER_LINE = 20
WARMUP_ITERATIONS = 3
MEASURED_ITERATIONS = 10

APPLICATION_HEADER = {"X-Application": "test_application"}

# Derived constants
CONTENT_LENGTH = SEGMENTS_PER_EDITION * LINES_PER_SEGMENT * CHARS_PER_LINE


# ---------------------------------------------------------------------------
# Graph builder — creates the full topology via raw Cypher
# ---------------------------------------------------------------------------

def _make_edition_key(name: str) -> dict:
    """Generate deterministic IDs for an edition node in the benchmark graph."""
    return {
        "work_id": f"bench_work_{name}",
        "text_id": f"bench_text_{name}",
        "edition_id": f"bench_ed_{name}",
        "display_sgn_id": f"bench_dsgn_{name}",
    }


def _segment_spans(n_segments: int, lines_per: int, chars_per_line: int) -> list[list[dict]]:
    """Generate segment span data: list of segments, each a list of line dicts."""
    spans = []
    pos = 0
    for _ in range(n_segments):
        lines = []
        for _ in range(lines_per):
            lines.append({"start": pos, "end": pos + chars_per_line})
            pos += chars_per_line
        spans.append(lines)
    return spans


EDITION_NAMES = ["root", "t1", "t2", "c1", "c2", "subc", "subsubc", "t3", "diamond"]

ALIGNMENT_EDGES = [
    ("t1", "root"),
    ("t2", "root"),
    ("c1", "t1"),
    ("c2", "t1"),
    ("subc", "c1"),
    ("subsubc", "subc"),
    ("t3", "c1"),
    ("diamond", "t2"),
    ("diamond", "c2"),
]


async def _build_graph(db) -> dict[str, dict]:
    """Insert the full benchmark graph directly via Cypher. Returns edition metadata."""
    editions = {name: _make_edition_key(name) for name in EDITION_NAMES}
    segment_data = _segment_spans(SEGMENTS_PER_EDITION, LINES_PER_SEGMENT, CHARS_PER_LINE)

    async with db.get_session() as session:
        # 1. Create all Work -> Text -> Edition -> Display Segmentation -> Segments -> Spans
        for name, ids in editions.items():
            await session.run(
                """
                MATCH (lang:Language {code: 'bo'}),
                      (license:LicenseType {name: 'public'}),
                      (cat:Category {id: 'category'}),
                      (role:RoleType {name: 'author'})
                MERGE (edtype:EditionType {name: 'critical'})
                CREATE (work:Work {id: $work_id})-[:HAS_CATEGORY]->(cat)
                CREATE (text:Text {id: $text_id})-[:TEXT_OF {original: true}]->(work)
                CREATE (text)-[:HAS_LANGUAGE]->(lang)
                CREATE (text)-[:HAS_LICENSE]->(license)
                CREATE (nomen:Nomen {id: $work_id + '_nomen'})
                CREATE (text)-[:HAS_TITLE]->(nomen)
                CREATE (lt:LocalizedText {text: $title})
                CREATE (nomen)-[:HAS_LOCALIZATION]->(lt)-[:HAS_LANGUAGE]->(lang)
                CREATE (person:Person {id: $work_id + '_person'})
                CREATE (pnomen:Nomen {id: $work_id + '_pnomen'})
                CREATE (person)-[:HAS_NAME]->(pnomen)
                CREATE (plt:LocalizedText {text: 'Author'})
                CREATE (pnomen)-[:HAS_LOCALIZATION]->(plt)-[:HAS_LANGUAGE]->(lang)
                CREATE (contrib:Contribution)-[:BY]->(person)
                CREATE (contrib)-[:WITH_ROLE]->(role)
                CREATE (text)-[:HAS_CONTRIBUTION]->(contrib)
                CREATE (edition:Edition {id: $edition_id})-[:EDITION_OF]->(text)
                CREATE (edition)-[:HAS_TYPE]->(edtype)
                CREATE (sgn:Segmentation:Display {id: $display_sgn_id})-[:SEGMENTATION_OF]->(edition)
                WITH sgn
                UNWIND $segments AS seg_data
                CREATE (seg:Segment {id: seg_data.id})-[:SEGMENT_OF]->(sgn)
                WITH seg, seg_data
                UNWIND seg_data.lines AS line
                CREATE (:Span {start: line.start, end: line.end})-[:SPAN_OF]->(seg)
                """,
                work_id=ids["work_id"],
                text_id=ids["text_id"],
                edition_id=ids["edition_id"],
                display_sgn_id=ids["display_sgn_id"],
                title=f"Bench {name}",
                segments=[
                    {"id": f"bench_dseg_{name}_{i}", "lines": segment_data[i]}
                    for i in range(SEGMENTS_PER_EDITION)
                ],
            )

        # 2. Create alignment edges (Aligned segmentation + Target segmentation + ALIGNED_TO)
        for source_name, target_name in ALIGNMENT_EDGES:
            source_ed = editions[source_name]["edition_id"]
            target_ed = editions[target_name]["edition_id"]
            aligned_sgn_id = f"bench_asgn_{source_name}_to_{target_name}"
            target_sgn_id = f"bench_tsgn_{source_name}_to_{target_name}"

            # Build segment + alignment data for this edge.
            # 1:1 mapping: source segment i aligns to target segment i.
            aligned_segs = [
                {"id": f"bench_aseg_{source_name}_to_{target_name}_{i}", "lines": segment_data[i]}
                for i in range(SEGMENTS_PER_EDITION)
            ]
            target_segs = [
                {"id": f"bench_tseg_{source_name}_to_{target_name}_{i}", "lines": segment_data[i]}
                for i in range(SEGMENTS_PER_EDITION)
            ]
            alignments = [
                {
                    "source_id": f"bench_aseg_{source_name}_to_{target_name}_{i}",
                    "target_id": f"bench_tseg_{source_name}_to_{target_name}_{i}",
                }
                for i in range(SEGMENTS_PER_EDITION)
            ]

            await session.run(
                """
                MATCH (source_edition:Edition {id: $source_ed}),
                      (target_edition:Edition {id: $target_ed})
                CREATE (asgn:Segmentation:Aligned {id: $aligned_sgn_id})-[:SEGMENTATION_OF]->(source_edition)
                CREATE (tsgn:Segmentation:Target {id: $target_sgn_id})-[:SEGMENTATION_OF]->(target_edition)
                WITH asgn, tsgn
                UNWIND $target_segs AS seg_data
                CREATE (seg:Segment {id: seg_data.id})-[:SEGMENT_OF]->(tsgn)
                WITH asgn, seg, seg_data
                UNWIND seg_data.lines AS line
                CREATE (:Span {start: line.start, end: line.end})-[:SPAN_OF]->(seg)
                WITH DISTINCT asgn
                UNWIND $aligned_segs AS seg_data
                CREATE (seg:Segment {id: seg_data.id})-[:SEGMENT_OF]->(asgn)
                WITH seg, seg_data
                UNWIND seg_data.lines AS line
                CREATE (:Span {start: line.start, end: line.end})-[:SPAN_OF]->(seg)
                RETURN count(*) AS _
                NEXT
                UNWIND $alignments AS a
                MATCH (src:Segment {id: a.source_id}), (tgt:Segment {id: a.target_id})
                CREATE (src)-[:ALIGNED_TO]->(tgt)
                RETURN count(*) AS cnt
                """,
                source_ed=source_ed,
                target_ed=target_ed,
                aligned_sgn_id=aligned_sgn_id,
                target_sgn_id=target_sgn_id,
                aligned_segs=aligned_segs,
                target_segs=target_segs,
                alignments=alignments,
            )

    return editions


# ---------------------------------------------------------------------------
# Benchmark scenarios
# ---------------------------------------------------------------------------

def _make_scenarios(editions: dict[str, dict]) -> list[dict]:
    """Define benchmark scenarios: each is a dict with name, edition_id, span_start, span_end, description."""
    mid = CONTENT_LENGTH // 2
    single_seg_end = LINES_PER_SEGMENT * CHARS_PER_LINE

    return [
        {
            "name": "leaf_upward_5_hops",
            "description": "From deepest leaf (SubSubC), traverse up 5 hops",
            "edition_id": editions["subsubc"]["edition_id"],
            "span_start": 0,
            "span_end": single_seg_end,
        },
        {
            "name": "root_downward_fanout",
            "description": "From root, traverse down to all descendants",
            "edition_id": editions["root"]["edition_id"],
            "span_start": 0,
            "span_end": single_seg_end,
        },
        {
            "name": "mid_tree_bidirectional",
            "description": "From C1 (mid-tree), traverse both up and down",
            "edition_id": editions["c1"]["edition_id"],
            "span_start": 0,
            "span_end": single_seg_end,
        },
        {
            "name": "root_wide_span",
            "description": "Full content span on root — max segment overlap",
            "edition_id": editions["root"]["edition_id"],
            "span_start": 0,
            "span_end": CONTENT_LENGTH,
        },
        {
            "name": "root_narrow_span",
            "description": "Single char range on root — minimal overlap",
            "edition_id": editions["root"]["edition_id"],
            "span_start": mid,
            "span_end": mid + 1,
        },
        {
            "name": "diamond_convergent",
            "description": "From diamond node reachable via two paths",
            "edition_id": editions["diamond"]["edition_id"],
            "span_start": 0,
            "span_end": single_seg_end,
        },
        {
            "name": "translation_of_commentary",
            "description": "From T3 (translation of commentary C1)",
            "edition_id": editions["t3"]["edition_id"],
            "span_start": 0,
            "span_end": single_seg_end,
        },
    ]


async def _run_scenario(client, scenario: dict, warmup: int, iterations: int) -> dict:
    """Run a single benchmark scenario, return timing statistics."""
    url = (
        f"/v2/editions/{scenario['edition_id']}/segments/related"
        f"?span_start={scenario['span_start']}&span_end={scenario['span_end']}"
    )

    for _ in range(warmup):
        resp = await client.get(url, headers=APPLICATION_HEADER)
        assert resp.status_code == 200, f"Warmup failed: {resp.status_code} {resp.text}"

    times_ms = []
    last_response = None
    for _ in range(iterations):
        start = time.perf_counter()
        resp = await client.get(url, headers=APPLICATION_HEADER)
        elapsed = (time.perf_counter() - start) * 1000
        assert resp.status_code == 200, f"Benchmark failed: {resp.status_code} {resp.text}"
        times_ms.append(elapsed)
        last_response = resp.json()

    total_segments = sum(
        len(seg)
        for group in (last_response or [])
        for sgn in group.get("segmentations", [])
        for seg in [sgn.get("segments", [])]
    )
    editions_returned = len(last_response or [])

    return {
        "name": scenario["name"],
        "description": scenario["description"],
        "min_ms": round(min(times_ms), 1),
        "median_ms": round(statistics.median(times_ms), 1),
        "p95_ms": round(sorted(times_ms)[int(len(times_ms) * 0.95)], 1),
        "max_ms": round(max(times_ms), 1),
        "mean_ms": round(statistics.mean(times_ms), 1),
        "iterations": iterations,
        "editions_returned": editions_returned,
        "segments_returned": total_segments,
    }


# ---------------------------------------------------------------------------
# Pytest entry point
# ---------------------------------------------------------------------------

@pytest.mark.benchmark
@pytest.mark.asyncio(loop_scope="session")
class TestRelatedSegmentsBenchmark:
    """Performance benchmark for related segments traversal."""

    @pytest.fixture(autouse=True)
    async def _setup_graph(self, test_database, client):
        """Build the benchmark graph once for all tests in this class."""
        self.editions = await _build_graph(test_database)
        self.client = client

        # Verify graph was created
        async with test_database.get_session() as session:
            result = await session.run("MATCH (s:Segment) RETURN count(s) AS cnt")
            record = await result.single()
            total_segments = record["cnt"]

        n_editions = len(EDITION_NAMES)
        n_edges = len(ALIGNMENT_EDGES)
        expected_segments = (
            n_editions * SEGMENTS_PER_EDITION  # display segments
            + n_edges * SEGMENTS_PER_EDITION * 2  # aligned + target segments per edge
        )
        assert total_segments == expected_segments, (
            f"Expected {expected_segments} segments, got {total_segments}"
        )

    async def test_benchmark_all_scenarios(self):
        """Run all benchmark scenarios and print results."""
        scenarios = _make_scenarios(self.editions)
        results = []

        print("\n")
        print("=" * 100)
        print(f"  Related Segments Benchmark — {SEGMENTS_PER_EDITION} segments/edition, "
              f"{len(EDITION_NAMES)} editions, {len(ALIGNMENT_EDGES)} alignment edges")
        print(f"  {WARMUP_ITERATIONS} warmup + {MEASURED_ITERATIONS} measured iterations per scenario")
        print("=" * 100)

        header = (
            f"{'Scenario':<30} {'Min':>8} {'Median':>8} {'P95':>8} "
            f"{'Max':>8} {'Mean':>8} {'Editions':>9} {'Segments':>9}"
        )
        print(header)
        print("-" * len(header))

        for scenario in scenarios:
            result = await _run_scenario(
                self.client, scenario,
                warmup=WARMUP_ITERATIONS,
                iterations=MEASURED_ITERATIONS,
            )
            results.append(result)

            print(
                f"{result['name']:<30} "
                f"{result['min_ms']:>7.1f}ms "
                f"{result['median_ms']:>7.1f}ms "
                f"{result['p95_ms']:>7.1f}ms "
                f"{result['max_ms']:>7.1f}ms "
                f"{result['mean_ms']:>7.1f}ms "
                f"{result['editions_returned']:>9} "
                f"{result['segments_returned']:>9}"
            )

        print("=" * len(header))
        print()

        output_path = Path(__file__).parent.parent / "bench_results.json"
        output_path.write_text(json.dumps({
            "config": {
                "segments_per_edition": SEGMENTS_PER_EDITION,
                "lines_per_segment": LINES_PER_SEGMENT,
                "chars_per_line": CHARS_PER_LINE,
                "editions": len(EDITION_NAMES),
                "alignment_edges": len(ALIGNMENT_EDGES),
                "warmup_iterations": WARMUP_ITERATIONS,
                "measured_iterations": MEASURED_ITERATIONS,
            },
            "results": results,
        }, indent=2))
        print(f"Results written to {output_path}")

    async def test_benchmark_leaf_correctness(self):
        """Verify the deepest leaf returns all ancestors."""
        url = (
            f"/v2/editions/{self.editions['subsubc']['edition_id']}/segments/related"
            f"?span_start=0&span_end={LINES_PER_SEGMENT * CHARS_PER_LINE}"
        )
        resp = await self.client.get(url, headers=APPLICATION_HEADER)
        assert resp.status_code == 200
        data = resp.json()

        returned_edition_ids = {group["edition_id"] for group in data}
        expected_ancestors = {"subc", "c1", "t1", "root"}
        expected_ids = {self.editions[name]["edition_id"] for name in expected_ancestors}
        assert expected_ids.issubset(returned_edition_ids), (
            f"Missing ancestors: {expected_ids - returned_edition_ids}"
        )

    async def test_benchmark_diamond_deduplication(self):
        """Verify diamond node doesn't return duplicate editions."""
        url = (
            f"/v2/editions/{self.editions['diamond']['edition_id']}/segments/related"
            f"?span_start=0&span_end={LINES_PER_SEGMENT * CHARS_PER_LINE}"
        )
        resp = await self.client.get(url, headers=APPLICATION_HEADER)
        assert resp.status_code == 200
        data = resp.json()

        edition_ids = [group["edition_id"] for group in data]
        assert len(edition_ids) == len(set(edition_ids)), "Duplicate edition IDs in response"
