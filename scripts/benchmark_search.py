#!/usr/bin/env python3
"""Benchmark and compare search performance between LanceDB and Meilisearch."""

import sys
import time
import statistics
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.storage.lancedb_client import LanceDBStore
from src.storage.meilisearch_client import MeiliSearchStore


def run_benchmark(iterations: int = 50):
    print("=" * 80)
    print("       SEARCH ENGINE PERFORMANCE BENCHMARK: LanceDB vs Meilisearch")
    print("=" * 80)

    lance = LanceDBStore()
    meili = MeiliSearchStore()

    if not meili.is_healthy():
        print("[ERROR] Meilisearch is not running. Start with `./scripts/meilisearch_server.sh start`")
        sys.exit(1)

    lance_count = len(lance.table) if len(lance.table) > 0 else 0
    meili_count = meili.get_stats().get("total_tweets", 0)

    print(f"📊 Dataset Size: LanceDB = {lance_count} documents | Meilisearch = {meili_count} documents")
    print(f"🔄 Benchmark Iterations per Query: {iterations}")
    print("-" * 80)

    test_cases = [
        {"category": "Exact Keyword", "q": "python", "tag": None, "author": None, "source": "all"},
        {"category": "Exact Keyword", "q": "ai", "tag": None, "author": None, "source": "all"},
        {"category": "Multi-Word", "q": "machine learning", "tag": None, "author": None, "source": "all"},
        {"category": "Multi-Word", "q": "agentic workflow", "tag": None, "author": None, "source": "all"},
        {"category": "Prefix / Partial", "q": "pyth", "tag": None, "author": None, "source": "all"},
        {"category": "Prefix / Partial", "q": "agen", "tag": None, "author": None, "source": "all"},
        {"category": "Typo / Fuzzy", "q": "pythn", "tag": None, "author": None, "source": "all"},
        {"category": "Tag Filter", "q": "", "tag": "AI", "author": None, "source": "all"},
        {"category": "Tag Filter", "q": "", "tag": "Tech", "author": None, "source": "all"},
        {"category": "Query + Tag", "q": "code", "tag": "AI", "author": None, "source": "all"},
        {"category": "Source Filter", "q": "python", "tag": None, "author": None, "source": "bookmark"},
    ]

    # Warmup
    for tc in test_cases[:3]:
        lance.search_hybrid(query=tc["q"], tag=tc["tag"], source=tc["source"], limit=24)
        meili.search(query=tc["q"], tag=tc["tag"], source=tc["source"], limit=24)

    results_table = []
    lance_all_latencies = []
    meili_all_latencies = []

    for tc in test_cases:
        cat = tc["category"]
        q_label = f"q='{tc['q']}'" if tc["q"] else ""
        if tc["tag"]:
            q_label += f" tag='{tc['tag']}'"
        if tc["source"] != "all":
            q_label += f" src='{tc['source']}'"

        # Benchmark LanceDB
        lance_times = []
        for _ in range(iterations):
            t0 = time.perf_counter()
            l_hits = lance.search_hybrid(query=tc["q"], tag=tc["tag"], source=tc["source"], limit=24)
            lance_times.append((time.perf_counter() - t0) * 1000.0)

        # Benchmark Meilisearch
        meili_times = []
        for _ in range(iterations):
            t0 = time.perf_counter()
            m_hits = meili.search(query=tc["q"], tag=tc["tag"], source=tc["source"], limit=24)
            meili_times.append((time.perf_counter() - t0) * 1000.0)

        lance_all_latencies.extend(lance_times)
        meili_all_latencies.extend(meili_times)

        l_p50 = statistics.median(lance_times)
        l_p95 = statistics.quantiles(lance_times, n=20)[18] if len(lance_times) >= 20 else max(lance_times)
        m_p50 = statistics.median(meili_times)
        m_p95 = statistics.quantiles(meili_times, n=20)[18] if len(meili_times) >= 20 else max(meili_times)

        results_table.append({
            "category": cat,
            "query": q_label,
            "lance_p50": l_p50,
            "lance_p95": l_p95,
            "lance_hits": len(l_hits),
            "meili_p50": m_p50,
            "meili_p95": m_p95,
            "meili_hits": len(m_hits),
            "ratio": l_p50 / m_p50 if m_p50 > 0 else 1.0,
        })

    # Print Table
    header = f"{'Category':<16} | {'Query / Filter':<26} | {'Lance p50':<10} | {'Meili p50':<10} | {'Lance p95':<10} | {'Meili p95':<10} | {'Speedup':<8}"
    print(header)
    print("-" * len(header))

    for r in results_table:
        speedup_str = f"{r['ratio']:.1f}x" if r['ratio'] >= 1.0 else f"{1.0/r['ratio']:.1f}x slower"
        print(f"{r['category']:<16} | {r['query']:<26} | {r['lance_p50']:>7.2f} ms | {r['meili_p50']:>7.2f} ms | {r['lance_p95']:>7.2f} ms | {r['meili_p95']:>7.2f} ms | {speedup_str:<8}")

    print("=" * 80)
    
    # Overall summary statistics
    l_overall_p50 = statistics.median(lance_all_latencies)
    l_overall_p95 = statistics.quantiles(lance_all_latencies, n=20)[18]
    l_overall_p99 = statistics.quantiles(lance_all_latencies, n=100)[98]
    l_qps = 1000.0 / statistics.mean(lance_all_latencies)

    m_overall_p50 = statistics.median(meili_all_latencies)
    m_overall_p95 = statistics.quantiles(meili_all_latencies, n=20)[18]
    m_overall_p99 = statistics.quantiles(meili_all_latencies, n=100)[98]
    m_qps = 1000.0 / statistics.mean(meili_all_latencies)

    print("📈 OVERALL AGGREGATE SUMMARY:")
    print(f"  • LanceDB     : p50 = {l_overall_p50:.2f} ms | p95 = {l_overall_p95:.2f} ms | p99 = {l_overall_p99:.2f} ms | QPS ≈ {l_qps:.0f} req/s")
    print(f"  • Meilisearch : p50 = {m_overall_p50:.2f} ms | p95 = {m_overall_p95:.2f} ms | p99 = {m_overall_p99:.2f} ms | QPS ≈ {m_qps:.0f} req/s")
    
    overall_speedup = l_overall_p50 / m_overall_p50 if m_overall_p50 > 0 else 1.0
    print(f"  • Meilisearch vs LanceDB median speedup: {overall_speedup:.2f}x faster on text search")
    print("=" * 80)


if __name__ == "__main__":
    iters = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    run_benchmark(iterations=iters)
