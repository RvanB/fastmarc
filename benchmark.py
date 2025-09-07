#!/usr/bin/env python
import argparse
import statistics
import time
from typing import Callable
from collections.abc import Sized

import pymarc
from fastmarc import MARCReader as FastMARCReader


def bench_once(reader_factory: Callable, path: str):
    t0 = time.perf_counter()
    count = 0
    with open(path, "rb") as fp:
        reader = reader_factory(fp)

        if isinstance(reader, Sized):
            count = len(reader)
        else:
            count = sum(1 for _ in reader)
        # else:
        #     for _ in reader:
        #         count += 1
    dt = time.perf_counter() - t0
    return count, dt


def bench_many(reader_factory: Callable, path: str, repeats: int = 3):
    counts = []
    times = []
    for _ in range(repeats):
        c, t = bench_once(reader_factory, path)
        counts.append(c)
        times.append(t)
    assert len(set(counts)) == 1, f"Inconsistent record counts: {counts}"
    return counts[0], times


def bench_iteration_only_fastmarc(path: str, repeats: int = 3):
    """
    Measure only the iteration-seek path after the seek map is already built.
    This highlights the benefit of building the index once and iterating many times.
    """
    with open(path, "rb") as fp:
        fm = FastMARCReader(fp)  # build seek map once
        # Second pass: iteration-only timing
        counts, times = [], []
        for _ in range(repeats):
            t0 = time.perf_counter()
            c = 0
            for _ in fm:
                c += 1
            dt = time.perf_counter() - t0
            counts.append(c)
            times.append(dt)
        assert len(set(counts)) == 1, f"Inconsistent record counts: {counts}"
        return counts[0], times


def main():
    parser = argparse.ArgumentParser(description="Benchmark fastmarc vs pymarc")
    parser.add_argument("marc_file", help="Path to a MARC .mrc file")
    parser.add_argument("--repeats", type=int, default=1, help="Number of runs per method")
    args = parser.parse_args()

    path = args.marc_file
    R = args.repeats

    # 1) pymarc baseline
    def pymarc_reader(fp):
        return pymarc.MARCReader(fp, to_unicode=True, permissive=True, utf8_handling="ignore")

    print("benchmarking pymarc...")
    pymarc_count, pymarc_times = bench_many(pymarc_reader, path, R)

    # 2) fastmarc total (includes index build at construction)
    def fast_reader(fp):
        return FastMARCReader(fp)

    print("benchmarking fastmarc...")
    fast_count, fast_times = bench_many(fast_reader, path, R)

    # 3) fastmarc iteration-only (after map built)
    # fast_iter_count, fast_iter_times = bench_iteration_only_fastmarc(path, R)

    print("\nRecords:", pymarc_count)
    print("\n--- pymarc.MARCReader (total) ---")
    print("runs:", [f"{t:.4f}s" for t in pymarc_times])
    print(f"best: {min(pymarc_times):.4f}s   mean: {statistics.mean(pymarc_times):.4f}s")

    print("\n--- fastmarc.MARCReader (build map + iterate) ---")
    print("runs:", [f"{t:.4f}s" for t in fast_times])
    print(f"best: {min(fast_times):.4f}s   mean: {statistics.mean(fast_times):.4f}s")

    # print("\n--- fastmarc iteration-only (map already built) ---")
    # print("runs:", [f"{t:.4f}s" for t in fast_iter_times])
    # print(f"best: {min(fast_iter_times):.4f}s   mean: {statistics.mean(fast_iter_times):.4f}s")

    # Quick relative speed hints
    base_best = min(pymarc_times)
    fm_best = min(fast_times)
    # fm_iter_best = min(fast_iter_times)
    print("\nSpeedup (fastmarc vs pymarc): x{:.2f}".format(base_best / fm_best if fm_best else float('inf')))
    # print("Speedup (fastmarc iter-only vs pymarc): x{:.2f}".format(base_best / fm_iter_best if fm_iter_best else float('inf')))
    print()


if __name__ == "__main__":
    main()

