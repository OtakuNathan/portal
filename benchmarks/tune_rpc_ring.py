#!/usr/bin/env python3
"""Size the four FF benchmark rings on Linux using sysfs and two measured workloads.

Example:
    python3 benchmarks/tune_rpc_ring.py --binary build/rpc_ring_multiflow_benchmark \
        --cpus 2,3 --out /tmp/rpc-ring-tuning

--max-frame-bytes is a capacity requirement, including wire headers. Timing always
uses the benchmark's four 1-KiB payload streams and a scheduling quantum of 16.
"""
import argparse
import csv
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess

CPU_ROOT = Path("/sys/devices/system/cpu")
STREAMS = 4
QUANTUM = 16
WIRE_BYTES = 1024 + 22
TOLERANCE = 1.05


def cpu_list(text):
    cpus = set()
    for part in text.strip().split(","):
        ends = list(map(int, part.split("-")))
        if len(ends) > 2 or ends[0] < 0 or ends[-1] < ends[0]:
            raise ValueError(f"invalid CPU list: {text}")
        cpus.update(range(ends[0], ends[-1] + 1))
    return cpus


def cache_kib(text):
    text = text.strip().upper()
    if text.endswith(("K", "M", "G")):
        return int(text[:-1]) * {"K": 1, "M": 1024, "G": 1024**2}[text[-1]]
    return int(text) // 1024


def probe(requested):
    allowed = os.sched_getaffinity(0)
    choices = sorted(allowed if requested is None else cpu_list(requested))
    if requested is not None and (len(choices) != 2 or not set(choices) <= allowed):
        raise ValueError("--cpus must select two CPUs from the process affinity mask")
    for cpu in choices:
        root = CPU_ROOT / f"cpu{cpu}"
        siblings = cpu_list((root / "topology/thread_siblings_list").read_text())
        caches = sorted((root / "cache").glob("index*"),
                        key=lambda p: int((p / "level").read_text()), reverse=True)
        for cache in caches:
            if (cache / "type").read_text().strip() not in ("Unified", "Data"):
                continue
            shared = cpu_list((cache / "shared_cpu_list").read_text())
            peers = sorted(set(choices) & shared - siblings)
            if peers:
                return {"cpus": [cpu, peers[0]], "shared_cpus": sorted(shared),
                        "cache_level": int((cache / "level").read_text()),
                        "cache_kib": cache_kib((cache / "size").read_text())}
    raise ValueError("no two allowed physical cores share a data cache in Linux sysfs")


def ceil_power_of_two(value):
    return 1 << (value - 1).bit_length()


def candidates(shared_kib, frame_bytes, ceiling):
    min_ring = ceil_power_of_two((max(WIRE_BYTES, frame_bytes) + 1023) // 1024)
    # Keep the previous default as a control if it fits the caller's budget.
    baseline_ring = max(min_ring, min(1024, ceiling))
    baseline = (baseline_ring, min(256, baseline_ring))
    last = min(ceiling, max(baseline_ring, shared_kib // STREAMS))
    if min_ring > last:
        raise ValueError("the maximum encoded frame does not fit the ring budget")
    # Include the smallest legal ring and capacities around/beyond a producer turn.
    turn_kib = 1 << ((QUANTUM * WIRE_BYTES // 1024).bit_length() - 1)
    rings = {min_ring}
    ring = max(min_ring, turn_kib)
    while ring <= last:
        rings.add(ring)
        ring *= 2
    configs = {baseline}
    for ring in rings:
        for high in (turn_kib, turn_kib * 4, max(1, ring // 4), ring):
            configs.add((ring, min(high, ring)))
    return sorted(configs), baseline


def measure(binary, configs, rounds, seed, path):
    jobs = list(itertools.product(configs, ("balanced", "paused")))
    results = {key: [] for key in jobs}
    rng = random.Random(seed)
    with path.open("w", newline="") as f:
        out = csv.writer(f)
        out.writerow(["round", "ring_kib", "high_water_kib", "scenario", "total_ms",
                      "healthy_done_ms", "healthy_frames_during_pause"])
        for i in range(rounds):
            rng.shuffle(jobs)
            for config, scenario in jobs:
                command = [str(binary), "ff", scenario, str(QUANTUM), *map(str, config)]
                row = subprocess.check_output(command, text=True, timeout=30).strip().split(",")
                if (len(row) != 7 or row[:4] != ["ff", scenario, str(QUANTUM), "64000"]):
                    raise ValueError(f"unexpected benchmark result: {row}")
                total, healthy = map(float, row[4:6])
                if not all(math.isfinite(v) and v > 0 for v in (total, healthy)):
                    raise ValueError(f"invalid benchmark timings: {row}")
                results[config, scenario].append(total if scenario == "balanced" else healthy)
                out.writerow([i + 1, *config, scenario, *row[4:]])
            f.flush()
    return {c: tuple(statistics.median(results[c, s]) for s in ("balanced", "paused"))
            for c in configs}


def select(medians):
    # Score both balanced throughput and progress of healthy streams during a stall.
    fastest = tuple(map(min, zip(*medians.values())))
    scores = {c: max(t / best for t, best in zip(times, fastest))
              for c, times in medians.items()}
    cutoff = min(scores.values()) * TOLERANCE
    return min((c for c in scores if scores[c] <= cutoff),
               key=lambda c: (c[0], scores[c], c[1]))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--cpus", help="two CPUs on distinct physical cores sharing a cache")
    parser.add_argument("--max-frame-bytes", type=int, default=WIRE_BYTES)
    parser.add_argument("--max-ring-kib", type=int, help="per-ring memory ceiling")
    args = parser.parse_args()
    if not args.binary.is_file() or args.max_frame_bytes < 1:
        parser.error("supply the benchmark binary and a positive maximum frame size")
    try:
        hw = probe(args.cpus)
        budget = args.max_ring_kib
        if budget is None:
            budget = max(1024, hw["cache_kib"] // STREAMS,
                         ceil_power_of_two((args.max_frame_bytes + 1023) // 1024))
        if budget < 2:
            raise ValueError("ring budget must be at least 2 KiB")
        configs, baseline = candidates(hw["cache_kib"], args.max_frame_bytes,
                                       1 << (budget.bit_length() - 1))
    except (ValueError, OSError, AttributeError) as exc:
        parser.error(str(exc))
    args.out.mkdir(parents=True, exist_ok=True)
    binary = args.binary.resolve()
    os.sched_setaffinity(0, hw["cpus"])
    subprocess.run([str(binary), "--check"], check=True, timeout=30)
    print(f"L{hw['cache_level']} shared {hw['cache_kib']} KiB; scanning {len(configs)} configurations",
          flush=True)
    sweep = measure(binary, configs, 5, 2753, args.out / "sweep.csv")
    pick = select(sweep)
    confirm = measure(binary, sorted({baseline, pick}), 7, 9371, args.out / "confirmation.csv")
    # Keep the control if the chosen geometry regresses either holdout median by >5%.
    accepted = all(t <= b * TOLERANCE for t, b in zip(confirm[pick], confirm[baseline]))
    ring, high = pick if accepted else baseline
    report = {"host": platform.node(), "machine": platform.machine(), "hardware": hw,
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "streams": STREAMS, "frame_bytes_measured": WIRE_BYTES, "frames_per_turn": QUANTUM,
              "max_frame_bytes_required": args.max_frame_bytes, "per_ring_budget_kib": budget,
              "candidate_count": len(configs), "baseline_kib": baseline, "selected_kib": pick,
              "selected_confirmed": accepted, "recommendation": {"capacity_kib": ring,
              "high_water_kib": high, "low_water_bytes": high * 512},
              "confirmation": [{"config_kib": c, "balanced_total_ms": times[0],
                                "paused_healthy_done_ms": times[1]} for c, times in confirm.items()]}
    (args.out / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
