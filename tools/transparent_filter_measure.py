#!/usr/bin/env python3
"""Compare Bloom and BIP 158 transparent activity filters on the mainnet day.

Three comparisons are kept separate, because conflating them would attribute a
coverage change to compression:

  1. bloom-supported   the existing experimental Bloom filters over the
                       supported-script set, full-interval delivery
  2. bip158-complete   BIP 158 over the complete script set, full-interval
                       delivery
  3. bloom-complete    Bloom over the IDENTICAL complete script set

(1) vs (2) mixes two changes: broader coverage and a different encoding.
(3) vs (2) isolates the encoding. (1) vs (3) isolates the coverage. All three
are reported; none is presented as "the" result.

Checkpoint-scoped delivery is measured by the interval sweep: a suffix of N
blocks is exactly what a wallet fetches when its checkpoint is N blocks behind.

Bandwidth superiority is an outcome to report, not a target. Where the new
filters are larger, that is stated.
"""

import argparse
import collections
import gzip
import json
import math
import pathlib
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from transparent_pir_sample import make_filter, supported  # noqa: E402

BLOOM_PROBABILITY = 1e-6
PROFILE_NAME = "zcash-transparent-basic-v1"

# Envelope framing, from docs/transparent_filter_envelope.md. Computed rather
# than measured so the accounting is auditable against the specification.
ENVELOPE_HEADER = 4 + 2 + 32 + 1 + len(PROFILE_NAME) + 8 + 32


def compact_size(value):
    if value <= 0xFC:
        return 1
    if value <= 0xFFFF:
        return 3
    if value <= 0xFFFFFFFF:
        return 5
    return 9


def envelope_bytes(filter_lengths):
    """Total serialized batch bytes for a run of filters, batches included."""
    total = 0
    for start in range(0, len(filter_lengths), 1000):
        batch = filter_lengths[start:start + 1000]
        total += ENVELOPE_HEADER + compact_size(len(batch))
        total += sum(8 + 32 + compact_size(n) + n for n in batch)
    return total


def load_blocks(path):
    opener = gzip.open if str(path).endswith(".gz") else open
    blocks = []
    with opener(path, "rt") as handle:
        for line in handle:
            record = json.loads(line)
            if record.get("type") == "block":
                blocks.append(record)
    return blocks


def complete_scripts(block):
    """Every filter element: all scripts, minus leading-OP_RETURN and empty."""
    elements = set()
    for tx in block["transactions"]:
        for item in tx["vout"] + tx["vin"]:
            script = item["script"]
            if script and not script.startswith("6a"):
                elements.add(script)
    return elements


def supported_scripts(block):
    """The narrower set the existing Bloom filters were built from."""
    return {s for s in complete_scripts(block) if supported(s)}


def bloom_filters(blocks, extractor, network="main"):
    """Bloom filters, one per block, using the existing seed derivation."""
    import hashlib
    import struct

    out = []
    for block in blocks:
        scripts = {bytes.fromhex(s) for s in extractor(block)}
        block_hash = bytes.fromhex(block["hash"])
        seed = hashlib.sha256(
            b"transparent-incremental-filter-v1\0" + network.encode() + block_hash
        ).digest()
        bits, hashes, _ = make_filter(scripts, seed, BLOOM_PROBABILITY)
        out.append({"bits": bits, "hashes": hashes, "seed": seed})
    return out


def bloom_matches(entry, scripts):
    import hashlib
    import struct

    bits, hashes, seed = entry["bits"], entry["hashes"], entry["seed"]
    size = len(bits)
    matched = []
    for script in scripts:
        key = bytes.fromhex(script)
        positions = (
            int.from_bytes(
                hashlib.sha256(seed + struct.pack(">I", i) + key).digest(), "big"
            )
            % (size * 8)
            for i in range(hashes)
        )
        if all(bits[p // 8] & (1 << (p % 8)) for p in positions):
            matched.append(script)
    return matched


def bip158_build(blocks, cli):
    requests = "".join(
        json.dumps({"block_hash_display": b["hash"],
                    "elements": sorted(complete_scripts(b))}) + "\n"
        for b in blocks
    )
    started = time.perf_counter()
    done = subprocess.run([str(cli), "batch-build"], input=requests,
                          capture_output=True, text=True, check=False)
    if done.returncode != 0:
        sys.exit(f"batch-build failed: {done.stderr.strip()}")
    elapsed = time.perf_counter() - started
    results = [json.loads(l) for l in done.stdout.splitlines() if l.strip()]
    return results, elapsed


def bip158_match(blocks, filters, scripts, cli):
    """Match one script set against every filter, in one invocation."""
    ordered = sorted(scripts)
    if not ordered:
        return [[] for _ in blocks], 0.0
    requests = "".join(
        json.dumps({"block_hash_display": b["hash"], "filter": f["filter"],
                    "scripts": ordered}) + "\n"
        for b, f in zip(blocks, filters)
    )
    started = time.perf_counter()
    done = subprocess.run([str(cli), "batch-match"], input=requests,
                          capture_output=True, text=True, check=False)
    if done.returncode != 0:
        sys.exit(f"batch-match failed: {done.stderr.strip()}")
    elapsed = time.perf_counter() - started
    out = []
    for line in done.stdout.splitlines():
        if not line.strip():
            continue
        out.append([ordered[i] for i in json.loads(line)["indices"]])
    return out, elapsed


def truth(blocks):
    """Scripts genuinely active in each block, from the sample itself."""
    return [complete_scripts(b) for b in blocks]


def build_profiles(blocks, activity):
    """Reproducible script sets. Selection is by sorted order, never by which
    scripts happen to produce favourable match counts."""
    by_count = collections.defaultdict(list)
    for script, count in activity.items():
        by_count[count].append(script)
    for scripts in by_count.values():
        scripts.sort()

    two = by_count.get(2, [])
    busiest = max(activity.items(), key=lambda kv: (kv[1], kv[0]))[0]
    # "Unchanged" scripts: deterministic, absent from the sample entirely, so
    # every match against them is a false positive by construction.
    absent = [("%040x" % i) for i in range(2000)]
    absent = ["76a914" + a[:40] + "88ac" for a in absent]
    absent = [s for s in absent if s not in activity][:1000]

    profiles = {
        "100-unchanged": absent[:100],
        "one-two-activity": two[:1],
        "ten-two-activity": two[:10],
        "busiest-address": [busiest],
        "mix-100-mostly-inactive": absent[:97] + two[:3],
        "mix-1000-mostly-inactive": absent[:995] + two[:5],
    }
    return profiles, busiest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=pathlib.Path, required=True)
    parser.add_argument("--baseline", type=pathlib.Path, required=True)
    parser.add_argument("--cli", type=pathlib.Path, required=True)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    args = parser.parse_args()

    blocks = load_blocks(args.sample)
    baseline = json.loads(args.baseline.read_text())
    per_height = dict(zip(
        range(baseline["start"], baseline["end"] + 1), baseline["bytes_per_height"]))
    if len(blocks) != len(baseline["bytes_per_height"]):
        sys.exit("sample and baseline cover different intervals")

    activity = collections.Counter()
    for block in blocks:
        for script in complete_scripts(block):
            activity[script] += 1

    profiles, busiest = build_profiles(blocks, activity)
    present = truth(blocks)

    # Whether the coverage change is even observable on this sample. If every
    # script here is one the old filters already supported, then the whole
    # Bloom-to-BIP158 difference is encoding, and any claim of a coverage
    # benefit would have to come from a different interval.
    unsupported = sorted(s for s in activity if not supported(s))

    # Build all three filter sets once over the whole day.
    bip_all, bip_build_seconds = bip158_build(blocks, args.cli)
    started = time.perf_counter()
    bloom_supported_all = bloom_filters(blocks, supported_scripts)
    bloom_supported_seconds = time.perf_counter() - started
    started = time.perf_counter()
    bloom_complete_all = bloom_filters(blocks, complete_scripts)
    bloom_complete_seconds = time.perf_counter() - started

    intervals = {
        "full-day-1152": 1152,
        "half-day-576": 576,
        "suffix-288": 288,
        "suffix-116": 116,
        "suffix-12": 12,
    }

    report = {
        "classification": "bounded single-day mainnet sample; not a wallet-population average",
        "sample": str(args.sample),
        "blocks": len(blocks),
        "start_height": blocks[0]["height"],
        "end_height": blocks[-1]["height"],
        "bloom_probability": BLOOM_PROBABILITY,
        "bip158": {"P": 19, "M": 784931, "profile": PROFILE_NAME},
        "distinct_active_scripts": len(activity),
        "scripts_outside_the_supported_set": len(unsupported),
        "coverage_difference_observable_here": bool(unsupported),
        "coverage_note": (
            "Every distinct script in this day is P2PKH or P2SH, so the "
            "supported-script set and the complete script set coincide and the "
            "measured size difference is entirely encoding. Broader coverage "
            "still changes what the filter can report on intervals that do "
            "contain other script forms; this sample cannot quantify that."
            if not unsupported else
            "Some scripts fall outside the supported set, so coverage and "
            "encoding both contribute and are reported separately."
        ),
        "busiest_script_activity": activity[busiest],
        "build_seconds": {
            "bip158_complete": round(bip_build_seconds, 3),
            "bloom_supported": round(bloom_supported_seconds, 3),
            "bloom_complete": round(bloom_complete_seconds, 3),
        },
        "encoding_and_coverage": {},
        "profiles": {},
    }

    # --- filter size, by interval and construction ---------------------------
    for name, length in intervals.items():
        window = slice(len(blocks) - length, len(blocks))
        bip_lengths = [f["bytes"] for f in bip_all[window]]
        bs_lengths = [len(f["bits"]) for f in bloom_supported_all[window]]
        bc_lengths = [len(f["bits"]) for f in bloom_complete_all[window]]
        heights = [b["height"] for b in blocks[window]]
        report["encoding_and_coverage"][name] = {
            "blocks": length,
            "height_range": [heights[0], heights[-1]],
            "elements_complete": sum(f["elements"] for f in bip_all[window]),
            "raw_filter_bytes": {
                "bip158_complete": sum(bip_lengths),
                "bloom_supported": sum(bs_lengths),
                "bloom_complete": sum(bc_lengths),
            },
            "delivered_envelope_bytes": {
                "bip158_complete": envelope_bytes(bip_lengths),
                "bloom_complete": envelope_bytes(bc_lengths),
                "bloom_supported": envelope_bytes(bs_lengths),
            },
            "ordinary_retrieval_bytes": sum(per_height[h] for h in heights),
        }

    # --- per-profile matching -------------------------------------------------
    for profile_name, scripts in profiles.items():
        scripts = list(scripts)
        entry = {"script_count": len(scripts), "intervals": {}}
        for name, length in intervals.items():
            window = slice(len(blocks) - length, len(blocks))
            sub_blocks = blocks[window]
            sub_bip = bip_all[window]
            sub_present = present[window]

            matches, match_seconds = bip158_match(sub_blocks, sub_bip, scripts, args.cli)
            bip_true = bip_false = 0
            matched_blocks = 0
            for got, real in zip(matches, sub_present):
                if got:
                    matched_blocks += 1
                for script in got:
                    if script in real:
                        bip_true += 1
                    else:
                        bip_false += 1
            real_blocks = sum(
                1 for real in sub_present if any(s in real for s in scripts))

            bloom_true = bloom_false = bloom_blocks = 0
            for f, real in zip(bloom_complete_all[window], sub_present):
                got = bloom_matches(f, scripts)
                if got:
                    bloom_blocks += 1
                for script in got:
                    if script in real:
                        bloom_true += 1
                    else:
                        bloom_false += 1

            bip_lengths = [f["bytes"] for f in sub_bip]
            entry["intervals"][name] = {
                "blocks_with_real_activity": real_blocks,
                "bip158": {
                    "matched_blocks": matched_blocks,
                    "true_positive_script_hits": bip_true,
                    "false_positive_script_hits": bip_false,
                    # A wallet coalesces repeated matches per script, so extra
                    # private work is counted per matched block, not per hit.
                    "extra_private_block_requests": matched_blocks - real_blocks,
                    "match_seconds": round(match_seconds, 3),
                },
                "bloom_complete": {
                    "matched_blocks": bloom_blocks,
                    "true_positive_script_hits": bloom_true,
                    "false_positive_script_hits": bloom_false,
                    "extra_private_block_requests": bloom_blocks - real_blocks,
                },
                "delivered_envelope_bytes_bip158": envelope_bytes(bip_lengths),
            }
        report["profiles"][profile_name] = entry

    # --- false-positive rate over a large deterministic absent set ------------
    absent = ["76a914" + ("%040x" % i)[:40] + "88ac" for i in range(5000, 15000)]
    absent = [s for s in absent if s not in activity]
    window = slice(len(blocks) - 12, len(blocks))
    matches, _ = bip158_match(blocks[window], bip_all[window], absent, args.cli)
    hits = sum(len(m) for m in matches)
    tests = len(absent) * 12
    report["false_positive_probe"] = {
        "classification": "deterministic absent scripts; a rate, not a proof of any bound",
        "scripts": len(absent),
        "blocks": 12,
        "script_filter_tests": tests,
        "matches": hits,
        "observed_rate": hits / tests if tests else None,
        "expected_rate_1_over_M": 1 / 784931,
        "coalesced_private_block_requests": sum(1 for m in matches if m),
    }

    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report["encoding_and_coverage"], indent=2, sort_keys=True))
    print(json.dumps(report["false_positive_probe"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
