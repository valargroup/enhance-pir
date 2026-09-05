#!/usr/bin/env python3
"""Check that a BIP 158 results file is internally consistent.

A summary is only useful if the numbers in it agree with each other. This
re-derives what it can and fails loudly rather than reporting a pleasant
number that nothing supports.
"""

import argparse
import json
import pathlib
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=pathlib.Path, required=True)
    args = parser.parse_args()
    report = json.loads(args.results.read_text())
    problems = []

    def check(condition, message):
        if not condition:
            problems.append(message)

    check(report["blocks"] == 1152, "expected the 1,152-block mainnet day")
    check(report["end_height"] - report["start_height"] + 1 == report["blocks"],
          "height range and block count disagree")

    intervals = report["encoding_and_coverage"]
    check(intervals["full-day-1152"]["ordinary_retrieval_bytes"] == 2888097,
          "full-day ordinary retrieval does not match the published baseline")
    check(intervals["suffix-12"]["ordinary_retrieval_bytes"] == 9492,
          "twelve-block ordinary retrieval does not match the published baseline")

    for name, entry in intervals.items():
        raw, delivered = entry["raw_filter_bytes"], entry["delivered_envelope_bytes"]
        for key in raw:
            check(delivered[key] > raw[key],
                  f"{name}/{key}: delivered bytes must exceed raw filter bytes")
        # The coverage claim in the prose depends on this actually holding.
        if report["scripts_outside_the_supported_set"] == 0:
            check(raw["bloom_complete"] == raw["bloom_supported"],
                  f"{name}: no unsupported scripts, so both Bloom sets must agree")

    for profile, entry in report["profiles"].items():
        for name, result in entry["intervals"].items():
            real = result["blocks_with_real_activity"]
            for encoding in ("bip158", "bloom_complete"):
                matched = result[encoding]["matched_blocks"]
                check(matched >= real,
                      f"{profile}/{name}/{encoding}: a filter missed real activity")
                check(result[encoding]["extra_private_block_requests"] == matched - real,
                      f"{profile}/{name}/{encoding}: extra requests miscounted")

    probe = report["false_positive_probe"]
    check(probe["script_filter_tests"] == probe["scripts"] * probe["blocks"],
          "false-positive probe test count is inconsistent")
    if probe["matches"] == 0:
        check("not a proof" in probe["classification"] or "rate, not a proof" in probe["classification"],
              "a zero-match probe must not be labelled as proving a bound")

    if problems:
        for problem in problems:
            print(f"FAIL: {problem}", file=sys.stderr)
        sys.exit(1)
    print(f"ok: {args.results} is internally consistent")


if __name__ == "__main__":
    main()
