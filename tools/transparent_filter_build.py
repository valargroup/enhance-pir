#!/usr/bin/env python3
"""Build BIP 158 filters for a collected sample, via the Rust CLI.

Reads a `transparent_pir_collect.py` sample and emits one JSON object per block
with the block's filter. Element extraction follows the profile in
`docs/transparent_bip158_implementation_handoff.md` section 1:

  * every nonempty output script except one whose FIRST byte is 0x6a;
  * the nonempty previous-output locking script of every non-coinbase input
    (the collector already resolved these, and already dropped coinbase inputs);
  * deduplicated by raw bytes across the whole block.

The Rust CLI is invoked ONCE for the whole run, not once per block or script:
process-per-item cost would swamp the measurement it feeds.
"""

import argparse
import gzip
import json
import pathlib
import subprocess
import sys


def open_sample(path):
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)


def block_elements(block):
    """Raw script hex for one block, deduplicated, in sorted order."""
    elements = set()
    for transaction in block["transactions"]:
        for out in transaction["vout"]:
            script = out["script"]
            # Exclude only a LEADING OP_RETURN. A 0x6a later in the script is
            # ordinary data and the script stays.
            if script and not script.startswith("6a"):
                elements.add(script)
        for vin in transaction["vin"]:
            script = vin["script"]
            if script and not script.startswith("6a"):
                elements.add(script)
    return sorted(elements)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=pathlib.Path, required=True)
    parser.add_argument("--cli", type=pathlib.Path, required=True,
                        help="transparent-filter-cli binary")
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--limit", type=int, default=0,
                        help="stop after this many blocks (0 = all)")
    args = parser.parse_args()

    blocks = []
    for record in open_sample(args.sample):
        if record.get("type") != "block":
            continue
        blocks.append(record)
        if args.limit and len(blocks) >= args.limit:
            break

    requests = "".join(
        json.dumps({"block_hash_display": block["hash"],
                    "elements": block_elements(block)}) + "\n"
        for block in blocks
    )
    completed = subprocess.run(
        [str(args.cli), "batch-build"],
        input=requests, capture_output=True, text=True, check=False,
    )
    if completed.returncode != 0:
        sys.exit(f"batch-build failed: {completed.stderr.strip()}")
    results = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    if len(results) != len(blocks):
        sys.exit(f"expected {len(blocks)} filters, got {len(results)}")

    with args.out.open("w") as handle:
        for block, result in zip(blocks, results):
            handle.write(json.dumps({
                "height": block["height"],
                "hash": block["hash"],
                "filter": result["filter"],
                "filter_hash": result["filter_hash"],
                "bytes": result["bytes"],
                "elements": result["elements"],
            }) + "\n")
    total = sum(result["bytes"] for result in results)
    print(f"{len(blocks)} filters, {total} filter bytes, "
          f"{sum(r['elements'] for r in results)} elements", file=sys.stderr)


if __name__ == "__main__":
    main()
