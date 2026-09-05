#!/usr/bin/env python3
"""Build a research script directory and fixed history pages from public samples.

This trusted-index prototype uses one or two SHA-256 buckets per script, retries a public
salt, and grows the table on overflow. It never drops colliding entries. It is
not a production protocol, authentication mechanism, or wallet discovery engine.
"""
from collections import defaultdict
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import struct

EVENT = struct.Struct("<B3xII32sIQ32s8x")
ENTRY = struct.Struct("<B25sIHII24x")
PAGE = struct.Struct("<B25sIII2x")


def bucket(script, salt, rows):
    return int.from_bytes(hashlib.sha256(salt.to_bytes(4, "little") + script).digest()[:8], "little") % rows


def candidates(script, salt, rows, choices):
    return [bucket(script, salt + i * 65536, rows) for i in range(choices)]


def build(sample, output, inline, row_bytes=3584, choices=1):
    encoded = sample.read_bytes()
    raw = gzip.decompress(encoded) if sample.suffix == ".gz" else encoded
    records = [json.loads(line) for line in raw.splitlines()]
    if records[-1].get("type") != "complete": raise ValueError("incomplete sample")
    events = defaultdict(list)
    for block in records[1:-1]:
        for tx in block["transactions"]:
            for kind, items in [(1, tx["vout"]), (2, tx["vin"])]:
                for item in items:
                    script = bytes.fromhex(item["script"])
                    if not ((len(script) == 25 and script[:3] == b"\x76\xa9\x14" and script[-2:] == b"\x88\xac")
                            or (len(script) == 23 and script[:2] == b"\xa9\x14" and script[-1:] == b"\x87")):
                        continue
                    parent = tx["txid"] if kind == 1 else item["txid"]
                    events[script].append(EVENT.pack(kind, block["height"], tx["index"], bytes.fromhex(parent),
                                                    item["n"], item["value_zat"],
                                                    bytes(32) if kind == 1 else bytes.fromhex(tx["txid"])))
    for script in events: events[script].sort(key=lambda e: (EVENT.unpack(e)[1:3], e))
    entry_bytes = ENTRY.size + inline * EVENT.size
    slots = (row_bytes - 4) // entry_bytes
    rows = 2048
    while True:
        fitted = False
        for salt in range(16):
            buckets = defaultdict(list)
            for script in sorted(events):
                row = min(candidates(script, salt, rows, choices), key=lambda r: len(buckets[r]))
                buckets[row].append(script)
            if max(map(len, buckets.values()), default=0) <= slots:
                fitted = True; break
        if fitted: break
        rows *= 2
        if rows > 131072: raise ValueError("directory exceeds bounded prototype limit")
    capacity = (row_bytes - PAGE.size) // EVENT.size
    pages, locators = [], {}
    for script, history in sorted(events.items()):
        inline_count = min(inline, len(history))
        older = history[:len(history) - inline_count]
        first, count = len(pages), (len(older) + capacity - 1) // capacity
        for index in range(count):
            payload = older[index * capacity:(index + 1) * capacity]
            page = PAGE.pack(len(script), script, index, count, len(payload)) + b"".join(payload)
            pages.append(page.ljust(row_bytes, b"\0"))
        locators[script] = (first, count, inline_count)
    directory = bytearray(rows * row_bytes)
    for row, scripts in buckets.items():
        struct.pack_into("<I", directory, row * row_bytes, len(scripts))
        for slot, script in enumerate(scripts):
            first, count, n = locators[script]
            tail = events[script][-n:] if n else []
            record = ENTRY.pack(len(script), script, len(events[script]), n, first, count) + b"".join(tail)
            offset = row * row_bytes + 4 + slot * entry_bytes
            directory[offset:offset + len(record)] = record
    page_rows = max(2048, ((len(pages) + 2047) // 2048) * 2048)
    page_data = b"".join(pages).ljust(page_rows * row_bytes, b"\0")
    output.mkdir(parents=True, exist_ok=True)
    (output / "directory.bin").write_bytes(directory)
    (output / "pages.bin").write_bytes(page_data)
    ranked = sorted(events, key=lambda s: (len(events[s]), s))
    absent = [b"\x76\xa9\x14" + hashlib.sha256(f"unused-{i}".encode()).digest()[:20] + b"\x88\xac" for i in range(100)]
    if any(s in events for s in absent): raise ValueError("fixture collision")
    mid = records[0]["start_height"] + (records[-1]["blocks"] // 2)
    cases = [("unused_20", absent[:20], None), ("unused_100", absent, None),
             ("small_10", ranked[:10], None), ("median_10", ranked[len(ranked)//2:len(ranked)//2+10], None),
             ("large_1", ranked[-1:], None), ("large_1_cached_prefix", ranked[-1:], mid)]
    workloads = []
    for name, scripts, checkpoint in cases:
        directory_queries = sorted({r for s in scripts for r in candidates(s, salt, rows, choices)})
        page_queries = set()
        expected = []
        for script in scripts:
            if script not in events: continue
            first, count, n = locators[script]
            for page_index in range(count):
                page_events = events[script][page_index * capacity:min((page_index + 1) * capacity, len(events[script]) - n)]
                if checkpoint is None or any(EVENT.unpack(e)[1] >= checkpoint for e in page_events):
                    page_queries.add(first + page_index)
            expected.extend(e for e in events[script] if checkpoint is None or EVENT.unpack(e)[1] >= checkpoint)
        workloads.append({"name": name, "scripts": [s.hex() for s in scripts], "checkpoint_height": checkpoint,
                          "directory_queries": directory_queries, "page_queries": sorted(page_queries),
                          "expected_event_count": len(expected),
                          "expected_events_sha256": hashlib.sha256(b"".join(sorted(expected))).hexdigest()})
    manifest = {"classification": "synthetic script groupings over bounded real chain events; not full restoration",
                "source_sha256": hashlib.sha256(raw).hexdigest(), "inline_capacity": inline,
                "event_bytes": EVENT.size, "entry_bytes": entry_bytes, "row_bytes": row_bytes,
                "directory_rows": rows, "page_rows": page_rows, "used_pages": len(pages),
                "script_count": len(events), "directory_salt": salt, "slots_per_bucket": slots,
                "candidate_buckets_per_script": choices,
                "maximum_bucket_occupancy": max(map(len,buckets.values()), default=0),
                "slot_utilization": len(events) / (rows * slots),
                "events_per_page": capacity, "workloads": workloads,
                "cached_prefix_assumption": "client already has the earlier pages and page-height bounds; locator discovery cost omitted"}
    (output / "layout.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(output, len(events), "scripts", rows, "directory rows", len(pages), "history pages")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--page-sweep", action="store_true", help="K=2 with rounded 4/8/16 KiB targets")
    args = parser.parse_args()
    if args.page_sweep:
        for size in [7168, 10752, 17920]:
            build(args.sample, args.out_dir / f"k2-r{size}", 2, row_bytes=size)
        return
    for inline in [0, 2, 4, 8]: build(args.sample, args.out_dir / f"k{inline}", inline)
    build(args.sample, args.out_dir / "k8-two", 8, choices=2)


if __name__ == "__main__": main()
