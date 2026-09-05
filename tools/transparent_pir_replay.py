#!/usr/bin/env python3
"""Run serialized PIR against row files and verify synthetic wallet event replay."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import statistics
import struct
import subprocess
import time

from transparent_pir_layout import ENTRY, EVENT, PAGE, candidates


def benchmark(binary, folder, table, row_bytes, rows, query_rows, out):
    if not query_rows: return None
    command = [str(binary), str(folder / f"{table}.bin"), str(row_bytes), str(rows),
               ",".join(map(str, query_rows)), str(max(32, len(query_rows)))]
    with (out / f"{table}-raw.json").open("w") as output:
        subprocess.run(command, stdout=output, check=True, timeout=1800)
    return json.loads((out / f"{table}-raw.json").read_text())


def replay(layout, workload, directory, pages):
    recovered = []
    for script_hex in workload["scripts"]:
        script = bytes.fromhex(script_hex)
        matches = []
        for row in set(candidates(script, layout["directory_salt"], layout["directory_rows"], layout.get("candidate_buckets_per_script", 1))):
            raw = directory[row]
            count = struct.unpack_from("<I", raw)[0]
            if count > layout["slots_per_bucket"]: raise ValueError("bucket overflow")
            for slot in range(count):
                offset = 4 + slot * layout["entry_bytes"]
                length, key, total, n, first, page_count = ENTRY.unpack_from(raw, offset)
                if key[:length] == script:
                    if n > layout["inline_capacity"] or n > total: raise ValueError("invalid inline count")
                    matches.append((total, n, first, page_count, raw[offset + ENTRY.size:offset + ENTRY.size + n * EVENT.size]))
        if len(matches) > 1: raise ValueError("duplicate directory identity")
        if not matches: continue
        total, n, first, count, inline = matches[0]
        events = [inline[i:i + EVENT.size] for i in range(0, len(inline), EVENT.size)]
        for index in range(count):
            page_id = first + index
            if page_id not in workload["page_queries"]:
                if workload["checkpoint_height"] is None: raise ValueError("missing uncached page")
                continue
            raw = pages[page_id]
            length, key, actual_index, actual_count, size = PAGE.unpack_from(raw)
            if key[:length] != script or actual_index != index or actual_count != count or size > layout["events_per_page"]:
                raise ValueError("page identity/count mismatch")
            events.extend(raw[PAGE.size + i * EVENT.size:PAGE.size + (i + 1) * EVENT.size] for i in range(size))
        if workload["checkpoint_height"] is None and len(events) != total: raise ValueError("incomplete history")
        recovered.extend(e for e in events if workload["checkpoint_height"] is None or EVENT.unpack(e)[1] >= workload["checkpoint_height"])
    if len(recovered) != workload["expected_event_count"] or hashlib.sha256(b"".join(sorted(recovered))).hexdigest() != workload["expected_events_sha256"]:
        raise ValueError("replayed event multiset differs from chain fixture")
    return len(recovered)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--layouts", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    for folder in sorted(args.layouts.iterdir()):
        if not (folder / "layout.json").exists(): continue
        layout = json.loads((folder / "layout.json").read_text())
        out = args.output / folder.name; out.mkdir(exist_ok=True)
        queries = {kind: sorted({r for w in layout["workloads"] for r in w[f"{kind}_queries"]}) for kind in ["directory", "page"]}
        directory = benchmark(args.binary, folder, "directory", layout["row_bytes"], layout["directory_rows"], queries["directory"], out)
        pages = benchmark(args.binary, folder, "pages", layout["row_bytes"], layout["page_rows"], queries["page"], out)
        decoded_directory = {r["row"]: bytes(r["bytes"]) for r in directory["decoded_rows"]}
        decoded_pages = {r["row"]: bytes(r["bytes"]) for r in pages["decoded_rows"]} if pages else {}
        results = []
        for workload in layout["workloads"]:
            count = replay(layout, workload, decoded_directory, decoded_pages)
            byte_count, core_ms = 0, 0
            for kind, run in [("directory", directory), ("page", pages)]:
                by_row = {s["row"]: s for s in run["samples"]} if run else {}
                for row in workload[f"{kind}_queries"]:
                    sample = by_row[row]
                    byte_count += sample["upload_bytes"] + sample["response_bytes"]
                    core_ms += sample["total_ms"]
            results.append({"name": workload["name"], "directory_queries": len(workload["directory_queries"]),
                            "page_queries": len(workload["page_queries"]), "verified_events": count,
                            "query_response_bytes": byte_count, "summed_measured_serial_core_ms": core_ms,
                            "cold_setup_download_bytes": directory["published_setup_bytes"] + (pages["published_setup_bytes"] if workload["page_queries"] else 0)})
        for run in [directory, pages]:
            if run: run.pop("decoded_rows")
        report = {"layout": layout, "machine": platform.platform(), "cpu_count": __import__('os').cpu_count(),
                  "classification": "real PIR decoded event replay for synthetic script sets; no HTTP, wallet UI, key derivation or full restoration",
                  "directory": directory, "pages": pages, "workloads": results}
        (out / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        print(folder.name, "verified", len(results), "workloads", flush=True)


if __name__ == "__main__": main()
