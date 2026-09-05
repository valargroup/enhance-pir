#!/usr/bin/env python3
"""Read archived address-transaction indexes through a RocksDB secondary.

Run on archive-vct-off with rocksdict 0.3.28 available. Never opens read/write,
creates primary column families, forces a flush, or changes the node service.
Counts address/transaction associations, not individual receives/spends.
"""
from array import array
from collections import Counter
import datetime
import json
import math
from pathlib import Path
import resource
import sys
import tempfile
import time
import urllib.request

from rocksdict import Rdict, Options, AccessType


def rpc(method, params):
    request = urllib.request.Request("http://127.0.0.1:8232", json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        {"Content-Type": "application/json"})
    response = json.load(urllib.request.urlopen(request, timeout=30))
    if response.get("error"):
        raise RuntimeError("archive RPC failed")
    return response["result"]


def hist_summary(hist):
    count = sum(hist.values())
    result = {"count": count, "sum": sum(k * v for k, v in hist.items()),
              "max": max(hist, default=0), "histogram": sorted(hist.items())}
    for p in [50, 90, 95, 99]:
        target, seen = math.ceil(count * p / 100), 0
        for value, frequency in sorted(hist.items()):
            seen += frequency
            if seen >= target:
                result[f"p{p}"] = value; break
    return result


def main():
    begun = time.monotonic()
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(hard, 65536), hard))
    path = "/mnt/data/zakura-cache/state/v28/mainnet"
    options = Options(raw_mode=True)
    options.create_if_missing(False)
    options.create_missing_column_families(False)
    columns = Rdict.list_cf(path, options)
    with tempfile.TemporaryDirectory(prefix="transparent-pir-secondary-") as secondary:
        db = Rdict(path, options, {c: options for c in columns}, access_type=AccessType.secondary(secondary))
        db.try_catch_up_with_primary()
        hashes = db.get_column_family("hash_by_height")
        it = hashes.iter(); it.seek_to_last()
        anchor, anchor_hash = int.from_bytes(it.key(), "big"), it.value()[::-1].hex()
        if rpc("getblockhash", [anchor]) != anchor_hash:
            raise RuntimeError("secondary anchor not canonical")
        timestamp = rpc("getblock", [str(anchor), 1])["time"]
        cutoffs = {}
        for days in [1, 30, 180]:
            low, high = 0, anchor
            while low < high:
                mid = (low + high) // 2
                if rpc("getblock", [str(mid), 1])["time"] < timestamp - days * 86400:
                    low = mid + 1
                else:
                    high = mid
            # Block timestamps can be nonmonotonic. This is an approximate
            # boundary; exact heights are reported and used for all counts.
            cutoffs[days] = low
        widths = [1, 16, 128, 1024]
        interval_counts = {w: array("I", [0]) * (anchor // w + 1) for w in widths}
        hist, suffix_hist = Counter(), {d: Counter() for d in cutoffs}
        last_address, count, suffix, last_intervals = None, 0, {}, {}
        scanned, associations = 0, 0

        def finish_address():
            if count:
                hist[count] += 1
                for d in cutoffs:
                    suffix_hist[d][suffix.get(d, 0)] += 1

        cf = db.get_column_family("tx_loc_by_transparent_addr_loc")
        it = cf.iter(); it.seek_to_first()
        while it.valid():
            key = it.key()
            if len(key) != 13 or it.value() != b"":
                raise RuntimeError("unexpected v28 address-transaction encoding")
            address, height = key[:8], int.from_bytes(key[8:11], "big")
            if address != last_address:
                finish_address()
                last_address, count, suffix, last_intervals = address, 0, {}, {}
            if height <= anchor:
                associations += 1; count += 1
                for d, cutoff in cutoffs.items():
                    suffix[d] = suffix.get(d, 0) + int(height >= cutoff)
                for width in widths:
                    interval = height // width
                    if last_intervals.get(width) != interval:
                        interval_counts[width][interval] += 1
                        last_intervals[width] = interval
            scanned += 1
            if scanned % 1000000 == 0:
                print(f"census: {scanned} address/transaction associations read", file=sys.stderr, flush=True)
            it.next()
        it.status()
        finish_address()
        if rpc("getblockhash", [anchor]) != anchor_hash:
            raise RuntimeError("anchor changed during census")
        result = {"classification": "complete scan of archived supported-address transaction index; not event history",
                  "collected_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  "database_format": 28, "database_access": "read-only secondary",
                  "anchor_height": anchor, "anchor_hash": anchor_hash,
                  "anchor_time": timestamp, "rpc_node_version": rpc("getnetworkinfo", []).get("subversion"),
                  "historically_indexed_addresses": sum(hist.values()),
                  "address_transaction_associations": associations,
                  "transactions_per_address": hist_summary(hist),
                  "recent_activity": {str(d): {"start_height": cutoffs[d],
                      "date_boundary": "approximate timestamp search; exact height is authoritative",
                      "active_addresses": sum(v for k, v in suffix_hist[d].items() if k),
                      "transactions_per_historical_address": hist_summary(suffix_hist[d])} for d in cutoffs},
                  "distinct_addresses_per_interval": {str(w): hist_summary(Counter(interval_counts[w])) for w in widths},
                  "elapsed_seconds": time.monotonic() - begun,
                  "limitations": ["transaction associations may contain several receive/spend events",
                                  "address identity is the index's first output location; unsupported scripts excluded",
                                  "secondary persisted tip can lag live RPC tip",
                                  "date cutoffs approximate due to nonmonotonic block timestamps"]}
        print(json.dumps(result, indent=2), flush=True)
        db.close()


if __name__ == "__main__": main()
