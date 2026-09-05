#!/usr/bin/env python3
"""Measure a collected sample's transparent protobuf and experimental filters.

Requires protoc and protobuf==6.33.5. No wallet secrets or network access.
The compact messages are reconstructed payloads, not captured gRPC traffic.
"""

import argparse
from collections import Counter
import gzip
import hashlib
import json
import math
from pathlib import Path
import platform
import struct
import subprocess
import tempfile
import zlib



def supported(script):
    raw = bytes.fromhex(script)
    return ((len(raw) == 25 and raw.startswith(b"\x76\xa9\x14") and raw.endswith(b"\x88\xac"))
            or (len(raw) == 23 and raw.startswith(b"\xa9\x14") and raw.endswith(b"\x87")))


def distribution(values):
    values = sorted(values)
    if not values:
        return {"count": 0}
    return {"count": len(values), "sum": sum(values),
            **{f"p{p}": values[math.ceil(p / 100 * len(values)) - 1]
               for p in [50, 90, 95, 99]}, "max": values[-1]}


def compact_class(proto):
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
    with tempfile.TemporaryDirectory() as directory:
        descriptor = Path(directory) / "compact.pb"
        subprocess.run(["protoc", f"-I{proto.parent}", f"--descriptor_set_out={descriptor}",
                        proto.name], check=True)
        files = descriptor_pb2.FileDescriptorSet.FromString(descriptor.read_bytes())
    pool = descriptor_pool.DescriptorPool()
    for file in files.file:
        pool.Add(file)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("cash.z.wallet.sdk.rpc.CompactBlock"))


def make_filter(scripts, seed, probability):
    # Experimental Bloom encoding only. SHA-256 is provided by hashlib; this is
    # not a PIR construction or a proposed authenticated chain filter protocol.
    byte_count = max(1, math.ceil(-len(scripts) * math.log(probability) / math.log(2)**2 / 8))
    bit_count = byte_count * 8
    hashes = max(1, round(bit_count / max(1, len(scripts)) * math.log(2)))
    bits = bytearray(byte_count)

    def positions(script):
        for i in range(hashes):
            yield int.from_bytes(hashlib.sha256(seed + struct.pack(">I", i) + script).digest(), "big") % bit_count

    for script in scripts:
        for position in positions(script):
            bits[position // 8] |= 1 << (position % 8)

    def contains(script):
        return all(bits[p // 8] & (1 << (p % 8)) for p in positions(script))

    if not all(contains(script) for script in scripts):
        raise ValueError("filter false negative")
    return bytes(bits), hashes, contains


def evaluate(sample, proto):
    import google.protobuf
    encoded = sample.read_bytes()
    raw = gzip.decompress(encoded) if sample.suffix == ".gz" else encoded
    records = [json.loads(line) for line in raw.splitlines()]
    manifest, completion = records[0], records[-1]
    if manifest.get("type") != "manifest" or completion.get("type") != "complete":
        raise ValueError("collection incomplete")
    blocks = records[1:-1]
    expected = manifest["anchor_height"] - manifest["start_height"] + 1
    if len(blocks) != expected or completion["blocks"] != expected or completion["unresolved_prevouts"]:
        raise ValueError("inconsistent coverage or unresolved prevouts")
    for i, block in enumerate(blocks):
        if block.get("type") != "block" or block["height"] != manifest["start_height"] + i:
            raise ValueError("height discontinuity")
        if i and block["prev_hash"] != blocks[i - 1]["hash"]:
            raise ValueError("hash discontinuity")
    if blocks[-1]["hash"] != manifest["anchor_hash"] or completion["anchor_hash"] != manifest["anchor_hash"]:
        raise ValueError("anchor mismatch")

    CompactBlock = compact_class(proto)
    messages = []
    received, spent, coinbase_received = Counter(), Counter(), Counter()
    unsupported_received, unsupported_spent = Counter(), Counter()
    changed_per_block = []
    outpoints_created, outpoints_spent = set(), set()
    for block in blocks:
        msg = CompactBlock(height=block["height"], hash=bytes.fromhex(block["hash"])[::-1],
                           prevHash=bytes.fromhex(block["prev_hash"])[::-1] if block["prev_hash"] else b"",
                           time=block["time"])
        changed = set()
        for transaction in block["transactions"]:
            tx = msg.vtx.add(index=transaction["index"], txid=bytes.fromhex(transaction["txid"])[::-1])
            for output in transaction["vout"]:
                if output["n"] != len(tx.vout):
                    raise ValueError("non-contiguous output indices")
                tx.vout.add(value=output["value_zat"], scriptPubKey=bytes.fromhex(output["script"]))
                key = (transaction["txid"], output["n"])
                if key in outpoints_created:
                    raise ValueError("duplicate receive")
                outpoints_created.add(key)
                if supported(output["script"]):
                    received[output["script"]] += 1
                    if transaction["index"] == 0:
                        coinbase_received[output["script"]] += 1
                    changed.add(bytes.fromhex(output["script"]))
                else:
                    unsupported_received[output["script"]] += 1
            for vin in transaction["vin"]:
                tx.vin.add(prevoutTxid=bytes.fromhex(vin["txid"])[::-1], prevoutIndex=vin["n"])
                key = (vin["txid"], vin["n"])
                if key in outpoints_spent:
                    raise ValueError("duplicate spend")
                outpoints_spent.add(key)
                if supported(vin["script"]):
                    spent[vin["script"]] += 1
                    changed.add(bytes.fromhex(vin["script"]))
                else:
                    unsupported_spent[vin["script"]] += 1
        messages.append(msg.SerializeToString(deterministic=True))
        changed_per_block.append(changed)

    events = received + spent
    all_scripts = {bytes.fromhex(s) for s in events}
    absent = [b"\x76\xa9\x14" + hashlib.sha256(f"absent-fixture-{i}".encode()).digest()[:20]
              + b"\x88\xac" for i in range(100)]
    if set(absent) & all_scripts:
        raise ValueError("synthetic absent fixture unexpectedly appears in sample")
    filters = []
    for interval in [n for n in [1, 16, 128, 1024] if n <= len(blocks)]:
        for probability in [1e-4, 1e-5, 1e-6]:
            sizes, distinct_counts, framed = [], [], []
            false_matches, false_scripts = 0, set()
            for start in range(0, len(blocks), interval):
                end = min(start + interval, len(blocks)) - 1
                scripts = set().union(*changed_per_block[start:end + 1])
                seed = bytes.fromhex(manifest["genesis_hash"] + blocks[end]["hash"])
                bits, hashes, contains = make_filter(scripts, seed, probability)
                # Explicit 88-byte experimental envelope: range + previous/end
                # hashes + bit count + hash count. It is not a production schema.
                envelope = struct.pack(">QQ32s32sII", blocks[start]["height"], blocks[end]["height"],
                                       bytes.fromhex(blocks[start]["prev_hash"]) if blocks[start]["prev_hash"] else bytes(32),
                                       bytes.fromhex(blocks[end]["hash"]), len(bits) * 8, hashes)
                encoded = envelope + bits
                framed.append(encoded)
                sizes.append(len(encoded))
                distinct_counts.append(len(scripts))
                for script in absent:
                    if contains(script):
                        false_matches += 1
                        false_scripts.add(script)
            filters.append({"interval_blocks": interval, "target_false_positive_rate": probability,
                            "filter_bytes_with_envelopes": sum(sizes),
                            "filter_batch_gzip_bytes": len(gzip.compress(b"".join(framed), mtime=0)),
                            "affected_scripts_per_interval": distribution(distinct_counts),
                            "observed_absent_script_filter_matches": false_matches,
                            "observed_distinct_absent_scripts_matching": len(false_scripts),
                            "false_negative_checks": "all inserted scripts passed"})

    # gRPC's five-byte message envelope; HTTP/2/TLS headers are not included.
    framed = b"".join(b"\0" + len(m).to_bytes(4, "big") + m for m in messages)
    return {"classification": "measured bounded sample; reconstructed protobuf and experimental Bloom filters",
            "manifest": manifest, "completion": completion,
            "environment": {"python": platform.python_version(),
                            "protobuf": google.protobuf.__version__, "zlib": zlib.ZLIB_RUNTIME_VERSION,
                            "protoc": subprocess.check_output(["protoc", "--version"], text=True).strip()},
            "sample_sha256": hashlib.sha256(raw).hexdigest(),
            "proto_sha256": hashlib.sha256(proto.read_bytes()).hexdigest(),
            "block_time_span_seconds": blocks[-1]["time"] - blocks[0]["time"],
            "counts": {"supported_receives": sum(received.values()), "supported_spends": sum(spent.values()),
                       "supported_coinbase_receives": sum(coinbase_received.values()),
                       "unsupported_receives": sum(unsupported_received.values()),
                       "unsupported_spends": sum(unsupported_spent.values()),
                       "distinct_supported_scripts_with_sample_activity": len(events),
                       "outputs_created_and_spent_within_sample": len(outpoints_created & outpoints_spent)},
            "sample_events_per_script": distribution(events.values()),
            "sample_inline_coverage": {str(k): sum(v <= k for v in events.values()) / max(1, len(events))
                                       for k in [0, 2, 4, 8]},
            "compact_payload": {"protobuf_bytes": sum(map(len, messages)),
                                "grpc_framed_uncompressed_bytes": len(framed),
                                "grpc_per_message_gzip_bytes": sum(5 + len(gzip.compress(m, mtime=0)) for m in messages),
                                "whole_batch_gzip_bytes": len(gzip.compress(framed, mtime=0)),
                                "excludes": ["shielded data", "chainMetadata", "fee", "HTTP/2 and TLS", "request bytes"],
                                "caveat": "reconstructed transparent-only payload; not captured gRPC or incremental mixed-pool wire"},
            "filters": filters,
            "limits": ["recent contiguous sample, not full history", "100 synthetic absent scripts, not real wallets",
                       "no directory/page PIR execution", "no mobile latency or memory measurement",
                       "prototype Bloom encoding, not a selected protocol"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=Path, required=True)
    parser.add_argument("--proto", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = evaluate(args.sample, args.proto.resolve())
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(args.output)


if __name__ == "__main__":
    main()
