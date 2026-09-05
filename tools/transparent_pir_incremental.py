#!/usr/bin/env python3
"""Research-only incremental history sync. Trusted indexer; no wallet/network adapter.

The state machine consumes public whole-range filters, then obtains exact directory
and page rows through an injected transport. PirTransport executes fresh serialized
IPIR queries using the pinned Rust harness. FileTransport is only for fault tests.
"""

import argparse
from collections import defaultdict
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import struct
import subprocess
import tempfile

from transparent_pir_layout import ENTRY, EVENT, bucket

# Exact script identity, ordinal/count, event count, minimum/maximum event height.
PAGE = struct.Struct("<B25sIIIII2x")
from transparent_pir_sample import make_filter, supported

FILTER = struct.Struct("<I32sII")
SCHEMA = "transparent-incremental-research-v1"

# Generation/filter schema, kept apart from the wallet-state SCHEMA above.
# A new filter format changes what a generation contains; it does not change
# what wallet state means, and conflating the two would have forced every
# existing wallet state to be discarded to add a filter encoding.
GENERATION_SCHEMA = "transparent-incremental-generation-v1"

# Filter formats. Absent means BLOOM_V1: generations built before the format
# field existed are Bloom generations, and must keep reproducing byte for byte.
BLOOM_V1 = "bloom-v1"
BIP158_V1 = "bip158-zcash-transparent-basic-v1"
FILTER_FORMATS = (BLOOM_V1, BIP158_V1)


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def save(path, value):
    """Replace the complete checkpoint and pending journal as one durable file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=".sync-")
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(encode(value))
            out.flush()
            os.fsync(out.fileno())
        os.replace(name, path)
        parent = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def load_sample(path):
    raw = Path(path).read_bytes()
    if str(path).endswith(".gz"):
        raw = gzip.decompress(raw)
    records = [json.loads(line) for line in raw.splitlines()]
    manifest, blocks, done = records[0], records[1:-1], records[-1]
    if done.get("type") != "complete" or done.get("unresolved_prevouts") != 0:
        raise ValueError("incomplete source")
    if (
        len(blocks) != done["blocks"]
        or len(blocks) != manifest["anchor_height"] - manifest["start_height"] + 1
    ):
        raise ValueError("source range mismatch")
    for i, b in enumerate(blocks):
        if b["height"] != manifest["start_height"] + i or (
            i and b["prev_hash"] != blocks[i - 1]["hash"]
        ):
            raise ValueError("source discontinuity")
    if (
        blocks[-1]["hash"] != manifest["anchor_hash"]
        or done["anchor_hash"] != manifest["anchor_hash"]
    ):
        raise ValueError("source anchor mismatch")
    return manifest, blocks


def source_events(blocks):
    """Index public events. Kind 3 preserves coinbase identity for wallet policy."""
    events = defaultdict(list)
    for b in blocks:
        for tx in b["transactions"]:
            for kind, items in [(1, tx["vout"]), (2, tx["vin"])]:
                for item in items:
                    if not supported(item["script"]):
                        continue
                    k = 3 if kind == 1 and tx["index"] == 0 and not tx["vin"] else kind
                    outpoint_tx = tx["txid"] if kind == 1 else item["txid"]
                    event = EVENT.pack(
                        k,
                        b["height"],
                        tx["index"],
                        bytes.fromhex(outpoint_tx),
                        item["n"],
                        item["value_zat"],
                        bytes.fromhex(tx["txid"]) if kind == 2 else bytes(32),
                    )
                    events[item["script"]].append(event)
    for history in events.values():
        history.sort(key=lambda e: (EVENT.unpack(e)[1:3], EVENT.unpack(e)[0] == 2, e))
    return events


def complete_scripts(block):
    """Every filter element for one block, as raw script hex.

    Deliberately NOT source_events(): that helper drops scripts the private
    history backend cannot serve, and a shared public filter built from it would
    answer "no activity" to a wallet that does have activity. Filter coverage is
    broader than retrieval coverage, and the two must not be conflated.

    Excludes a script whose FIRST byte is 0x6a (OP_RETURN). A 0x6a appearing
    later is ordinary data and the script stays. The collector has already
    resolved previous-output scripts and dropped coinbase inputs.
    """
    elements = set()
    for tx in block["transactions"]:
        for item in tx["vout"] + tx["vin"]:
            script = item["script"]
            if script and not script.startswith("6a"):
                elements.add(script)
    return sorted(elements)


def bip158_filters(blocks, cli):
    """Serialized BIP 158 filters for each block, via one CLI invocation.

    Batched on purpose. A process per block, or worse per script, would cost
    more than the matching it is meant to measure.
    """
    requests = "".join(
        json.dumps({"block_hash_display": block["hash"],
                    "elements": complete_scripts(block)}) + "\n"
        for block in blocks
    )
    completed = subprocess.run(
        [str(cli), "batch-build"], input=requests,
        capture_output=True, text=True, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"batch-build failed: {completed.stderr.strip()}")
    results = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    if len(results) != len(blocks):
        raise RuntimeError("batch-build returned the wrong number of filters")
    return [bytes.fromhex(result["filter"]) for result in results]


def build_generation(network, blocks, output, page_bytes=17920, probability=1e-6,
                     filter_format=BLOOM_V1, filter_cli=None):
    """Publish an immutable whole-range generation; page width is independent."""
    if filter_format not in FILTER_FORMATS:
        raise ValueError(f"unknown filter format {filter_format!r}")
    if filter_format == BIP158_V1 and filter_cli is None:
        raise ValueError("the BIP 158 format needs --filter-cli")
    if not blocks or page_bytes not in [3584, 7168, 10752, 17920]:
        raise ValueError("invalid generation geometry")
    for i, block in enumerate(blocks):
        if i and (
            block["height"] != blocks[i - 1]["height"] + 1
            or block["prev_hash"] != blocks[i - 1]["hash"]
        ):
            raise ValueError("discontinuous generation")
    events = source_events(blocks)
    directory_bytes, inline = 3584, 2
    entry_bytes, slots = ENTRY.size + inline * EVENT.size, 13
    rows = 2048
    while True:
        for salt in range(16):
            buckets = defaultdict(list)
            for script in sorted(events):
                buckets[bucket(bytes.fromhex(script), salt, rows)].append(script)
            if max(map(len, buckets.values()), default=0) <= slots:
                break
        else:
            rows *= 2
            if rows > 131072:
                raise ValueError("prototype directory limit")
            continue
        break
    capacity = (page_bytes - PAGE.size) // EVENT.size
    pages, locators = [], {}
    for script, history in sorted(events.items()):
        older, n = history[:-inline], min(inline, len(history))
        first, count = len(pages), math.ceil(len(older) / capacity)
        for i in range(count):
            part = older[i * capacity : (i + 1) * capacity]
            pages.append(
                (
                    PAGE.pack(
                        len(bytes.fromhex(script)),
                        bytes.fromhex(script),
                        i,
                        count,
                        len(part),
                        EVENT.unpack(part[0])[1],
                        EVENT.unpack(part[-1])[1],
                    )
                    + b"".join(part)
                ).ljust(page_bytes, b"\0")
            )
        locators[script] = first, count, n
    directory = bytearray(rows * directory_bytes)
    for row, scripts in buckets.items():
        struct.pack_into("<I", directory, row * directory_bytes, len(scripts))
        for slot, script in enumerate(scripts):
            first, count, n = locators[script]
            record = ENTRY.pack(
                len(bytes.fromhex(script)),
                bytes.fromhex(script),
                len(events[script]),
                n,
                first,
                count,
            ) + b"".join(events[script][-n:])
            offset = row * directory_bytes + 4 + slot * entry_bytes
            directory[offset : offset + len(record)] = record
    page_rows = max(2048, math.ceil(len(pages) / 2048) * 2048)
    page_data = b"".join(pages).ljust(page_rows * page_bytes, b"\0")
    filters = bytearray()
    if filter_format == BIP158_V1:
        # hashes is recorded as 0: the BIP 158 profile fixes P and M, so there
        # is no per-filter hash count. read_targets refuses a nonzero value
        # rather than silently reading a Bloom header as a BIP 158 one.
        for block, bits in zip(blocks, bip158_filters(blocks, filter_cli)):
            filters.extend(
                FILTER.pack(block["height"], bytes.fromhex(block["hash"]), 0, len(bits))
                + bits
            )
    else:
        for block in blocks:
            scripts = {bytes.fromhex(s) for s in source_events([block])}
            block_hash = bytes.fromhex(block["hash"])
            seed = hashlib.sha256(
                b"transparent-incremental-filter-v1\0" + network.encode() + block_hash
            ).digest()
            bits, hashes, _ = make_filter(scripts, seed, probability)
            filters.extend(
                FILTER.pack(block["height"], block_hash, hashes, len(bits)) + bits
            )
    manifest = {
        "schema": SCHEMA,
        "generation_schema": GENERATION_SCHEMA,
        "filter_format": filter_format,
        "network": network,
        "start": blocks[0]["height"],
        "end": blocks[-1]["height"],
        "parent": blocks[0]["prev_hash"],
        "anchor": blocks[-1]["hash"],
        "sealed_through": max(blocks[0]["height"] - 1, blocks[-1]["height"] - 2),
        "inline": inline,
        "entry_bytes": entry_bytes,
        "slots": slots,
        "page_capacity": capacity,
        "directory": {
            "rows": rows,
            "row_bytes": directory_bytes,
            "salt": salt,
            "sha256": digest(directory),
        },
        "pages": {
            "rows": page_rows,
            "row_bytes": page_bytes,
            "sha256": digest(page_data),
        },
        "filters_sha256": digest(filters),
        "filter_probability": probability,
    }
    generation = digest(encode(manifest))
    out = Path(output) / generation
    if out.exists():
        if (out / "manifest.json").read_bytes() != encode(manifest):
            raise ValueError("immutable generation conflict")
        for name, raw in [
            ("directory.bin", directory),
            ("pages.bin", page_data),
            ("filters.bin", filters),
        ]:
            if (out / name).read_bytes() != raw:
                raise ValueError("immutable generation modified")
        return out
    out.mkdir(parents=True)
    for name, raw in [
        ("directory.bin", directory),
        ("pages.bin", page_data),
        ("filters.bin", filters),
        ("manifest.json", encode(manifest)),
    ]:
        (out / name).write_bytes(raw)
    return out


def new_state(network, scripts, height, block_hash, utxos=None):
    scripts = sorted(set(scripts))
    if not scripts or any(not supported(s) for s in scripts):
        raise ValueError("unsupported/empty script scope")
    return {
        "schema": SCHEMA,
        "network": network,
        "scripts": scripts,
        "height": height,
        "hash": block_hash,
        "scope": digest(encode(scripts)),
        "base_height": height,
        "base_hash": block_hash,
        "base_utxos": utxos or {},
        "utxos": utxos or {},
        "chain": {str(height): block_hash},
        "events": [],
        "pending": None,
        "cost": {
            "public_download_bytes": 0,
            "upload_bytes": 0,
            "response_bytes": 0,
            "setup_download_bytes": 0,
            "queries": 0,
            "core_ms": 0.0,
        },
        "completed_generations": [],
    }


def apply_events(base, events):
    utxos = dict(base)
    identities = set()
    for script, raw in sorted(
        events,
        key=lambda pair: (
            EVENT.unpack(bytes.fromhex(pair[1]))[1:3],
            EVENT.unpack(bytes.fromhex(pair[1]))[0] == 2,
            pair,
        ),
    ):
        kind, height, index, txid, n, value, spender = EVENT.unpack(bytes.fromhex(raw))
        outpoint = f"{txid.hex()}:{n}"
        identity = (1 if kind in [1, 3] else kind, outpoint, spender.hex())
        if identity in identities:
            raise ValueError("duplicate event identity")
        identities.add(identity)
        if kind in [1, 3]:
            if spender != bytes(32) or outpoint in utxos:
                raise ValueError("invalid receive")
            utxos[outpoint] = {
                "script": script,
                "value": value,
                "height": height,
                "coinbase": kind == 3,
            }
        elif kind == 2:
            prior = utxos.get(outpoint)
            if (
                spender == bytes(32)
                or prior is None
                or prior["script"] != script
                or prior["value"] != value
            ):
                raise ValueError("unresolved or inconsistent spend")
            del utxos[outpoint]
        else:
            raise ValueError("unknown event kind")
    return utxos


def rewind(state, height, block_hash, accepted):
    if (
        not state["base_height"] <= height <= state["height"]
        or accepted.get(height) != block_hash
        or state["chain"].get(str(height)) != block_hash
    ):
        raise ValueError("reorg requires an accepted ancestor within retained history")
    state["events"] = [
        e for e in state["events"] if EVENT.unpack(bytes.fromhex(e[1]))[1] <= height
    ]
    state["utxos"] = apply_events(state["base_utxos"], state["events"])
    state["height"], state["hash"], state["pending"] = height, block_hash, None
    state["completed_generations"] = []
    state["chain"] = {h: v for h, v in state["chain"].items() if int(h) <= height}


class FileTransport:
    """Cleartext test double. Never used as evidence of private query execution."""

    def __init__(self):
        self.calls = []

    def fetch(self, folder, manifest, table, rows):
        self.calls.append((table, rows))
        geometry = manifest[table]
        raw = (folder / f"{table}.bin").read_bytes()
        if (
            digest(raw) != geometry["sha256"]
            or len(raw) != geometry["rows"] * geometry["row_bytes"]
        ):
            raise ValueError("table does not belong to generation")
        return {
            r: raw[r * geometry["row_bytes"] : (r + 1) * geometry["row_bytes"]]
            for r in rows
        }, {}


class ChargedFailure(RuntimeError):
    def __init__(self, cost):
        super().__init__("injected lost response after measured PIR execution")
        self.cost = cost


class PirTransport:
    """Adaptive fresh PIR batches. Local IPC contains selections; no wire claim."""

    def __init__(self, binary, output, public_cache=None):
        self.binary = Path(binary).resolve()
        self.output = Path(output)
        self.output.mkdir(parents=True, exist_ok=True)
        self.calls = []
        self.sequence = len(list(self.output.glob("*.json")))
        self.drop_once = False
        self.public_cache = public_cache

    def fetch(self, folder, manifest, table, rows):
        geometry = manifest[table]
        table_path = folder / f"{table}.bin"
        if table_path.stat().st_size != geometry["rows"] * geometry["row_bytes"]:
            raise ValueError("table size does not match generation")
        with table_path.open("rb") as table_file:
            checksum = hashlib.sha256()
            for chunk in iter(lambda: table_file.read(1024 * 1024), b""):
                checksum.update(chunk)
        if checksum.hexdigest() != geometry["sha256"]:
            raise ValueError("table does not belong to generation")
        number = self.sequence
        self.sequence += 1
        self.calls.append((table, rows))
        path = self.output / f"{number:04d}-{table}.json"
        cached_mask = sum(
            1 << slot
            for slot in range(4)
            if self.public_cache is not None
            and (str(self.binary), folder.name, table, slot) in self.public_cache
        )
        environment = dict(os.environ, PIR_CACHED_PUBLIC_MASK=str(cached_mask))
        with path.open("w") as out:
            subprocess.run(
                [
                    str(self.binary),
                    str(folder / f"{table}.bin"),
                    str(geometry["row_bytes"]),
                    str(geometry["rows"]),
                    ",".join(map(str, rows)),
                    str(len(rows)),
                ],
                stdout=out,
                env=environment,
                check=True,
                timeout=1800,
            )
        result = json.loads(path.read_text())
        decoded = {r["row"]: bytes(r["bytes"]) for r in result.pop("decoded_rows")}
        if set(decoded) != set(rows):
            raise ValueError("PIR response coverage mismatch")
        path.write_text(json.dumps(result, indent=2) + "\n")
        published = result["published_setup_bytes"]
        if self.public_cache is not None:
            sets = result.get("public_sets", 1)
            if published % sets:
                raise ValueError("nonuniform public decoding data")
            downloaded = 0
            for slot in range(sets):
                identity = (str(self.binary), folder.name, table, slot)
                if identity not in self.public_cache:
                    downloaded += published // sets
                    self.public_cache.add(identity)
            published = downloaded
        result["charged_public_download_bytes"] = published
        path.write_text(json.dumps(result, indent=2) + "\n")
        cost = {
            "queries": len(result["samples"]),
            "setup_download_bytes": published,
            "upload_bytes": sum(s["upload_bytes"] for s in result["samples"]),
            "response_bytes": sum(s["response_bytes"] for s in result["samples"]),
            "core_ms": sum(s["total_ms"] for s in result["samples"]),
        }
        if self.drop_once:
            self.drop_once = False
            raise ChargedFailure(cost)
        return decoded, cost


def read_targets(folder, manifest, scripts, checkpoint, accepted, filter_cli=None):
    """Scripts whose filters matched, and the public bytes charged for them.

    Dispatches on the generation's recorded filter format. An old generation has
    no format field and is Bloom; its bytes are never reinterpreted under the
    new format, so old generation ids and old evidence stay reproducible.
    """
    filter_format = manifest.get("filter_format", BLOOM_V1)
    if filter_format not in FILTER_FORMATS:
        raise ValueError(f"unknown filter format {filter_format!r}")
    if filter_format == BIP158_V1 and filter_cli is None:
        raise ValueError("the BIP 158 format needs --filter-cli")

    raw = (folder / "filters.bin").read_bytes()
    if digest(raw) != manifest["filters_sha256"]:
        raise ValueError("missing/corrupt filters")
    offset, expected, targets = 0, manifest["start"], set()
    pending = []
    while offset < len(raw):
        if offset + FILTER.size > len(raw):
            raise ValueError("truncated filter")
        height, block_hash, hashes, size = FILTER.unpack_from(raw, offset)
        offset += FILTER.size
        # A Bloom header carries a hash count in 1..64; a BIP 158 header pins it
        # to 0 because the profile fixes P and M. Requiring the right one here
        # stops one format's bytes being read as the other's.
        hashes_ok = hashes == 0 if filter_format == BIP158_V1 else 1 <= hashes <= 64
        if (
            height != expected
            or accepted.get(height) != block_hash.hex()
            or not hashes_ok
            or not 1 <= size <= 16 * 1024 * 1024
            or offset + size > len(raw)
        ):
            raise ValueError("filter coverage/anchor mismatch")
        bits = raw[offset : offset + size]
        offset += size
        expected += 1
        if height <= checkpoint:
            continue
        if filter_format == BIP158_V1:
            # Collected and matched in one batch below. This harness stores
            # the hash as bytes.fromhex(block["hash"]), i.e. already in display
            # order, so it is handed to the CLI as-is; the CLI does the display
            # to internal conversion the BIP 158 keys require.
            pending.append((block_hash.hex(), bits.hex()))
            continue
        seed = hashlib.sha256(
            b"transparent-incremental-filter-v1\0"
            + manifest["network"].encode()
            + block_hash
        ).digest()
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
                targets.add(script)
    if expected != manifest["end"] + 1:
        raise ValueError("missing filter interval")
    if pending:
        targets |= bip158_matches(pending, scripts, filter_cli)
    return sorted(targets), len(raw)


def bip158_matches(pending, scripts, cli):
    """Scripts matching any of the given filters, in one CLI invocation.

    The wallet's script list never leaves the machine; this is local matching
    against already-downloaded filters, not a query to any server.
    """
    ordered = sorted(scripts)
    if not ordered:
        return set()
    requests = "".join(
        json.dumps({"block_hash_display": block_hash, "filter": bits,
                    "scripts": ordered}) + "\n"
        for block_hash, bits in pending
    )
    completed = subprocess.run(
        [str(cli), "batch-match"], input=requests,
        capture_output=True, text=True, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"batch-match failed: {completed.stderr.strip()}")
    results = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    if len(results) != len(pending):
        raise RuntimeError("batch-match returned the wrong number of results")
    matched = set()
    for result in results:
        for index in result["indices"]:
            matched.add(ordered[index])
    return matched


def directory_records(raw, manifest, targets):
    if len(raw) != manifest["directory"]["row_bytes"]:
        raise ValueError("directory row length")
    count = struct.unpack_from("<I", raw)[0]
    if count > manifest["slots"]:
        raise ValueError("directory overflow")
    found = {}
    for slot in range(count):
        offset = 4 + slot * manifest["entry_bytes"]
        length, key, total, n, first, pages = ENTRY.unpack_from(raw, offset)
        if length not in [23, 25] or not supported(key[:length].hex()):
            raise ValueError("invalid directory identity")
        script = key[:length].hex()
        if script not in targets:
            continue
        if (
            script in found
            or n != min(total, manifest["inline"])
            or pages != math.ceil((total - n) / manifest["page_capacity"])
            or first + pages > manifest["pages"]["rows"]
        ):
            raise ValueError("invalid directory record")
        inline = raw[offset + ENTRY.size : offset + ENTRY.size + n * EVENT.size]
        found[script] = {
            "total": total,
            "first": first,
            "pages": pages,
            "inline": [
                inline[i : i + EVENT.size].hex()
                for i in range(0, len(inline), EVENT.size)
            ],
        }
    return found


def sync(path, folder, accepted, transport, budget=1000, navigation=False,
         filter_cli=None):
    """Advance coverage atomically only after all private work is durably present.

    accepted is wallet-owned chain state, never taken from the generation itself.
    A budget pause retains generation-bound rows; no public lookup fallback exists.
    navigation privately searches the trusted sorted history for a checkpoint suffix.
    """
    path, folder = Path(path), Path(folder)
    state = json.loads(path.read_text())
    if state.get("schema") != SCHEMA:
        raise ValueError("unsupported checkpoint schema")
    manifest_raw = (folder / "manifest.json").read_bytes()
    manifest = json.loads(manifest_raw)
    generation = digest(encode(manifest))
    if state.get("scope") != digest(encode(state["scripts"])):
        raise ValueError(
            "script scope changed; earlier coverage must be established separately"
        )
    if (
        manifest["directory"]["row_bytes"] != 3584
        or manifest["pages"]["row_bytes"] not in [3584, 7168, 10752, 17920]
        or manifest["inline"] != 2
        or manifest["entry_bytes"] != 256
        or manifest["slots"] != 13
        or manifest["page_capacity"]
        != (manifest["pages"]["row_bytes"] - PAGE.size) // EVENT.size
    ):
        raise ValueError("unsupported geometry")
    if (
        any(
            not 2048 <= manifest[t]["rows"] <= 131072 or manifest[t]["rows"] % 2048
            for t in ["directory", "pages"]
        )
        or not 1 <= manifest["end"] - manifest["start"] + 1 <= 10000
    ):
        raise ValueError("generation bounds exceeded")
    if (
        budget < 0
        or manifest.get("schema") != SCHEMA
        or manifest["network"] != state["network"]
        or folder.name != generation
    ):
        raise ValueError("unsupported/misbound generation")
    filter_format = manifest.get("filter_format", BLOOM_V1)
    if filter_format not in FILTER_FORMATS:
        raise ValueError(f"unknown filter format {filter_format!r}")
    # A generation that declares a generation schema must declare one we know.
    # Its absence means a pre-format generation, which is Bloom by definition.
    if manifest.get("generation_schema", GENERATION_SCHEMA) != GENERATION_SCHEMA:
        raise ValueError("unsupported generation schema")
    if (
        accepted.get(state["height"]) != state["hash"]
        or accepted.get(manifest["end"]) != manifest["anchor"]
        or accepted.get(manifest["start"] - 1) != manifest["parent"]
    ):
        raise ValueError("stale or unaccepted chain")
    if manifest["end"] <= state["height"]:
        return state
    if manifest["start"] > state["height"] + 1:
        raise ValueError("coverage gap")
    pending = state["pending"]
    if pending and pending["generation"] != generation:
        raise ValueError(
            "resume requires the original retained generation or explicit rewind"
        )
    if pending and pending.get("filter_format", BLOOM_V1) != filter_format:
        raise ValueError(
            "resume requires the original filter format or explicit rewind"
        )
    if pending is None:
        targets, size = read_targets(
            folder, manifest, state["scripts"], state["height"], accepted,
            filter_cli=filter_cli,
        )
        pending = {
            "generation": generation,
            "filter_format": filter_format,
            "targets": targets,
            "directory": {},
            "pages": {},
        }
        state["pending"] = pending
        state["cost"]["public_download_bytes"] += len(manifest_raw) + size
        save(path, state)

    def fetch(table, rows):
        nonlocal budget
        need = [r for r in sorted(set(rows)) if str(r) not in pending[table]][:budget]
        if need:
            try:
                decoded, cost = transport.fetch(folder, manifest, table, need)
            except ChargedFailure as error:
                for key, value in error.cost.items():
                    state["cost"][key] += value
                save(path, state)
                raise
            for key, value in cost.items():
                state["cost"][key] += value
            for row, raw in decoded.items():
                pending[table][str(row)] = raw.hex()
            budget -= len(need)
            save(path, state)
        return all(str(r) in pending[table] for r in rows)

    targets = pending["targets"]
    rows = {
        bucket(
            bytes.fromhex(s),
            manifest["directory"]["salt"],
            manifest["directory"]["rows"],
        )
        for s in targets
    }
    if not fetch("directory", rows):
        return state
    records = {}
    for row in rows:
        for script, record in directory_records(
            bytes.fromhex(pending["directory"][str(row)]), manifest, targets
        ).items():
            if (
                bucket(
                    bytes.fromhex(script),
                    manifest["directory"]["salt"],
                    manifest["directory"]["rows"],
                )
                != row
                or script in records
            ):
                raise ValueError("misplaced/duplicate directory record")
            records[script] = record

    def page_part(script, record, i):
        raw = bytes.fromhex(pending["pages"][str(record["first"] + i)])
        if len(raw) != manifest["pages"]["row_bytes"]:
            raise ValueError("page length mismatch")
        length, key, index, count, size, low, high = PAGE.unpack_from(raw)
        expected_size = min(
            manifest["page_capacity"],
            record["total"] - len(record["inline"]) - i * manifest["page_capacity"],
        )
        if (
            key[:length].hex() != script
            or length not in [23, 25]
            or index != i
            or count != record["pages"]
            or size != expected_size
        ):
            raise ValueError("page identity mismatch")
        part = [
            raw[PAGE.size + j * EVENT.size : PAGE.size + (j + 1) * EVENT.size].hex()
            for j in range(size)
        ]
        heights = [EVENT.unpack(bytes.fromhex(event))[1] for event in part]
        if (
            not heights
            or low != min(heights)
            or high != max(heights)
            or heights != sorted(heights)
            or not manifest["start"] <= low <= high <= manifest["end"]
        ):
            raise ValueError("page height bounds mismatch")
        return part, low, high

    starts = {}
    for script, record in records.items():
        lo, hi = 0, record["pages"]
        # The trusted builder sorts history by height. Search the first page
        # whose last event is strictly newer than the wallet checkpoint.
        # Replaying the search from journaled rows makes budget resumes exact.
        if navigation and state["height"] >= manifest["start"]:
            while lo < hi:
                mid = (lo + hi) // 2
                if not fetch("pages", [record["first"] + mid]):
                    return state
                _, _, high = page_part(script, record, mid)
                if high <= state["height"]:
                    lo = mid + 1
                else:
                    hi = mid
        starts[script] = lo
    needed = {
        record["first"] + i
        for script, record in records.items()
        for i in range(starts[script], record["pages"])
    }
    if not fetch("pages", needed):
        return state
    recovered = []
    for script, record in records.items():
        history = []
        previous_high = None
        # Validate ordering of every observed page, including navigation probes.
        for i in range(record["pages"]):
            if str(record["first"] + i) not in pending["pages"]:
                continue
            part, low, high = page_part(script, record, i)
            if previous_high is not None and low < previous_high:
                raise ValueError("nonmonotonic page history")
            previous_high = high
            if i >= starts[script]:
                history.extend(part)
        inline_heights = [EVENT.unpack(bytes.fromhex(e))[1] for e in record["inline"]]
        if inline_heights != sorted(inline_heights) or (
            previous_high is not None
            and inline_heights
            and previous_high > inline_heights[0]
        ):
            raise ValueError("nonmonotonic inline history")
        history.extend(record["inline"])
        skipped = min(
            starts[script] * manifest["page_capacity"],
            record["total"] - len(record["inline"]),
        )
        if len(history) != record["total"] - skipped or len(set(history)) != len(
            history
        ):
            raise ValueError("incomplete/duplicate history")
        for event in history:
            height = EVENT.unpack(bytes.fromhex(event))[1]
            if not manifest["start"] <= height <= manifest["end"]:
                raise ValueError("event outside generation")
            if height > state["height"]:
                recovered.append([script, event])
    all_events = state["events"] + recovered
    utxos = apply_events(state["base_utxos"], all_events)
    state.update(
        events=all_events,
        utxos=utxos,
        height=manifest["end"],
        hash=manifest["anchor"],
        pending=None,
    )
    state["completed_generations"].append(generation)
    state["chain"].update(
        {str(h): accepted[h] for h in range(manifest["start"], manifest["end"] + 1)}
    )
    save(path, state)
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest, blocks = load_sample(args.sample)
    print(build_generation(manifest["network"], blocks, args.output))


if __name__ == "__main__":
    main()
