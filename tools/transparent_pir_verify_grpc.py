#!/usr/bin/env python3
"""Check every served transparent input/output against the archive fixture."""
import argparse
import gzip
import json
from pathlib import Path
import struct
from transparent_pir_sample import compact_class


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--sample', type=Path, required=True)
    p.add_argument('--frames', type=Path, required=True)
    p.add_argument('--proto', type=Path, required=True)
    args = p.parse_args()
    source = [json.loads(line) for line in gzip.decompress(args.sample.read_bytes()).splitlines()]
    if source[-1].get('type') != 'complete': raise ValueError('incomplete source')
    Block = compact_class(args.proto)
    frames, offset, blocks = gzip.decompress(args.frames.read_bytes()), 0, []
    while offset < len(frames):
        flag, size = struct.unpack_from('>BI', frames, offset)
        offset += 5
        if flag or offset + size > len(frames): raise ValueError('invalid frame')
        blocks.append(Block.FromString(frames[offset:offset+size])); offset += size
    if len(blocks) != len(source)-2: raise ValueError('coverage mismatch')
    for expected, actual in zip(source[1:-1], blocks):
        if expected['height'] != actual.height or expected['hash'] != actual.hash[::-1].hex():
            raise ValueError('block identity mismatch')
        actual_txs = {t.txid[::-1].hex(): t for t in actual.vtx if t.vin or t.vout}
        expected_txs = {t['txid']: t for t in expected['transactions'] if t['vin'] or t['vout']}
        if actual_txs.keys() != expected_txs.keys(): raise ValueError('transaction coverage mismatch')
        for txid, tx in expected_txs.items():
            served = actual_txs[txid]
            if tx['index'] != served.index: raise ValueError('transaction position mismatch')
            if [(v['txid'],v['n']) for v in tx['vin']] != [(v.prevoutTxid[::-1].hex(),v.prevoutIndex) for v in served.vin]:
                raise ValueError('input mismatch')
            if [(v['value_zat'],v['script']) for v in tx['vout']] != [(v.value,v.scriptPubKey.hex()) for v in served.vout]:
                raise ValueError('output mismatch')
    print(f'PASS: all transparent inputs/outputs match across {len(blocks)} blocks')


if __name__ == '__main__': main()
