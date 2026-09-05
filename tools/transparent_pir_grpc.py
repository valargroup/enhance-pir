#!/usr/bin/env python3
"""Measure public CompactBlock messages with Vizor's actual pool choices.

Uses grpcurl JSON locally or identity gRPC over HTTP/2 curl on an SSH host.
Reported bytes are reserialized application messages, excluding HTTP/TLS.
The grpcurl JSON mode cannot preserve unknown fields absent from its schema.
"""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import struct
import shlex
import subprocess
import tempfile
import time

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory, json_format


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--proto-dir', type=Path, required=True)
    p.add_argument('--endpoint', default='us.zec.stardust.rest:443')
    p.add_argument('--ssh-host', help='Optional measurement host with HTTP/2 curl; captures raw gRPC frames')
    p.add_argument('--runs', type=int, default=3)
    p.add_argument('--start', type=int, required=True)
    p.add_argument('--end', type=int, required=True)
    p.add_argument('--out-dir', type=Path, required=True)
    args = p.parse_args()
    if args.start > args.end: raise ValueError('invalid range')
    args.out_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        desc = Path(tmp) / 'service.pb'
        subprocess.run(['protoc', f'-I{args.proto_dir}', '--include_imports',
                        f'--descriptor_set_out={desc}', 'service.proto'], check=True)
        files = descriptor_pb2.FileDescriptorSet.FromString(desc.read_bytes())
    pool = descriptor_pool.DescriptorPool()
    for f in files.file: pool.Add(f)
    Range = message_factory.GetMessageClass(pool.FindMessageTypeByName('cash.z.wallet.sdk.rpc.BlockRange'))
    Block = message_factory.GetMessageClass(pool.FindMessageTypeByName('cash.z.wallet.sdk.rpc.CompactBlock'))
    results = []
    for name, pools in [('vizor_shielded', [2, 3, 4]), ('candidate_combined', [1, 2, 3, 4]),
                        ('candidate_transparent', [1])]:
        for run in range(args.runs):
            started = time.perf_counter()
            blocks = []
            batches = 0
            for start in range(args.start, args.end + 1, 100):
                request = {'start': {'height': start}, 'end': {'height': min(start + 99, args.end)}, 'poolTypes': pools}
                if args.ssh_host:
                    req = json_format.ParseDict(request, Range()).SerializeToString()
                    command = ['curl', '-fsS', '--http2', '--connect-timeout', '10', '--max-time', '60',
                               '-H', 'content-type: application/grpc', '-H', 'te: trailers',
                               '-H', 'grpc-accept-encoding: identity', '--data-binary', '@-',
                               'https://' + args.endpoint + '/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange']
                    response = subprocess.run(['ssh', '-o', 'BatchMode=yes', args.ssh_host, shlex.join(command)],
                        input=b'\0' + struct.pack('>I', len(req)) + req, capture_output=True, timeout=75)
                    if response.returncode: raise RuntimeError(response.stderr.decode())
                    wire, offset = response.stdout, 0
                    while offset < len(wire):
                        if offset + 5 > len(wire): raise ValueError('truncated frame header')
                        flag, size = struct.unpack_from('>BI', wire, offset)
                        offset += 5
                        if flag != 0 or offset + size > len(wire): raise ValueError('invalid identity frame')
                        blocks.append(Block.FromString(wire[offset:offset + size]))
                        offset += size
                else:
                    response = subprocess.run(['grpcurl', '-connect-timeout', '10', '-max-time', '60', '-max-msg-sz', '67108864', '-import-path', str(args.proto_dir),
                        '-proto', 'service.proto', '-H', 'grpc-accept-encoding:identity', '-d', json.dumps(request),
                        args.endpoint, 'cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange'],
                        capture_output=True, text=True, timeout=70)
                    if response.returncode:
                        raise RuntimeError(f"gRPC batch {start} failed: {response.stderr.strip()}")
                    decoder, raw = json.JSONDecoder(), response.stdout
                    while raw.strip():
                        item, end = decoder.raw_decode(raw.lstrip())
                        blocks.append(json_format.ParseDict(item, Block()))
                        raw = raw.lstrip()[end:]
                batches += 1
            elapsed = time.perf_counter() - started
            if len(blocks) != args.end - args.start + 1: raise ValueError('incomplete range')
            for i, block in enumerate(blocks):
                if block.height != args.start + i or (i and block.prevHash != blocks[i - 1].hash):
                    raise ValueError('range discontinuity')
            messages = [b.SerializeToString(deterministic=True) for b in blocks]
            frames = b''.join(b'\0' + struct.pack('>I', len(m)) + m for m in messages)
            item = {'case': name, 'run': run, 'pool_types': pools, 'batches': batches,
                'elapsed_seconds_including_transport_setup_and_parsing': elapsed,
                'reserialized_protobuf_bytes': sum(map(len, messages)),
                'inferred_uncompressed_grpc_frame_bytes': len(frames),
                'block_count': len(blocks), 'anchor_hash': blocks[-1].hash[::-1].hex(),
                'transparent_inputs': sum(len(t.vin) for b in blocks for t in b.vtx),
                'transparent_outputs': sum(len(t.vout) for b in blocks for t in b.vtx),
                'shielded_elements': sum(len(t.spends) + len(t.outputs) + len(t.actions) + len(t.ironwoodActions)
                                         for b in blocks for t in b.vtx),
                'reserialized_frames_sha256': hashlib.sha256(frames).hexdigest()}
            if run == 0: (args.out_dir / f'{name}.grpc.gz').write_bytes(gzip.compress(frames, mtime=0))
            results.append(item)
            print(name, run, len(frames), 'bytes', round(elapsed, 3), 's', flush=True)
    (args.out_dir / 'grpc-results.json').write_text(json.dumps({
        'transport': 'ssh-curl-http2' if args.ssh_host else 'local-grpcurl',
        'endpoint': args.endpoint, 'start': args.start, 'end': args.end,
        'classification': 'real server responses; identity frames reconstructed from decoded messages; no wallet scan, HTTP/TLS byte accounting, or mobile timing',
        'vizor_revision': '635bf29b349fb66303b1797ded7bbed4dffc8052',
        'vizor_pir_pr_revision': 'ddccc519fc1f2d3aa5abc73c919931b5e993650d',
        'runs': results}, indent=2) + '\n')


if __name__ == '__main__': main()
