#!/usr/bin/env python3
"""Summarize preserved measurements without treating core timings as wallet latency."""
import argparse
import json
import math
from pathlib import Path
import statistics


def quantiles(values):
    values = sorted(values)
    return {'count': len(values), 'min': values[0], 'max': values[-1],
            **{f'p{p}': values[max(0, math.ceil(len(values) * p / 100) - 1)] for p in [50, 90, 95, 99]}}


def summarize(root):
    layouts = {}
    for path in sorted(root.glob('*/**/results.json')):
        data = json.loads(path.read_text())
        layouts.setdefault(path.parent.name, []).append(data)
    output = {'classification': 'aggregate measured in-process PIR samples; not mobile or network latency', 'layouts': {}}
    for name, runs in sorted(layouts.items()):
        record = {'independent_runs': len(runs), 'geometry': {k: runs[0]['layout'][k] for k in
                  ['row_bytes', 'inline_capacity', 'directory_rows', 'page_rows', 'used_pages',
                   'candidate_buckets_per_script', 'script_count', 'slot_utilization']},
                  'workloads_first_run': runs[0]['workloads']}
        for kind in ['directory', 'pages']:
            tables = [r[kind] for r in runs]
            samples = [s for t in tables for s in t['samples']]
            sizes = {(s['upload_bytes'], s['response_bytes']) for s in samples}
            if len(sizes) != 1: raise ValueError('wire sizes changed between repeats')
            upload, response = sizes.pop()
            record[kind] = {'upload_bytes': upload, 'response_bytes': response,
                'published_setup_bytes': tables[0]['published_setup_bytes'],
                **{k: quantiles([s[k] for s in samples]) for k in ['prepare_ms', 'server_ms', 'decode_ms', 'total_ms']},
                'combined_client_server_rss_bytes': quantiles([t['combined_client_server_ready_rss_bytes'] for t in tables]),
                'server_setup_ms': quantiles([t['server_setup_ms'] for t in tables])}
        output['layouts'][name] = record
    return output


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--evidence', type=Path, default=Path('docs/transparent-pir-evaluation/mainnet-study'))
    p.add_argument('--check', action='store_true')
    args = p.parse_args()
    encoded = json.dumps(summarize(args.evidence), indent=2, sort_keys=True) + '\n'
    destination = args.evidence / 'pir-summary.json'
    if args.check:
        if destination.read_text() != encoded: raise ValueError('summary is stale')
        print('PASS: preserved PIR summaries reproduce')
    else: destination.write_text(encoded)


if __name__ == '__main__': main()
