#!/usr/bin/env python3
"""
Writes throughput-baseline.json: the 25th percentile of successful ops per
(branch, workload, nemesis), from past nightly runs. run-jepsen.py reports a
passing run as "low-throughput" below `fraction` of it.

Inputs are any mix of:
  - results-sorted/runs_<nemesis>.jsonl artifacts (every run), and
  - Jenkins consoleText files (only runs whose 30-line tail shows :stats).

Usage: ./throughput-baseline.py --out throughput-baseline.json FILE...
"""

import argparse
import collections
import datetime
import json
import re

from verdict import branch_of

TAIL_HEADER = re.compile(r'Last 30 lines of file .*/logs/(.*)_stdout\.log:')
LOG_LINE = re.compile(r'^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d+ \[')
RUN_NAME = re.compile(r'^(ysql|ycql)-(.+)_nemesis_(.+)_\d+$')
STATS_TOTAL = re.compile(r':stats \{:valid\? \S+,\s*:count \d+,\s*:ok-count (\d+)')
VERSION = re.compile(r'yugabyte-(\d+\.\d+\.\d+\.\d+-b\d+)')


def runs_in_jsonl(text):
    for line in text.splitlines():
        run = json.loads(line)
        if run['ok'] is not None:
            yield branch_of(run['version']), run['workload'], run['nemesis'], run['ok']


def runs_in_console(text):
    m = VERSION.search(text)
    if not m:
        return
    branch = branch_of(m.group(1))
    blocks, current = {}, None
    for line in text.splitlines():
        header = TAIL_HEADER.search(line)
        if header:
            current = header.group(1)
            blocks[current] = []
        elif LOG_LINE.match(line):
            current = None
        elif current:
            blocks[current].append(line)
    for name, lines in blocks.items():
        run, stats = RUN_NAME.match(name), STATS_TOTAL.search('\n'.join(lines))
        if run and stats:
            yield branch, '%s/%s' % run.group(1, 2), run.group(3), int(stats.group(1))


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('files', nargs='+')
    parser.add_argument('--out', default='throughput-baseline.json')
    parser.add_argument('--fraction', type=float, default=0.1)
    parser.add_argument('--min-runs', type=int, default=5)
    args = parser.parse_args()

    samples = collections.defaultdict(list)
    for path in args.files:
        with open(path, errors='replace') as f:
            text = f.read()
        runs = runs_in_jsonl(text) if path.endswith('.jsonl') else runs_in_console(text)
        for branch, workload, nemesis, ok in runs:
            samples[(branch, workload, nemesis)].append(ok)

    lows = {}
    for (branch, workload, nemesis), oks in sorted(samples.items()):
        if len(oks) >= args.min_runs:
            oks.sort()
            lows.setdefault(branch, {}).setdefault(workload, {})[nemesis] = {
                'p25_ok': oks[(len(oks) - 1) // 4], 'n': len(oks)}

    with open(args.out, 'w') as f:
        json.dump({'generated': datetime.date.today().isoformat(),
                   'fraction': args.fraction,
                   'lows': lows}, f, indent=1, sort_keys=True)
        f.write('\n')
    print("%d baselines -> %s" % (sum(len(n) for w in lows.values() for n in w.values()), args.out))


if __name__ == '__main__':
    main()
