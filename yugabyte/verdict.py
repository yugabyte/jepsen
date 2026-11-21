#!/usr/bin/env python3
"""
Verdict rules for one Jepsen run, shared by run-jepsen.py (JUnit, i.e. the
Jenkins result) and sort-results.sh (results-sorted categories, i.e. Slack).

Usage: verdict.py rc-acceptable RUN_DIR   (exit 0 if acceptable)
"""

import json
import os
import re
import sys

_DELIMS = set('()[]{}" \t\r\n,')


class EdnError(ValueError):
    pass


class _Reader:
    """Reads the EDN Jepsen writes to results.edn. Keywords stay strings with
    their colon (":valid?"); tagged literals (#jepsen.history.Op{...}) are
    read as their untagged value."""

    def __init__(self, text):
        self.s, self.i = text, 0

    def _skip_ws(self):
        while self.i < len(self.s) and self.s[self.i] in ' \t\r\n,':
            self.i += 1

    def _token(self):
        start = self.i
        while self.i < len(self.s) and self.s[self.i] not in _DELIMS:
            self.i += 1
        return self.s[start:self.i]

    def _seq(self, close):
        out = []
        while True:
            self._skip_ws()
            if self.i >= len(self.s):
                raise EdnError("unterminated collection")
            if self.s[self.i] == close:
                self.i += 1
                return out
            out.append(self.read())

    def read(self):
        self._skip_ws()
        if self.i >= len(self.s):
            raise EdnError("unexpected end of input")
        c = self.s[self.i]
        if c in '([':
            self.i += 1
            return self._seq(')' if c == '(' else ']')
        if c == '{':
            self.i += 1
            items = self._seq('}')
            return {_hashable(k): v for k, v in zip(items[::2], items[1::2])}
        if c == '"':
            return self._string()
        if c == '#':
            self.i += 1
            if self.s[self.i] == '{':
                self.i += 1
                return [_hashable(x) for x in self._seq('}')]
            self._token()
            return self.read()
        if c in ')]}':
            raise EdnError("unexpected %r at %d" % (c, self.i))
        return _atom(self._token())

    def _string(self):
        self.i += 1
        out = []
        while self.s[self.i] != '"':
            if self.s[self.i] == '\\':
                self.i += 1
            out.append(self.s[self.i])
            self.i += 1
        self.i += 1
        return ''.join(out)


def _atom(tok):
    if tok in ('nil', 'true', 'false'):
        return {'nil': None, 'true': True, 'false': False}[tok]
    try:
        return int(tok)
    except ValueError:
        pass
    try:
        return float(tok)
    except ValueError:
        return tok


def _hashable(x):
    if isinstance(x, list):
        return tuple(_hashable(v) for v in x)
    return repr(x) if isinstance(x, dict) else x


def parse_edn(text):
    return _Reader(text).read()


def load_results(run_dir):
    """Parsed results.edn, or None if missing or unreadable (e.g. a crash)."""
    try:
        with open(os.path.join(run_dir, 'results.edn'), errors='replace') as f:
            results = parse_edn(f.read())
    except (OSError, ValueError, IndexError):
        return None
    return results if isinstance(results, dict) else None


def rc_unknown_acceptable(results):
    """(accepted, reason) for a read-committed run. Elle marks a whole run
    :unknown when any cycle search times out, even for an anomaly read
    committed allows; that is still an RC pass. Anything else :unknown (no
    successful ops, XOR :partial, ...) or false is not."""
    if results is None:
        return False, "no results.edn"
    if results.get(':valid?') != ':unknown':
        return False, "overall :valid? is %s" % results.get(':valid?')
    bad = {k: v.get(':valid?') for k, v in results.items()
           if isinstance(v, dict) and k != ':workload' and v.get(':valid?') is not True}
    if bad:
        return False, "checkers not valid: %s" % bad
    workload = results.get(':workload') or {}
    if workload.get(':anomaly-types') != [':cycle-search-timeout']:
        return False, ":workload anomaly types %s" % workload.get(':anomaly-types')
    # Elle puts the models a timed-out search could violate under :not / :also-not.
    if any(':read-committed' in (workload.get(k) or []) for k in (':not', ':also-not')):
        return False, "a timed-out cycle search could violate read committed"
    return True, "only Elle cycle-search timeouts"


def stat(results, key):
    """A :stats total (key 'ok-count', 'fail-count', 'info-count', 'count'), or None."""
    return ((results or {}).get(':stats') or {}).get(':' + key)


def xor_status(results):
    status = ((results or {}).get(':xor-consistency') or {}).get(':status')
    return status.lstrip(':') if status else None


def test_flag(run_dir, name):
    """A top-level test-map value as printed near the start of jepsen.log."""
    try:
        with open(os.path.join(run_dir, 'jepsen.log'), errors='replace') as f:
            head = f.read(65536)
    except OSError:
        return None
    m = re.search(r'(?m)^ :%s (\S+)$' % re.escape(name), head)
    return m.group(1) if m else None


def load_baseline(path):
    with open(path) as f:
        return json.load(f)


def branch_of(version):
    """'2.33.0.0-b67' -> 'master', '2026.2.0.0-b19' -> '2026.2'."""
    major, minor = version.split('.')[:2]
    return 'master' if major == '2' else '%s.%s' % (major, minor)


def low_throughput(baseline, branch, workload, nemesis, ok_count):
    """(flagged, detail): ok_count below the baseline fraction of the 25th
    percentile for this branch, workload and nemesis. A branch with no
    baseline yet uses master's; pairs without one are never flagged."""
    lows = baseline['lows']
    entry = lows.get(branch if branch in lows else 'master', {}).get(workload, {}).get(nemesis)
    if ok_count is None or entry is None:
        return False, "no baseline"
    detail = "%d ok ops vs 25th percentile %d (n=%d)" % (ok_count, entry['p25_ok'], entry['n'])
    return ok_count < entry['p25_ok'] * baseline['fraction'], detail


if __name__ == '__main__':
    if len(sys.argv) != 3 or sys.argv[1] != 'rc-acceptable':
        sys.exit(__doc__)
    accepted, reason = rc_unknown_acceptable(load_results(sys.argv[2]))
    print(reason)
    sys.exit(0 if accepted else 1)
