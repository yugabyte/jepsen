#!/usr/bin/env python3
"""Tests for verdict.py. Run: python3 verdict_test.py"""

import os
import tempfile
import unittest

import verdict

HERE = os.path.dirname(os.path.abspath(__file__))


def fixture(name):
    with open(os.path.join(HERE, 'test', 'verdict-fixtures', name)) as f:
        return f.read()


# Real results from jepsen_nightly #10 (2026.2.0.0-b19).
RC_TIMEOUT = fixture('rc-cycle-search-timeout.edn')  # rc.append partition
SI_INVALID = fixture('si-append-table-invalid.edn')  # si.append-table pause-master

# 2025.1 #536 rc.geo.append: no client connected, so :stats is false and Elle
# had nothing to check. The old rule passed it.
RC_ZERO_OK = (RC_TIMEOUT
              .replace(':stats {:valid? true,', ':stats {:valid? false,')
              .replace('(:cycle-search-timeout)', '(:empty-transaction-graph)')
              .replace('\n :valid? :unknown}', '\n :valid? false}'))

# 2025.2 #134 rc.wr: XOR :partial (-> :unknown). The old rule passed it.
RC_XOR_PARTIAL = (RC_TIMEOUT
                  .replace(':status :ok,', ':status :partial,')
                  .replace(':tablets "10/10",\n                   :valid? true}',
                           ':tablets "1/2",\n                   :valid? :unknown}'))


def check(text):
    return verdict.rc_unknown_acceptable(verdict.parse_edn(text))[0]


class RcAcceptanceTest(unittest.TestCase):
    def test_cycle_search_timeout_accepted(self):
        self.assertTrue(check(RC_TIMEOUT))

    def test_zero_ok_rejected(self):
        self.assertFalse(check(RC_ZERO_OK))

    def test_xor_partial_rejected(self):
        self.assertFalse(check(RC_XOR_PARTIAL))

    def test_extra_anomaly_type_rejected(self):
        self.assertFalse(check(RC_TIMEOUT.replace(
            '(:cycle-search-timeout)', '(:cycle-search-timeout :G1c)')))

    def test_timeout_that_could_violate_rc_rejected(self):
        self.assertFalse(check(RC_TIMEOUT.replace(
            ':not #{:cursor-stability :read-atomic}',
            ':not #{:cursor-stability :read-atomic :read-committed}')))

    def test_invalid_rejected(self):
        self.assertFalse(check(SI_INVALID))

    def test_missing_results_rejected(self):
        self.assertFalse(verdict.rc_unknown_acceptable(verdict.load_results('/nonexistent'))[0])


class RunDirTest(unittest.TestCase):
    def test_reads_results_and_flags(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, 'results.edn'), 'w') as f:
                f.write(SI_INVALID)
            with open(os.path.join(d, 'jepsen.log'), 'w') as f:
                f.write('x\n{:trace-cql false\n :yb-colocated true\n :connection-manager false\n')
            r = verdict.load_results(d)
            self.assertEqual(verdict.stat(r, 'ok-count'), 21469)
            self.assertEqual(verdict.xor_status(r), 'ok')
            self.assertEqual(verdict.test_flag(d, 'yb-colocated'), 'true')
            self.assertEqual(verdict.test_flag(d, 'connection-manager'), 'false')


class LowThroughputTest(unittest.TestCase):
    BASELINE = verdict.load_baseline(os.path.join(HERE, 'throughput-baseline.json'))

    def flagged(self, branch, workload, nemesis, ok):
        return verdict.low_throughput(self.BASELINE, branch, workload, nemesis, ok)[0]

    def test_known_collapses_flagged(self):
        self.assertTrue(self.flagged('master', 'ysql/si.append-table', 'partition', 413))  # 2099
        self.assertTrue(self.flagged('master', 'ysql/sz.append-table', 'partition', 107))  # 2106

    def test_normal_and_low_mode_runs_not_flagged(self):
        self.assertFalse(self.flagged('master', 'ysql/si.append-table', 'partition', 9000))
        # sz.g2 has a recurring low mode around 800 ok; not a collapse.
        self.assertFalse(self.flagged('2026.1', 'ysql/sz.g2', 'none', 838))

    def test_unknown_pair_not_flagged(self):
        self.assertFalse(self.flagged('master', 'ysql/no-such-workload', 'none', 0))

    def test_new_branch_uses_master(self):
        self.assertTrue(self.flagged('2026.2', 'ysql/si.append-table', 'partition', 413))

    def test_branch_of(self):
        self.assertEqual(verdict.branch_of('2.33.0.0-b67'), 'master')
        self.assertEqual(verdict.branch_of('2026.2.0.0-b19'), '2026.2')


if __name__ == '__main__':
    unittest.main()
