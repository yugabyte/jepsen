#!/usr/bin/env python

#
# Copyright (c) YugaByte, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations
# under the License.
#

# This script aims to be compatible with both Python 3.


"""
A script to run multiple YugaByte DB Jepsen tests in a loop and organize the results.
"""

import argparse
import json
import logging
import os
import random
import re
import signal
import socket
import subprocess
from collections import namedtuple

import atexit
import errno
import sys
import time
from itertools import zip_longest, chain

import requests
from junit_xml import TestCase, TestSuite, to_xml_report_string

CmdResult = namedtuple('CmdResult',
                       ['output',
                        'returncode',
                        'timed_out',
                        'everything_looks_good',
                        'cycle_search_timeout_only',
                        'has_valid_unknown'])


def is_cycle_search_timeout_only(lines):
    """
    Check if test output indicates :valid? :unknown with only :cycle-search-timeout anomalies.
    This is acceptable for rc.ol workloads where Elle's cycle search times out but finds no
    actual consistency violations.

    Output format example:
        :anomaly-types (:cycle-search-timeout),
        :anomalies {:cycle-search-timeout [{:type :cycle-search-timeout, ...}]}
    """
    has_valid_unknown = False
    has_cycle_search_timeout = False
    has_other_anomalies = False

    # Real anomaly types that indicate actual consistency violations
    real_anomalies = [':G0', ':G1a', ':G1b', ':G1c', ':G1', ':G2', ':G-single', ':G-nonadjacent',
                      ':dirty-update', ':lost-update', ':internal', ':incompatible-order']

    for line in lines:
        if ':valid? :unknown' in line:
            has_valid_unknown = True
        if ':cycle-search-timeout' in line:
            has_cycle_search_timeout = True
        # Check for actual anomalies in :anomaly-types line
        if ':anomaly-types' in line:
            for anomaly in real_anomalies:
                if anomaly in line:
                    has_other_anomalies = True
                    break

    return has_valid_unknown and has_cycle_search_timeout and not has_other_anomalies

# Only for workload, doesn't include test results analysis. Customized for the "set" test.
SINGLE_TEST_RUN_TIME = 600

# The set test might time out if you let it run for 10 minutes and leave 10 more
# minutes for analysis, so cut its running time in half.
SINGLE_TEST_RUN_TIME_FOR_SET_TEST = 300
SINGLE_TEST_RUN_TIME_FOR_RC_APPEND_TEST = 300

# Includes test results analysis. The rc/si workloads (wr, upsert, types,
# monotonic, g2) produce histories whose Elle analysis alone can take longer
# than the test itself: a valid ysql/si.wr + partition run has been observed
# to need ~25 min end to end against the old 20-min budget, getting a passing
# run binned as timed-out. Only binds when a run actually overruns, so the
# extra headroom costs nothing on healthy runs.
TEST_AND_ANALYSIS_TIMEOUT_SEC = 2400
DEFAULT_TARBALL_URL = "https://downloads.yugabyte.com/yugabyte-1.3.1.0-linux.tar.gz"

TEST_PER_VERSION = [
    {
        "start_version": "1.3.1.0",
        "tests": [
            # YCQL snapshot isolation
            "ycql/counter",
            "ycql/set",
            "ycql/set-index",
            "ycql/bank",
            "ycql/long-fork",
            "ycql/single-key-acid",
            "ycql/multi-key-acid",
            # Disabled https://github.com/yugabyte/yugabyte-db/issues/10328
            # Related to multipage index scan https://github.com/yugabyte/yugabyte-db/issues/13502
            # "ycql/bank-inserts",

            # YSQL serializable
            "ysql/sz.counter",
            "ysql/sz.set",
            "ysql/sz.bank-contention",
            "ysql/sz.bank-multitable",
            "ysql/sz.long-fork",
            "ysql/sz.single-key-acid",
            "ysql/sz.multi-key-acid",
            "ysql/sz.default-value",

            # YSQL snapshot isolation
            "ysql/si.bank-contention",
            "ysql/si.bank-multitable",
            "ysql/si.counter",
            "ysql/si.set",
        ]
    },
    {
        # RC pessimistic locking available since 2.15
        "start_version": "2.15.0.0-b1",
        "tests": [
            "ysql/rc.append",
        ]
    },
    {
        # SI pessimistic locking available since 2.17.2
        "start_version": "2.17.2.0-b1",
        "tests": [
            "ysql/si.append",
        ]
    },
    {
        "start_version": "2.18.0.0-b1",
        "tests": [
            "ysql/rc.geo.append",
            "ysql/si.geo.append",
            "ysql/sz.geo.append",
        ]
    },
    {
        # SZ pessimistic locking available since 2.20
        "start_version": "2.20.0.0-b1",
        "tests": [
            "ysql/sz.append",
        ]
    },
    {
        "start_version": "2.29.0.0-b500",
        # Skip append-table tests for 2026.1
        "start_version_stable": "2026.2.0.0-b1",
        "tests": [
            "ysql/sz.append-table",
            "ysql/si.append-table",
            "ysql/rc.append-table",
        ]
    },
    {
        # Enrichment workloads (rc/si focus, plus a couple of anomaly tests that
        # only bite at their native isolation level):
        #   wr        - Elle write-read register (complements list-append)
        #   upsert    - INSERT ... ON CONFLICT / LWT uniqueness under contention
        #   types     - numeric boundary round-trip (overflow / truncation)
        #   monotonic - per-session monotonic reads
        #   g2        - Adya predicate write-skew (serializable only)
        #   long-fork - snapshot-isolation long-fork anomaly (also at si)
        "start_version": "2.20.0.0-b1",
        "tests": [
            "ysql/rc.wr",
            "ysql/si.wr",
            "ysql/rc.upsert",
            "ysql/si.upsert",
            "ysql/rc.types",
            "ysql/si.types",
            "ysql/rc.monotonic",
            "ysql/si.monotonic",
            "ysql/sz.g2",
            "ysql/si.long-fork",
            "ycql/upsert",
            "ycql/types",
            "ycql/monotonic",
        ]
    }
]
NEMESES = [
    "none",
    "kill-tserver",
    "kill-master",
    "pause-tserver",
    "pause-master",
    "partition",
    # "clock-skew",
]
TESTS = list(chain(*[test["tests"] for test in TEST_PER_VERSION]))

SCRIPT_DIR = os.path.abspath(os.path.dirname(sys.argv[0]))
STORE_DIR = os.path.join(SCRIPT_DIR, "store")
LOGS_DIR = os.path.join(SCRIPT_DIR, "logs")
SORT_RESULTS_SH = os.path.join(SCRIPT_DIR, "sort-results.sh")
REGEX_MAJOR_VERSION = r"^(\d+)\.(\d+)"

child_processes = []


def is_stable_version(version):
    """Check if version uses the stable/production format (2024.x, 2025.x, etc.)
    Master versions use 2.x format (e.g. 2.29.0.0), stable use year-based (e.g. 2025.2.0.0)."""
    first = int(re.split(r'\.|-b', version)[0])
    return first >= 2024


def get_workload_version(workload, target_version=None):
    """Get the minimum version for a workload. When target_version is a stable/production
    release and the workload has a start_version_stable, use that instead of the master
    start_version."""
    for el in TEST_PER_VERSION:
        for tests in el["tests"]:
            if workload in tests:
                if target_version and is_stable_version(target_version) and "start_version_stable" in el:
                    return el["start_version_stable"]
                return el["start_version"]
    raise EnvironmentError(f"Unable to find workload in tests: {TESTS}")


def is_version_at_least(v_least, v_actual):
    v_least_split = re.split('\.|-b', v_least)
    v_actual_split = re.split('\.|-b', v_actual)
    return next((i < j
                 for i, j in zip_longest(map(int, v_least_split), map(int, v_actual_split),
                                         fillvalue=0) if i != j), True)


def kill_process_tree(p):
    """Kill a run_cmd child and everything it spawned. Commands run via
    shell=True, so p.pid is the shell's pid: a bare p.kill() kills only the
    shell and orphans the real work (lein -> JVM), which keeps running - a
    'timed out' jepsen run would then finish analysis and write results
    minutes after we declared it dead, and could still be chewing on the
    cluster while the next test starts. run_cmd starts children in their own
    session (start_new_session), so the process group id is the shell's pid
    and killpg takes down the whole tree."""
    try:
        os.killpg(p.pid, signal.SIGKILL)
    except (OSError, ProcessLookupError) as e:
        if isinstance(e, OSError) and e.errno not in (errno.ESRCH, errno.EPERM):
            raise
        try:
            p.kill()
        except OSError as e2:
            if e2.errno != errno.ESRCH:
                raise


def cleanup():
    deadline = time.time() + 5
    for p in child_processes:
        while p.poll() is None and time.time() < deadline:
            time.sleep(1)
        kill_process_tree(p)


def truncate_line(line, max_chars=500):
    if len(line) <= max_chars:
        return line
    res_candidate = line[:max_chars] + "... (skipped %d chars)" % (len(line) - max_chars)

    return line if len(line) <= len(res_candidate) else res_candidate


def get_last_lines(file_path, n_lines):
    total_num_lines = int(subprocess.check_output(['wc', '-l', file_path]).strip().split()[0])
    return (
        subprocess.check_output(['tail', '-n', str(n_lines), file_path]).decode().split("\n"),
        total_num_lines
    )


def show_last_lines(file_path, n_lines):
    if n_lines is None:
        return
    if not os.path.exists(file_path):
        logging.warning("File does not exist: %s, cannot show last %d lines",
                        file_path, n_lines)
        return
    lines, total_num_lines = get_last_lines(file_path, n_lines)
    logging.info(
        "%s of file %s:\n%s",
        "Last %d lines" % n_lines if total_num_lines > n_lines else 'Contents',
        file_path,
        "\n".join([truncate_line(line) for line in lines])
    )


def run_cmd(cmd,
            timeout=None,
            exit_on_error=True,
            log_name_prefix=None,
            num_lines_to_show=None):
    logging.info("Running command: %s", cmd)
    stdout_path = None
    stderr_path = None
    keep_output_log_file = True
    if log_name_prefix is not None:
        stdout_path = os.path.join(LOGS_DIR, f'{log_name_prefix}_stdout.log')
        stderr_path = os.path.join(LOGS_DIR, f'{log_name_prefix}_stderr.log')
        logging.info("stdout log: %s", stdout_path)
        logging.info("stderr log: %s", stderr_path)

    stdout_file = None
    stderr_file = None

    # start_new_session puts the shell and everything under it (lein, the JVM)
    # into their own process group, so a timeout kill can take down the whole
    # tree - see kill_process_tree.
    popen_kwargs = dict(shell=True, start_new_session=True)
    try:
        if log_name_prefix is None:
            p = subprocess.Popen(cmd, **popen_kwargs)
        else:
            stdout_file = open(stdout_path, 'wb')
            stderr_file = open(stderr_path, 'wb')
            p = subprocess.Popen(cmd, stdout=stdout_file, stderr=stderr_file, **popen_kwargs)

        child_processes.append(p)

        deadline = time.time() + timeout if timeout else float('inf')
        while p.poll() is None and (timeout is None or time.time() < deadline):
            time.sleep(1)

        if p.poll() is None:
            timed_out = True
            kill_process_tree(p)
            returncode = p.wait()
        else:
            timed_out = False
            returncode = p.returncode

        child_processes.remove(p)
        if returncode != 0:
            logging.error("Failed running command (exit code: %d): %s", returncode, cmd)
            if exit_on_error:
                sys.exit(returncode)
        everything_looks_good = False
        cycle_search_timeout_only = False
        has_valid_unknown = False
        last_lines_of_output = []
        if stdout_path is not None and os.path.exists(stdout_path):
            last_lines_of_output, _ = get_last_lines(stdout_path, 50)
            everything_looks_good = any(
                line.startswith('Everything looks good!') for line in last_lines_of_output)
            if not everything_looks_good:
                cycle_search_timeout_only = is_cycle_search_timeout_only(last_lines_of_output)
                has_valid_unknown = any(
                    ':valid? :unknown' in line for line in last_lines_of_output)
        if everything_looks_good:
            keep_output_log_file = False
        return CmdResult(
            output=None if everything_looks_good else "\n".join(
                [truncate_line(line) for line in last_lines_of_output]),
            returncode=returncode,
            timed_out=timed_out,
            everything_looks_good=everything_looks_good,
            cycle_search_timeout_only=cycle_search_timeout_only,
            has_valid_unknown=has_valid_unknown)

    finally:
        if stdout_file is not None:
            stdout_file.close()
            show_last_lines(stdout_path, num_lines_to_show)
            if not keep_output_log_file:
                try:
                    os.remove(stdout_path)
                except IOError as ex:
                    logging.error("Error deleting output log %s, ignoring: %s", stdout_path, ex)
        if stderr_file is not None:
            stderr_file.close()
            show_last_lines(stderr_path, num_lines_to_show)
            if not keep_output_log_file:
                try:
                    os.remove(stderr_path)
                except IOError as ex:
                    logging.error("Error deleting stderr log %s, ignoring: %s", stderr_path, ex)


def get_ip_from_dns():
    """
    Resolves a list of DNS names to IP addresses.
    """
    dns_names = ['n1', 'n2', 'n3', 'n4', 'n5']
    ip_addresses = []
    for dns_name in dns_names:
        try:
            ip = socket.gethostbyname(dns_name)
            ip_addresses.append(ip)
        except socket.gaierror:
            print(f"Could not resolve DNS name: {dns_name}")
            return None  # Or handle the error differently if some names might not resolve

    return ",".join(ip_addresses)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--build_url',
        default="",
        help='Jenkins build URL')
    parser.add_argument(
        '--url',
        default=DEFAULT_TARBALL_URL,
        help='YugaByte DB tarball URL to use')
    parser.add_argument(
        '--max-time-sec',
        type=int,
        help='Maximum time to run for. The actual run time could be a few minutes longer than '
             'this.')
    parser.add_argument(
        '--test-time-sec',
        type=int,
        default=0,
        help='Test execution time.')
    parser.add_argument(
        '--reportportal_base_url',
        default="",
        help='Deprecated')
    parser.add_argument(
        '--reportportal_project_name',
        default="",
        help='Deprecated')
    parser.add_argument(
        '--reportportal_api_token',
        default="",
        help='Deprecated')
    parser.add_argument(
        '--enable-clock-skew',
        action='store_true',
        help='Enable clock skew nemesis. This will not work on LXC.')
    parser.add_argument(
        '--concurrency',
        default='6n',
        help='Concurrency to specify, e.g. 2n, 4n, or 5n, where n means the number of nodes.')
    parser.add_argument(
        '--workloads',
        default=','.join(TESTS),
        help='Comma-seperated list of workloads. Default: ' + ','.join(TESTS))
    parser.add_argument(
        '--nemeses',
        default=','.join(NEMESES),
        help='Comma-seperated list of nemeses. Default: ' + ','.join(NEMESES))
    parser.add_argument(
        '--iterations',
        type=int,
        help='Run each workload repeatedly for this many iterations.')
    parser.add_argument(
        '--locking',
        default=None,
        choices=['mixed', 'optimistic', 'pessimistic'],
        help='Locking mode for append workloads: mixed (default), optimistic, or pessimistic')
    parser.add_argument(
        '--stress-tuning',
        action='store_true',
        help='Enable stress-test flags with tiny thresholds for internal subsystems')
    parser.add_argument(
        '--random-seed',
        type=int,
        default=None,
        help='Random seed for deterministic test execution. If not provided, Jepsen generates one.')
    parser.add_argument(
        '--connection-manager',
        action='store_true',
        default=False,
        help='Force enable connection manager (overrides random selection)')
    parser.add_argument(
        '--master-flags',
        action='append',
        default=[],
        help='Extra gflag for master (repeatable): flag_name or flag_name=value')
    parser.add_argument(
        '--tserver-flags',
        action='append',
        default=[],
        help='Extra gflag for tserver (repeatable): flag_name or flag_name=value')
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(filename)s:%(lineno)d %(levelname)s] %(message)s")
    args = parse_args()

    atexit.register(cleanup)

    # Sort old results in the beginning if it did not happen at the end of the last run.
    # run_cmd(SORT_RESULTS_SH)

    start_time = time.time()
    nemeses = args.nemeses
    if args.enable_clock_skew:
        nemeses += ',clock-skew'

    num_tests_run = 0
    num_timed_out_tests = 0
    total_test_time_sec = 0

    if os.path.isdir(LOGS_DIR):
        logging.info(f"Directory {LOGS_DIR} already exists", )
    else:
        logging.info(f"Creating directory {LOGS_DIR}")
        os.mkdir(LOGS_DIR)

    test_index = 0
    num_everything_looks_good = 0
    num_not_everything_looks_good = 0
    num_zero_exit_code = 0
    num_non_zero_exit_code = 0
    url = args.url

    version = None
    for match in re.finditer(r"(?<=yugabyte-)(\d+\.\d+(\.\d+){0,2}(-b\d+)?)", url, re.MULTILINE):
        version = match.group()
        break
    if version is None:
        raise AttributeError(f"Failed to parse version from URL {url}")

    not_good_tests = []
    # need to disable connection manager forcefully for older versions
    # randomly enable for supported versions
    if args.connection_manager:
        connection_manager_flag = "--connection-manager"
        logging.info("Connection manager explicitly enabled")
    elif not (is_version_at_least("2024.1.0.0-b1", version) or
              is_version_at_least("2.25.1.0-b1", version)):
        connection_manager_flag = "--connection-manager false"
    elif random.choice([True, False]):
        connection_manager_flag = "--connection-manager"
        logging.info("Randomly enabled connection manager")
    else:
        connection_manager_flag = ""
    replication_factor = random.choice([3, 5])
    replication_factor_flag = f"--replication-factor {replication_factor}"
    logging.info("Replication factor: %d", replication_factor)

    os.environ["JAVA_HOME"] = "/usr/lib/jvm/zulu-17.jdk"
    java_version = subprocess.check_output(
        [os.path.join(os.environ["JAVA_HOME"], "bin", "java"), "-version"],
        stderr=subprocess.STDOUT).decode().strip()
    logging.info("Java version:\n%s", java_version)
    locking_flag = f"--locking {args.locking}" if args.locking else ""
    stress_flag = "--stress-tuning" if args.stress_tuning else ""
    random_seed_flag = f"--random-seed {args.random_seed}" if args.random_seed is not None else ""
    master_flags = " ".join(f"--master-flags '{f}'" for f in args.master_flags)
    tserver_flags = " ".join(f"--tserver-flags '{f}'" for f in args.tserver_flags)
    lein_cmd = " ".join(filter(None, ["lein run test",
                         "--os debian",
                         f"--url {url}",
                         f"--nemesis {nemeses}",
                         f"--nodes {get_ip_from_dns()}",
                         connection_manager_flag,
                         replication_factor_flag,
                         locking_flag,
                         stress_flag,
                         random_seed_flag,
                         master_flags,
                         tserver_flags]))

    if args.iterations:
        lein_cmd += " --test-count 1"
        iteration_cnt = args.iterations
    else:
        iteration_cnt = 1

    all_workloads = args.workloads.split(',')
    workloads_to_evaluate = [workload for workload in all_workloads
                             if is_version_at_least(get_workload_version(workload, version),
                                                    version)]
    workloads_to_skip = set(all_workloads) - set(workloads_to_evaluate)

    if not workloads_to_evaluate:
        logging.error(
            f"No workloads for evaluate have been found because of version incompatibility\n"
            f"Should be skipped: {workloads_to_skip}\n"
            f"Workloads to evaluate: {workloads_to_evaluate}")
        exit(1)

    test_cases = {}
    for test in workloads_to_evaluate:
        for iteration in range(iteration_cnt):
            total_elapsed_time_sec = time.time() - start_time
            if args.max_time_sec is not None and total_elapsed_time_sec > args.max_time_sec:
                logging.info(
                    "Elapsed time is %.1f seconds, it has exceeded the max allowed time %.1f, "
                    "stopping", total_elapsed_time_sec, args.max_time_sec)
                break

            test_index += 1
            test_description_str = f"workload {test}, nemesis {nemeses}"
            logging.info(
                "\n%s\nStarting test run #%d - %s\n%s",
                "=" * 80,
                test_index,
                test_description_str,
                "=" * 80)
            test_start_time_sec = time.time()
            if '/set' in test:
                test_run_time_limit_no_analysis_sec = SINGLE_TEST_RUN_TIME_FOR_SET_TEST if args.test_time_sec == 0 else args.test_time_sec
            elif '/rc.' in test and ('append' in test or '.wr' in test):
                # rc.wr is an Elle cycle workload like rc.append; give it the
                # same longer analysis budget so cycle search isn't cut short.
                test_run_time_limit_no_analysis_sec = SINGLE_TEST_RUN_TIME_FOR_RC_APPEND_TEST if args.test_time_sec == 0 else args.test_time_sec
            else:
                test_run_time_limit_no_analysis_sec = SINGLE_TEST_RUN_TIME if args.test_time_sec == 0 else args.test_time_sec
            if 'append-table' in test:
                concurrency = '3'
            elif '/sz.' in test:
                concurrency = '2n'
            else:
                concurrency = args.concurrency
            full_cmd = lein_cmd + \
                       f" --concurrency {concurrency}" + \
                       " --time-limit " + str(test_run_time_limit_no_analysis_sec) + \
                       " --workload " + test
            result = run_cmd(
                full_cmd,
                timeout=TEST_AND_ANALYSIS_TIMEOUT_SEC,
                exit_on_error=False,
                log_name_prefix=f"{test.replace('/', '-')}_nemesis_{nemeses}_{test_index}",
                num_lines_to_show=30)

            test_elapsed_time_sec = time.time() - test_start_time_sec
            if result.timed_out:
                jepsen_log_file = os.path.join(STORE_DIR, 'current', 'jepsen.log')
                num_timed_out_tests += 1
                logging.info("Test timed out. Updating the log at %s", jepsen_log_file)
                if os.path.exists(jepsen_log_file):
                    msg = "Test run timed out!"
                    logging.info(msg)
                    with open(jepsen_log_file, "a") as f:
                        f.write(msg)
                else:
                    logging.error("File %s does not exist!", jepsen_log_file)

            test_name = f"{test}_{nemeses}"
            tc = TestCase(name=test_name,
                          classname=test.split('/')[0],
                          elapsed_sec=test_elapsed_time_sec,
                          url=args.build_url,
                          stderr=result.output)
            logging.info(
                "Test run #%d: elapsed_time=%.1f, returncode=%d, everything_looks_good=%s",
                test_index, test_elapsed_time_sec, result.returncode,
                result.everything_looks_good)

            # For read committed workloads, accept valid-unknown results (e.g. cycle-search-timeout)
            is_rc_unknown_acceptable = (
                '/rc.' in test and
                result.has_valid_unknown and
                not result.timed_out
            )

            if result.everything_looks_good or is_rc_unknown_acceptable:
                if is_rc_unknown_acceptable:
                    logging.info("Accepting read committed test with valid-unknown result")
                num_everything_looks_good += 1

                if test_name not in test_cases:
                    test_cases[test_name] = tc
            else:
                tc.add_error_info(test_description_str)
                num_not_everything_looks_good += 1
                not_good_tests.append(test_description_str)

                if result.timed_out:
                    message = "Timed out"

                    tc.add_error_info(message)
                elif not result.everything_looks_good:
                    message = "Failure on result validation"

                    tc.add_error_info(message)
                else:
                    message = f"Process exited with error code {result.returncode}"

                    tc.add_failure_info(message, failure_type="exit code")

                # always add latest failed run for the results
                test_cases[test_name] = tc

            if result.returncode == 0:
                num_zero_exit_code += 1
            else:
                num_non_zero_exit_code += 1

            run_cmd(f"{SORT_RESULTS_SH} {nemeses}")

            logging.info(
                "\n%s\nFinished test run #%d (%s)\n%s",
                "=" * 80, test_index, test_description_str, "=" * 80)

            num_tests_run += 1
            total_test_time_sec += test_elapsed_time_sec

            total_elapsed_time_sec = time.time() - start_time
            logging.info("Finished running %d tests.", num_tests_run)
            logging.info("    %d okay, %d problems (%d timed-out)",
                         num_everything_looks_good, num_not_everything_looks_good,
                         num_timed_out_tests)
            logging.info("    %d tests (out of %d total) returned non-zero exit code",
                         num_non_zero_exit_code, num_tests_run)
            logging.info("Elapsed time: %.1f sec, test time: %.1f sec, avg test time: %.1f sec",
                         total_elapsed_time_sec, total_test_time_sec,
                         total_test_time_sec / num_tests_run)
            if not_good_tests:
                logging.info("Tests where something does not look good:\n    %s",
                             "\n    ".join(not_good_tests))
        else:
            # Next workload
            continue
        # Inner loop broken, skip remaining workloads
        break

    logging.warning(f"Skipped workloads because of version incompatibility {workloads_to_skip}")

    logging.info("Sending JUnit XML report")
    ts = TestSuite(f"Jepsen {nemeses.replace(',', '-')} {version}", test_cases.values())

    logging.info("Storing JUnit XML reports locally")
    with open(f"jepsen-junit-{nemeses.replace(',', '-')}.xml", "w") as xml_report:
        xml_report.write(to_xml_report_string([ts]))

    if not_good_tests:
        exit(1)
    else:
        exit(0)


if __name__ == '__main__':
    main()
