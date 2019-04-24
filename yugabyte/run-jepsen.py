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

# This script aims to be compatible with both Python 2.7 and Python 3.


"""
A script to run multiple YugaByte DB Jepsen tests in a loop and organize the results.
"""

import atexit
import errno
import os
import subprocess
import sys
import time
import logging
import argparse

from collections import namedtuple

CmdResult = namedtuple('CmdResult',
                       ['returncode',
                        'timed_out'])

SINGLE_TEST_RUN_TIME = 600  # Only for workload, doesn't include test results analysis.
TEST_TIMEOUT = 1200  # Includes test results analysis.
NODES_FILE = os.path.expanduser("~/code/jepsen/nodes")
DEFAULT_TARBALL_URL = "https://downloads.yugabyte.com/yugabyte-ce-1.2.4.0-linux.tar.gz"

TESTS = [
   "single-key-acid",
   "multi-key-acid",
   "counter-inc",
   "counter-inc-dec",
   "bank",
   "set",
   "set-index",
   "long-fork"
]
NEMESES = [
    "none",
    "stop-tserver",
    "kill-tserver",
    "pause-tserver",
    "stop-master",
    "kill-master",
    "pause-master",
    "stop",
    "kill",
    "pause",
    "partition-half",
    "partition-one",
    "partition-ring",
    "partition"
]

SCRIPT_DIR = os.path.abspath(os.path.dirname(sys.argv[0]))
STORE_DIR = os.path.join(SCRIPT_DIR, "store")
LOGS_DIR = os.path.join(SCRIPT_DIR, "logs")
SORT_RESULTS_SH = os.path.join(SCRIPT_DIR, "sort-results.sh")

child_processes = []


def cleanup():
    deadline = time.time() + 5
    for p in child_processes:
        while p.poll() is None and time.time() < deadline:
            time.sleep(1)
        try:
            p.kill()
        except OSError, e:
            if e.errno != errno.ESRCH:
                raise e


def truncate_line(line, max_chars=200):
    if len(line) <= max_chars:
        return line
    res_candidate = line[:max_chars] + "... (skipped %d bytes)" % (len(line) - max_characters)
    if len(line) <= len(res_candidate):
        return line
    return res_candidate


def show_last_lines(file_path, n_lines):
    if n_lines is None:
        return
    if not os.path.exists(file_path):
        logging.warning("File does not exist: %s, cannot show last %d lines",
                        file_path, n_lines)
        return
    total_num_lines = int(subprocess.check_output(['wc', '-l', file_path]).strip().split()[0])

    last_lines = subprocess.check_output(['tail', '-n', str(n_lines), file_path])

    logging.info(
        "%s of file %s:\n%s",
        ("Last %d lines" % n_lines if total_num_lines > n_lines else 'Contents'),
        n_lines, file_path,
        "\n".join([truncate_line(line) for line in last_lines.split("\n")])
    )


def run_cmd(cmd,
            shell=True,
            timeout=None,
            exit_on_error=True,
            log_name_prefix=None,
            keep_output_log_file=True,
            num_lines_to_show=None):

    logging.info("Running command: %s", cmd)
    stdout_path = None
    stderr_path = None
    if log_name_prefix is not None:
        timestamp_str = time.strftime('%Y-%m-%dT%H-%M-%S')
        log_name_prefix += '_' + timestamp_str
        stdout_path = os.path.join(LOGS_DIR, log_name_prefix + '_stdout.log')
        stderr_path = os.path.join(LOGS_DIR, log_name_prefix + '_stderr.log')
        logging.info("stdout log: %s, stderr log: %s", stdout_path, stderr_path)

    stdout_file = None
    stderr_file = None

    popen_kwargs = dict(shell=True)
    try:
        if log_name_prefix is None:
            p = subprocess.Popen(cmd, **popen_kwargs)
        else:
            stdout_file = open(stdout_path, 'wb')
            stderr_file = open(stderr_path, 'wb')
            p = subprocess.Popen(cmd, stdout=stdout_file, stderr=stderr_file, **popen_kwargs)

        child_processes.append(p)

        if timeout:
            deadline = time.time() + timeout
        while p.poll() is None and (timeout is None or time.time() < deadline):
            time.sleep(1)

        if p.poll() is None:
            timed_out = True
            p.terminate()
        else:
            timed_out = False

        child_processes.remove(p)
        if p.returncode != 0:
            logging.error("Failed running command (exit code: %d): %s", p.returncode, cmd)
            if exit_on_error:
                sys.exit(p.returncode)
        return CmdResult(returncode=p.returncode, timed_out=timed_out)

    finally:
        if stdout_file is not None:
            stdout_file.close()
            show_last_lines(stdout_path, num_lines_to_show)
            if not keep_output_log_file:
                try:
                    os.remove(stdout_path)
                except IOError, ex:
                    logging.error("Error deleting output log %s, ignoring: %s",
                                  stdout_path, ex)
        if stderr_file is not None:
            stderr_file.close()
            show_last_lines(stderr_path, num_lines_to_show)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--tarball-url',
        default=DEFAULT_TARBALL_URL,
        help='YugaByte DB tarball URL to use')
    parser.add_argument(
        '--max-time-sec',
        type=int,
        help='Maximum time to run for. The actual run time could a few minutes longer than this.')
    parser.add_argument(
        '--enable-clock-skew',
        action='store_true',
        help='Enable clock skew nemesis. This will not work on LXC.')
    parser.add_argument(
        '--concurrency',
        default='4n',
        help='Concurrency to specify, e.g. 2n, 4n, or 5n, where n means the number of nodes.')
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(filename)s:%(lineno)d %(levelname)s] %(message)s")
    args = parse_args()

    atexit.register(cleanup)

    # Sort old results in the beginning if it did not happen at the end of the last run.
    run_cmd(SORT_RESULTS_SH)

    start_time = time.time()
    nemeses = NEMESES
    if args.enable_clock_skew:
        nemeses += ['clock-skew']

    num_tests_run = 0
    total_test_time_sec = 0

    is_done = False
    if os.path.isdir(LOGS_DIR):
        logging.info("Directory %s already exists", LOGS_DIR)
    else:
        logging.info("Creating directory %s", LOGS_DIR)
        os.mkdir(LOGS_DIR)

    while not is_done:
        for nemesis in nemeses:
            if is_done:
                break
            for test in TESTS:
                elapsed_time_sec = time.time() - start_time
                if args.max_time_sec is not None and elapsed_time_sec > args.max_time_sec:
                    logging.info(
                        "Elapsed time is %.1f seconds, it has exceeded the max allowed time %.1f, "
                        "stopping", elapsed_time_sec, args.max_time_sec)
                    is_done = True
                    break

                test_start_time_sec = time.time()
                result = run_cmd(
                    " ".join([
                        "lein run test",
                        "--os debian",
                        "--url " + args.tarball_url,
                        "--workload " + test,
                        "--nemesis " + nemesis,
                        "--concurrency " + args.concurrency,
                        "--time-limit " + str(TEST_TIMEOUT)
                    ]),
                    timeout=TEST_TIMEOUT,
                    exit_on_error=False,
                    log_name_prefix="{}_nemesis_{}".format(test, nemesis),
                    keep_output_log_file=False,
                    num_lines_to_show=50
                )

                if result.timed_out:
                    logging.info("Test timed out. Updating all jepsen.log files in %s", STORE_DIR)
                    for root, dirs, files in os.walk(STORE_DIR):
                        for file_name in files:
                            if file_name == "jepsen.log":
                                msg = "Test run timed out!"
                                logging.info(msg)
                                with open(os.path.join(root, file_name), "a") as f:
                                    f.write(msg)

                run_cmd(SORT_RESULTS_SH)
                test_elapsed_time_sec = time.time() - test_start_time_sec

                num_tests_run += 1
                total_test_time_sec += test_elapsed_time_sec

                logging.info(
                    "Finished running %d tests, total test time: %.1f sec, avg test time: %.1f",
                    num_tests_run, total_test_time_sec, total_test_time_sec / num_tests_run)


if __name__ == '__main__':
    main()
