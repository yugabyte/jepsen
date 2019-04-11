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
    "partition",
    # "clock-skew",
]

SCRIPT_DIR = os.path.abspath(os.path.dirname(sys.argv[0]))
STORE_DIR = os.path.join(SCRIPT_DIR, "store")
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


def run_cmd(cmd, shell=True, timeout=None, exit_on_error=True):
    logging.info("Running command: %s", cmd)
    p = subprocess.Popen(cmd, shell=True)
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
    if exit_on_error and p.returncode != 0:
        logging.error(
                "Failed running command (exit code: %d): %s",
                p.returncode, cmd)
        sys.exit(p.returncode)
    return CmdResult(returncode=p.returncode, timed_out=timed_out)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--tarball-url',
        default=DEFAULT_TARBALL_URL,
        help='YugaByte DB tarball URL to use')
    parser.add_argument(
        '--max-time-sec',
        type=int,
        help="Maximum time to run for. The actual run time could a few minutes longer than this.")
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(filename)s:%(lineno)d %(levelname)s] %(message)s")
    args = parse_args()

    atexit.register(cleanup)

    run_cmd(SORT_RESULTS_SH)

    while True:
        for nemesis in NEMESES:
            for test in TESTS:
                result = run_cmd(
                    "lein run test "
                    "--os debian "
                    "--url {url} "
                    "--workload {test} "
                    "--nemesis {nemesis} "
                    "--concurrency 5n "
                    "--time-limit {run_time}".format(
                        url=args.tarball_url,
                        test=test,
                        nemesis=nemesis,
                        run_time=SINGLE_TEST_RUN_TIME
                    ),
                    timeout=TEST_TIMEOUT,
                    exit_on_error=False
                )
                if result.timed_out:
                    for root, dirs, files in os.walk(STORE_DIR):
                        for file in files:
                            if file == "jepsen.log":
                                msg = "Test run timed out!"
                                logging.info(msg)
                                with open(os.path.join(root, file), "a") as f:
                                    f.write(msg)

                run_cmd(SORT_RESULTS_SH)


if __name__ == '__main__':
    main()
