#!/usr/bin/env python3

# ===================================================================================
# YugabyteDB Leader/Follower Consistency Checker
#
# Description:
# This script connects to a YugabyteDB cluster and checks consistency between
# leader reads and follower reads using yb-ts-cli based approach (RPC).
# For SQL-based approach with yb_hash_code(), use consistency_check.py
#
# Prerequisites:
#   - Python 3.6+
#   - DB Version 2.29.0.0, 2025.2.2, 2025.1.4, 2024.2.8 or higher
#   - yb-admin and yb-ts-cli binaries in PATH or specified with --tools-path
#   - TLS certificates in specified directory with --certs-dir (optional)
#   - Connectivity to all universe nodes
#
# Usage:
#   # Scan all tables in database (auto-detect table types)
#   # Using default master port (7100)
#   python follower_consistency_check_v2.py \
#       --master_leader_address 172.151.28.192 \
#       --database mydb \
#       --scan-all
#
#   # Scan all tables across ALL databases
#   python follower_consistency_check_v2.py \
#       --master_leader_address 172.151.28.192 \
#       --scan-all-databases
#
#   # Scan tables matching a pattern
#   python follower_consistency_check_v2.py \
#       --master_leader_address 172.151.28.192:7100 \
#       --database mydb \
#       --scan-all \
#       --tables-filter "test_cdc_.*"
#
#   # Explicit table specification
#   python follower_consistency_check_v2.py \
#       --master_leader_address 172.151.28.192:7100 \
#       --database mydb \
#       --table tbl_1 \
#       --table schema2.tbl_2
#
#   # Both tools-path and certs-dir
#   python follower_consistency_check_v2.py \
#       --master_leader_address 10.12.16.169:7100 \
#       --tools-path ./master/bin/ \
#       --certs-dir /home/yugabyte/yugabyte-client-tls-config \
#       --database yugabyte \
#       --scan-all
#
# ===================================================================================

import sys
import os
import io
import re
import json
import shlex
import argparse
import random
import subprocess
import shutil
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from datetime import datetime, timezone
from traceback import format_exc
from collections import defaultdict
from typing import List, Optional, Tuple, Dict

SCRIPT_VERSION = "2.2"

# A dump that timed out (its own deadline, or the run's remaining budget) leaves that
# tablet uncovered. That is missing coverage, never evidence of divergence, and never
# proof of a sick tserver — so it is kept out of the query-error bucket.
UNCOVERED = "not covered: dump timed out"
TIMEOUT_MARKERS = ("timed out", "deadline for query passed", "operation expired")

COLOR_GREEN = '\033[0;32m'
COLOR_RED = '\033[0;31m'
COLOR_YELLOW = '\033[1;33m'
COLOR_BLUE = '\033[0;34m'
COLOR_CYAN = '\033[0;36m'
COLOR_NC = '\033[0m'


class TableConfig:
    """Configuration for a table to check."""

    def __init__(self, table_name: str, table_id: str, schema_name: str = "public"):
        self.table_name = table_name
        self.table_id = table_id
        self.schema_name = schema_name


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='YugabyteDB Leader/Follower Consistency Checker',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Scan all tables (auto-detect primary keys)
  %(prog)s --master_leader_address 10.0.0.1:7100 --database mydb --scan-all

  # Scan all tables across ALL databases
  %(prog)s --master_leader_address 10.0.0.1:7100 --scan-all-databases

  # Scan tables matching a regex pattern
  %(prog)s --master_leader_address 10.0.0.1:7100 --database mydb --scan-all --tables-filter "orders_.*"

  # Using default master port (7100)
  %(prog)s --master_leader_address 10.0.0.1 --database mydb --scan-all

  # Check single table (explicit mode)
  %(prog)s --master_leader_address 10.0.0.1:7100 --database mydb --table users.id

  # Check multiple tables
  %(prog)s --master_leader_address 10.0.0.1:7100 --database cdc_db --table test_cdc_1 --table schema2.tbl_2

  # With tools-path (yb-admin, yb-ts-cli in custom directory)
  %(prog)s --master_leader_address 10.12.16.169:7100 --tools-path ./master/bin/ --database yugabyte --scan-all

  # With TLS certificates (secure cluster)
  %(prog)s --master_leader_address 10.12.16.169:7100 --certs-dir /home/yugabyte/yugabyte-client-tls-config --database yugabyte --table tbl1

  # Both tools-path and certs-dir
  %(prog)s --master_leader_address 10.12.16.169:7100 --tools-path ./master/bin/ --certs-dir /home/yugabyte/yugabyte-client-tls-config --database yugabyte --scan-all

Table format: TABLE_NAME
  - TABLE_NAME: Name of the table to check
  - SCHEMA_NAME: Schema name of the table (default: public)
        '''
    )

    parser.add_argument(
        '--master_leader_address', '-m',
        required=True,
        metavar='ADDRESS',
        help='Address of any active Master node (host or host:7100).'
    )

    parser.add_argument(
        '--database', '-d',
        help='Database name (required unless --scan-all-databases is used)'
    )

    parser.add_argument(
        '--scan-all-databases',
        action='store_true',
        help='Apply the scan to all databases. When used without --table, '
             'it implies --scan-all.'
    )

    parser.add_argument(
        '--table', '-t',
        action='append',
        dest='tables',
        metavar='TABLE_SPEC',
        help='Table name with optional schema prefix (e.g., "schema.table" or "table")'
        'Can be specified multiple times.'
    )

    parser.add_argument(
        '--scan-all',
        action='store_true',
        help='Scan all tables in the database (auto-detects primary keys)'
    )

    parser.add_argument(
        '--tables-filter',
        metavar='REGEX',
        help='Regex pattern to filter table names when using --scan-all (e.g., "test_.*" or "orders_\\d+")'
    )

    parser.add_argument(
        '--print-queries',
        action='store_true',
        help='Print each SQL query before executing (can be very verbose in tablet-level mode)'
    )

    parser.add_argument(
        '--tools-path',
        metavar='DIR',
        help='Optional directory containing yb-admin and yb-ts-cli binaries'
    )

    parser.add_argument(
        '--certs-dir',
        metavar='DIR',
        help='Optional directory containing TLS certificates (--certs_dir_name for yb-admin and yb-ts-cli)'
    )

    parser.add_argument(
        '--tserver-rpc-port',
        type=int,
        default=9100,
        help='yb-tserver RPC port for yb-ts-cli (default: 9100)'
    )

    parser.add_argument(
        '--read-ht',
        type=int,
        default=None,
        metavar='HT',
        help='Hybrid time for point-in-time consistency (default: current UTC). Use same value for all tablets'
    )

    parser.add_argument(
        '--per-tablet-read-ht',
        action='store_true',
        help='Capture a fresh hybrid time per tablet instead of once per table. The check is '
             'per-tablet cross-peer (it never relies on cross-tablet point-in-time consistency), so a '
             'per-tablet read_ht is valid and avoids the fixed read_ht slipping below a short '
             'timestamp_history_retention_interval_sec during a multi-tablet scan. Ignored when --read-ht is given.'
    )

    parser.add_argument(
        '--admin-timeout',
        type=int,
        default=120,
        metavar='SECONDS',
        help='Timeout in seconds for yb-admin commands like list_tables and list_tablets (default: 120)'
    )

    parser.add_argument(
        '--ts-cli-timeout',
        type=int,
        default=120,
        metavar='SECONDS',
        help='Timeout in seconds for yb-ts-cli dump_tablet_data subprocess (default: 120)'
    )

    parser.add_argument(
        '--dump-retries',
        type=int,
        default=2,
        metavar='N',
        help='Retries for a transient yb-ts-cli dump_tablet_data failure per peer (default: 2)'
    )

    parser.add_argument(
        '--admin-retries',
        type=int,
        default=2,
        metavar='N',
        help='Retries for a failed yb-admin list_tables/list_tablets call (default: 2)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=1,
        metavar='N',
        help='Number of tables to check in parallel (default: 1)'
    )

    parser.add_argument(
        '--tablet-workers',
        type=int,
        default=1,
        metavar='N',
        help='Number of tablets to check in parallel within one table (default: 1)'
    )

    parser.add_argument(
        '--max-concurrent-dumps',
        type=int,
        default=24,
        metavar='N',
        help='Upper bound on yb-ts-cli dumps in flight across the whole run, so '
             '--workers x --tablet-workers x replicas cannot overwhelm the tservers '
             '(default: 24)'
    )

    parser.add_argument(
        '--min-dump-seconds',
        type=int,
        default=60,
        metavar='SECONDS',
        help='Do not start a tablet dump when less than this much --max-runtime budget '
             'is left; the tablet is reported as unchecked instead of as an error '
             '(default: 60)'
    )

    parser.add_argument(
        '--shuffle-seed',
        type=int,
        default=0,
        metavar='N',
        help='Shuffle table and tablet order with this seed so a budget-limited run '
             'covers a different slice each time (0 = keep discovery order, default: 0)'
    )

    parser.add_argument(
        '--max-runtime',
        type=int,
        default=0,
        metavar='SECONDS',
        help='Stop checking new tables and tablets after this many seconds and print a '
             'partial summary with exit code 3 (0 = no limit, default: 0)'
    )

    args = parser.parse_args()

    if not args.database and not args.scan_all_databases:
        parser.error("--database or --scan-all-databases is required")

    if args.scan_all_databases and not args.tables:
        # --scan-all-databases implies --scan-all unless explicit tables are provided
        args.scan_all = True

    if not args.tables and not args.scan_all:
        parser.error("Either --table or --scan-all must be specified")

    if args.tables_filter and not args.scan_all:
        parser.error("--tables-filter can only be used with --scan-all")

    return args


def get_current_hybrid_time() -> int:
    """
    Get the current hybrid time from UTC now using YugabyteDB's encoding.
    
    Formula: int(utc_timestamp_microseconds) << 12
    Same encoding as encodeHT.py for datetime-to-hybrid-time conversion.
    
    Returns:
        Current hybrid time as integer
    """
    x = datetime.now(timezone.utc)
    return int(x.timestamp() * 1_000_000) << 12




def find_yb_ts_cli(tools_path: Optional[str] = None) -> Optional[str]:
    """
    Find yb-ts-cli binary in tools_path, PATH, or current directory.

    Args:
        tools_path: Optional directory containing yb-ts-cli

    Returns:
        Path to yb-ts-cli binary or None if not found
    """
    if tools_path:
        path = os.path.join(tools_path, "yb-ts-cli")
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    yb_ts_cli = shutil.which("yb-ts-cli")
    if yb_ts_cli:
        return yb_ts_cli
    local_path = os.path.join(os.getcwd(), "yb-ts-cli")
    if os.path.isfile(local_path) and os.access(local_path, os.X_OK):
        return local_path
    return None


def find_yb_admin(tools_path: Optional[str] = None) -> Optional[str]:
    """
    Find yb-admin binary in tools_path, PATH, or current directory.

    Args:
        tools_path: Optional directory containing yb-admin

    Returns:
        Path to yb-admin binary or None if not found
    """
    if tools_path:
        path = os.path.join(tools_path, "yb-admin")
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    yb_admin = shutil.which("yb-admin")
    if yb_admin:
        return yb_admin
    local_path = os.path.join(os.getcwd(), "yb-admin")
    if os.path.isfile(local_path) and os.access(local_path, os.X_OK):
        return local_path
    return None


def parse_master_leader_address(addr: str, default_port: int = 7100) -> Tuple[str, int]:
    """
    Parse master leader address string.
    
    Supports: "host" or "host:port"
    Returns: (host, port)
    """
    addr = addr.strip()
    if ':' in addr:
        parts = addr.rsplit(':', 1)
        if len(parts) == 2 and parts[1].isdigit():
            return parts[0], int(parts[1])
    return addr, default_port


class ConsistencyChecker:
    """
    YugabyteDB leader/follower consistency checker using yb-ts-cli RPC.
    Configuration and state are stored as member variables.
    """

    def __init__(self, args):
        self.db_version = None
        self.print_queries = args.print_queries
        self.master_leader_address = args.master_leader_address
        self.database = args.database
        self.tools_path = args.tools_path
        self.certs_dir = args.certs_dir
        self.tables = args.tables or []
        self.scan_all = args.scan_all
        self.scan_all_databases = args.scan_all_databases
        self.tables_filter = args.tables_filter
        self.tserver_rpc_port = args.tserver_rpc_port
        self.read_ht: Optional[int] = args.read_ht
        self.per_tablet_read_ht: bool = args.per_tablet_read_ht
        self.admin_timeout: int = args.admin_timeout
        self.ts_cli_timeout: int = args.ts_cli_timeout
        self.dump_retries: int = args.dump_retries
        self.admin_retries: int = args.admin_retries
        self.workers: int = max(1, args.workers)
        self.tablet_workers: int = max(1, args.tablet_workers)
        self.min_dump_seconds: int = max(1, args.min_dump_seconds)
        self.shuffle_seed: int = args.shuffle_seed
        self.max_runtime: int = args.max_runtime
        self._dump_slots = threading.Semaphore(max(1, args.max_concurrent_dumps))
        self._deadline: Optional[float] = None
        self.tablets_checked = 0
        self.tablets_total = 0
        self._partial_tables: Dict[str, Tuple[int, int]] = {}
        self._counter_lock = threading.Lock()

        self.yb_admin_path: Optional[str] = None
        self.yb_ts_cli_path: Optional[str] = None

        # table display name -> count of tablets skipped because their raft
        # config has a single peer (e.g. tablespace with num_replicas=1).
        self._skip_lock = threading.Lock()
        self._single_replica_skips: Dict[str, int] = {}

    def out_of_time(self) -> bool:
        return self._deadline is not None and time.monotonic() > self._deadline

    def _dump_budget(self) -> Optional[int]:
        """Seconds this dump may run, or None when too little budget is left to bother.

        A dump that cannot finish inside the remaining budget only burns a slot another
        tablet could have used, so anything under --min-dump-seconds is not started.
        """
        if self._deadline is None:
            return self.ts_cli_timeout
        remaining = int(self._deadline - time.monotonic())
        if remaining < self.min_dump_seconds:
            return None
        return max(1, min(self.ts_cli_timeout, remaining))

    @staticmethod
    def parse_table_arg(table_arg: str) -> Tuple[str, str]:
        """
        Parse a table argument in format: TABLE_NAME
        or just TABLE_NAME for table-level checks.

        Examples:
            my_table                        - Table-level check (auto-detect type)
            schema.my_table                 - Schema-qualified table name
        """

        if '.' in table_arg:
            schema_name, table_name = table_arg.split('.', 1)
        else:
            schema_name = 'public'
            table_name = table_arg

        return (table_name, schema_name)

    def _run_subprocess(self, cmd, timeout=None, **kwargs):
        """Run subprocess with Python 3.6-compatible defaults. Captures output as text.

        Args:
            cmd: Command to run (list or str).
            timeout: Optional timeout in seconds.
            **kwargs: Additional args passed to subprocess.run (override defaults).

        Returns:
            subprocess.CompletedProcess with stdout/stderr as str.
        """
        run_kwargs = {
            'stdout': subprocess.PIPE,
            'stderr': subprocess.PIPE,
            'universal_newlines': True,
        }
        if timeout is not None:
            run_kwargs['timeout'] = timeout
        run_kwargs.update(kwargs)
        if self.print_queries:
            cmd_str = ' '.join(shlex.quote(str(arg)) for arg in cmd) if isinstance(cmd, (list, tuple)) else str(cmd)
            print(f"{COLOR_CYAN}[DEBUG] {cmd_str}{COLOR_NC}")
        return subprocess.run(cmd, **run_kwargs)

    def _certs_args(self) -> List[str]:
        """Return args to pass --certs_dir_name when certs_dir is set."""
        if self.certs_dir:
            return ["--certs_dir_name", self.certs_dir]
        return []

    def _yb_admin_cmd_base(self) -> List[str]:
        master_host, master_port = parse_master_leader_address(self.master_leader_address, 7100)
        master_rpc = f"{master_host}:{master_port}"

        return [self.yb_admin_path, "--master_addresses", master_rpc] + self._certs_args()
    
    def _yb_ts_cli_cmd_base(self) -> List[str]:
        return [self.yb_ts_cli_path] + self._certs_args()


    def _is_transient_error(self, text: str) -> bool:
        """True if an error string looks like a transient/retryable cluster condition."""
        low = (text or "").lower()
        patterns = (
            "no route to host",
            "not the leader",
            "leader not ready",
            "timed out",
            "timeout",
            "connection refused",
            "connection reset",
            "service unavailable",
            "network error",
            "broken pipe",
            "could not find the master leader",
            "try again",
            "shutting down",
        )
        return any(p in low for p in patterns)

    def _run_admin_checked(self, cmd, what: str):
        """Run a yb-admin command, retrying on failure/timeout. Raises RuntimeError after retries."""
        last_err = ""
        for attempt in range(self.admin_retries + 1):
            try:
                result = self._run_subprocess(cmd, timeout=self.admin_timeout)
                if result.returncode == 0:
                    return result
                last_err = result.stderr.strip() if result.stderr else result.stdout.strip()
            except subprocess.TimeoutExpired:
                last_err = f"timed out ({self.admin_timeout}s)"
            if attempt < self.admin_retries:
                print(f"{COLOR_YELLOW}yb-admin {what} attempt {attempt + 1} failed ({last_err}); retrying...{COLOR_NC}")
                time.sleep(min(5, 2 ** attempt))
        raise RuntimeError(f"yb-admin {what} failed after {self.admin_retries + 1} attempt(s): {last_err}")

    def _check_one_peer(
            self,
            endpoint: str,
            tablet_id: str,
            read_ht: int
    ) -> Tuple[str, Dict]:
        """Run yb-ts-cli dump_tablet_data for one peer, retrying transient failures.

        Returns (endpoint, result_dict).
        """
        last_error = "yb-ts-cli failed"

        for attempt in range(self.dump_retries + 1):
            try:
                # Budget-bounded, and measured after the semaphore hands over a slot:
                # a thread can queue for minutes, and a deadline computed before the
                # wait would let the dump run long past the caller's SSH timeout,
                # which kills the process before it prints FINAL SUMMARY.
                with self._dump_slots:
                    dump_timeout = self._dump_budget()
                    if dump_timeout is None:
                        return endpoint, {'error': UNCOVERED}

                    # Keep yb-ts-cli's own RPC deadline just under the subprocess wall-timeout so it
                    # returns a clean error instead of being SIGKILLed by the subprocess timeout.
                    internal_ms = str(max(1000, (dump_timeout - 5) * 1000))
                    cmd = self._yb_ts_cli_cmd_base() + [
                        "--server_address",
                        endpoint,
                        "--timeout_ms",
                        internal_ms,
                        "dump_tablet_data",
                        tablet_id,
                        "HASH_ONLY",
                        str(read_ht),
                    ]
                    result = self._run_subprocess(cmd, timeout=dump_timeout)

                if result.returncode != 0:
                    error_msg = result.stderr.strip() if result.stderr else result.stdout.strip()
                    lowered = error_msg.lower()
                    if any(marker in lowered for marker in TIMEOUT_MARKERS):
                        return endpoint, {'error': UNCOVERED}
                    last_error = f"yb-ts-cli failed: {error_msg}"
                    if self._is_transient_error(error_msg) and attempt < self.dump_retries:
                        time.sleep(min(5, 2 ** attempt))
                        continue
                    return endpoint, {'error': last_error}

                output = result.stdout
                row_count = None
                xor_hash = None

                for line in output.split('\n'):
                    line = line.strip()
                    if line.startswith('Row count:'):
                        row_count = int(line.split(':', 1)[1].strip())
                    elif line.startswith('XOR hash:'):
                        xor_hash = int(line.split(':', 1)[1].strip())

                if row_count is None or xor_hash is None:
                    return endpoint, {'error': f"Failed to parse yb-ts-cli output: {output}"}

                return endpoint, {'row_count': row_count, 'xor_hash': xor_hash}

            except subprocess.TimeoutExpired:
                return endpoint, {'error': UNCOVERED}
            except Exception as e:
                return endpoint, {'error': f'yb-ts-cli error: {str(e)}'}

        return endpoint, {'error': last_error}

    def _check_tablet_consistency(
            self,
            tablet_id: str,
            read_ht: int,
            endpoints: List[str]
    ) -> Tuple[bool, Dict[str, Dict]]:
        """Check consistency for a single tablet using yb-ts-cli dump_tablet_data.
        Runs commands across peers in parallel.

        Args:
            tablet_id: Tablet ID to check
            read_ht: Hybrid time for point-in-time consistency across all tablets
            endpoints: List of "host:port" from tablet JSON (leader + followers)
        """
        results = {}
        has_error = False

        with ThreadPoolExecutor(max_workers=len(endpoints)) as executor:
            futures = {
                executor.submit(self._check_one_peer, ep, tablet_id, read_ht): ep
                for ep in endpoints
            }
            for future in as_completed(futures):
                endpoint, result = future.result()
                results[endpoint] = result
                if 'error' in result:
                    has_error = True

        return has_error, results

    def get_tablets(self, table_id: str) -> List[Dict]:
        """
        Get tablet list using yb-admin list_tablets.
        
        Returns list of dicts with keys: id, partition_debug_string
        """
        cmd = self._yb_admin_cmd_base() + [
            "list_tablets",
            f"tableid.{table_id}",
            # max_tablets: yb-admin DEFAULTS TO 10 when this positional is omitted,
            # silently limiting the consistency check to the first 10 tablets of the
            # table. Pass 0 to enumerate ALL tablets.
            "0",
            "JSON",
            "include_followers"
        ]

        result = self._run_admin_checked(cmd, "list_tablets")

        data = json.loads(result.stdout)
        tablets = data.get("tablets", [])

        # Example output:
        # {
        #     "tablets": [
        #         {
        #             "id": "9b69d1aef691499a9358e354b16c03da",
        #             "partition_debug_string": "range: [<start>, DocKey([], [10]))",
        #             "leader": {
        #                 "uuid": "41c8fb2c028a4356995dcdbd0b1fc2cc",
        #                 "endpoint": "127.0.0.1:9100",
        #                 "role": "LEADER"
        #             },
        #             "followers": [
        #                 {
        #                     "uuid": "4c6d5d39841d4dd0a2a770d033fb3219",
        #                     "endpoint": "127.0.0.2:9100",
        #                     "role": "FOLLOWER"
        #                 },
        #                 {
        #                     "uuid": "0dcfde7b5c084d5886626e5ed59d14d5",
        #                     "endpoint": "127.0.0.3:9100",
        #                     "role": "FOLLOWER"
        #                 }
        #             ]
        #         },
        #         {
        #             "id": "2f629f22b09c45e9bd1bad30f70ea23e",
        #             "partition_debug_string": "range: [DocKey([], [10]), DocKey([], [20]))",
        #             "leader": {
        #                 "uuid": "0dcfde7b5c084d5886626e5ed59d14d5",
        #                 "endpoint": "127.0.0.3:9100",
        #                 "role": "LEADER"
        #             },
        #             "followers": [
        #                 {
        #                     "uuid": "4c6d5d39841d4dd0a2a770d033fb3219",
        #                     "endpoint": "127.0.0.2:9100",
        #                     "role": "FOLLOWER"
        #                 },
        #                 {
        #                     "uuid": "41c8fb2c028a4356995dcdbd0b1fc2cc",
        #                     "endpoint": "127.0.0.1:9100",
        #                     "role": "FOLLOWER"
        #                 }
        #             ]
        #         }
        #     ]
        # }
        return [
            {
                "id": t["id"],
                "partition_debug_string": t.get("partition_debug_string", ""),
                "leader": t.get("leader", {}),
                "followers": t.get("followers", []),
            }
            for t in tablets
        ]

    def _check_one_tablet(self, tablet: Dict, table_read_ht: Optional[int]) -> Tuple[str, str, int, int]:
        """Check one tablet across its peers, into a private buffer.

        tablet:         tablet JSON from get_tablets()
        table_read_ht:  table-level hybrid time, or None to capture one per tablet
        Returns (output, status, row_count, xor_hash) where status is one of
        ok | mismatch | error | skip | notime.
        """
        buf = io.StringIO()
        tablet_id = tablet["id"]
        partition_info = tablet.get("partition_debug_string", "")
        peer_endpoints = []
        if tablet.get("leader", {}).get("endpoint"):
            peer_endpoints.append(tablet["leader"]["endpoint"])
        for f in tablet.get("followers", []):
            if f.get("endpoint"):
                peer_endpoints.append(f["endpoint"])

        if not peer_endpoints:
            print(f"{COLOR_YELLOW}Warning: No leader/follower endpoints for tablet {tablet_id}, skipping{COLOR_NC}\n", file=buf)
            return buf.getvalue(), 'skip', 0, 0
        peer_endpoints.sort()

        if self.out_of_time():
            return "", 'notime', 0, 0

        print("=================================================", file=buf)
        print(f"Checking Tablet: {COLOR_YELLOW}{tablet_id}{COLOR_NC}", file=buf)
        print(f"{partition_info}", file=buf)
        print(f"Peers: {peer_endpoints}", file=buf)

        # A raft config with a single peer (e.g. tablespace with
        # num_replicas=1) has nothing to compare against — skip it
        # instead of flagging an error. Distinct from the case below
        # where >=2 peers are configured but <2 responded.
        if len(peer_endpoints) < 2:
            print(f"\n  {COLOR_YELLOW}--> SKIPPED: single-replica placement "
                  f"({len(peer_endpoints)} peer in raft config), nothing to compare{COLOR_NC}\n", file=buf)
            return buf.getvalue(), 'skip', 0, 0

        # Fresh per-tablet read_ht when --per-tablet-read-ht is set; otherwise reuse the
        # table-level value chosen by the caller (explicit --read-ht or one-shot capture).
        read_ht = table_read_ht if table_read_ht is not None else get_current_hybrid_time()
        if table_read_ht is None:
            print(f"{COLOR_BLUE}Using hybrid time: {read_ht}{COLOR_NC}", file=buf)

        results = defaultdict(dict)
        _, cli_results = self._check_tablet_consistency(
            tablet_id=tablet_id,
            read_ht=read_ht,
            endpoints=peer_endpoints
        )

        for node_key, result in cli_results.items():
            if 'error' in result:
                results[node_key]['error'] = result['error']
            else:
                results[node_key]['row_count'] = result['row_count']
                results[node_key]['xor_hash'] = result['xor_hash']

        errors_found = []
        for node, res in sorted(results.items()):
            if 'error' in res:
                print(f"  {node} -> ERROR: {res['error']}", file=buf)
                errors_found.append(f"{node}: {res['error']}")

        good_peers = [n for n, r in results.items() if 'row_count' in r and 'xor_hash' in r]

        # A single peer's transient failure must not void the tablet. Only flag it
        # un-checkable when fewer than 2 replicas responded (after per-peer retries).
        if len(good_peers) < 2:
            # Peers that never ran because the budget ran out are not evidence of a
            # problem, so they must not land in the "query errors" bucket.
            if errors_found and all(UNCOVERED in e for e in errors_found):
                return "", 'notime', 0, 0
            print(f"\n  {COLOR_YELLOW}--> COULD NOT VERIFY: fewer than 2 replicas responded{COLOR_NC}", file=buf)
            for err in errors_found:
                print(f"     {COLOR_YELLOW}{err}{COLOR_NC}", file=buf)
            print("", file=buf)
            return buf.getvalue(), 'error', 0, 0

        if errors_found:
            print(f"  {COLOR_YELLOW}Note: {len(errors_found)} peer(s) unreachable; comparing the {len(good_peers)} replica(s) that responded{COLOR_NC}", file=buf)

        all_row_counts = {results[n]['row_count'] for n in good_peers}
        all_xor_hashes = {results[n]['xor_hash'] for n in good_peers}

        if len(all_row_counts) > 1 or len(all_xor_hashes) > 1:
            print(f"  {COLOR_RED}--> !! MISMATCH DETECTED !!{COLOR_NC}", file=buf)
            for node in sorted(good_peers):
                print(f"  {node} -> row_count={results[node]['row_count']}, xor_hash={results[node]['xor_hash']}", file=buf)
            if len(all_row_counts) > 1:
                print(f"  {COLOR_RED}    Row counts differ: {sorted(all_row_counts)}{COLOR_NC}", file=buf)
            if len(all_xor_hashes) > 1:
                print(f"  {COLOR_RED}    XOR hashes differ: {sorted(all_xor_hashes)}{COLOR_NC}", file=buf)
            print("", file=buf)
            return buf.getvalue(), 'mismatch', 0, 0

        row_count = list(all_row_counts)[0]
        xor_hash = list(all_xor_hashes)[0]
        print(f"  {COLOR_GREEN}--> OK: All replicas consistent (row_count={row_count}, xor_hash={xor_hash}){COLOR_NC}", file=buf)
        print("", file=buf)
        return buf.getvalue(), 'ok', row_count, xor_hash

    def _check_table_consistency(self, table_config: TableConfig, out=None) -> int:
        """Check consistency at the TABLET level using yb-ts-cli RPC.

        out:  file-like to write progress to (defaults to stdout); per-table buffer
              when run in parallel so worker output does not interleave.
        """
        out = out or sys.stdout
        mismatches_found = 0
        tablets_with_errors = 0
        single_replica_skips = 0

        try:
            # read_ht selection:
            #   - explicit --read-ht always wins: one fixed value across every tablet.
            #   - --per-tablet-read-ht (and no --read-ht): defer capture into the per-tablet
            #     loop below so a short timestamp_history_retention_interval_sec can't age the
            #     read_ht out mid-scan. Valid because the check is per-tablet cross-peer and
            #     never relies on cross-tablet point-in-time consistency.
            #   - default: capture one hybrid time here and reuse it for every tablet.
            table_read_ht: Optional[int] = None
            if self.read_ht is not None:
                table_read_ht = self.read_ht
            elif not self.per_tablet_read_ht:
                table_read_ht = get_current_hybrid_time()
            if table_read_ht is not None:
                print(f"{COLOR_BLUE}Using hybrid time: {table_read_ht}{COLOR_NC}\n", file=out)

            tablets = self.get_tablets(table_config.table_id)
            if not tablets:
                print(f"{COLOR_RED}Error: No tablets found for table_id {table_config.table_id}{COLOR_NC}", file=out)
                return 1

            if self.shuffle_seed:
                random.Random(f"{self.shuffle_seed}:{table_config.table_id}").shuffle(tablets)

            print(f"{COLOR_GREEN}Found {len(tablets)} tablets. Starting consistency check{COLOR_NC}\n", file=out)

            total_row_count = 0
            total_xor_hash = 0
            unchecked = 0

            with ThreadPoolExecutor(max_workers=self.tablet_workers) as executor:
                futures = []
                for tablet in tablets:
                    if self.out_of_time():
                        unchecked = len(tablets) - len(futures)
                        break
                    futures.append(executor.submit(self._check_one_tablet, tablet, table_read_ht))

                for future in futures:
                    output, status, row_count, xor_hash = future.result()
                    out.write(output)
                    if status == 'ok':
                        total_row_count += row_count
                        total_xor_hash ^= xor_hash
                    elif status == 'mismatch':
                        mismatches_found += 1
                    elif status == 'error':
                        tablets_with_errors += 1
                    elif status == 'skip':
                        single_replica_skips += 1
                    elif status == 'notime':
                        unchecked += 1

            checked = len(tablets) - unchecked
            with self._counter_lock:
                self.tablets_total += len(tablets)
                self.tablets_checked += checked
                if unchecked:
                    schema_prefix = f"{table_config.schema_name}." if table_config.schema_name != 'public' else ""
                    self._partial_tables[f"{schema_prefix}{table_config.table_name}"] = (checked, len(tablets))

            if unchecked:
                print(f"{COLOR_YELLOW}Partial: {checked}/{len(tablets)} tablets checked before the "
                      f"--max-runtime budget{COLOR_NC}", file=out)

            if single_replica_skips:
                schema_prefix = f"{table_config.schema_name}." if table_config.schema_name != 'public' else ""
                with self._skip_lock:
                    self._single_replica_skips[f"{schema_prefix}{table_config.table_name}"] = single_replica_skips

            scope = "partial, tablets checked so far" if unchecked else "whole table"
            print(f"{COLOR_CYAN}Table row_count={total_row_count}, xor_hash={total_xor_hash} "
                  f"({scope}){COLOR_NC}\n", file=out)

        except Exception as e:
            print(f"{COLOR_RED}An unexpected error occurred: {e}{COLOR_NC}", file=out)
            print(f"{COLOR_YELLOW}{format_exc()}{COLOR_NC}", file=out)
            return 1

        if tablets_with_errors > 0 and mismatches_found == 0:
            return -1
        return mismatches_found

    def _check_one_table(self, idx: int, total: int, table_config: TableConfig) -> Tuple[TableConfig, int, str]:
        """Check one table into a private buffer so parallel workers don't interleave output.

        idx:    1-based position of this table in the run
        total:  total number of tables in the run
        Returns (table_config, result_code, captured_output).
        """
        buf = io.StringIO()
        schema_prefix = f"{table_config.schema_name}." if table_config.schema_name != 'public' else ""
        print(f"\n{COLOR_BLUE}{'#' * 60}", file=buf)
        print(f"# {COLOR_CYAN}[{idx}/{total}] Table: {schema_prefix}{table_config.table_name} (ID: {table_config.table_id})", file=buf)
        print(f"{COLOR_BLUE}{'#' * 60}{COLOR_NC}", file=buf)
        result = self._check_table_consistency(table_config, out=buf)
        return table_config, result, buf.getvalue()


    def discover_tables_v2024(self) -> List[TableConfig]:
        """
        Discover tables using yb-admin list_tables include_table_type.
        Ignores catalog table types; only includes user tables.
        """

        if not self.scan_all:
            tables_to_check = set(self.parse_table_arg(table_arg) for table_arg in self.tables)

        cmd = self._yb_admin_cmd_base() + [
            "list_tables",
            "include_table_id",
            "include_table_type"
        ]

        result = self._run_admin_checked(cmd, "list_tables")

        table_configs = []
        tables_filter = re.compile(self.tables_filter) if self.tables_filter else None

        for line in result.stdout.split('\n'):
            line = line.strip()
            if not line:
                continue

            # Format: <namespace>.<table-name> <table-id> <table-type>
            # Versions 2024.2 and below:
            # Example: db1.emp 00004000000030008000000000004000 table
            # Example: system.peers 2ede4ffbefe8491bb768c4aff7bcd67a catalog

            parts = line.split()
            if len(parts) < 1:
                continue

            if len(parts) != 3:
                raise RuntimeError(f"Invalid table name: {line}")

            full_name = parts[0]
            table_id = parts[1]
            table_type = parts[2]
            # Default to public schema
            schema_name = "public"

            # Ignore catalog table types
            if table_type.lower() == "catalog":
                continue

            # Colocation parent table name suffix colocation.parent.tablename
            if full_name.endswith("colocation.parent.tablename"):
                continue

            # Parse namespace.table_name
            name_parts = full_name.split('.')

            if len(name_parts) != 2:
                raise RuntimeError(f"Invalid table name: {full_name}")

            namespace, table_name = name_parts[0], name_parts[1]

            if not self.scan_all_databases and namespace != self.database:
                continue

            if not self.scan_all and (table_name, schema_name) not in tables_to_check:
                continue

            if tables_filter and not tables_filter.match(table_name):
                continue

            table_configs.append(TableConfig(
                table_name=table_name,
                table_id=table_id,
                schema_name=schema_name
            ))

        return sorted(table_configs, key=lambda tc: (tc.schema_name, tc.table_name))


    def discover_tables_new(self) -> List[TableConfig]:
        """
        Discover tables using yb-admin list_tables include_table_type.
        Ignores catalog table types; only includes user tables.
        """

        if not self.scan_all:
            tables_to_check = set(self.parse_table_arg(table_arg) for table_arg in self.tables)

        cmd = self._yb_admin_cmd_base() + [
            "list_tables",
            "include_table_type"
        ]

        result = self._run_admin_checked(cmd, "list_tables")

        table_configs = []
        tables_filter = re.compile(self.tables_filter) if self.tables_filter else None

        for line in result.stdout.split('\n'):
            line = line.strip()
            if not line:
                continue

            # Format: <namespace>.<table-name> [ysql_schema=<schema_name>] [<table-id>] <table-type>
            # Example: db1.emp [ysql_schema=public] [00004000000030008000000000004000] table
            # Example: system.peers [2ede4ffbefe8491bb768c4aff7bcd67a] catalog

            parts = line.split()
            if len(parts) < 1:
                continue

            if len(parts) < 3 or len(parts) > 4:
                raise RuntimeError(f"Invalid table name: {line}")

            full_name = parts[0]
            if len(parts) == 4:
                schema_name_str = parts[1]
            else:
                # No schema name in the output
                # ybdemo.cassandrakeyvalue [fe65c5ade4f443b4aebfcb7c40874b4d] table
                schema_name_str = None
            table_id = re.search(r'\[(.*)\]', parts[-2]).group(1)
            table_type = parts[-1]

            # Ignore catalog table types
            if table_type.lower() == "catalog":
                continue

            # Colocation parent table name suffix colocation.parent.tablename
            if full_name.endswith("colocation.parent.tablename"):
                continue

            # Parse namespace.table_name
            name_parts = full_name.split('.')
            if len(name_parts) != 2:
                raise RuntimeError(f"Invalid table name: {full_name}")

            namespace, table_name = name_parts[0], name_parts[1]

            if not self.scan_all_databases and namespace != self.database:
                continue

            if schema_name_str is not None:
                # Parse schema name from [ysql_schema=<schema_name>]
                schema_name = re.search(r'ysql_schema=(\w+)', schema_name_str).group(1)
            else:
                schema_name = "public"

            if not self.scan_all and (table_name, schema_name) not in tables_to_check:
                continue

            if tables_filter and not tables_filter.match(table_name):
                continue

            table_configs.append(TableConfig(
                table_name=table_name,
                table_id=table_id,
                schema_name=schema_name
            ))

        return sorted(table_configs, key=lambda tc: (tc.schema_name, tc.table_name))


    def discover_tables(self) -> List[TableConfig]:
        if self.db_version is None or int(self.db_version[0]) == 2024:
            return self.discover_tables_v2024()
        else:
            return self.discover_tables_new()

    def print_db_version(self) -> None:
        try:
            result = self._run_subprocess(
                self._yb_admin_cmd_base() + ["--version"],
                timeout=5
            )
            if result.returncode != 0:
                return
            output = result.stdout or result.stderr or ""
            # Parse "version 2.29.0.0 build ..."
            match = re.search(r'version\s+(\d+\.\d+\.\d+\.\d+)', output, re.IGNORECASE)
            if match:
                db_version = match.group(1)
                print(f"{COLOR_CYAN}YugabyteDB version: {db_version}{COLOR_NC}\n")
                self.db_version = db_version.split(".")
            else:
                print(f"{COLOR_YELLOW}Warning: Could not detect YugabyteDB version{COLOR_NC}\n")
        except Exception as e:
            print(f"{COLOR_YELLOW}Warning: Failed to detect DB version: {e}{COLOR_NC}\n")


    def run(self) -> int:
        """Main entry point to perform the consistency check."""
        # Budget starts here, not after discovery: on a large database the yb-admin
        # table walk is itself minutes of the caller's wall-clock allowance.
        self._deadline = time.monotonic() + self.max_runtime if self.max_runtime > 0 else None
        print(f"{COLOR_CYAN}YugabyteDB Consistency Check Script v{SCRIPT_VERSION}{COLOR_NC}")

        self.yb_admin_path = find_yb_admin(self.tools_path)
        if not self.yb_admin_path:
            print(f"{COLOR_RED}Error: yb-admin binary not found in {self.tools_path} or PATH or current directory{COLOR_NC}")
            return 1

        self.yb_ts_cli_path = find_yb_ts_cli(self.tools_path)
        if not self.yb_ts_cli_path:
            print(f"{COLOR_RED}Error: yb-ts-cli binary not found in {self.tools_path} or PATH or current directory{COLOR_NC}")
            return 1

        self.print_db_version()

        print(f"{COLOR_BLUE}=================================================")
        print("YugabyteDB Consistency Check")
        print("=================================================")
        if self.database is not None:
            print(f"Database: {self.database}")
        if self.tables_filter:
            print(f"Filter: {self.tables_filter}")
        print(f"{COLOR_NC}")

        try:
            table_configs = self.discover_tables()
            if not table_configs:
                filter_info = f" matching '{self.tables_filter}'" if self.tables_filter else ""
                print(f"{COLOR_RED}Error: No tables{filter_info} found in database{COLOR_NC}")
                return 1

        except RuntimeError as e:
            print(f"{COLOR_RED}Error during table discovery: {e}{COLOR_NC}")
            return 1

        if self.shuffle_seed:
            random.Random(self.shuffle_seed).shuffle(table_configs)
            print(f"{COLOR_BLUE}Order shuffled with seed {self.shuffle_seed}: a budget-limited "
                  f"run covers a different slice than the last one{COLOR_NC}")

        print(f"{COLOR_GREEN}Tables to check: {len(table_configs)}{COLOR_NC}")
        for tc in table_configs:
            schema_prefix = f"{tc.schema_name}." if tc.schema_name != 'public' else ""
            print(f"  - {schema_prefix}{tc.table_name}")
        print("")

        total = len(table_configs)
        deadline = self._deadline
        results: List[Tuple[TableConfig, int]] = []
        stopped_early = False

        if self.workers > 1:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                in_flight = set()
                table_iter = iter(enumerate(table_configs, 1))
                while True:
                    while len(in_flight) < self.workers and not stopped_early:
                        if deadline and time.monotonic() > deadline:
                            stopped_early = True
                            break
                        nxt = next(table_iter, None)
                        if nxt is None:
                            break
                        idx, tc = nxt
                        in_flight.add(executor.submit(self._check_one_table, idx, total, tc))
                    if not in_flight:
                        break
                    done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                    for future in done:
                        tc, result, output = future.result()
                        sys.stdout.write(output)
                        results.append((tc, result))
                    if stopped_early:
                        for future in in_flight:
                            tc, result, output = future.result()
                            sys.stdout.write(output)
                            results.append((tc, result))
                        break
        else:
            for idx, tc in enumerate(table_configs, 1):
                if deadline and time.monotonic() > deadline:
                    stopped_early = True
                    break
                tc, result, output = self._check_one_table(idx, total, tc)
                sys.stdout.write(output)
                results.append((tc, result))

        checked = len(results)
        tables_with_errors = [tc for tc, r in results if r < 0]
        tables_with_mismatches = [(tc, r) for tc, r in results if r > 0]
        total_mismatches = sum(r for _, r in tables_with_mismatches)
        if self._partial_tables:
            stopped_early = True

        print("\n" + "=" * 60)
        print("FINAL SUMMARY")
        print("-" * 60)
        print(f"Tables checked: {checked}")
        print(f"Tablets checked: {self.tablets_checked}/{self.tablets_total}")
        if stopped_early:
            reason = (
                f"hit --max-runtime {self.max_runtime}s budget"
                if self.max_runtime > 0 and self.out_of_time()
                else "tablet dumps timed out"
            )
            print(f"{COLOR_YELLOW}Stopped early: {reason}; "
                  f"{self.tablets_checked}/{self.tablets_total} tablets checked across "
                  f"{checked}/{total} tables{COLOR_NC}")
        if self._partial_tables:
            print(f"{COLOR_YELLOW}Tables checked only in part ({len(self._partial_tables)}):{COLOR_NC}")
            for name in sorted(self._partial_tables):
                done_tablets, all_tablets = self._partial_tables[name]
                print(f"  {COLOR_YELLOW}- {name}: {done_tablets}/{all_tablets} tablets{COLOR_NC}")

        if self._single_replica_skips:
            total_skipped = sum(self._single_replica_skips.values())
            print(f"\n{COLOR_CYAN}Tables with skipped single-replica tablets "
                  f"({len(self._single_replica_skips)}), {total_skipped} tablet(s) total:{COLOR_NC}")
            for name in sorted(self._single_replica_skips):
                print(f"  {COLOR_CYAN}- {name}: {self._single_replica_skips[name]} tablet(s){COLOR_NC}")
            print(f"{COLOR_CYAN}Single-replica placement has no other replica to compare against; not an error{COLOR_NC}")

        if tables_with_errors:
            print(f"\n{COLOR_YELLOW}Tables with query errors ({len(tables_with_errors)}):{COLOR_NC}")
            for tc in tables_with_errors:
                schema_prefix = f"{tc.schema_name}." if tc.schema_name != 'public' else ""
                print(f"  {COLOR_YELLOW}- {schema_prefix}{tc.table_name}{COLOR_NC}")
            print(f"{COLOR_YELLOW}Please fix the errors above and retry these tables{COLOR_NC}")

        if tables_with_mismatches:
            print(f"\n{COLOR_RED}Failure: Found inconsistencies in {len(tables_with_mismatches)} table(s):{COLOR_NC}")
            for tc, count in tables_with_mismatches:
                schema_prefix = f"{tc.schema_name}." if tc.schema_name != 'public' else ""
                print(f"  {COLOR_RED}- {schema_prefix}{tc.table_name}: {count} tablet(s) with mismatches{COLOR_NC}")
        elif not tables_with_errors and not stopped_early:
            print(f"\n{COLOR_GREEN}Success: No inconsistencies found across all tables{COLOR_NC}")
        elif total_mismatches == 0:
            print(f"\n{COLOR_GREEN}No inconsistencies found in tables that were successfully checked{COLOR_NC}")

        print("=" * 60)

        # Exit codes: 0 = clean, 1 = inconsistencies, 2 = some tables unverifiable,
        # 3 = stopped early on --max-runtime before all tables were scanned.
        if tables_with_mismatches:
            return 1
        if tables_with_errors:
            return 2
        if stopped_early:
            return 3
        return 0


if __name__ == "__main__":
    # Line-buffer stdout so a killed run still leaves partial progress on disk.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    args = parse_args()
    checker = ConsistencyChecker(args)

    exit_code = 1
    try:
        exit_code = checker.run()
    except RuntimeError as e:
        print(f"{COLOR_RED}Error: {e}{COLOR_NC}")
    sys.exit(exit_code)
