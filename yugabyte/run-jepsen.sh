#!/bin/bash

set -euo pipefail

readonly TIMEOUT=600
readonly SAVED_DIR=$(pwd)
readonly NODES_FILE="$HOME/code/jepsen/nodes"
readonly TESTS=(
  single-row-inserts
  single-key-acid
  multi-key-acid
)
readonly NEMESES=(
  none
  start-stop-tserver
  start-kill-tserver
  start-stop-master
  start-kill-master
  start-stop-node
  start-kill-node
  partition-random-halves
  partition-random-node
  partition-majorities-ring
  small-skew
  medium-skew
  large-skew
  xlarge-skew
)
readonly SCRIPT_DIR="${0%/*}"
readonly STORE_DIR="$SCRIPT_DIR/store"


trap 'cd $SAVED_DIR' EXIT SIGHUP SIGINT SIGTERM

cd "${0%/*}"
$SCRIPT_DIR/sort-results.sh
while true; do
  for test in "${TESTS[@]}"; do
    for nemesis in "${NEMESES[@]}"; do
      set +e
      /usr/bin/timeout --foreground --kill-after=5 $TIMEOUT lein run test --nodes-file $NODES_FILE --test $test --nemesis $nemesis
      EC=$?
      set -e
      if [[ $EC -eq 124 ]]; then
        pkill -9 java  || true
        log_path=$(find $STORE_DIR -name "jepsen.log" -printf "%T+\t%p\n" | sort | cut -f2 | tail -n1)
        msg="Test run timed out!"
        echo "$msg: $log_path"
        echo
        echo "$msg" >>"$log_path"
      fi
      $SCRIPT_DIR/sort-results.sh
      # Small delay to be able to catch CTRL+C out of clojure.
      sleep 0.2
    done
  done
done
