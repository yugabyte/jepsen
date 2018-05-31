#!/bin/bash

set -euo pipefail

readonly SAVED_DIR=$(pwd)
readonly NODES_FILE="$HOME/code/jepsen/nodes"
readonly TEST_COUNT_PER_RUN=5
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
readonly FAILED_DIR="$SCRIPT_DIR/store/failed"

mkdir -p $FAILED_DIR

trap 'cd $SAVED_DIR' EXIT SIGHUP SIGINT SIGTERM

cd "${0%/*}"
while true; do
  for test in "${TESTS[@]}"; do
    for nemesis in "${NEMESES[@]}"; do
      set +e
      lein run test --nodes-file $NODES_FILE --test-count $TEST_COUNT_PER_RUN --test $test --nemesis $nemesis
      set -e
      # Small delay to be able to catch CTRL+C out of clojure.
      sleep 0.2
    done
  done
done
