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

trap 'cd $SAVED_DIR' EXIT SIGHUP SIGINT SIGTERM

cd "${0%/*}"
while true; do
  for test in "${TESTS[@]}"; do
    for nemesis in "${NEMESES[@]}"; do
      set +e
      /usr/bin/timeout --foreground --kill-after=5 $TIMEOUT lein run test --nodes-file $NODES_FILE --test $test --nemesis $nemesis
      pkill java
      set -e
      # Small delay to be able to catch CTRL+C out of clojure.
      sleep 0.2
    done
  done
done
