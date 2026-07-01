(ns yugabyte.wr
  "Write-read register workload built on Elle's rw-register cycle checker.

  Complements the list-append workload: instead of appending to lists, each
  transaction is a mix of single-key reads and writes of unique values, and we
  look for cycles in the resulting dependency graph. rw-register catches
  anomalies (e.g. via read/write and anti-dependency edges over plain registers)
  that the append datatype cannot express."
  (:require [elle.core :as elle]
            [jepsen.tests.cycle.wr :as wr]))

(defn workload-si
  [opts]
  (wr/test {:key-count          10
            :max-txn-length     4
            :max-writes-per-key 256
            :anomalies          [:internal :G-nonadjacent :G1 :G-SI]
            :consistency-models [:snapshot-isolation]
            :additional-graphs  [elle/realtime-graph]}))

(defn workload-rc
  [opts]
  (wr/test {:key-count          10
            :max-txn-length     4
            :max-writes-per-key 256
            :anomalies          [:G0 :G1a :G1b]
            :consistency-models [:read-committed]
            :additional-graphs  [elle/realtime-graph]}))

; No workload-serializable: at serializable, multi-key-acid already covers
; multi-key register transactions (via linearizability), so it would overlap.
