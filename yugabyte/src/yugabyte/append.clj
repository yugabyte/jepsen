(ns yugabyte.append
  "Values are lists of integers. Each operation performs a transaction,
  comprised of micro-operations which are either reads of some value (returning
  the entire list) or appends (adding a single number to whatever the present
  value of the given list is). We detect cycles in these transactions using
  Jepsen's cycle-detection system."
  (:require [elle.core :as elle]
            [jepsen.tests.cycle.append :as append]))

(defn workload-si
  [opts]
  (-> (append/test {:key-count          32
                    :max-txn-length     4
                    :max-writes-per-key 1024
                    :anomalies          [:internal :G-nonadjacent :G1 :G-SI]
                    :consistency-models [:snapshot-isolation]
                    :additional-graphs  [elle/realtime-graph]})))

(defn workload-rc
  [opts]
  (-> (append/test {:key-count          32
                    :max-txn-length     4
                    :max-writes-per-key 512
                    :anomalies          [:G0 :G1a :G1b]
                    :consistency-models [:read-committed]
                    :additional-graphs  [elle/realtime-graph]})))

(defn workload-serializable
  [opts]
  (-> (append/test {:key-count          32
                    :max-txn-length     4
                    :max-writes-per-key 1024
                    :anomalies          [:G1 :G2]
                    ; :consistency-models [:strict-serializable] ; default value
                    :additional-graphs  [elle/realtime-graph]})))
;     (update :generator (partial gen/stagger 1/5)))

; Append-table workloads use lower limits because each key is a separate table
; and every read fetches all rows — O(n) per read instead of O(1).
(defn workload-si-table
  [opts]
  (-> (append/test {:key-count          16
                    :max-txn-length     4
                    :max-writes-per-key 128
                    :anomalies          [:internal :G-nonadjacent :G1 :G-SI]
                    :consistency-models [:snapshot-isolation]
                    :additional-graphs  [elle/realtime-graph]})))

(defn workload-rc-table
  [opts]
  (-> (append/test {:key-count          16
                    :max-txn-length     4
                    :max-writes-per-key 128
                    :anomalies          [:G0 :G1a :G1b]
                    :consistency-models [:read-committed]
                    :additional-graphs  [elle/realtime-graph]})))

(defn workload-serializable-table
  [opts]
  (-> (append/test {:key-count          16
                    :max-txn-length     4
                    :max-writes-per-key 128
                    :anomalies          [:G1 :G2]
                    :additional-graphs  [elle/realtime-graph]})))
