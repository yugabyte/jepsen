(ns yugabyte.append
  "Values are lists of integers. Each operation performs a transaction,
  comprised of micro-operations which are either reads of some value (returning
  the entire list) or appends (adding a single number to whatever the present
  value of the given list is). We detect cycles in these transactions using
  Jepsen's cycle-detection system."
  (:require [elle.core :as elle]
            [jepsen.tests.cycle.append :as append]))

(defn workload-rr
  [opts]
  (-> (append/test {:tx-isolation       :repeatable-read
                    :key-count          32
                    :max-txn-length     4
                    :max-writes-per-key 1024
                    :anomalies          [:G1 :G2-item]
                    :consistency-models [:repeatable-read] ; todo :snapshot-isolation?
                    :additional-graphs  [elle/realtime-graph]})))

(defn workload-rc
  [opts]
  (-> (append/test {:tx-isolation       :read-committed
                    :key-count          32
                    :max-txn-length     4
                    :max-writes-per-key 1024
                    :anomalies          [:G1]
                    :consistency-models [:read-committed]
                    :additional-graphs  [elle/realtime-graph]})))

(defn workload-serializable
  [opts]
  (-> (append/test {:tx-isolation       :serializable
                    :key-count          32
                    :max-txn-length     4
                    :max-writes-per-key 1024
                    :anomalies          [:G1 :G2]
                    ; :consistency-models [:strict-serializable] ; default value
                    :additional-graphs  [elle/realtime-graph]})))
;     (update :generator (partial gen/stagger 1/5)))
