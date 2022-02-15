(ns yugabyte.append
  "Values are lists of integers. Each operation performs a transaction,
  comprised of micro-operations which are either reads of some value (returning
  the entire list) or appends (adding a single number to whatever the present
  value of the given list is). We detect cycles in these transactions using
  Jepsen's cycle-detection system."
  (:require [elle.core :as elle]
            [jepsen.generator :as gen]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.append :as append]))

(defn workload-rc
  [opts]
  (-> (append/test {:key-count          32
                    :max-txn-length     4
                    :max-writes-per-key 1024
                    :anomalies          [:G1 :G2]
                    :consistency-models [:read-committed]
                    :additional-graphs  [elle/realtime-graph]})))

(defn workload
  [opts]
  (-> (append/test {:key-count          32
                    :max-txn-length     4
                    :max-writes-per-key 1024
                    :anomalies          [:G1 :G2]
                    :additional-graphs  [elle/realtime-graph]})))
;     (update :generator (partial gen/stagger 1/5)))
