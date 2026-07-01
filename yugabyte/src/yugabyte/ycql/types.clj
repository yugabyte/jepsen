(ns yugabyte.ycql.types
  "YCQL client for the numeric boundary workload (yugabyte.types). Writes
  edge-case 64-bit values into a bigint column and reads them back; the shared
  checker verifies every value read was actually written (no overflow /
  truncation). CQL INSERT is an upsert, matching the workload's last-write
  semantics."
  (:require [yugabyte.ycql.client :as c]))

(def keyspace "jepsen")
(def table "types")

(c/defclient CQLTypes keyspace []
  (setup! [this test]
    (c/create-table conn table
                    {:k           :int
                     :v           :bigint
                     :primary-key [:k]}))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :write
        (let [[k v] (:value op)]
          (c/insert! conn table {:k k, :v v})
          (assoc op :type :ok))

        :read
        (let [m (->> (c/select conn table :columns [:k :v])
                     (map (fn [r] [(:k r) (some-> (:v r) long)]))
                     (into {}))]
          (assoc op :type :ok, :value m)))))

  (teardown! [this test]))
