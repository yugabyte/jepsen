(ns yugabyte.ycql.upsert
  "YCQL client for the upsert uniqueness workload (yugabyte.upsert), using a
  Cassandra-style lightweight transaction: INSERT ... IF NOT EXISTS. The LWT's
  [applied] result tells us whether this insert actually won the key. The shared
  checker then verifies at most one insert won per key and reads agree."
  (:require [yugabyte.ycql.client :as c]))

(def keyspace "jepsen")
(def table "upsert")

(c/defclient CQLUpsert keyspace []
  (setup! [this test]
    (c/create-table conn table
                    {:k           :int
                     :v           :int
                     :primary-key [:k]}))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :upsert
        (let [[k v]   (:value op)
              res     (c/execute! conn
                                  (str "INSERT INTO " keyspace "." table
                                       " (k, v) VALUES (" k ", " v ") IF NOT EXISTS"))
              applied (get (first res) (keyword "[applied]"))]
          (assoc op :type :ok, :value [k v (boolean applied)]))

        :read
        (let [m (->> (c/select conn table :columns [:k :v])
                     (map (juxt :k :v))
                     (into {}))]
          (assoc op :type :ok, :value m)))))

  (teardown! [this test]))
