(ns yugabyte.ycql.types
  "YCQL client for the numeric boundary workload (yugabyte.types). Writes
  edge-case 64-bit values into a bigint column and reads them back; the shared
  checker verifies every value read was actually written (no overflow /
  truncation).

  The table is transactional (required for YCQL secondary indexes) with an index
  on k2 (a mirror of the key) INCLUDEing v. Reads are randomized between a full
  scan and an index read (WHERE k2 IN ...), which YCQL routes through the index
  automatically, so the index's copy of v is exercised too. Note: unlike the
  YCQL upsert workload we can add an index here because plain INSERT upserts
  coexist with secondary indexes; LWT (IF NOT EXISTS) does not, so upsert stays
  index-free."
  (:require [jepsen.random :as random]
            [yugabyte.ycql.client :as c]
            [yugabyte.types :as types]))

(def keyspace "jepsen")
(def table "types")

(c/defclient CQLTypes keyspace []
  (setup! [this test]
    (c/create-transactional-table conn table
                                  {:k           :int
                                   :k2          :int
                                   :v           :bigint
                                   :primary-key [:k]})
    (c/create-index conn
      (str "CREATE INDEX IF NOT EXISTS types_by_k2 ON " table " (k2) INCLUDE (v)")))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :write
        (let [[k v] (:value op)]
          (c/insert! conn table {:k k, :k2 k, :v v})
          (assoc op :type :ok))

        :read
        (let [use-index? (zero? (random/long 2))
              rows       (if use-index?
                           (c/select conn table
                                     :columns [:k :v]
                                     :where [[:in :k2 (range types/key-count)]])
                           (c/select conn table :columns [:k :v]))
              m          (->> rows
                              (map (fn [r] [(:k r) (some-> (:v r) long)]))
                              (into {}))]
          (assoc op :type :ok, :value m)))))

  (teardown! [this test]))
