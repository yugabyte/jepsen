(ns yugabyte.ysql.wr
  "YSQL client for the write-read register workload (yugabyte.wr).

  Registers are rows in `wr (k int primary key, k2 int, v int)` with a secondary
  index on k2 that INCLUDEs v. A transaction is a sequence of micro-ops [f k v]:
    :r  read  -> SELECT v WHERE k = k, OR (chosen at random) an index-only scan
                 SELECT v WHERE k2 = k, so both the base table and the secondary
                 index are exercised and must agree with committed writes.
    :w  write -> upsert (k2 mirrors k; writes are unique)
  Multi-op transactions run inside a JDBC transaction at the client's isolation
  level; single-op transactions run without an explicit BEGIN."
  (:require [clojure.java.jdbc :as j]
            [jepsen.random :as random]
            [clojure.tools.logging :refer [info]]
            [yugabyte.ysql.client :as c]))

(def table-name "wr")
(def index-name "idx_wr")

(defn read-register
  "Reads register k, coerced to a Long. Randomly reads via the primary key or
  via an index-only scan on the secondary index, so both paths are covered."
  [conn k]
  (let [use-index? (zero? (random/long 2))
        sql        (if use-index?
                     (str "/*+ IndexOnlyScan(" table-name " " index-name ") */ "
                          "select v from " table-name " where k2 = ?")
                     (str "select v from " table-name " where k = ?"))]
    (info table-name (if use-index? "IndexOnlyScan(k2)" "PrimaryScan(k)") "k=" k)
    (some-> conn (c/query [sql k]) first :v long)))

(defn write-register!
  "Upserts register k = v (k2 mirrors k). Returns v."
  [conn k v]
  (c/execute! conn [(str "insert into " table-name " (k, k2, v) values (?, ?, ?) "
                         "on conflict (k) do update set v = ?") k k v v])
  v)

(defn mop!
  "Executes a micro-op [f k v] on a connection, returning the completed op."
  [conn [f k v]]
  [f k (case f
         :r (read-register conn k)
         :w (write-register! conn k v))])

(defrecord WRClient [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name
                                      [[:k :int "PRIMARY KEY"]
                                       [:k2 :int]
                                       [:v :int]]
                                      {:conditional? true}))
    (c/execute! c (str "CREATE INDEX " index-name " ON " table-name " (k2) INCLUDE (v)")))

  (invoke-op! [this test op c conn-wrapper]
    (let [txn      (:value op)
          use-txn? (< 1 (count txn))
          txn'     (if use-txn?
                     (j/with-db-transaction [c c {:isolation isolation}]
                                            (mapv (partial mop! c) txn))
                     (mapv (partial mop! c) txn))]
      (assoc op :type :ok, :value txn')))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))

(c/defclient Client WRClient)
