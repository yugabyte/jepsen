(ns yugabyte.ysql.wr
  "YSQL client for the write-read register workload (yugabyte.wr).

  Registers are rows in a single table `wr (k int primary key, v int)`. A
  transaction is a sequence of micro-ops [f k v]:
    :r  read  -> SELECT v FROM wr WHERE k = k        (returns the value, or nil)
    :w  write -> INSERT (k, v) ON CONFLICT (k) UPDATE (writes are unique)
  Multi-op transactions run inside a JDBC transaction at the client's isolation
  level; single-op transactions run without an explicit BEGIN, matching the
  append client."
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [yugabyte.ysql.client :as c]))

(def table-name "wr")

(defn read-register
  "Reads the value of register k, coerced to a Long so it compares equal to the
  Long values Elle generates (a JDBC int column reads back as Integer)."
  [conn k]
  (some-> conn
          (c/query [(str "select v from " table-name " where k = ?") k])
          first
          :v
          long))

(defn write-register!
  "Upserts register k = v. Returns v."
  [conn k v]
  (c/execute! conn [(str "insert into " table-name " (k, v) values (?, ?) "
                         "on conflict (k) do update set v = ?") k v v])
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
                                       [:v :int]]
                                      {:conditional? true})))

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
