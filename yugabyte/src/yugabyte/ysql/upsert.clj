(ns yugabyte.ysql.upsert
  "YSQL client for the upsert uniqueness workload (yugabyte.upsert).

  Table `upsert (k int primary key, k2 int, v int)` with a secondary index on k2
  INCLUDEing v.
    :upsert [k v] -> INSERT (k, k2=k, v) ON CONFLICT (k) DO NOTHING; returns
                     [k v inserted?] (inserted? true iff a row was written).
    :read         -> the full {k v} map, read at random either by full scan or
                     by an index-only scan over the key range, so the secondary
                     index is exercised and must agree with the base table."
  (:require [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [jepsen.random :as random]
            [yugabyte.ysql.client :as c]
            [yugabyte.upsert :as upsert]))

(def table-name "upsert")
(def index-name "idx_upsert")

(defn read-all
  "Reads all rows as a {k v} map, via full scan or an index-only scan on k2."
  [conn]
  (let [use-index? (zero? (random/long 2))
        sql        (if use-index?
                     (str "/*+ IndexOnlyScan(" table-name " " index-name ") */ "
                          "select k, v from " table-name
                          " where k2 in (" (str/join ", " (range upsert/key-count)) ")")
                     (str "select k, v from " table-name))]
    (->> (c/query conn [sql])
         (map (juxt :k :v))
         (into {}))))

(defrecord UpsertClient [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name
                                      [[:k :int "PRIMARY KEY"]
                                       [:k2 :int]
                                       [:v :int]]
                                      {:conditional? true}))
    (c/execute! c (str "CREATE INDEX " index-name " ON " table-name " (k2) INCLUDE (v)")))

  (invoke-op! [this test op c conn-wrapper]
    ; Run at the client's isolation (see note in ysql.types): without it the op
    ; uses the connection default (serializable) rather than si./rc.
    (j/with-db-transaction [c c {:isolation isolation}]
      (case (:f op)
        :upsert
        (let [[k v]  (:value op)
              result (c/execute! c [(str "insert into " table-name " (k, k2, v) values (?, ?, ?) "
                                         "on conflict (k) do nothing") k k v])]
          (assoc op :type :ok, :value [k v (pos? (first result))]))

        :read
        (assoc op :type :ok, :value (read-all c)))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))

(c/defclient Client UpsertClient)
