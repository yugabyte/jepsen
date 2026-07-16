(ns yugabyte.ysql.types
  "YSQL client for the numeric boundary workload (yugabyte.types).

  Table `types (k int primary key, k2 int, v bigint)` with a secondary index on
  k2 INCLUDEing v.
    :write [k v] -> upsert v (a bigint) into key k (k2 mirrors k).
    :read        -> the full {k v} map (v coerced to Long), read at random by
                    full scan or by an index-only scan over the key range, so
                    the index's copy of v is checked for overflow/truncation too."
  (:require [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [jepsen.random :as random]
            [yugabyte.ysql.client :as c]
            [yugabyte.types :as types]))

(def table-name "types")
(def index-name "idx_types")

(defn read-all
  "Reads all rows as a {k (long v)} map, via full scan or index-only scan on k2."
  [conn]
  (let [use-index? (zero? (random/long 2))
        sql        (if use-index?
                     (str "/*+ IndexOnlyScan(" table-name " " index-name ") */ "
                          "select k, v from " table-name
                          " where k2 in (" (str/join ", " (range types/key-count)) ")")
                     (str "select k, v from " table-name))]
    (->> (c/query conn [sql])
         (map (fn [r] [(:k r) (some-> (:v r) long)]))
         (into {}))))

(defrecord TypesClient [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name
                                      [[:k :int "PRIMARY KEY"]
                                       [:k2 :int]
                                       [:v :bigint]]
                                      {:conditional? true}))
    (c/execute! c (str "CREATE INDEX " index-name " ON " table-name " (k2) INCLUDE (v)")))

  (invoke-op! [this test op c conn-wrapper]
    ; Run at the client's isolation. Without this the op uses the connection
    ; default (serializable), so si./rc. would silently run serializable and
    ; the hot key space would deadlock-storm every write to failure.
    (j/with-db-transaction [c c {:isolation isolation}]
      (case (:f op)
        :write
        (let [[k v] (:value op)]
          (c/execute! c [(str "insert into " table-name " (k, k2, v) values (?, ?, ?) "
                              "on conflict (k) do update set v = ?") k k v v])
          (assoc op :type :ok))

        :read
        (assoc op :type :ok, :value (read-all c)))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))

(c/defclient Client TypesClient)
