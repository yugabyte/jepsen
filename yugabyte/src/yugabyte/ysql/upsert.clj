(ns yugabyte.ysql.upsert
  "YSQL client for the upsert uniqueness workload (yugabyte.upsert).

  Table `upsert (k int primary key, v int)`.
    :upsert [k v] -> INSERT (k,v) ON CONFLICT (k) DO NOTHING; returns
                     [k v inserted?] where inserted? is true iff a row was
                     actually written (JDBC rowcount 1).
    :read         -> SELECT k, v FROM upsert; returns a map {k v}."
  (:require [clojure.java.jdbc :as j]
            [yugabyte.ysql.client :as c]))

(def table-name "upsert")

(defrecord UpsertClient [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name
                                      [[:k :int "PRIMARY KEY"]
                                       [:v :int]]
                                      {:conditional? true})))

  (invoke-op! [this test op c conn-wrapper]
    (case (:f op)
      :upsert
      (let [[k v]  (:value op)
            result (c/execute! c [(str "insert into " table-name " (k, v) values (?, ?) "
                                       "on conflict (k) do nothing") k v])]
        (assoc op :type :ok, :value [k v (pos? (first result))]))

      :read
      (let [rows (c/query c [(str "select k, v from " table-name)])]
        (assoc op :type :ok, :value (into {} (map (juxt :k :v) rows))))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))

(c/defclient Client UpsertClient)
