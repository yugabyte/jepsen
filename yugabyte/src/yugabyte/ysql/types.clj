(ns yugabyte.ysql.types
  "YSQL client for the numeric boundary workload (yugabyte.types).

  Table `types (k int primary key, v bigint)`.
    :write [k v] -> upsert v (a bigint) into key k; returns the op.
    :read        -> SELECT k, v FROM types; returns a map {k v} with v coerced
                    to Long so it compares equal to the written boundary value."
  (:require [clojure.java.jdbc :as j]
            [yugabyte.ysql.client :as c]))

(def table-name "types")

(defrecord TypesClient [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name
                                      [[:k :int "PRIMARY KEY"]
                                       [:v :bigint]]
                                      {:conditional? true})))

  (invoke-op! [this test op c conn-wrapper]
    (case (:f op)
      :write
      (let [[k v] (:value op)]
        (c/execute! c [(str "insert into " table-name " (k, v) values (?, ?) "
                            "on conflict (k) do update set v = ?") k v v])
        (assoc op :type :ok))

      :read
      (let [rows (c/query c [(str "select k, v from " table-name)])]
        (assoc op :type :ok
               :value (into {} (map (fn [r] [(:k r) (some-> (:v r) long)]) rows))))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))

(c/defclient Client TypesClient)
