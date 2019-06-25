(ns yugabyte.ysql.adya
  (:require [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.reconnect :as rc]
            [yugabyte.ysql.client :as c]))

(def table-name-a "adya_a")
(def table-name-b "adya_b")

(defrecord YSQLAdyaYbClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (let [schema [[:id :int "PRIMARY KEY"]
                  [:key :int]
                  [:value :int]]]
      (c/execute! c (j/create-table-ddl table-name-a schema))
      (c/execute! c (j/create-table-ddl table-name-b schema))))


  (invoke-op! [this test op c conn-wrapper]
    (case (:f op)
      :insert
      (let [[key [a-id b-id]] (:value op)]
        (c/with-txn
          c
          (let [where-clause     (str "key = " key " AND value % 3 = 0")
                select-res-a     (c/select-first-row c table-name-a where-clause)
                select-res-b     (c/select-first-row c table-name-b where-clause)
                already-present? (or select-res-a select-res-b)
                table-to-insert  (if a-id table-name-a table-name-b)
                id-to-insert     (or a-id b-id)]
            (if already-present?
              (assoc op :type :fail, :error :row-already-present)
              (do (c/insert! c table-to-insert {:key key, :id id-to-insert, :value 30})
                  (assoc op :type :ok))))))))


  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name-a)
    (c/drop-table c table-name-b)))


(c/defclient YSQLAdyaClient YSQLAdyaYbClient)
