(ns yugabyte.ysql.multi-key-acid
  "This test uses INSERT ... ON CONFLICT DO UPDATE"
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [jepsen.independent :as independent]
            [jepsen.random :as random]
            [jepsen.txn.micro-op :as mop]
            [yugabyte.ysql.client :as c]))

(def table-name "multi_key_acid")
(def index-name "idx_multi_key_acid")

(defrecord YSQLMultiKeyAcidYbClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name [[:k1 :int]
                                                  [:k2 :int]
                                                  [:val :int]
                                                  ["PRIMARY KEY" "(k1, k2)"]]))
    (c/execute! c (str "CREATE INDEX " index-name " ON " table-name " (k2, k1, val)")))

  (invoke-op! [this test op c conn-wrapper]
    (let [[k2 ops] (:value op)]
      (case (:f op)
        :read
        (let [k1s  (map mop/key ops)
              ; Look up values, randomly using secondary index
              use-index? (zero? (random/long 2))
              _ (info table-name (if use-index? "IndexOnlyScan" "SeqScan") "k2=" k2)
              vs   (->> (str (when use-index?
                               (str "/*+ IndexOnlyScan(" table-name " " index-name ") */ "))
                             "SELECT k1, val FROM " table-name " WHERE k2 = " k2 " AND k1 " (c/in k1s))
                        (c/query op c)
                        (map (juxt :k1 :val))
                        (into {}))
              ; Rewrite ops to use those values
              ops' (mapv (fn [[f k1 _]] [f k1 (get vs k1)]) ops)]
          (assoc op :type :ok, :value (independent/tuple k2 ops')))

        :write
        (c/with-txn
          c
          (doseq [[f k1 v] ops]
            (assert (= :w f))
            ; Since there's no UPSERT for SQL...
            (let [update-str (str "INSERT INTO " table-name " (k1, k2, val)"
                                  " VALUES (" k1 ", " k2 ", " v ")"
                                  " ON CONFLICT ON CONSTRAINT " table-name "_pkey"
                                  " DO UPDATE SET val = " v)]
              (c/execute! op c update-str)))
          (assoc op :type :ok)))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))


(c/defclient YSQLMultiKeyAcidClient YSQLMultiKeyAcidYbClient)
