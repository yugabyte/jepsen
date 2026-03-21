(ns yugabyte.ysql.single-key-acid
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [jepsen.independent :as independent]
            [jepsen.random :as random]
            [yugabyte.single-key-acid :as ska]
            [yugabyte.ysql.client :as c]))

(def table-name "single_key_acid")
(def index-name "idx_single_key_acid")

(defrecord YSQLSingleKeyAcidYbClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name [[:id :int "PRIMARY KEY"]
                                                  [:val :int]]))
    (c/execute! c (str "CREATE INDEX " index-name " ON " table-name " (id, val)"))
    (doseq [id (range ska/keys-count)]
      (c/insert! c table-name {:id id :val 0})))

  (invoke-op! [this test op c conn-wrapper]
    (let [[id val] (:value op)]
      (case (:f op)
        :write
        (do (c/update! op c table-name {:val val} ["id = ?" id])
            (assoc op :type :ok))

        :cas
        (let [[expected-val new-val] val
              res     (c/update! op c table-name
                                 {:val new-val}
                                 ["id = ? AND val = ?" id expected-val])
              applied (> (first res) 0)]
          (assoc op :type (if applied :ok :fail)))

        :read
        (let [use-index? (zero? (random/long 2))
              _ (info table-name (if use-index? "IndexOnlyScan" "SeqScan") "id=" id)
              value (if use-index?
                      (-> (c/query op c (str "/*+ IndexOnlyScan(" table-name " " index-name ") */ SELECT val FROM " table-name " WHERE id = " id))
                          first :val)
                      (c/select-single-value c table-name :val (str "id = " id)))]
          (assoc op :type :ok :value (independent/tuple id value))))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))


(c/defclient YSQLSingleKeyAcidClient YSQLSingleKeyAcidYbClient)
