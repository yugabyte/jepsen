(ns yugabyte.ysql.counter
  "Something like YCQL 'counter' test. SQL does not have counter type though, so we just use int."
  (:require [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.random :as random]
            [jepsen.reconnect :as rc]
            [yugabyte.ysql.client :as c]))

(def table-name "counter")
(def index-name "idx_counter")

(defrecord YSQLCounterYbClient [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name [[:id :int "PRIMARY KEY"]
                                                  [:count :int]]))
    (c/execute! c (str "CREATE INDEX " index-name " ON " table-name " (id, count)"))
    (c/insert! c table-name {:id 0 :count 0}))

  (invoke-op! [this test op c conn-wrapper]
    (j/with-db-transaction [c c {:isolation isolation}]
      (case (:f op)
        ; update! can't handle column references
        :add (do (c/execute! op c [(str "UPDATE " table-name " SET count = count + ? WHERE id = 0") (:value op)])
                 (assoc op :type :ok))

        :read (let [use-index? (zero? (random/long 2))
                    _ (info table-name (if use-index? "IndexOnlyScan" "SeqScan"))
                    value (if use-index?
                            (-> (c/query op c (str "/*+ IndexOnlyScan(" table-name " " index-name ") */ SELECT count FROM " table-name " WHERE id = 0"))
                                first :count)
                            (c/select-single-value op c table-name :count "id = 0"))]
                ; Checker asserts the value is Long; JDBC INT → java.lang.Integer
                (assoc op :type :ok :value (some-> value long))))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))


(c/defclient YSQLCounterClient YSQLCounterYbClient)
