(ns yugabyte.ysql.counter
  "Something like YCQL 'counter' test. SQL does not have counter type though, so we just use int."
  (:require [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.reconnect :as rc]
            [yugabyte.ysql.client :as c]))

(def table-name "counter")

(defrecord YSQLCounterClientInner []
  c/YSQLClientBase

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name [[:id :int "PRIMARY KEY"]
                                                  [:count :int]]))
    (c/insert! c table-name {:id 0 :count 0}))

  (invoke-inner! [this test op c conn-wrapper]
    (case (:f op)
      ; update! can't handle column references
      :add (do (c/execute! c [(str "UPDATE " table-name " SET count = count + ? WHERE id = 0") (:value op)])
               (assoc op :type :ok))

      :read (let [value (->> (str "SELECT count FROM " table-name " WHERE id = 0")
                             (c/query c)
                             first
                             (:count))]
              (assoc op :type :ok :value value))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))


(c/defclient YSQLCounterClient YSQLCounterClientInner)
