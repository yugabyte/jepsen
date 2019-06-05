(ns yugabyte.ysql.counter
  "Something like YCQL 'counter' test. SQL does not have counter type though, so we just use int."
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.reconnect :as rc]
            [jepsen.util :refer [meh]]
            [clojure.java.jdbc :as j]
            [yugabyte.ysql.client :as c]))

(def table-name "counter")

(defrecord YSQLCounterClient [tbl-created? conn]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/client node)))

  (setup! [this test]
    (c/setup-once tbl-created?
                  (c/with-conn [c conn]
                               (j/execute! c (j/create-table-ddl table-name [[:id :int "PRIMARY KEY"]
                                                                             [:count :int]]))

                               (c/insert! c table-name {:id 0 :count 1}))))


  (invoke! [this test op]
    (c/with-exception->op op
                          (c/with-conn [c conn]
                                       (c/with-txn-retry
                                         (case (:f op)
                                           :add (do (c/update! c table-name
                                                               {:count (str "count + " (:value op))}
                                                               ["id = ?" 0])
                                                    (assoc op :type :ok))

                                           :read (let [value (c/query c (str "SELECT count(*) FROM " table-name " WHERE id = 0"))]
                                                   (assoc op :type :ok :value value)))))))

  (teardown! [this test]
    (c/with-timeout
      (meh (c/with-conn [c conn]
                        (c/drop-table c table-name)))))

  (close! [this test]
    (rc/close! conn)))
