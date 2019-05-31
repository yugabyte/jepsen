(ns yugabyte.ysql.counter
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.reconnect :as rc]
            [clojure.java.jdbc :as j]
            [yugabyte.ysql.client :as c]))

(def table-name "counter")

(defrecord YSQLCounterClient [conn]
  client/Client

  (open! [this test node]
    (info " === Open conn... ===")
    (assoc node :conn (c/client node)))

  (setup! [this test]
    (info " === Setup running... ===")
    (c/with-conn [c conn]
                 (info " === j/execute! ===")
                 (j/execute! c (j/create-table-ddl table-name [[:id :int "PRIMARY KEY"]
                                                               [:count :counter]]))

                 (info " === c/update! ===")
                 (c/update! c table-name {:count "count + 1"} ["id = ?" 0])
                 (info " === Setup done! ===")))

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
      (c/with-conn [c conn]
                   (c/drop-table c table-name))))

  (close! [this test]
    (rc/close! conn)))
