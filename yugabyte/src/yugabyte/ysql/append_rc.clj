(ns yugabyte.ysql.append-rc
  "Implementation for READ COMMITTED transaction isolation"
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [yugabyte.ysql.append :as append]
            [yugabyte.ysql.client :as c])
  (:import (java.sql Connection)))

(defrecord InternalClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (->> (range (append/table-count test))
         (map append/table-name)
         (map (fn [table]
                (info "Creating table" table)
                (c/execute! c (j/create-table-ddl
                                table
                                (into
                                  [;[:k :int "unique"]
                                   [:k :int "PRIMARY KEY"]
                                   [:k2 :int]]
                                  ; Columns for n values packed in this row
                                  (map (fn [i] [(append/col-for test i) :text])
                                       (range append/keys-per-row)))
                                {:conditional? true}))))
         dorun))

  (invoke-op! [this test op c conn-wrapper]
    (let [txn (:value op)
          use-txn? (< 1 (count txn))
          ; use-txn?  false ; Just for making sure the checker actually works
          isolation Connection/TRANSACTION_READ_COMMITTED
          txn' (if use-txn?
                 (j/with-db-transaction [c c]
                                        (mapv (partial append/mop! c test) txn) {:isolation isolation})
                 (mapv (partial append/mop! c test) txn))]
      (assoc op :type :ok, :value txn'))))

(c/defclient Client InternalClient)
