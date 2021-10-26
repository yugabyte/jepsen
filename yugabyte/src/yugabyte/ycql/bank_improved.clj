(ns yugabyte.ycql.bank-improved
  (:refer-clojure :exclude
                  [test])
  (:require [clojure.tools.logging :refer [debug info warn]]
            [clojurewerkz.cassaforte.client :as cassandra]
            [clojurewerkz.cassaforte.cql :as cql]
            [yugabyte.bank-improved :as bank-improved]
            [clojurewerkz.cassaforte.query
             :as q
             :refer :all]
            [yugabyte.ycql.client :as c]))

(def setup-lock (Object.))
(def keyspace "jepsen")
(def table-name "accounts")
(def insert-ctr (atom (+ bank-improved/end-key 1)))

(c/defclient CQLBankImproved keyspace []
  (setup! [this test]
    (c/create-transactional-table
      conn table-name
      (q/if-not-exists)
      (q/column-definitions
        {:id          :int
         :balance     :bigint
         :primary-key [:id]}))
    (info "Creating accounts")
    (c/with-retry
      (cql/insert-with-ks conn keyspace table-name
                          {:id      (first (:accounts test))
                           :balance (:total-amount test)})
      (doseq [a (rest (:accounts test))]
        (cql/insert conn table-name
                    {:id a, :balance 0}))))

  (invoke! [this test op]
    (let [from (rand-nth (:accounts test))
          to (rand-nth (:accounts test))]
      (if (not= from to)
        (case (:f op)
          :read
          (c/with-errors
            op #{:read}
            (->> (cql/select-with-ks conn keyspace table-name)
                 (map (juxt :id :balance))
                 (into (sorted-map))
                 (assoc op :type :ok, :value)))

          :update
          (c/with-errors
            op #{:read}
            (let [{:keys [amount]} (:value op)]
              (do
                (cassandra/execute
                  conn
                  ; TODO: separate reads from updates?
                  (str "BEGIN TRANSACTION "
                       "UPDATE " keyspace "." table-name
                       " SET balance = balance - " amount " WHERE id = " from ";"

                       "UPDATE " keyspace "." table-name
                       " SET balance = balance + " amount " WHERE id = " to ";"
                       "END TRANSACTION;"))
                (assoc op :type :ok :value {:from from, :to to, :amount amount}))))


          :insert
          (let [transaction-result (c/with-errors
                                     op #{:read}
                                     (let [{:keys [amount]} (:value op)
                                           inserted-key (swap! insert-ctr inc)]
                                       (do
                                         (cassandra/execute
                                           conn
                                           (str "BEGIN TRANSACTION "
                                                "INSERT INTO " keyspace "." table-name
                                                " (id, balance) values (" inserted-key "," amount ");"

                                                "UPDATE " keyspace "." table-name
                                                " SET balance = balance - " amount " WHERE id = " from ";"
                                                "END TRANSACTION;"))
                                         (assoc op :type :ok :value {:from from, :to inserted-key, :amount amount}))))]
            (bank-improved/increment-atomic-on-ok transaction-result insert-ctr)))
        (assoc op :type :fail))))

  (teardown! [this test]))