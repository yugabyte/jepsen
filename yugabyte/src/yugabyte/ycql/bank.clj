(ns yugabyte.ycql.bank
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.random :as random]
            [yugabyte.ycql.client :as c]))

(def setup-lock (Object.))
(def keyspace   "jepsen")
(def table-name "accounts")

(c/defclient CQLBank keyspace []
  (setup! [this test]
    (c/create-transactional-table
      conn table-name
      {:id          :int
       :balance     :bigint
       :primary-key [:id]})
    (info "Creating accounts")
    (c/with-retry
      (c/insert! conn table-name
                 {:id (first (:accounts test))
                  :balance (:total-amount test)}
                 :keyspace keyspace)
      (doseq [a (rest (:accounts test))]
        (c/insert! conn table-name
                   {:id a, :balance 0}))))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :read
        (->> (c/select conn table-name :keyspace keyspace)
             (map (juxt :id :balance))
             (into (sorted-map))
             (assoc op :type :ok, :value))

        :transfer
        (let [{:keys [from to amount]} (:value op)]
          (c/execute!
            conn
            ; TODO: separate reads from updates?
            (str "BEGIN TRANSACTION "
                 "UPDATE " keyspace "." table-name
                 " SET balance = balance - " amount " WHERE id = " from ";"

                 "UPDATE " keyspace "." table-name
                 " SET balance = balance + " amount " WHERE id = " to ";"
                 "END TRANSACTION;"))
          (assoc op :type :ok)))))

  (teardown! [this test]))

;; Shouldn't be used until we support transactions with selects.
(c/defclient CQLMultiBank keyspace []
  (setup! [this test]
    (info "Creating accounts")
    (doseq [a (:accounts test)]
      (info "Creating table" a)
      (c/create-transactional-table
        conn (str table-name a)
        {:id          :int
         :balance     :bigint
         :primary-key [:id]})

      (info "Populating account" a)
      (c/with-retry
        (c/insert! conn (str table-name a)
                   {:id      a
                    :balance (if (= a (first (:accounts test)))
                               (:total-amount test)
                               0)}
                   :keyspace keyspace))))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :read
        (let [as (random/shuffle (:accounts test))]
          (->> as
               (mapv (fn [x]
                       ;; TODO - should be wrapped in a transaction after we
                       ;; support transactions with selects.
                       (->> (c/select conn (str table-name x)
                                      :keyspace keyspace
                                      :where [[:= :id x]])
                            first
                            :balance)))
               (zipmap as)
               (assoc op :type :ok, :value)))

        :transfer
        (let [{:keys [from to amount]} (:value op)]
          (c/execute! conn
                      (str "BEGIN TRANSACTION "
                           (str "UPDATE " keyspace "." table-name from
                                " SET balance = balance - " amount
                                " WHERE id = " from ";")
                           (str "UPDATE " keyspace "." table-name to
                                " SET balance = balance + " amount
                                " WHERE id = " to ";")
                           "END TRANSACTION;"))
          (assoc op :type :ok)))))

  (teardown! [this test]))
