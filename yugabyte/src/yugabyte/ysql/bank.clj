(ns yugabyte.ysql.bank
  "Simulates transfers between bank accounts"
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.reconnect :as rc]
            [clojure.java.jdbc :as j]
            [yugabyte.ysql.client :as c]))

(def table-name "accounts")

(defrecord YSQLBankClient [conn-wrapper allow-negatives? setup? teardown?]
  client/Client

  (open! [this test node]
    (assoc this :conn-wrapper (c/conn-wrapper node)))

  (setup! [this test]
    (c/once-per-cluster
      setup?
      (info "Running setup")
      (c/with-conn
        [c conn-wrapper]
        (j/execute! c (j/create-table-ddl table-name [[:id :int "PRIMARY KEY"]
                                                      [:balance :bigint]]))
        (c/with-retry
          (info "Creating accounts")
          (c/insert! c table-name {:id      (first (:accounts test))
                                   :balance (:total-amount test)})
          (doseq [acct (rest (:accounts test))]
            (c/insert! c table-name {:id      acct,
                                     :balance 0}))))))

  (invoke! [this test op]
    (c/with-errors
      op
      (c/with-conn
        [c conn-wrapper]
        (c/with-retry
          (case (:f op)
            :read
            (->> (str "SELECT id, balance FROM " table-name)
                 (c/query c)
                 (map (juxt :id :balance))
                 (into (sorted-map))
                 (assoc op :type :ok, :value))

            :transfer
            (let [{:keys [from to amount]} (:value op)]
              (c/with-txn
                c
                (let [query-str     (str "SELECT balance FROM " table-name " WHERE id = ?")
                      b-from-before (:balance (first (c/query c [query-str from])))
                      b-to-before   (:balance (first (c/query c [query-str to])))
                      b-from-after  (- b-from-before amount)
                      b-to-after    (+ b-to-before amount)
                      allowed?      (or allow-negatives? (pos? b-from-after))]
                  (if (not allowed?)
                    (assoc op :type :fail, :error [:negative from b-from-after])
                    (do (c/update! c table-name {:balance b-from-after} ["id = ?" from])
                        (c/update! c table-name {:balance b-to-after} ["id = ?" to])
                        (assoc op :type :ok)))))))))))

  (teardown! [this test]
    (c/once-per-cluster
      teardown?
      (info "Running teardown")
      (c/with-timeout
        (c/with-conn
          [c conn-wrapper]
          (c/with-retry
            (c/drop-table c table-name))))))

  (close! [this test]
    (rc/close! conn-wrapper)))
