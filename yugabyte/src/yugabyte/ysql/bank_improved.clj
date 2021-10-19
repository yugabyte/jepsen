(ns yugabyte.ysql.bank-improved
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.reconnect :as rc]
            [yugabyte.ysql.client :as c]))

(def table-name "accounts")
(def table-index "idx_accounts")


;
; Single-table bank test
;

(defn- read-accounts-map
  "Read {id balance} accounts map from a unified bank table using force index flag"
  [op c]
  (->>
    (str "/*+ IndexOnlyScan(" table-name " " table-index ") */ SELECT id, balance FROM " table-name)
    (c/query op c)
    (map (juxt :id :balance))
    (into (sorted-map))))

(defrecord YSQLBankImprovedYBClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c
                (j/create-table-ddl table-name
                                    [[:id :int "PRIMARY KEY"]
                                     [:balance :bigint]]))
    (c/execute! c [(str "create index " table-index " on " table-name " (id, balance)")])
    (c/with-retry
     (info "Creating accounts")
     (c/insert! c table-name
                {:id      (first (:accounts test))
                 :balance (:total-amount test)})
     (doseq [acct (rest (:accounts test))]
       (do
         (swap! counter-end inc)
         (c/insert! c table-name
                    {:id      acct,
                     :balance 0})))))


  (invoke-op! [this test op c conn-wrapper]
    (case (:f op)
      :read
      (assoc op :type :ok, :value (read-accounts-map op c))

      :transfer
      (c/with-txn
       c
       (let [{:keys [from to amount]} (:value op)
             b-from-before            (c/select-single-value op c table-name :balance (str "id = " from))
             b-to-before              (c/select-single-value op c table-name :balance (str "id = " to))]
         (cond
           (or (nil? b-from-before) (nil? b-to-before))
           (assoc op :type :fail)

           (= (:operation-type op) :insert)
           (let [b-from-after         (- b-from-before amount)]
             (do
               (c/update! op c table-name {:balance b-from-after} ["id = ?" from])
               (c/insert! op c table-name {:id to :balance amount})
               (assoc op :type :ok :value {:from from, :to to, :amount amount})))

           (= (:operation-type op) :update)
           (let [b-from-after         (- b-from-before amount)
                 b-to-after           (+ b-to-before amount)]
             (do
               (c/update! op c table-name {:balance b-from-after} ["id = ?" from])
               (c/update! op c table-name {:balance b-to-after} ["id = ?" to])
               (assoc op :type :ok)))

           (= (:operation-type op) :delete)
           (let [b-to-after-delete    (+ b-to-before b-from-before)]
             (do
               (c/execute! op c [(str "delete from " table-name " where id = ?") from])
               (c/update! op c table-name {:balance b-to-after-delete} ["id = ?" to])
               (assoc op :type :ok :value {:from from, :to to, :amount b-from-before}))))))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))


(c/defclient YSQLBankImprovedClient YSQLBankImprovedYBClient)
