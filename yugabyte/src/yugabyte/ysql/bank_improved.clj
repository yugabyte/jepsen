(ns yugabyte.ysql.bank-improved
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [yugabyte.bank-improved :as bank-improved]
            [jepsen.reconnect :as rc]
            [yugabyte.ysql.client :as c]))

(def table-name "accounts")
(def table-index "idx_accounts")
(def insert-ctr (atom (+ bank-improved/end-key 1)))
(def delete-ctr (atom bank-improved/start-key))

;
; Single-table bank improved test
;

(defn- read-accounts-map
  "Read {id balance} accounts map from a unified bank table using force index flag"
  ([c]
   (->>
    (str "SELECT id, balance FROM " table-name)
    (c/query c)
    (map (juxt :id :balance))
    (into (sorted-map))))
  ([op c]
   (->>
    (str "/*+ IndexOnlyScan(" table-name " " table-index ") */ SELECT id, balance FROM " table-name)
    (c/query op c)
    (map (juxt :id :balance))
    (into (sorted-map)))))

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
         (c/insert! c table-name
                    {:id      acct,
                     :balance 0})))))


  (invoke-op! [this test op c conn-wrapper]
    (let [{:keys [amount]} (:value op)
          from             (+ @delete-ctr (rand-int (- @insert-ctr @delete-ctr)))
          to               (+ @delete-ctr (rand-int (- @insert-ctr @delete-ctr)))]
      (if (not= from to)
        (case (:f op)
          :read
          (let [table-data            (read-accounts-map op c)]
            (assoc op :type :ok, :value (read-accounts-map op c)))

          :update
          (c/with-txn
           c
           (let [b-from-before            (c/select-single-value op c table-name :balance (str "id = " from))
                 b-to-before              (c/select-single-value op c table-name :balance (str "id = " to))
                 op                       (assoc op :value {:from from :to to :amount amount})]
             (cond
               (or (nil? b-from-before) (nil? b-to-before))
               (assoc op :type :fail)

               :else
               (let [b-from-after         (- b-from-before amount)
                     b-to-after           (+ b-to-before amount)]
                 (do
                   (c/update! op c table-name {:balance b-from-after} ["id = ?" from])
                   (c/update! op c table-name {:balance b-to-after} ["id = ?" to])
                   (assoc op :type :ok :value {:from from, :to to, :amount b-from-before}))))))

          :delete
          (bank-improved/increment-atomic-on-ok
           (c/with-txn
            c
            (let [b-from-before            (c/select-single-value op c table-name :balance (str "id = " @delete-ctr))
                  b-to-before              (c/select-single-value op c table-name :balance (str "id = " to))
                  op                       (assoc op :value {:from from :to to :amount b-from-before})]
              (cond
                (or (nil? b-from-before) (nil? b-to-before))
                (assoc op :type :fail)

                :else
                (let [b-to-after-delete    (+ b-to-before b-from-before)]
                  (do
                    (c/update! op c table-name {:balance b-to-after-delete} ["id = ?" to])
                    (c/execute! op c [(str "delete from " table-name " where id = ?") @delete-ctr])
                    (assoc op :type :ok :value {:from from, :to to, :amount b-from-before}))))))
           delete-ctr)

          :insert
          (bank-improved/increment-atomic-on-ok
           (c/with-txn
            c
            (let [b-from-before            (c/select-single-value op c table-name :balance (str "id = " from))
                  op                       (assoc op :value {:from from :to @insert-ctr :amount amount})]
              (cond
                ; need to check only b-from-before here
                (nil? b-from-before)
                (assoc op :type :fail)

                :else
                (let [b-from-after         (- b-from-before amount)]
                  (do
                    (c/update! op c table-name {:balance b-from-after} ["id = ?" from])
                    (c/insert! op c table-name {:id @insert-ctr :balance amount})
                    (assoc op :type :ok :value {:from from, :to @insert-ctr, :amount amount}))))))
           insert-ctr))
        (assoc op :type :fail))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))


(c/defclient YSQLBankImprovedClient YSQLBankImprovedYBClient)
