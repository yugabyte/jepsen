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
(def end-key-thick 10)
(def insert-ctr (atom end-key-thick))
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
                 ; needed for improve logging in case of fail
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
          (let [transaction-result
                (c/with-txn
                 c
                 (let [b-from-before            (c/select-single-value op c table-name :balance (str "id = " @delete-ctr))
                       b-to-before              (c/select-single-value op c table-name :balance (str "id = " to))
                       ; needed for improve logging in case of fail
                       op                       (assoc op :value {:from @delete-ctr :to to :amount b-from-before})]
                   (cond
                     (or (nil? b-from-before) (nil? b-to-before) (= to @delete-ctr))
                     (assoc op :type :fail)

                     :else
                     (let [b-to-after-delete    (+ b-to-before b-from-before)]
                       (do
                         (c/update! op c table-name {:balance b-to-after-delete} ["id = ?" to])
                         (c/execute! op c [(str "delete from " table-name " where id = ?") @delete-ctr])
                         (assoc op :type :ok :value {:from @delete-ctr, :to to, :amount b-from-before}))))))]
            ; increment atomic only if transaction is most-likely-fine
            (bank-improved/increment-atomic-on-ok transaction-result delete-ctr))


          :insert
          (let [transaction-result
                (c/with-txn
                 c
                 (let [b-from-before             (c/select-single-value op c table-name :balance (str "id = " from))
                       b-to-before               (c/select-single-value op c table-name :balance (str "id = " @insert-ctr))
                       ; needed for improve logging in case of fail
                       op                        (assoc op :value {:from from :to @insert-ctr :amount amount})]
                   (cond
                     (or (nil? b-from-before) (not (nil? b-to-before)))
                     (assoc op :type :fail)

                     :else
                     (let [b-from-after         (- b-from-before amount)]
                       (do
                         (c/insert! op c table-name {:id @insert-ctr :balance amount})
                         (c/update! op c table-name {:balance b-from-after} ["id = ?" from])
                         (assoc op :type :ok :value {:from from, :to @insert-ctr, :amount amount}))))))]
            ; increment atomic only if transaction is most-likely-fine
            (bank-improved/increment-atomic-on-ok transaction-result insert-ctr)))
        (assoc op :type :fail))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))


(c/defclient YSQLBankImprovedClient YSQLBankImprovedYBClient)


(defrecord YSQLBankContentionYBClient []
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
    (case (:f op)
      :read
      (let [table-data    (read-accounts-map op c)]
        (assoc op :type :ok, :value (read-accounts-map op c)))

      :update
      (c/with-txn
       c
       (let [{:keys [from to amount]} (:value op)
             b-from-before            (c/select-single-value op c table-name :balance (str "id = " from))
             b-to-before              (c/select-single-value op c table-name :balance (str "id = " to))]
         (cond
           (or (nil? b-from-before) (nil? b-to-before))
           (assoc op :type :fail)

           :else
           (let [b-from-after         (- b-from-before amount)
                 b-to-after           (+ b-to-before amount)]
             (do
               (c/update! op c table-name {:balance b-from-after} ["id = ?" from])
               (c/update! op c table-name {:balance b-to-after} ["id = ?" to])
               (assoc op :type :ok))))))

      :delete
      (c/with-txn
       c
       (let [{:keys [from to amount]} (:value op)
             b-from-before            (c/select-single-value op c table-name :balance (str "id = " from))
             b-to-before              (c/select-single-value op c table-name :balance (str "id = " to))]
         (cond
           (or (nil? b-from-before) (nil? b-to-before))
           (assoc op :type :fail)

           :else
           (let [b-to-after-delete    (+ b-to-before b-from-before)]
             (do
               (c/execute! op c [(str "delete from " table-name " where id = ?") from])
               (c/update! op c table-name {:balance b-to-after-delete} ["id = ?" to])
               (assoc op :type :ok :value {:from from, :to to, :amount b-from-before}))))))

      :insert
      (c/with-txn
       c
       (let [{:keys [from to amount]} (:value op)
             b-from-before            (c/select-single-value op c table-name :balance (str "id = " from))
             b-to-before              (c/select-single-value op c table-name :balance (str "id = " to))]
         (cond
           (or (nil? b-from-before) (not (nil? b-to-before)))
           (assoc op :type :fail)

           :else
           (let [b-from-after         (- b-from-before amount)]
             (do
               (c/update! op c table-name {:balance b-from-after} ["id = ?" from])
               (c/insert! op c table-name {:id to :balance amount})
               (assoc op :type :ok :value {:from from, :to to, :amount amount}))))))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))

(c/defclient YSQLBankContentionClient YSQLBankContentionYBClient)