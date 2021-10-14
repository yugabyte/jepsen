(ns yugabyte.ysql.bank-improved
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.reconnect :as rc]
            [yugabyte.ysql.client :as c]))

(def table-name "accounts")
(def counter-start (atom 0))
(def counter-end (atom 0))


;
; Single-table bank test
;

(defn- read-accounts-map
  "Read {id balance} accounts map from a unified bank table"
  [op c]
  (->> (str "SELECT id, balance FROM " table-name)
       (c/query op c)
       (map (juxt :id :balance))
       (into (sorted-map))))

(defrecord YSQLBankYbClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name [[:id :int "PRIMARY KEY"]
                                                  [:balance :bigint]]))
    (c/execute! c [(str "create index idx_account on " table-name " (id, balance)")])
    (c/with-retry
     (info "Creating accounts")
     (c/insert! c table-name {:id      (first (:accounts test))
                              :balance (:total-amount test)})
     (doseq [acct (rest (:accounts test))]
       (do
         (swap! counter-end inc)
         (c/insert! c table-name {:id      acct,
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
             b-to-before              (c/select-single-value op c table-name :balance (str "id = " to))
             from-empty               (nil? b-from-before)
             to-empty                 (nil? b-to-before)
             dice                     (rand-nth ["insert" "update" "delete"])]
         (cond
           (and from-empty (not to-empty))
           (let [b-to-after           (- b-to-before amount)
                 counter-value        (swap! counter-end inc)]
             (do
               (c/insert! op c table-name {:id counter-value :balance amount})
               (c/update! op c table-name {:balance b-to-after} ["id = ?" to])
               (info "Insert: " {:from counter-value, :to to, :amount amount})
               (assoc op :type :ok :value {:from counter-value, :to to, :amount amount})))

           (and to-empty (not from-empty))
           (let [b-from-after         (- b-from-before amount)
                 counter-value        (swap! counter-end inc)]
             (do
               (c/insert! op c table-name {:id counter-value :balance amount})
               (c/update! op c table-name {:balance b-from-after} ["id = ?" to])
               (info "Insert: " {:from counter-value, :to to, :amount amount})
               (assoc op :type :ok :value {:from counter-value, :to to, :amount amount})))

           (and to-empty from-empty)
           (do
             (info "Skipped")
             (assoc op :type :fail))

           (= dice "insert")
           (let [b-from-after         (- b-from-before amount)
                 counter-value        (swap! counter-end inc)]
             (do
               (c/insert! op c table-name {:id counter-value :balance amount})
               (c/update! op c table-name {:balance b-from-after} ["id = ?" to])
               (info "Insert: " {:from counter-value, :to to, :amount amount})
               (assoc op :type :ok :value {:from counter-value, :to to, :amount amount})))

           (= dice "update")
           (let [b-from-after         (- b-from-before amount)
                 b-to-after           (+ b-to-before amount)]
             (do
               (c/update! op c table-name {:balance b-from-after} ["id = ?" from])
               (c/update! op c table-name {:balance b-to-after} ["id = ?" to])
               (info "Update: " {:from from, :to to, :amount amount})
               (assoc op :type :ok)))

           (= dice "delete")
           (let [counter-value        (swap! counter-start inc)
                 b-from-before        (c/select-single-value op c table-name :balance (str "id = " counter-value))]
             ; fail transaction if target is equal to deleted one
             ; or from value is nil
             (if (or (= counter-value to) (nil? b-from-before))
               (do
                 (info "Skipped delete")
                 (assoc op :type :fail))
               (let [b-to-after-delete    (+ b-to-before b-from-before)]
                 (do
                   (c/execute! op c [(str "delete from " table-name " where id = ?") counter-value])
                   (c/update! op c table-name {:balance b-to-after-delete} ["id = ?" to])
                   (info "Delete: " {:from counter-value, :to to, :amount b-from-before})
                   (assoc op :type :ok :value {:from counter-value, :to to, :amount b-from-before}))))))))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))


(c/defclient YSQLBankClient YSQLBankYbClient)
