(ns yugabyte.ysql.bank
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen.random :as random]
            [yugabyte.ysql.client :as c]))

(def table-name "accounts")
(def index-name "idx_accounts")

;
; Single-table bank test
;

(defn- read-accounts-map
  "Read {id balance} accounts map from a unified bank table"
  [op c]
  (let [use-index? (zero? (random/long 2))]
    (info table-name (if use-index? "IndexOnlyScan" "SeqScan"))
    (->> (str (when use-index?
                (str "/*+ IndexOnlyScan(" table-name " " index-name ") */ "))
              "SELECT id, balance FROM " table-name)
         (c/query op c)
         (map (juxt :id :balance))
         (into (sorted-map)))))

(defrecord YSQLBankYbClient [allow-negatives? isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name [[:id :int "PRIMARY KEY"]
                                                  [:balance :bigint]]))
    (c/execute! c (str "CREATE INDEX " index-name " ON " table-name " (id, balance)"))
    (c/with-retry
      (info "Creating accounts")
      (c/insert! c table-name {:id      (first (:accounts test))
                               :balance (:total-amount test)})
      (doseq [acct (rest (:accounts test))]
        (c/insert! c table-name {:id      acct,
                                 :balance 0}))))


  (invoke-op! [this test op c conn-wrapper]
    (case (:f op)
      :read
      (assoc op :type :ok, :value (read-accounts-map op c))

      :transfer
      (j/with-db-transaction [c c {:isolation isolation}]
        (let [{:keys [from to amount]} (:value op)]
          (let [b-from-before (c/select-single-value op c table-name :balance (str "id = " from))
                b-to-before   (c/select-single-value op c table-name :balance (str "id = " to))
                b-from-after  (- b-from-before amount)
                b-to-after    (+ b-to-before amount)
                allowed?      (or allow-negatives? (pos? b-from-after))]
            (if (not allowed?)
              (assoc op :type :fail, :error [:negative from b-from-after])
              (do (c/update! op c table-name {:balance b-from-after} ["id = ?" from])
                  (c/update! op c table-name {:balance b-to-after} ["id = ?" to])
                  (assoc op :type :ok))))))))


  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))


(c/defclient YSQLBankClient YSQLBankYbClient)


;
; Multi-table bank test
;

(defrecord YSQLMultiBankYbClient [allow-negatives? isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]

    (doseq [a (:accounts test)]
      (let [acc-table-name (str table-name a)
            acc-index-name (str index-name a)
            balance        (if (= a (first (:accounts test)))
                             (:total-amount test)
                             0)]
        (info "Creating table" a)
        (c/execute! c (j/create-table-ddl acc-table-name [[:id :int "PRIMARY KEY"]
                                                          [:balance :bigint]]))
        (c/execute! c (str "CREATE INDEX " acc-index-name " ON " acc-table-name " (id, balance)"))

        (info "Populating account" a " (balance =" balance ")")
        (c/with-retry
          (c/insert! c acc-table-name {:id      a
                                       :balance balance})))))


  (invoke-op! [this test op c conn-wrapper]
    (case (:f op)
      :read
      (j/with-db-transaction [c c {:isolation isolation}]
        (let [accs (random/shuffle (:accounts test))]
          (->> accs
               (mapv (fn [a]
                       (let [tbl (str table-name a)
                             idx (str index-name a)
                             use-index? (zero? (random/long 2))]
                         (info tbl (if use-index? "IndexOnlyScan" "SeqScan"))
                         (if use-index?
                           (-> (c/query op c (str "/*+ IndexOnlyScan(" tbl " " idx ") */ SELECT balance FROM " tbl " WHERE id = " a))
                               first :balance)
                           (c/select-single-value op c tbl :balance (str "id = " a))))))
               (zipmap accs)
               (assoc op :type :ok, :value))))

      :transfer
      (let [{:keys [from to amount]} (:value op)]
        (j/with-db-transaction [c c {:isolation isolation}]
          (let [b-from-before (c/select-single-value op c (str table-name from) :balance (str "id = " from))
                b-to-before   (c/select-single-value op c (str table-name to) :balance (str "id = " to))
                b-from-after  (- b-from-before amount)
                b-to-after    (+ b-to-before amount)
                allowed?      (or allow-negatives? (pos? b-from-after))]
            (if (not allowed?)
              (assoc op :type :fail, :error [:negative from b-from-after])
              (do (c/update! op c (str table-name from) {:balance b-from-after} ["id = ?" from])
                  (c/update! op c (str table-name to) {:balance b-to-after} ["id = ?" to])
                  (assoc op :type :ok))))))))


  (teardown-cluster! [this test c conn-wrapper]
    (doseq [a (:accounts test)]
      (c/drop-table c (str table-name a)))))


(c/defclient YSQLMultiBankClient YSQLMultiBankYbClient)
