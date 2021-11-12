(ns yugabyte.bank-improved
  "Reworked original bank workload that now include inserts and deletes.

  Generator now throws dice in [:insert :delete :update]

  :update behaves as default bank workload operation.

  :insert appends new key to the end of list. Uses atomic counter incremental that initial value is MAX_KEY.
  To maintain invariant add account (balance = amount) for insert and set (balance = balance - amount) for update.
  Atomic will be incremented only if transaction is :ok using increment-atomic-on-ok function

  :delete removes key from the beginning of the list. It use atomic counter stat starts with beginning.
  Atomic will be incremented only if transaction is :ok using increment-atomic-on-ok function

  Due to atomic connection between gen and actual clinet implementation, generator now only provides
  stream of operations (insert/update/delete + amount) while FROM and TO will be selected by client itself.
  This allow us to leave atomics and atomic manage on client side"
  (:refer-clojure :exclude
                  [test])
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info]]
            [jepsen.tests.bank :as bank]
            [knossos.op :as op]
            [clojure.core.reducers :as r]
            [jepsen
             [generator :as gen]
             [checker :as checker]
             [util :as util]]))

(def start-key 0)
(def end-key 5)

(def insert-key-ctr (atom end-key))
(def contention-keys (range end-key (+ end-key 3)))

(defn transfer-with-inserts
  "Copied from original jepsen.tests.bank workload

  Default transfer function with insert support. Special case for YCQL"
  [test process]
  (let [dice (rand-nth [:insert :update])]
    (cond
      (= dice :insert)
      {:type  :invoke
       :f     dice
       :value {:from   (rand-nth (:accounts test))
               :to     (swap! insert-key-ctr inc)
               :amount (+ 1 (rand-int (:max-transfer test)))}}

      (= dice :update)
      {:type  :invoke
       :f     dice
       :value {:from   (rand-nth (:accounts test))
               :to     (rand-nth (:accounts test))
               :amount (+ 1 (rand-int (:max-transfer test)))}})))

(defn transfer-contention-keys
  "Copied from original jepsen.tests.bank workload

  To produce contation we have single key that will be inserted, deleted or may be updates.

  Generator of a transfer: a random amount between two randomly selected
  accounts."
  [test process]
  (let [dice (rand-nth (:operations test))]
    (cond
      (= dice :insert)
      {:type  :invoke
       :f     dice
       :value {:from   (rand-nth (:accounts test))
               :to     (rand-nth contention-keys)
               :amount (+ 1 (rand-int (:max-transfer test)))}}

      (= dice :update)
      {:type  :invoke
       :f     dice
       :value {:from   (rand-nth (concat (:accounts test) contention-keys))
               :to     (rand-nth (concat (:accounts test) contention-keys))
               :amount (+ 1 (rand-int (:max-transfer test)))}}

      (= dice :delete)
      {:type  :invoke
       :f     dice
       :value {:from   (rand-nth contention-keys)
               :to     (rand-nth (:accounts test))
               :amount (+ 1 (rand-int (:max-transfer test)))}})))

(def diff-transfer-insert
  "Copied from original jepsen.tests.bank workload

  Transfers only between different accounts."
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              transfer-with-inserts))

(def diff-transfer-contention
  "Copied from original jepsen.tests.bank workload

  Transfers only between different accounts."
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              transfer-contention-keys))

(defn check-op
  "Copied code from original jepsen.test.bank/check-op
  Here we need to exclude :negative-value and :unexpected-key checks"
  [accts total op]
  (let [ks (keys (:value op))
        balances (vals (:value op))]
    (cond
      (some nil? balances)
      {:type :nil-balance
       :nils (->> (:value op)
                  (remove val)
                  (into {}))
       :op   op}

      (not= total (reduce + balances))
      {:type  :wrong-total
       :total (reduce + balances)
       :op    op})))

(defn checker
  "Copied code from original jepsen.test.bank/checker
  Since we have internal check-op call this function needs to be modified"
  [checker-opts]
  (reify
    checker/Checker
    (check [this test history opts]
      (let [accts (set (:accounts test))
            total (:total-amount test)
            reads (->> history
                       (r/filter op/ok?)
                       (r/filter #(= :read (:f %))))
            errors (->> reads
                        (r/map
                          (partial check-op
                                   accts
                                   total))
                        (r/filter identity)
                        (group-by :type))]
        {:valid?      (every? empty? (vals errors))
         :read-count  (count (into [] reads))
         :error-count (reduce + (map count (vals errors)))
         :first-error (util/min-by (comp :index :op) (map first (vals errors)))
         :errors      (->> errors
                           (map
                             (fn [[type errs]]
                               [type
                                (merge
                                  {:count (count errs)
                                   :first (first errs)
                                   :worst (util/max-by
                                            (partial bank/err-badness test)
                                            errs)
                                   :last  (peek errs)}
                                  (if (= type :wrong-total)
                                    {:lowest  (util/min-by :total errs)
                                     :highest (util/max-by :total errs)}
                                    {}))]))
                           (into {}))}))))

(defn workload-with-inserts
  [opts]
  {:max-transfer 5
   :total-amount 100
   :accounts     (vec (range end-key))
   :checker      (checker/compose
                   {:SI   (checker opts)
                    :plot (bank/plotter)})
   :generator    (gen/mix [diff-transfer-insert
                           bank/read])})

(defn workload-contention-keys
  [opts]
  {:max-transfer 5
   :total-amount 100
   :accounts     (vec (range end-key))
   :operations   [:insert :update :delete]
   :checker      (checker/compose
                   {:SI   (checker opts)
                    :plot (bank/plotter)})
   :generator    (gen/mix [diff-transfer-contention
                           bank/read])})
