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
             [store :as store]
             [util :as util]]
            [yugabyte.generator :as ygen]))

(def start-key 0)
(def end-key 8)
(def contention-key end-key)

(defn increment-atomic-on-ok
  [result atomic]
  (if (= :ok (:type result))
    (do
      (swap! atomic inc)
      result)
    result))

(defn transfer-contention
  "Copied from original jepsen.tests.bank workload

  To produce contation we have single key that will be inserted, deleted or may be updates.

  Generator of a transfer: a random amount between two randomly selected
  accounts."
  [test process]
  (let [dice         (rand-nth (:operations test))]
    (cond
      (= dice :insert)
      {:type  :invoke
       :f     dice
       :value {:from   (rand-nth (:accounts test))
               :to     contention-key
               :amount (+ 1 (rand-int (:max-transfer test)))}}

      (= dice :update)
      {:type  :invoke
       :f     dice
       :value {:from   (rand-nth (conj (:accounts test) contention-key))
               :to     (rand-nth (conj (:accounts test) contention-key))
               :amount (+ 1 (rand-int (:max-transfer test)))}}

      (= dice :delete)
      {:type  :invoke
       :f     dice
       :value {:from   contention-key
               :to     (rand-nth (:accounts test))
               :amount (+ 1 (rand-int (:max-transfer test)))}})))

(defn transfer
  "Copied from original jepsen.tests.bank workload

  Generates only type of transction and leaves FROM and TO selection to a client

  Generator of a transfer: a random amount between two randomly selected
  accounts."
  [test process]
  (let [dice         (rand-nth (:operations test))]
    {:type  :invoke
     :f     dice
     :value {:from   nil
             :to     nil
             :amount (+ 1 (rand-int (:max-transfer test)))}}))

(defn check-op
  "Copied code from original jepsen.test.bank/check-op
  Here we need to exclude :negative-value and :unexpected-key checks"
  [accts total op]
  (let [ks       (keys (:value op))
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
          (let [accts  (set (:accounts test))
                total  (:total-amount test)
                reads  (->> history
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

(defn workload-insert-update
  [opts]
  {:max-transfer 5
   :total-amount 100
   :accounts     (vec (range end-key))
   :operations   [:insert :update]
   :checker      (checker/compose
                  {:SI   (checker opts)
                   :plot (bank/plotter)})
   :generator    (gen/mix [transfer
                           bank/read])})

(defn workload-all-contention
  [opts]
  {:max-transfer 5
   :total-amount 100
   :accounts     (vec (range end-key))
   :operations   [:insert :update :delete]
   :checker      (checker/compose
                  {:SI   (checker opts)
                   :plot (bank/plotter)})
   :generator    (gen/mix [transfer-contention
                           bank/read])})

(defn workload-all
  [opts]
  {:max-transfer 5
   :total-amount 100
   :accounts     (vec (range end-key))
   :operations   [:insert :update :delete]
   :checker      (checker/compose
                  {:SI   (checker opts)
                   :plot (bank/plotter)})
   :generator    (gen/mix [transfer
                           bank/read])})
