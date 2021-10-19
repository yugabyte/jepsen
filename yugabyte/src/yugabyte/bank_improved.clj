(ns yugabyte.bank-improved
  "Improved default bank workload that now include inserts and deletes.

  On each operation we throw dice in [:insert :delete :update]

  :update behaves as default bank workload operation

  :insert appends new key to the end of list. Uses atomic counter incremental that initial value is MAX_KEY.
  To maintain invariant add account (balance = amount) for insert and set (balance = balance - amount) for update

  :delete removes key from the beginning of the list. Uses atomic counter incremental that initial value is 0.
  To maintain invariant store value from key-to-remove and transafer store value to existing account."
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

(def end-key 1000)

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
   :checker      (checker/compose
                  {:SI   (checker opts)
                   :plot (bank/plotter)})
   :generator    (ygen/with-insert-deletes (bank/generator) 0 (+ end-key 1) [:insert :update])})

(defn workload-all
  [opts]
  {:max-transfer 5
   :total-amount 100
   :accounts     (vec (range end-key))
   :checker      (checker/compose
                   {:SI   (checker opts)
                    :plot (bank/plotter)})
   :generator    (ygen/with-insert-deletes (bank/generator) 0 (+ end-key 1) [:insert :update :delete])})