(ns yugabyte.bank-improved
  "Simulates transfers between bank accounts with inserts and deletes"
  (:refer-clojure :exclude [test])
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info]]
            [jepsen.tests.bank :as bank]
            [knossos.op :as op]
            [clojure.core.reducers :as r]
            [jepsen [generator :as gen]
             [checker :as checker]
             [store :as store]
             [util :as util]]
            [yugabyte.generator :as ygen]))

(defn check-op
  "Takes a single op and returns errors in its balance"
  [accts total negative-balances? op]
  (let [ks       (keys (:value op))
        balances (vals (:value op))]
    (cond (not-every? accts ks)
      (some nil? balances)
      {:type    :nil-balance
       :nils    (->> (:value op)
                     (remove val)
                     (into {}))
       :op      op}

      (not= total (reduce + balances))
      {:type     :wrong-total
       :total    (reduce + balances)
       :op       op})))


(defn err-badness
  "Takes a bank error and returns a number, depending on its type. Bigger
  numbers mean more egregious errors."
  [test err]
  (case (:type err)
    :nil-balance    (count (:nils err))
    :wrong-total    (Math/abs (float (/ (- (:total err) (:total-amount test))
                                        (:total-amount test))))))

(defn checker
  "Verifies that all reads must sum to (:total test), and, unless
  :negative-balances? is true, checks that all balances are
  non-negative."
  [checker-opts]
  (reify checker/Checker
         (check [this test history opts]
                (let [accts (set (:accounts test))
                      total (:total-amount test)
                      reads (->> history
                                 (r/filter op/ok?)
                                 (r/filter #(= :read (:f %))))
                      errors (->> reads
                                  (r/map (partial check-op
                                                  accts
                                                  total
                                                  (:negative-balances? checker-opts)))
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
                                         (merge {:count (count errs)
                                                 :first (first errs)
                                                 :worst (util/max-by
                                                         (partial err-badness test)
                                                         errs)
                                                 :last  (peek errs)}
                                                (if (= type :wrong-total)
                                                  {:lowest  (util/min-by :total errs)
                                                   :highest (util/max-by :total errs)}
                                                  {}))]))
                                     (into {}))}))))

(defn workload
  [opts]
  ([opts]
   {:max-transfer  5
    :total-amount  100
    :accounts      (vec (range 8))
    :checker       (checker/compose {:SI    (bank/checker opts)
                                     :plot  (bank/plotter)})
    :generator     (bank/generator)}))