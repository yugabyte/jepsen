(ns yugabyte.upsert
  "Concurrent INSERT ... ON CONFLICT DO NOTHING against a small, contended key
  space. Each :upsert reports whether it actually inserted a row. Because the
  primary key makes at most one insert win per key (and DO NOTHING never
  updates), the invariants are:

    1. At most one upsert reports success per key (no duplicate winners / lost
       uniqueness).
    2. Every value read for a key equals the winning insert's value.

  Catches lost-upsert / duplicate-key bugs under contention. Valid at every
  isolation level (uniqueness must hold under read-committed and snapshot too)."
  (:require [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [yugabyte.generator :as ygen]))

(def key-count
  "Small key space so upserts contend heavily."
  20)

(defn upserts
  []
  (->> (range)
       (map (fn [i] {:type :invoke, :f :upsert, :value [(mod i key-count) i]}))
       (map gen/once)))

(defn reads
  []
  {:type :invoke, :f :read, :value nil})

(defn checker
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [oks     (filter #(= :ok (:type %)) history)
            ; key -> set of values whose upsert reported success
            winners (reduce (fn [m op]
                              (if (= :upsert (:f op))
                                (let [[k v inserted?] (:value op)]
                                  (if inserted?
                                    (update m k (fnil conj #{}) v)
                                    m))
                                m))
                            {} oks)
            dup-winners (into {} (filter (fn [[_ vs]] (> (count vs) 1)) winners))
            read-errs   (for [op   oks
                              :when (= :read (:f op))
                              [k v] (:value op)
                              :let  [win (get winners k)]
                              :when (and win (not (contains? win v)))]
                          {:key k, :read v, :expected win})]
        {:valid?            (and (empty? dup-winners) (empty? read-errs))
         :duplicate-winners dup-winners
         :read-errors       (vec read-errs)}))))

(defn workload
  [opts]
  (let [threads (:concurrency opts)]
    ; reads must be an infinite generator (repeat), not a single op map: a lone
    ; map emits once then exhausts, starving reads to one op for the whole run.
    {:generator (->> (gen/reserve (quot threads 2) (upserts) (repeat (reads)))
                     (gen/stagger (/ 1 threads))
                     (ygen/with-op-index))
     :checker   (checker)}))
