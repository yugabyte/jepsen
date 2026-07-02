(ns yugabyte.monotonic
  "Monotonic-reads session guarantee: a single client that keeps reading a
  monotonically-increasing register must never observe the value going
  backwards. Half the workers increment a shared register; the rest read it.
  A per-process decrease is a monotonic-reads violation (e.g. a stale follower
  read or a connection re-routed to a lagging replica)."
  (:require [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [yugabyte.generator :as ygen]))

(defn checker
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [reads   (filter #(and (= :ok (:type %)) (= :read (:f %))) history)
            errs    (->> (group-by :process reads)
                         (mapcat (fn [[p ops]]
                                   (->> (map :value ops)
                                        (remove nil?)
                                        (partition 2 1)
                                        (keep (fn [[a b]]
                                                (when (> a b)
                                                  {:process p, :from a, :to b}))))))
                         vec)]
        {:valid?        (empty? errs)
         :non-monotonic errs}))))

(defn workload
  [opts]
  (let [threads (:concurrency opts)]
    {:generator (->> (gen/reserve (quot threads 2)
                                  (repeat {:type :invoke, :f :inc})
                                  ; Must be an infinite generator, not a bare op
                                  ; map: a lone map emits once and then exhausts,
                                  ; which starves reads to a single op.
                                  (repeat {:type :invoke, :f :read}))
                     (gen/stagger (/ 1 threads))
                     (ygen/with-op-index))
     :checker   (checker)}))
