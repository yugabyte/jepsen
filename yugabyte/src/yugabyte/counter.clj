(ns yugabyte.counter
  (:require [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.history :as history]
            [jepsen.checker.timeline :as timeline]
            [yugabyte.generator :as ygen]))


(defn add []  {:type :invoke :f :add :value 1})
(defn sub []  {:type :invoke :f :add :value -1})
(defn r   []  {:type :invoke :f :read})

(defn counter-checker
  "Wraps jepsen.checker/counter so it only sees client operations. jepsen
  0.3.11's counter checker asserts every op's :value is nil or a Long, which
  blows up on nemesis ops whose :value is a map (e.g. {node \"\"})."
  []
  (let [c (checker/counter)]
    (reify checker/Checker
      (check [_ test h opts]
        (checker/check c test (history/client-ops h) opts)))))

(defn workload
  [opts]
  {:generator (->> (repeat 100 add)
                   (cons r)
                   gen/mix
                   (gen/delay 1/10)
                   (ygen/with-op-index))
   :checker   (checker/compose
                {:timeline (timeline/html)
                 :counter  (counter-checker)})})

(defn workload-dec
  [opts]
  (assoc (workload opts)
    :generator (->> (take 100 (cycle [add sub]))
                    (cons r)
                    gen/mix
                    (gen/delay 1/10)
                    (ygen/with-op-index))))
