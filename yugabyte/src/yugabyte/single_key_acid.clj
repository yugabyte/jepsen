(ns yugabyte.single-key-acid
  "Given a single table of hash column primary key and one value column with
  (value of --concurrency divided by 2 and by # of nodes) independent rows,
  verify that concurrent reads, writes and read-modify-write (UPDATE IF) operations
  results in linearizable history.

  Here's the deal. Each group of 2N consequent worker threads allocated by --concurrency are assigned
  to a separate row key. Of these 2N workers, first N are performing writes/updates and
  the last N are reading current state. Worker groups (i.e. table rows) are completely independent.
  To illustrate this further, given --concurrency 20 and N = 5:
  - Workers  0 to  9 will be working with row #0
  - Workers 10 to 19 will be working with row #1
  - Workers 0 to 4 and 10 to 14 will be updating their respective rows
  - Workers 5 to 9 and 15 to 19 will be reading their respective rows"
  (:require [clojure [pprint :refer :all]]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [knossos.model :as model]
            [yugabyte.generator :as ygen]
            [jepsen.checker.timeline :as timeline]))

(def keys-count 2)

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int keys-count)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int keys-count) (rand-int keys-count)]})

(defn workload
  [opts]
  (let [n (count (:nodes opts))
        threads (:concurrency opts)]
    {:generator (ygen/with-op-index
                  (independent/concurrent-generator
                    (/ threads keys-count)
                    (cycle (range keys-count))
                    (fn [k]
                      (->> (gen/reserve (/ threads (* 2 keys-count)) (gen/mix [w cas cas]) r)
                           (gen/stagger 1)
                           (gen/process-limit threads)))))
     :checker   (independent/checker
                  (checker/compose
                    {:timeline (timeline/html)
                     :linear   (checker/linearizable
                                 {:model (model/cas-register 0)})}))}))
