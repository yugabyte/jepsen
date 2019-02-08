(ns yugabyte.counter
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen [client    :as client]
                    [checker   :as checker]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [clojurewerkz.cassaforte [client :as cassandra]
                                     [query :as q]
                                     [cql :as cql]]
            [yugabyte [client :as c]
                      [core :refer :all]]))

(def keyspace "jepsen")
(def table-name "counter")

(c/defclient CQLCounterClient keyspace []
  (setup! [this test]
    (c/create-table conn table-name
                    (q/if-not-exists)
                    (q/column-definitions {:id :int
                                         :count :counter
                                         :primary-key [:id]}))
    (cql/update conn table-name {:count (q/increment-by 0)}
                (q/where [[= :id 0]])))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :add (do (cql/update conn table-name
                                  {:count (q/increment-by (:value op))}
                                  (q/where [[= :id 0]]))
                 (assoc op :type :ok))

        :read (let [value (->> (cql/select conn table-name
                                                (q/where [[= :id 0]]))
                                    first
                                    :count)]
                     (assoc op :type :ok :value value)))))

  (teardown! [this test]))

(def add {:type :invoke :f :add :value 1})
(def sub {:type :invoke :f :add :value -1})
(def r   {:type :invoke :f :read})

(defn test-inc
  [opts]
  (yugabyte-test
    (merge opts
           {:name             "cql-counter-inc"
            :client           (->CQLCounterClient)
            :client-generator (->> (repeat 100 add)
                                   (cons r)
                                   gen/mix
                                   (gen/delay 1/10))
            :model            nil
            :checker         (checker/compose
                               {:perf     (checker/perf)
                                :timeline (timeline/html)
                                :counter  (checker/counter)})})))

(defn test-inc-dec
  [opts]
  (yugabyte-test
    (merge opts
           {:name             "cql-counter-inc-dec"
            :client           (->CQLCounterClient)
            :client-generator (->> (take 100 (cycle [add sub]))
                                   (cons r)
                                   gen/mix
                                   (gen/delay 1/10))
            :model            nil
            :checker          (checker/compose
                                {:perf     (checker/perf)
                                 :timeline (timeline/html)
                                 :counter  (checker/counter)})})))
