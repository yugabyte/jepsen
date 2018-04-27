(ns yugabyte.multi-key-acid
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen [client    :as client]
                    [checker   :as checker]
                    [generator :as gen]
                    [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [clojurewerkz.cassaforte [client :as cassandra]
                                     [query :refer :all]
                                     [policies :refer :all]
                                     [cql :as cql]]
            [yugabyte.core :refer :all]
            )
  (:import (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)))

(def table-name "multi_key_acid")

(defrecord CQLMultiKey [conn]
  client/Client
  (open! [this test node]
    (info "Opening connection to " node)
    (assoc this :conn (cassandra/connect [node] {:protocol-version 3})))
  (setup! [this test]
          (locking setup-lock
            (cql/create-keyspace conn keyspace
                                 (if-not-exists)
                                 (with
                                   {:replication
                                    {"class"              "SimpleStrategy"
                                     "replication_factor" 3}}))
            (cql/use-keyspace conn keyspace)
            (cassandra/execute conn (str "CREATE TABLE IF NOT EXISTS " table-name " (id INT PRIMARY KEY, val INT) "
                                         "WITH transactions = { 'enabled' : true }"))
            (->CQLMultiKey conn)))
  (invoke! [this test op]
           (case (:f op)
             :write (try
                      (let [[k1 k2 val] (:value op)]
                        (cassandra/execute conn "BEGIN TRANSACTION")
                        (cql/insert conn table-name {:id k1 :val val})
                        (cql/insert conn table-name {:id k2 :val val})
                        (cassandra/execute conn "END TRANSACTION;")
                        (assoc op :type :ok))
                      (catch UnavailableException e
                        (assoc op :type :fail :value (.getMessage e)))
                      (catch WriteTimeoutException e
                        (assoc op :type :info :value :timed-out))
                      (catch NoHostAvailableException e
                        (info "All nodes are down - sleeping 2s")
                        (Thread/sleep 2000)
                        (assoc op :type :fail :value (.getMessage e))))
             :read  (try (wait-for-recovery 30 conn)
                      (assoc op :type :ok :value (cql/select conn table-name))
                      (catch UnavailableException e
                        (info "Not enough replicas - failing")
                        (assoc op :type :fail :value (.getMessage e)))
                      (catch ReadTimeoutException e
                        (assoc op :type :fail :value :timed-out))
                      (catch NoHostAvailableException e
                        (info "All nodes are down - sleeping 2s")
                        (Thread/sleep 2000)
                        (assoc op :type :fail :value (.getMessage e))))))
  (teardown! [this test])
  (close! [this test]
    (info "Closing client with conn" conn)
    (cassandra/disconnect! conn)))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value [(rand-int 10) (rand-int 10) (rand-int 100)]})

(defn test
  [opts]
  (yugabyte-test
   (merge opts
          {:name             "Multi key ACID"
           :client           (CQLMultiKey. nil)
           :client-generator (->> (gen/reserve (quot (:concurrency opts) 2) w r)
                                  (gen/delay-til 1/2)
                                  (gen/stagger 0.1)
                                  (gen/limit 100))
;           :model (model/cas-register 0)
           :checker          (checker/compose
                              {:perf     (checker/perf)
                               :timeline (timeline/html)
;                               :linear   (checker/linearizable)
                               })})))
