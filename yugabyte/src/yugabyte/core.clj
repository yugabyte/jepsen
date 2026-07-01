(ns yugabyte.core
  "Integrates workloads, nemeses, and automation to construct test maps."
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.random :as random]
            [jepsen.tests :as tests]
            [jepsen.os.debian :as debian]
            [jepsen.os.centos :as centos]
            [yugabyte [append :as append]
             [default-value :as default-value]
             [wr :as wr]
             [upsert :as upsert]
             [types :as types]
             [g2 :as g2]
             [monotonic :as monotonic]]
            [yugabyte.auto :as auto]
            [yugabyte.bank :as bank]
            [yugabyte.bank-improved :as bank-improved]
            [yugabyte.counter :as counter]
            [yugabyte.long-fork :as long-fork]
            [yugabyte.multi-key-acid :as multi-key-acid]
            [yugabyte.nemesis :as nemesis]
            [yugabyte.single-key-acid :as single-key-acid]
            [yugabyte.set :as set]
            [yugabyte.utils :as utils]
            [yugabyte.utils :refer :all]
            [yugabyte.ycql.bank]
            [yugabyte.ycql.bank-improved]
            [yugabyte.ycql.counter]
            [yugabyte.ycql.long-fork]
            [yugabyte.ycql.multi-key-acid]
            [yugabyte.ycql.set]
            [yugabyte.ycql.single-key-acid]
            [yugabyte.ycql.upsert]
            [yugabyte.ycql.types]
            [yugabyte.ycql.monotonic]
            [yugabyte.ysql [append :as ysql.append]
             [append-table :as ysql.append-table]
             [default-value :as ysql.default-value]
             [wr :as ysql.wr]
             [upsert :as ysql.upsert]
             [types :as ysql.types]
             [g2 :as ysql.g2]
             [monotonic :as ysql.monotonic]]
            [yugabyte.ysql.bank]
            [yugabyte.ysql.bank-improved]
            [yugabyte.ysql.counter]
            [yugabyte.ysql.long-fork]
            [yugabyte.ysql.multi-key-acid]
            [yugabyte.ysql.set]
            [yugabyte.ysql.single-key-acid])
  (:import (jepsen.client Client)))

(def version-regex #"(?<=yugabyte\-)(\d+\.\d+(\.\d+){0,2}(-b\d+)?)")

(defn noop-test
  "NOOP test, exists to validate setup/teardown phases"
  [opts]
  (merge tests/noop-test opts))

(defn sleep-test
  "NOOP test that gives you time to log into nodes and poke around, trying stuff manually.
  Sleeps for durations specified by --time-limit (in seconds), defaults to 60."
  [opts]
  (merge tests/noop-test
         {:client (reify Client
                    (setup! [this test]
                      (let [wait-sec (:time-limit opts)]
                        (info "Sleeping for" wait-sec "s...")
                        (Thread/sleep (long (* wait-sec 1000)))))
                    (teardown! [this test])
                    (invoke! [this test op] (assoc op :type :ok))
                    (open! [this test node] this)
                    (close! [this test]))}
         opts))

(defn is-stub-workload
  "Whether workload defined by the given keyword is just a stub, or is a real one"
  [w]
  (or (= (name w) "none") (= (name w) "sleep")))

(defmacro with-client
  [workload client-ctor]
  "Wraps a workload function to add :client entry to the result.
  Made as macro to re-evaluate client on every invocation."
  `(fn [~'opts] (assoc (~workload ~'opts) :client ~client-ctor)))

(def workloads-ycql
  "A map of workload names to functions that can take option maps and construct workloads."
  #:ycql{:none            noop-test
         :counter         (with-client counter/workload (yugabyte.ycql.counter/->CQLCounterClient))
         :set             (with-client set/workload (yugabyte.ycql.set/->CQLSetClient))
         :set-index       (with-client set/workload (yugabyte.ycql.set/->CQLSetIndexClient))
         :bank            (with-client bank/workload-allow-neg (yugabyte.ycql.bank/->CQLBank))
         :bank-inserts    (with-client bank-improved/workload-with-inserts (yugabyte.ycql.bank-improved/->CQLBankImproved))
         ; Shouldn't be used until we support transactions with selects.
         ; :bank-multitable (with-client bank/workload-allow-neg (yugabyte.ycql.bank/->CQLMultiBank))
         :long-fork       (with-client long-fork/workload (yugabyte.ycql.long-fork/->CQLLongForkIndexClient))
         :single-key-acid (with-client single-key-acid/workload (yugabyte.ycql.single-key-acid/->CQLSingleKey))
         :multi-key-acid  (with-client multi-key-acid/workload (yugabyte.ycql.multi-key-acid/->CQLMultiKey))
         ; INSERT ... IF NOT EXISTS uniqueness via lightweight transactions.
         :upsert          (with-client upsert/workload (yugabyte.ycql.upsert/->CQLUpsert))
         ; Numeric boundary round-trip (overflow / truncation).
         :types           (with-client types/workload (yugabyte.ycql.types/->CQLTypes))
         ; Per-session monotonic reads over a counter.
         :monotonic       (with-client monotonic/workload (yugabyte.ycql.monotonic/->CQLMonotonic))})

(def workloads-ysql
  "A map of workload names to functions that can take option maps and construct workloads."
  #:ysql{:none               noop-test
         :sleep              sleep-test
         :sz.counter         (with-client counter/workload (yugabyte.ysql.counter/->YSQLCounterClient :serializable))
         :sz.set             (with-client set/workload (yugabyte.ysql.set/->YSQLSetClient :serializable))
         ; This one doesn't work because of https://github.com/YugaByte/yugabyte-db/issues/1554
         ; :set-index       (with-client set/workload (yugabyte.ysql.set/->YSQLSetIndexClient))
         ; We'd rather allow negatives for now because it makes reproducing error easier
         :sz.bank            (with-client bank/workload-allow-neg (yugabyte.ysql.bank/->YSQLBankClient true :serializable))
         :sz.bank-multitable (with-client bank/workload-allow-neg (yugabyte.ysql.bank/->YSQLMultiBankClient true :serializable))
         :sz.bank-contention (with-client bank-improved/workload-contention-keys (yugabyte.ysql.bank-improved/->YSQLBankContentionClient :serializable))
         :sz.long-fork       (with-client long-fork/workload (yugabyte.ysql.long-fork/->YSQLLongForkClient :serializable))
         :sz.single-key-acid (with-client single-key-acid/workload (yugabyte.ysql.single-key-acid/->YSQLSingleKeyAcidClient))
         :sz.multi-key-acid  (with-client multi-key-acid/workload (yugabyte.ysql.multi-key-acid/->YSQLMultiKeyAcidClient))
         :sz.geo.append      (with-client append/workload-serializable (ysql.append/->Client :serializable (or (:locking opts) :mixed) :geo))
         :sz.append          (with-client append/workload-serializable (ysql.append/->Client :serializable (or (:locking opts) :mixed) :no-geo))
         :sz.append-table    (with-client append/workload-serializable-table (ysql.append-table/->Client :serializable))
         :sz.default-value   (with-client default-value/workload (ysql.default-value/->Client))
         :rc.geo.append      (with-client append/workload-rc (ysql.append/->Client :read-committed (or (:locking opts) :mixed) :geo))
         :rc.append          (with-client append/workload-rc (ysql.append/->Client :read-committed (or (:locking opts) :mixed) :no-geo))
         ; See https://docs.yugabyte.com/latest/architecture/transactions/isolation-levels/
         ; :snapshot-isolation maps to :repeatable_read SQL
         :si.geo.append      (with-client append/workload-si (ysql.append/->Client :repeatable-read (or (:locking opts) :mixed) :geo))
         :si.append          (with-client append/workload-si (ysql.append/->Client :repeatable-read (or (:locking opts) :mixed) :no-geo))
         :si.bank            (with-client bank/workload-allow-neg (yugabyte.ysql.bank/->YSQLBankClient true :repeatable-read))
         :si.bank-multitable (with-client bank/workload-allow-neg (yugabyte.ysql.bank/->YSQLBankClient true :repeatable-read))
         :si.bank-contention (with-client bank-improved/workload-contention-keys (yugabyte.ysql.bank-improved/->YSQLBankContentionClient :repeatable-read))
         :si.append-table    (with-client append/workload-si-table (ysql.append-table/->Client :repeatable-read))
         :si.counter         (with-client counter/workload (yugabyte.ysql.counter/->YSQLCounterClient :repeatable-read))
         :si.set             (with-client set/workload (yugabyte.ysql.set/->YSQLSetClient :repeatable-read))
         :rc.append-table    (with-client append/workload-rc-table (ysql.append-table/->Client :read-committed))

         ; Elle write-read register (complements list-append). Anomaly set is
         ; calibrated per isolation level, like the append workloads. Only rc/si:
         ; at serializable, sz.multi-key-acid already covers multi-key register
         ; transactions (via linearizability), so a sz.wr would overlap it.
         :si.wr              (with-client wr/workload-si (ysql.wr/->Client :repeatable-read))
         :rc.wr              (with-client wr/workload-rc (ysql.wr/->Client :read-committed))

         ; INSERT ... ON CONFLICT uniqueness under contention.
         :si.upsert          (with-client upsert/workload (ysql.upsert/->Client :repeatable-read))
         :rc.upsert          (with-client upsert/workload (ysql.upsert/->Client :read-committed))

         ; Numeric boundary round-trip (overflow / truncation).
         :si.types           (with-client types/workload (ysql.types/->Client :repeatable-read))
         :rc.types           (with-client types/workload (ysql.types/->Client :read-committed))

         ; Adya G2 predicate write-skew. Serializable only: write skew is legal
         ; under snapshot and read-committed, so it would false-positive there.
         :sz.g2              (with-client g2/workload (ysql.g2/->Client :serializable))

         ; Long fork is an SI-level anomaly (forbidden at snapshot and
         ; serializable). We already run it at serializable; also run at
         ; snapshot/repeatable-read.
         :si.long-fork       (with-client long-fork/workload (yugabyte.ysql.long-fork/->YSQLLongForkClient :repeatable-read))

         ; Per-session monotonic reads over a monotonically increasing register.
         :si.monotonic       (with-client monotonic/workload (ysql.monotonic/->Client :repeatable-read))
         :rc.monotonic       (with-client monotonic/workload (ysql.monotonic/->Client :read-committed))})

(def workloads
  (merge workloads-ycql workloads-ysql))

(def workload-options
  "For each workload, a map of workload options to all the values that option
  supports. Used for test-all."
  ; If we ever need additional options - merge them onto this base set
  (merge (map-values workloads-ycql (fn [_] {}))
         (map-values workloads-ysql (fn [_] {}))))

(def workload-options-expected-to-pass
  "Only workloads and options that we think should pass. Also used for
  test-all."
  (-> workload-options
      (dissoc :ycql/bank-multitable
              :ycql/none
              :ysql/none
              :ysql/sleep
              :ysql/append-table)))

(def nemesis-specs
  "These are the types of failures that the nemesis can perform."
  #{:partition
    :partition-half
    :partition-ring
    :partition-one
    :kill
    :kill-master
    :kill-tserver
    :pause-master
    :pause-tserver
    :pause
    :stop
    :stop-master
    :stop-tserver
    :clock-skew})

(def all-nemeses
  "All nemesis specs to run as a part of a complete test suite."
  (->> [[]                                                  ; No faults
        [:kill-tserver]                                     ; Just tserver
        [:kill-master]                                      ; Just master
        [:pause-tserver]                                    ; Just pause tserver
        [:pause-master]                                     ; Just pause master
        [:clock-skew]                                       ; Just clocks
        [:partition-one                                     ; Just partitions
         :partition-half
         :partition-ring]
        [:kill-tserver
         :kill-master
         :pause-tserver
         :pause-master
         :clock-skew
         :partition-one
         :partition-half
         :partition-ring]]
       ; Turn these into maps with each key being true
       (map (fn [faults] (zipmap faults (repeat true))))))

(defn yugabyte-ssh-defaults
  "A partial test map with SSH options for a test running in Yugabyte's
  internal testing environment."
  []
  {:ssh {:port                     54422
         :strict-host-key-checking false
         :username                 "yugabyte"
         :private-key-path         (str (System/getenv "HOME")
                                        "/.yugabyte/yugabyte-dev-aws-keypair.pem")}})

(def trace-logging
  "Logging configuration for the test which sets up traces for queries."
  {:logging {:overrides {;"com.datastax"                            :trace
                         ;"com.yugabyte"                            :trace
                         "com.datastax.driver.core.RequestHandler" :trace
                         ;"com.datastax.driver.core.CodecRegistry"  :info
                         }}})

(defn all-combos
  "Takes a map of options to collections of values for that option. Computes a
  collection of maps with the combinatorial expansion of every possible option
  value."
  ([opts]
   (all-combos {} opts))
  ([m opts]
   (if (seq opts)
     (let [[k vs] (first opts)]
       (mapcat (fn [v]
                 (all-combos (assoc m k v) (next opts)))
               vs))
     (list m))))

(defn all-workload-options
  "Expands workload-options into all possible CLI opts for each combination of
  workload options."
  [workload-options]
  (mapcat (fn [[workload opts]]
            (all-combos {:workload workload} opts))
          workload-options))

(defn test-1
  "Initial test construction from a map of CLI options. Establishes the test
  name, OS, DB."
  [opts]
  (let [api (keyword (namespace (:workload opts)))
        url-version (first (re-find version-regex (get opts :url "")))]
    (when (and (= :ycql api) (:connection-manager opts))
      (warn "Connection manager is a YSQL-only feature; disabling it for YCQL workload"
            (:workload opts)))
    (assoc opts
      :version (or url-version (:version opts))
      :api api
      ; Serializable workloads conflict heavily; run them with fewer worker
      ; threads (~half) so contention doesn't drown out useful throughput.
      ; Keep the result a multiple of 4 (and >= 4): the *-key-acid and set
      ; workloads split threads via (/ threads 2) and (/ threads 4), and jepsen
      ; asserts those group sizes are integers, so an odd count crashes.
      :concurrency (let [c (:concurrency opts)]
                     (if (utils/is-test-serializable? opts)
                       (min c (max 4 (* 4 (quot c 8))))
                       c))
      ; Connection manager (YSQL Connection Manager / Odyssey) only applies to
      ; YSQL. Never enable it for YCQL tests, regardless of the CLI flag.
      :connection-manager (and (not= :ycql api) (:connection-manager opts))
      :name (str "yb_" (-> (or (:url opts) (:version opts))
                           (str/split #"/")
                           (last))
                 "_" (name api)
                 "_" (name (:workload opts))
                 (when-not (= [:interval] (keys (:nemesis opts)))
                   (str "_nemesis_" (->> (dissoc (:nemesis opts) :interval)
                                         keys
                                         (map name)
                                         sort
                                         (str/join ",")))))
      :pure-generators true
      :os (case (:os opts)
            :centos centos/os
            :debian debian/os)
      :db (auto/->YugaByteDB))))

(defn test-2
  "Second phase of test construction. Builds the workload and nemesis, and
  finalizes the test."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)
        nemesis (nemesis/nemesis opts)
        gen (->> (:generator workload)
                 (gen/nemesis (:generator nemesis))
                 (gen/time-limit (:time-limit opts)))
        gen (if (:final-generator workload)
              (gen/phases gen
                          (gen/log "Healing cluster")
                          (gen/nemesis (:final-generator nemesis))
                          (gen/log "Waiting for recovery...")
                          (gen/sleep (:final-recovery-time opts))
                          (gen/clients (:final-generator workload)))
              gen)
        perf (checker/perf
               {:nemeses #{{:name       "kill master"
                            :start      #{:kill-master :stop-master}
                            :stop       #{:start-master}
                            :fill-color "#E9A4A0"}
                           {:name       "kill tserver"
                            :start      #{:kill-tserver :stop-tserver}
                            :stop       #{:start-tserver}
                            :fill-color "#E9C3A0"}
                           {:name       "pause master"
                            :start      #{:pause-master}
                            :stop       #{:resume-master}
                            :fill-color "#A0B1E9"}
                           {:name       "pause tserver"
                            :start      #{:pause-tserver}
                            :stop       #{:resume-tserver}
                            :fill-color "#B8A0E9"}
                           {:name       "clock skew"
                            :start      #{:bump-clock :strobe-clock}
                            :stop       #{:reset-clock}
                            :fill-color "#D2E9A0"}
                           {:name       "partition"
                            :start      #{:start-partition}
                            :stop       #{:stop-partition}
                            :fill-color "#888888"}}})
        checker (if (is-stub-workload (:workload opts))
                  (:checker workload)
                  (checker/compose {:perf                 perf
                                    :stats                (checker/stats)
                                    :unhandled-exceptions (checker/unhandled-exceptions)
                                    :clock                (checker/clock-plot)
                                    :workload             (:checker workload)}))]
    (merge tests/noop-test
           opts
           (dissoc workload
                   :generator
                   :final-generator
                   :checker)
           (when (:yugabyte-ssh opts) (yugabyte-ssh-defaults))
           (when (:trace-cql opts) (trace-logging))
           {:client          (:client workload)
            :nemesis         (:nemesis nemesis)
            :generator       gen
            :pure-generators true
            :checker         checker})))

(defn test-3
  "Final phase where we define global cluster configuration parameters"
  [opts]
  (let [packed-columns-enabled (random/bool)
        colocated (and (not (utils/is-test-geo-partitioned? opts)) (random/bool))]
    (assoc opts :yb-packed-columns-enabled packed-columns-enabled :yb-colocated colocated)))

(defn yb-test
  "Constructs a yugabyte test from CLI options."
  [opts]
  (-> opts test-1 test-2 test-3))
