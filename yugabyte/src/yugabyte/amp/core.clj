(ns yugabyte.amp.core
  "Builds Jepsen tests against AMP compute endpoints (stock PostgreSQL 18).
  No SSH, no DB install, no nemesis: ampqa provisions the endpoints and its
  chaos runner owns faults."
  (:require [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.tests :as tests]
            ; yugabyte.auto (loaded via yugabyte.ysql.append) imports these
            ; record classes without requiring their namespaces
            [jepsen.os.centos]
            [jepsen.os.debian]
            [yugabyte.append :as append]
            [yugabyte.bank :as bank]
            [yugabyte.long-fork :as long-fork]
            [yugabyte.amp.client :as ac]
            [yugabyte.amp.db :as adb]
            [yugabyte.amp.returning :as returning]
            [yugabyte.amp.temporal :as temporal]
            [yugabyte.ysql.append :as ysql.append]
            [yugabyte.ysql.bank :as ysql.bank]
            [yugabyte.ysql.long-fork :as ysql.long-fork]))

(defn- with-inner
  "Wraps a workload fn so the result carries an inner YSQLYbClient record built
  by (inner-fn opts)."
  [workload-fn inner-fn]
  (fn [opts] (assoc (workload-fn opts) :client (inner-fn opts))))

(defn- append-client
  [isolation]
  (fn [opts] (ysql.append/->InternalClient isolation (or (:locking opts) :mixed) :no-geo)))

(def workloads
  "Workload names to fns of CLI options returning workload maps."
  #:amp{; YugabyteDB-inspired: existing YSQL clients, PostgreSQL-compatible paths only
        :sz.append     (with-inner append/workload-serializable (append-client :serializable))
        :si.append     (with-inner append/workload-si (append-client :repeatable-read))
        :rc.append     (with-inner append/workload-rc (append-client :read-committed))
        :sz.bank       (with-inner bank/workload-allow-neg (fn [_] (ysql.bank/->YSQLBankYbClient true :serializable)))
        :si.bank       (with-inner bank/workload-allow-neg (fn [_] (ysql.bank/->YSQLBankYbClient true :repeatable-read)))
        :sz.long-fork  (with-inner long-fork/workload (fn [_] (ysql.long-fork/->YSQLLongForkYbClient :serializable)))
        :si.long-fork  (with-inner long-fork/workload (fn [_] (ysql.long-fork/->YSQLLongForkYbClient :repeatable-read)))

        ; PostgreSQL 18: RETURNING OLD/NEW
        :sz.returning  (with-inner returning/workload-serializable (fn [_] (returning/->ReturningClient :serializable)))
        :si.returning  (with-inner returning/workload-si (fn [_] (returning/->ReturningClient :repeatable-read)))
        :rc.returning  (with-inner returning/workload-rc (fn [_] (returning/->ReturningClient :read-committed)))

        ; PostgreSQL 18: temporal primary key WITHOUT OVERLAPS
        :sz.temporal   (with-inner temporal/workload (fn [_] (temporal/->TemporalClient :serializable)))
        :si.temporal   (with-inner temporal/workload (fn [_] (temporal/->TemporalClient :repeatable-read)))
        :rc.temporal   (with-inner temporal/workload (fn [_] (temporal/->TemporalClient :read-committed)))})

(defn amp-test
  "Constructs a test map from CLI options."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)
        gen      (->> (:generator workload)
                      gen/clients
                      (gen/time-limit (:time-limit opts)))
        gen      (if-let [final (:final-generator workload)]
                   (gen/phases gen
                               (gen/log "Running final generator")
                               (gen/clients final))
                   gen)]
    (merge tests/noop-test
           opts
           (dissoc workload :generator :final-generator :checker :client)
           {:name            (str "amp_" (name (:workload opts)))
            :ssh             {:dummy? true}
            :db              (adb/->AmpDB)
            ; One schema per run; see yugabyte.amp.client
            :schema          (str "jepsen_" (System/currentTimeMillis))
            :client          (ac/amp-client (:client workload))
            :generator       gen
            :pure-generators true
            :checker         (checker/compose
                               {:perf                 (checker/perf)
                                :stats                (checker/stats)
                                :unhandled-exceptions (checker/unhandled-exceptions)
                                :workload             (:checker workload)})})))
