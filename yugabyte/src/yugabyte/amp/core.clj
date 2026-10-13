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
            [yugabyte [append :as append]
                      [bank :as bank]
                      [counter :as counter]
                      [g2 :as g2]
                      [long-fork :as long-fork]
                      [monotonic :as monotonic]
                      [multi-key-acid :as multi-key-acid]
                      [queue :as queue]
                      [set :as set]
                      [single-key-acid :as single-key-acid]
                      [types :as types]
                      [upsert :as upsert]
                      [wr :as wr]]
            [yugabyte.amp.client :as ac]
            [yugabyte.amp.db :as adb]
            [yugabyte.amp.returning :as returning]
            [yugabyte.amp.temporal :as temporal]
            [yugabyte.ysql [append :as ysql.append]
                           [bank :as ysql.bank]
                           [counter :as ysql.counter]
                           [g2 :as ysql.g2]
                           [long-fork :as ysql.long-fork]
                           [monotonic :as ysql.monotonic]
                           [multi-key-acid :as ysql.multi-key-acid]
                           [queue :as ysql.queue]
                           [set :as ysql.set]
                           [single-key-acid :as ysql.single-key-acid]
                           [types :as ysql.types]
                           [upsert :as ysql.upsert]
                           [wr :as ysql.wr]]))

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
        :sz.bank-multitable  (with-inner bank/workload-allow-neg (fn [_] (ysql.bank/->YSQLMultiBankYbClient true :serializable)))
        :si.bank-multitable  (with-inner bank/workload-allow-neg (fn [_] (ysql.bank/->YSQLMultiBankYbClient true :repeatable-read)))
        :sz.counter          (with-inner counter/workload (fn [_] (ysql.counter/->YSQLCounterYbClient :serializable)))
        :si.counter          (with-inner counter/workload (fn [_] (ysql.counter/->YSQLCounterYbClient :repeatable-read)))
        :sz.set              (with-inner set/workload (fn [_] (ysql.set/->YSQLSetYbClient :serializable)))
        :si.set              (with-inner set/workload (fn [_] (ysql.set/->YSQLSetYbClient :repeatable-read)))
        :sz.single-key-acid  (with-inner single-key-acid/workload (fn [_] (ysql.single-key-acid/->YSQLSingleKeyAcidYbClient)))
        :sz.multi-key-acid   (with-inner multi-key-acid/workload (fn [_] (ysql.multi-key-acid/->YSQLMultiKeyAcidYbClient)))
        :sz.g2               (with-inner g2/workload (fn [_] (ysql.g2/->G2Client :serializable)))
        :si.wr               (with-inner wr/workload-si (fn [_] (ysql.wr/->WRClient :repeatable-read)))
        :rc.wr               (with-inner wr/workload-rc (fn [_] (ysql.wr/->WRClient :read-committed)))
        :si.upsert           (with-inner upsert/workload (fn [_] (ysql.upsert/->UpsertClient :repeatable-read)))
        :rc.upsert           (with-inner upsert/workload (fn [_] (ysql.upsert/->UpsertClient :read-committed)))
        :si.types            (with-inner types/workload (fn [_] (ysql.types/->TypesClient :repeatable-read)))
        :rc.types            (with-inner types/workload (fn [_] (ysql.types/->TypesClient :read-committed)))
        :si.monotonic        (with-inner monotonic/workload (fn [_] (ysql.monotonic/->MonotonicClient :repeatable-read)))
        :rc.monotonic        (with-inner monotonic/workload (fn [_] (ysql.monotonic/->MonotonicClient :read-committed)))
        :si.queue            (with-inner queue/workload (fn [_] (ysql.queue/->QueueClient :repeatable-read)))
        :rc.queue            (with-inner queue/workload (fn [_] (ysql.queue/->QueueClient :read-committed)))
        ; Stock PostgreSQL keeps SKIP LOCKED at serializable; YugabyteDB
        ; downgrades it to blocking (yugabyte-db#11761), so YSQL has no sz.queue
        :sz.queue            (with-inner queue/workload (fn [_] (ysql.queue/->QueueClient :serializable)))

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
  (let [; The *-key-acid and set workloads split threads into groups of 2
        ; and 4, and jepsen requires integer group sizes: round down to a
        ; multiple of 4, as yugabyte.core/test-1 does
        opts     (update opts :concurrency #(max 4 (* 4 (quot % 4))))
        workload ((get workloads (:workload opts)) opts)
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
