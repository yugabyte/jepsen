(ns yugabyte.amp.returning
  "PostgreSQL 18 RETURNING OLD/NEW over an Elle rw-register.

  Every write is an upsert

    INSERT ... ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
    RETURNING old.v, new.v

  old.v is the version this write replaced (NULL when the row was inserted).
  The client records it as a read immediately before the write, [:r k old]
  [:w k new], so Elle sees each write's direct predecessor (:wfr-keys?) and
  flags :lost-update when two committed writes report the same predecessor.
  PostgreSQL forbids that at every isolation level for ON CONFLICT DO UPDATE.

  At read committed a SELECT earlier in the transaction may legally return an
  older version than old.v, which would break the writes-follow-reads
  assumption. The rc generator therefore drops reads from transactions that
  write; read-only transactions are kept."
  (:require [clojure.java.jdbc :as j]
            [elle.core :as elle]
            [jepsen.generator :as gen]
            [jepsen.tests.cycle.wr :as wr]
            [yugabyte.generator :as ygen]
            [yugabyte.ysql.client :as c]))

(def table-name "returning_reg")

(defn read-register
  [op c k]
  (some-> (c/query op c [(str "SELECT v FROM " table-name " WHERE k = ?") k])
          first
          :v
          long))

(defn upsert!
  "Writes k = v. Returns the replaced version, or nil when the row was new."
  [op c k v]
  (let [row (first (c/query op c [(str "INSERT INTO " table-name " (k, v) VALUES (?, ?)"
                                       " ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v"
                                       " RETURNING old.v AS prev, new.v AS cur")
                                  k v]))]
    (assert (= v (:cur row)) (str "RETURNING new.v " (:cur row) " != written " v))
    (some-> (:prev row) long)))

(defn apply-txn
  [op c txn]
  (reduce (fn [out [f k v]]
            (case f
              :r (conj out [:r k (read-register op c k)])
              :w (let [prev (upsert! op c k v)]
                   (conj out [:r k prev] [:w k v]))))
          []
          txn))

(defrecord ReturningClient [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name [[:k :int "PRIMARY KEY"]
                                                  [:v :int]])))

  (invoke-op! [this test op c conn-wrapper]
    (let [txn (:value op)]
      (assoc op :type :ok
                :value (if (< 1 (count txn))
                         (j/with-db-transaction [c c {:isolation isolation}]
                           (apply-txn op c txn))
                         (apply-txn op c txn)))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name true)))

(defn writes-only
  "Drops reads from any transaction that writes."
  [txn]
  (if (some (comp #{:w} first) txn)
    (filterv (comp #{:w} first) txn)
    txn))

(defn workload
  [checker-opts {:keys [rc?]}]
  (let [opts (merge {:key-count          10
                     :max-txn-length     4
                     :max-writes-per-key 256
                     :wfr-keys?          true
                     :additional-graphs  [elle/realtime-graph]}
                    checker-opts)
        w    (wr/test opts)]
    (ygen/workload-with-op-index
      (cond-> w
        rc? (update :generator (partial gen/map #(update % :value writes-only)))))))

(defn workload-serializable
  [opts]
  (workload {:anomalies [:G1 :G2 :lost-update]} {}))

(defn workload-si
  [opts]
  (workload {:anomalies          [:internal :G-nonadjacent :G1 :G-SI :lost-update]
             :consistency-models [:snapshot-isolation]}
            {}))

(defn workload-rc
  [opts]
  (workload {:anomalies          [:G0 :G1a :G1b :lost-update]
             :consistency-models [:read-committed]}
            {:rc? true}))
