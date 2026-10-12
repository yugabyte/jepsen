(ns yugabyte.amp.client
  "Stock-PostgreSQL client layer for AMP compute endpoints.

  Nodes are \"host:port\" endpoint strings. Connections use pgjdbc through
  PGSimpleDataSource, so the YugabyteDB driver on the same classpath is never
  selected. Each test runs in its own schema (:schema in the test map), so
  reruns and concurrent runs on one endpoint do not collide.

  AmpClient wraps any yugabyte.ysql.client/YSQLYbClient record, so the
  existing YSQL workload clients run unchanged. The password is read from the
  PGPASSWORD environment variable and is never stored in the test map, which
  Jepsen writes to store/."
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [jepsen.client :as client]
            [jepsen.reconnect :as rc]
            [yugabyte.ysql.client :as c])
  (:import (java.sql BatchUpdateException Connection SQLException)
           (org.postgresql.ds PGSimpleDataSource)))

(def timeout-s "Connect, login and socket timeout in seconds" 30)

(defn parse-endpoint
  "Splits a \"host:port\" node name into [host port]."
  [node]
  (let [[_ host port] (re-matches #"(.+):(\d+)" (name node))]
    (assert host (str "Endpoint must be host:port, got " (pr-str node)))
    [host (Integer/parseInt port)]))

(defn open-conn
  "Opens a pgjdbc connection to the endpoint, with search_path set to the
  test's schema. Default isolation is SERIALIZABLE, like the YSQL clients, so
  single-statement operations outside an explicit transaction keep the same
  semantics."
  [test node]
  (let [[host port] (parse-endpoint node)
        ds          (doto (PGSimpleDataSource.)
                      (.setServerNames (into-array String [host]))
                      (.setPortNumbers (int-array [port]))
                      (.setDatabaseName (:database test))
                      (.setUser (:db-user test))
                      (.setPassword (System/getenv "PGPASSWORD"))
                      (.setCurrentSchema (str (:schema test) ",public"))
                      (.setApplicationName "jepsen")
                      (.setConnectTimeout timeout-s)
                      (.setLoginTimeout timeout-s)
                      (.setSocketTimeout timeout-s))
        conn        (.getConnection ds)]
    (.setTransactionIsolation conn Connection/TRANSACTION_SERIALIZABLE)
    (j/add-connection {} conn)))

(defn open-wrapper
  [test node]
  (rc/open!
    (rc/wrapper {:name  node
                 :open  #(open-conn test node)
                 :close c/close-conn
                 :log?  false})))

(defn sql-state
  "SQLSTATE of a JDBC exception, looking through batch wrappers. nil for
  non-SQL exceptions."
  [^Throwable e]
  (cond
    (instance? BatchUpdateException e)
    (or (some-> ^BatchUpdateException e .getNextException sql-state)
        (.getSQLState ^SQLException e))

    (instance? SQLException e)
    (.getSQLState ^SQLException e)))

(def error-names
  {"40001" :serialization-failure
   "40P01" :deadlock
   "23505" :unique-violation
   "23P01" :exclusion-violation
   "25006" :read-only-transaction
   "55P03" :lock-not-available})

(def definite-classes
  "SQLSTATE classes which guarantee the transaction did not commit:
  23 integrity violation, 25 invalid transaction state, 40 transaction
  rollback, 55 object not in prerequisite state."
  #{"23" "25" "40" "55"})

(defn exception->op
  "Maps an exception to a partial op. :fail when the transaction definitely
  did not commit, :info when the outcome is unknown. nil for errors in the
  test itself (SQLSTATE classes 22 and 42), which are rethrown."
  [^Throwable e]
  (let [state (sql-state e)
        m     (.getMessage e)]
    (if (nil? state)
      (when (instance? clojure.lang.ExceptionInfo e)
        ; jepsen.reconnect has no connection open
        {:type :fail, :error [:no-conn m]})
      (let [cls (subs state 0 2)
            err [(error-names state :sql) state m]]
        (cond
          ; Connection could not be established: nothing was sent
          (#{"08001" "08004"} state) {:type :fail, :error err}
          (definite-classes cls)     {:type :fail, :error err}
          (#{"22" "42"} cls)         nil
          ; Connection loss (08), shutdown (57P0x), cancel (57014), anything
          ; else: the transaction may have committed
          :else                      {:type :info, :error err})))))

(def read-fs
  "Operation functions which never write; their :info results become :fail."
  #{:read})

(defn definite-failure?
  "True when the server rejected the transaction (SQLSTATE class 23, 25, 40
  or 55). The session is still healthy: the server answered and
  with-db-transaction rolled back."
  [e]
  (when-let [state (sql-state e)]
    (boolean (definite-classes (subs state 0 2)))))

(defn invoke-with-conn
  "Runs (f c) with a connection from the wrapper and maps exceptions to ops.
  Definite failures are mapped inside jepsen.reconnect/with-conn and keep the
  connection, so high rejection rates do not turn into reconnect storms. Any
  other error escapes with-conn, which reopens the connection, so the next op
  never inherits a broken session."
  [conn-wrapper op f]
  (let [op' (try
              (rc/with-conn [c conn-wrapper]
                (try (f c)
                     (catch SQLException e
                       (if (definite-failure? e)
                         (merge op (exception->op e))
                         (throw e)))))
              (catch Exception e
                (if-let [partial-op (exception->op e)]
                  (merge op partial-op)
                  (throw e))))]
    (if (and (= :info (:type op')) (read-fs (:f op')))
      (assoc op' :type :fail)
      op')))

(defrecord AmpClient [conn-wrapper inner setup? teardown?]
  client/Client

  (open! [this test node]
    (assoc this :conn-wrapper (open-wrapper test node)))

  (setup! [this test]
    (c/once-per-cluster
      setup?
      (info "Creating schema" (:schema test))
      (rc/with-conn [c conn-wrapper]
        (c/execute! c (str "CREATE SCHEMA IF NOT EXISTS " (:schema test)))
        (c/setup-cluster! inner test c conn-wrapper))
      (info "Setup successful")))

  (invoke! [this test op]
    (invoke-with-conn conn-wrapper op
                      #(c/invoke-op! inner test op % conn-wrapper)))

  (teardown! [this test]
    (c/once-per-cluster
      teardown?
      (if (:leave-db-running? test)
        (info "Leaving schema" (:schema test) "in place")
        (rc/with-conn [c conn-wrapper]
          (info "Dropping schema" (:schema test))
          (c/execute! c (str "DROP SCHEMA IF EXISTS " (:schema test) " CASCADE"))))))

  (close! [this test]
    (rc/close! conn-wrapper)))

(defn amp-client
  "Wraps an inner YSQLYbClient record into a Jepsen client for AMP."
  [inner]
  (->AmpClient nil inner (atom false) (atom false)))
