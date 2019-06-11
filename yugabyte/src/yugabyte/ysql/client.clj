(ns yugabyte.ysql.client
  "Helper functions for working with YSQL clients."
  (:require [clojure.java.jdbc :as j]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.util :as util]
            [jepsen.control.net :as cn]
            [jepsen.reconnect :as rc]
            [dom-top.core :as dt]
            [wall.hack :as wh]
            [slingshot.slingshot :refer [try+ throw+]]))

(def timeout-delay "Default timeout for operations in ms" 20000)
(def max-timeout "Longest timeout, in ms" 40000)

(def isolation-level "Default isolation level for txns" :serializable)

(def ysql-port 5433)

(defn db-spec
  "Assemble a JDBC connection specification for a given Jepsen node."
  [node]
  {:dbtype         "postgresql"
   :dbname         "postgres"
   :classname      "org.postgresql.Driver"
   :host           (name node)
   :port           ysql-port
   :user           "postgres"
   :password       ""
   :loginTimeout   (/ max-timeout 1000)
   :connectTimeout (/ max-timeout 1000)
   :socketTimeout  (/ max-timeout 1000)})

(defn close-conn
  "Given a JDBC connection, closes it and returns the underlying spec."
  [conn]
  (when-let [c (j/db-find-connection conn)]
    (.close c))
  (dissoc conn :connection))

(defn conn-wrapper
  "Constructs a network client for a node, and opens it"
  [node]
  (rc/open!
    (rc/wrapper
      {:name  node
       :open  (fn open []
                (util/timeout max-timeout
                              (throw (RuntimeException.
                                       (str "Connection to " node " timed out")))
                              (util/retry 0.1
                                          (let [spec  (db-spec node)
                                                conn  (j/get-connection spec)
                                                spec' (j/add-connection spec conn)]
                                            (assert spec')
                                            spec'))))
       :close close-conn
       :log?  true})))

(defn exception->op
  "Takes an exception and maps it to a partial op, like {:type :info, :error
  ...}. nil if unrecognized."
  [e]
  (when-let [m (.getMessage e)]
    (condp instance? e
      java.sql.SQLTransactionRollbackException
      {:type :fail, :error [:rollback m]}

      java.sql.BatchUpdateException
      (if (re-find #"getNextExc" m)
        ; Wrap underlying exception error with [:batch ...]
        (when-let [op (exception->op (.getNextException e))]
          (update op :error (partial vector :batch)))
        {:type :info, :error [:batch-update m]})

      org.postgresql.util.PSQLException
      (condp re-find (.getMessage e)
        #"(?i)Conflicts with [a-z]+ transaction"
        {:type :fail, :error [:conflicting-transaction m]}

        #"(?i)Catalog Version Mismatch"
        {:type :fail, :error [:catalog-version-mismatch m]}

        #"(?i)Operation expired"
        {:type :fail, :error [:operation-expired m]}

        {:type :info, :error [:psql-exception m]})

      clojure.lang.ExceptionInfo
      (condp = (:type (ex-data e))
        :conn-not-ready {:type :fail, :error :conn-not-ready}
        nil)

      (condp re-find m
        #"^timeout$"
        {:type :info, :error :timeout}

        nil))))

(defn retryable?
  "Whether given exception indicates that an operation can be retried"
  [ex]
  (let [op     (exception->op ex)                           ; either {:type ... :error ...} or nil
        op-str (str op)]
    (re-find #"(?i)try again" op-str)))

(defmacro once-per-cluster
  "Runs the given code once per cluster. Requires an atomic boolean (set to false)
  shared across clients.
  This is needed mainly because concurrent DDL is not supported and results in an error."
  [atomic-bool & body]
  `(locking ~atomic-bool
     (when (compare-and-set! ~atomic-bool false true) ~@body)))

(defn drop-table
  ([c table-name]
   (drop-table c table-name false))
  ([c table-name if-exists?]
   (j/execute! c [(str "DROP TABLE " (if if-exists? "IF EXISTS " "") table-name)])))

(defmacro with-conn
  "Like jepsen.reconnect/with-conn, but also asserts that the connection has
  not been closed. If it has, throws an ex-info with :type :conn-not-ready.
  Delays by 1 second to allow time for the DB to recover."
  [[c client] & body]
  `(rc/with-conn [~c ~client]
                 (when (.isClosed (j/db-find-connection ~c))
                   (Thread/sleep 1000)
                   (throw (ex-info "Connection not yet ready."
                                   {:type :conn-not-ready})))
                 ~@body))

(defn with-idempotent
  "Takes a predicate on operation functions, and an op, presumably resulting
  from a client call. If (idempotent? (:f op)) is truthy, remaps :info types to
  :fail."
  [idempotent? op]
  (if (and (idempotent? (:f op)) (= :info (:type op)))
    (assoc op :type :fail)
    op))

(defmacro with-timeout
  "Like util/timeout, but throws (RuntimeException. \"timeout\") for timeouts.
  Throwing means that when we time out inside a with-conn, the connection state
  gets reset, so we don't accidentally hand off the connection to a later
  invocation with some incomplete transaction."
  [& body]
  `(util/timeout timeout-delay
                 (throw (RuntimeException. "timeout"))
                 ~@body))

(defmacro with-txn-retry
  "Catches YSQL \"try again\"-style errors and retries body a bunch of times,
  with exponential backoffs."
  [& body]
  `(util/with-retry [attempts# 30
                     backoff# 20]
                    ~@body

                    (catch java.sql.SQLException e#
                      (if (and (pos? attempts#)
                               (retryable? e#))
                        (do (Thread/sleep backoff#)
                            (~'retry (dec attempts#)
                              (* backoff# (+ 4 (* 0.5 (- (rand) 0.5))))))
                        (throw e#)))))

(defmacro with-txn
  "Wrap a evaluation within a SQL transaction."
  [[c conn] & body]
  `(j/with-db-transaction [~c ~conn {:isolation isolation-level}]
                          ~@body))


(defmacro with-exception->op
  "Takes an operation and a body. Evaluates body, catches exceptions, and maps
  them to ops with :type :info and a descriptive :error."
  [op & body]
  `(try ~@body
        (catch Exception e#
          (if-let [ex-op# (exception->op e#)]
            (merge ~op ex-op#)
            (throw e#)))))

(defn query
  "Like jdbc query, but includes a default timeout in ms.
  Requires query to be wrapped in a vector."
  [conn sql-params]
  (j/query conn sql-params {:timeout timeout-delay}))

(defn insert!
  "Like jdbc insert!, but includes a default timeout."
  [conn table values]
  (j/insert! conn table values {:timeout timeout-delay}))

(defn update!
  "Like jdbc update!, but includes a default timeout."
  [conn table values where]
  (j/update! conn table values where {:timeout timeout-delay}))

(defn execute!
  "Like jdbc execute!!, but includes a default timeout."
  [conn sql-params]
  (j/execute! conn sql-params {:timeout timeout-delay}))
