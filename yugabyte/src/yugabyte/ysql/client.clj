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

;(defmacro with-retry
;  "Retries CQL unavailable/timeout errors for up to 120 seconds. Helpful for
;  setting up initial data; YugaByte loves to throw 10+ second latencies at us
;  early in the test."
;  [& body]
;  `(let [deadline# (+ (util/linear-time-nanos) (util/secs->nanos 120))
;         sleep#    100] ; ms
;     (dt/with-retry []
;       ~@body
;       (catch NoHostAvailableException e#
;         (if (< deadline# (util/linear-time-nanos))
;           (throw e#)
;           (do (info "Timed out, retrying")
;               (Thread/sleep (rand-int sleep#))
;               (~'retry))))
;       (catch OperationTimedOutException e#
;         (if (< deadline# (util/linear-time-nanos))
;           (throw e#)
;           (do (info "Timed out, retrying")
;               (Thread/sleep (rand-int sleep#))
;               (~'retry)))))))

;(defmacro with-errors
;  "Takes an op, a set of idempotent operation :fs, and a body. Evalates body,
;  and catches common errors, returning an appropriate completion for `op`."
;  [op idempotent & body]
;  `(let [crash# (if (~idempotent (:f ~op)) :fail :info)]
;     (try
;       ~@body
;       (catch UnavailableException e#
;         ; I think this was used back when we blocked on all nodes being online
;         ; (info "Not enough replicas - failing")
;         (assoc ~op :type :fail, :error [:unavailable (.getMessage e#)]))
;
;       (catch WriteTimeoutException e#
;         (assoc ~op :type crash#, :error :write-timed-out))
;
;       (catch ReadTimeoutException e#
;         (assoc ~op :type crash#, :error :read-timed-out))
;
;       (catch OperationTimedOutException e#
;         (assoc ~op :type crash#, :error :operation-timed-out))
;
;       (catch TransportException e#
;         (condp re-find (.getMessage e#)
;           #"Connection has been closed"
;           (assoc ~op :type crash#, :error :connection-closed)
;
;           (throw e#)))
;
;       (catch NoHostAvailableException e#
;         (condp re-find (.getMessage e#)
;           #"no host was tried"
;           (do (info "All nodes are down - sleeping 2s")
;               (Thread/sleep 2000)
;               (assoc ~op :type :fail :error [:no-host-available (.getMessage e#)]))
;           (assoc ~op :type crash#, :error [:no-host-available (.getMessage e#)])))
;
;       (catch DriverException e#
;         (if (re-find #"Value write after transaction start|Conflicts with higher priority transaction|Conflicts with committed transaction|Operation expired: Failed UpdateTransaction.* status: COMMITTED .*: Transaction expired"
;                      (.getMessage e#))
;           ; Definitely failed
;           (assoc ~op :type :fail, :error (.getMessage e#))
;           (throw e#)))
;
;       (catch InvalidQueryException e#
;         ; This can actually mean timeout
;         (if (re-find #"RPC to .+ timed out after " (.getMessage e#))
;           (assoc ~op :type crash#, :error [:rpc-timed-out (.getMessage e#)])
;           (throw e#))))))

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

;(defmacro with-conn
;  "Acquires a read lock, takes a connection from the wrapper, and evaluates
;  body with that connection bound to c. If any Exception is thrown, closes the
;  connection and opens a new one."
;  [[c wrapper] & body]
;  ; We want to hold the read lock while executing the body, but we're going to
;  ; release it in complicated ways, so we can't use the with-read-lock macro
;  ; here.
;  `(let [read-lock# (.readLock ^java.util.concurrent.locks.ReentrantReadWriteLock (:lock ~wrapper))]
;     (.lock read-lock#)
;     (let [~c (rc/conn ~wrapper)]
;       (try ~@body
;            (catch Exception e#
;              (warn " === with-conn ex 1 ===")
;              (warn e#)
;              ; We can't acquire the write lock until we release our read lock,
;              ; because ???
;              (.unlock read-lock#)
;              (try
;                (rc/with-write-lock ~wrapper
;                                    (when (identical? ~c (rc/conn ~wrapper))
;                                      ; This is the same conn that yielded the error
;                                      (when (:log? ~wrapper)
;                                        (warn (str "Encountered error with conn "
;                                                   (pr-str (:name ~wrapper))
;                                                   "; reopening")))
;                                      (rc/reopen! ~wrapper)))
;                (catch Exception e2#
;                  (warn " === with-conn ex 2 ===")
;                  (warn e2#)
;                  ; We don't want to lose the original exception, but we will
;                  ; log the reconnect error here. If we don't throw the
;                  ; original exception, our caller might not know what kind of
;                  ; error occurred in their transaction logic!
;                  (when (:log? ~wrapper)
;                    (warn e2# "Error reopening" (pr-str (:name ~wrapper)))))
;                (finally
;                  (.lock read-lock#)))
;              ; Right, that's done with, now we can propagate the exception
;              (throw e#))
;            (finally
;              (.unlock read-lock#))))))

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
                    ;(try
                    ;  (if (< attempts# 30) (info " === RETRYING... ==="))
                    ;  (let [res# ~@body]
                    ;    (if (< attempts# 30) (info " === RETRY SUCCEEDED ==="))
                    ;    res#)
                    ;  (catch Throwable th#
                    ;    (if (< attempts# 30)
                    ;      (do (info " === RETRY FAILED ===")
                    ;          (info th#)))
                    ;    (throw th#)))
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

(defn wait-for-conn
  "Spins until a client is ready. Somehow, I think exceptions escape from this."
  [client]
  (util/timeout 60000 (throw (RuntimeException. "Timed out waiting for conn"))
                (while (try
                         (with-conn [c client]
                                    (j/query c ["select 1"])
                                    false)
                         (catch RuntimeException e
                           true)))))

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



