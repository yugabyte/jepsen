(ns yugabyte.ysql.client
  "Helper functions for working with Cassaforte clients."
  (:require [clojurewerkz.cassaforte [client :as c]
             [query :as q]
             [policies :as policies]
             [cql :as cql]]
            [clojure.java.jdbc :as j]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen.util :as util]
            [jepsen.control.net :as cn]
            [jepsen.reconnect :as rc]
            [dom-top.core :as dt]
            [wall.hack :as wh]
            [slingshot.slingshot :refer [try+ throw+]]
            [yugabyte.auto :refer [db-name]]))

(def timeout-delay "Default timeout for operations in ms" 10000)
(def max-timeout "Longest timeout, in ms" 30000)

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

;(defn create-index
;  "Index creation is also slow in YB, so we run it with a custom timeout. Works
;  just like cql/create-index, or you can pass a string if you need to use YB
;  custom syntax.
;
;  Also you, like, literally *can't* tell Cassaforte (or maybe Cassandra's
;  client or CQL or YB?) to create an index if it doesn't exist, so we're
;  swallowing the duplicate table execeptions here.
;  TODO: update YB Cassaforte fork, so we can use `CREATE INDEX IF NOT EXISTS`.
;  "
;  [conn & index-args]
;  (let [statement (if (and (= 1 (count index-args))
;                           (string? (first index-args)))
;                    (first index-args)
;                    (apply q/create-index index-args))]
;    (try (execute-with-timeout! conn 30000 statement)
;         (catch InvalidQueryException e
;           (if (re-find #"already exists" (.getMessage e))
;             :already-exists
;             (throw e))))))

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

(defn db-conn-spec
  "Assemble a JDBC connection specification for a given Jepsen node."
  [node]
  {:classname      "org.postgresql.Driver"
   :subprotocol    "postgresql"
   :loginTimeout   (/ max-timeout 1000)
   :connectTimeout (/ max-timeout 1000)
   :socketTimeout  (/ max-timeout 1000)
   :subname        (str "//" (name node) ":" ysql-port "/" db-name)
   :user           "postgres"
   :password       ""})

(defn close-conn
  "Given a JDBC connection, closes it and returns the underlying spec."
  [conn]
  (when-let [c (j/db-find-connection conn)]
    (.close c))
  (dissoc conn :connection))

(defn client
  "Constructs a network client for a node, and opens it"
  [node]
  (info " === (defn client === " node)
  (rc/open!
    (rc/wrapper
      {:name  node
       :open  (fn open []
                (util/timeout max-timeout
                              (throw (RuntimeException.
                                       (str "Connection to " node " timed out")))
                              (util/retry 0.1
                                          (info " === Connecting to" (db-conn-spec node) "===")
                                          (let [spec  (db-conn-spec node)
                                                conn  (j/get-connection spec)
                                                spec' (j/add-connection spec conn)]
                                            (assert spec')
                                            spec'))))
       :close close-conn
       :log?  true})))

(defn drop-table [c table-name]
  (j/execute! c [(str "DROP TABLE " table-name)]))

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

(defmacro with-txn-retry-as-fail
  "Takes an op, runs body, catches PSQL 'restart transaction' errors, and
  converts them to :fails"
  [op & body]
  `(try ~@body
        (catch java.sql.SQLException e#
          (if (re-find #"ERROR: restart transaction"
                       (str (exception->op e#)))
            (assoc ~op
              :type :fail
              :error (str/replace
                       (.getMessage e#) #"ERROR: restart transaction: " ""))
            (throw e#)))))

(defmacro with-txn-retry
  "Catches PSQL 'restart transaction' errors and retries body a bunch of times,
  with exponential backoffs."
  [& body]
  `(util/with-retry [attempts# 30
                     backoff# 20]
                    ~@body
                    (catch java.sql.SQLException e#
                      (if (and (pos? attempts#)
                               (re-find #"ERROR: restart transaction"
                                        (str (exception->op e#))))
                        (do (Thread/sleep backoff#)
                            (~'retry (dec attempts#)
                              (* backoff# (+ 4 (* 0.5 (- (rand) 0.5))))))
                        (throw e#)))))

(defmacro with-txn
  "Wrap a evaluation within a SQL transaction."
  [[c conn] & body]
  `(j/with-db-transaction [~c ~conn {:isolation isolation-level}]
                          ~@body))

(defmacro with-restart
  "Wrap an evaluation within a CockroachDB retry block."
  [c & body]
  `(util/with-retry [attempts# 10
                     backoff# 20]
                    (j/execute! ~c ["savepoint cockroach_restart"])
                    ~@body
                    (catch java.sql.SQLException e#
                      (if (and (pos? attempts#)
                               (re-find #"ERROR: restart transaction"
                                        (str (exception->op e#))))
                        (do (j/execute! ~c ["rollback to savepoint cockroach_restart"])
                            (Thread/sleep backoff#)
                            (info "txn-restart" attempts#)
                            (~'retry (dec attempts#)
                              (* backoff# (+ 4 (* 0.5 (- (rand) 0.5))))))
                        (throw e#)))))

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
        #"Connection .+? refused"
        {:type :fail, :error :connection-refused}

        #"context deadline exceeded"
        {:type :fail, :error :context-deadline-exceeded}

        #"rejecting command with timestamp in the future"
        {:type :fail, :error :reject-command-future-timestamp}

        #"encountered previous write with future timestamp"
        {:type :fail, :error :previous-write-future-timestamp}

        #"restart transaction"
        {:type :fail, :error [:restart-transaction m]}

        {:type :info, :error [:psql-exception m]})

      clojure.lang.ExceptionInfo
      (condp = (:type (ex-data e))
        :conn-not-ready {:type :fail, :error :conn-not-ready}
        nil)

      (condp re-find m
        #"^timeout$"
        {:type :info, :error :timeout}

        nil))))


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
  "Like jdbc query, but includes a default timeout in ms."
  ([conn expr]
   (query conn expr {}))
  ([conn [sql & params] opts]
   (let [s (j/prepare-statement (j/db-find-connection conn)
                                sql
                                {:timeout (/ timeout-delay 1000)})]
     (try
       (j/query conn (into [s] params) opts)
       (finally
         (.close s))))))

(defn insert!
  "Like jdbc insert!, but includes a default timeout."
  [conn table values]
  (j/insert! conn table values {:timeout timeout-delay}))

(defn insert-with-rowid!
  "Like insert!, but includes the auto-generated :rowid."
  [conn table record]
  (let [keys        (->> record
                         keys
                         (map name)
                         (str/join ", "))
        placeholder (str/join ", " (repeat (count record) "?"))]
    (merge record
           (first (query conn
                         (into [(str "insert into " table " (" keys
                                     ") values (" placeholder
                                     ") returning rowid;")]
                               (vals record)))))))

(defn update!
  "Like jdbc update!, but includes a default timeout."
  [conn table values where]
  (j/update! conn table values where {:timeout timeout-delay}))


;(defn await-setup
;  "Used at the start of a test. Takes a node, opens a connection to it, and
;  evalulates some basic commands to make sure the cluster is ready to accept
;  requests. Retries when necessary."
;  [node]
;  (let [max-tries 1000]
;    (dt/with-retry [tries max-tries]
;      (when (< 0 tries max-tries)
;        (Thread/sleep 1000))
;
;      (when (zero? tries)
;        (info "Zero?, tries " tries)
;        (throw (RuntimeException.
;                 "Client gave up waiting for cluster setup.")))
;
;      (let [conn (connect node)]
;        (try
;          ; We need to do this serially to avoid a race in table creation
;          (locking await-setup
;            ; This... doesn't actually seem to guarantee that subsequent
;            ; attempts to create keyspaces, tables, and rows will work. Grrr.
;            (cql/create-keyspace conn "jepsen_setup"
;                                 (q/if-not-exists)
;                                 (q/with
;                                   {:replication
;                                    {"class"              "SimpleStrategy"
;                                     "replication_factor" 3}}))
;
;            (execute-with-timeout!
;              conn 10000
;              (str "CREATE TABLE IF NOT EXISTS jepsen_setup.waiting"
;                   " (id INT PRIMARY KEY, balance BIGINT)"
;                   " WITH transactions = { 'enabled': true }"))
;
;            (cql/insert-with-ks conn "jepsen_setup" "waiting"
;                                {:id 0, :balance 5}))
;          (info "Cluster ready")
;
;          (finally
;            (c/disconnect! conn))))
;
;      (catch com.datastax.driver.core.exceptions.InvalidQueryException e
;        (condp re-find (.getMessage e)
;          #"num_tablets should be greater than 0"
;          (do (info "Waiting for cluster setup: num_tablets was 0")
;              (retry (dec tries)))
;
;          #"Not enough live tablet servers to create table with replication factor"
;          (do (info "Waiting for cluster setup: Not enough live tablet servers")
;              (retry (dec tries)))
;
;          (throw e)))
;
;      (catch OperationTimedOutException e
;        (info "Waiting for cluster setup: Timed out")
;        (retry (dec tries)))
;
;      (catch NoHostAvailableException e
;        (info "Waiting for cluster setup: No host available")
;        (retry (dec tries))))))
