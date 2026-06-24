(ns yugabyte.ycql.client
  "Helper functions for working with the DataStax Cassandra Java driver."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [random :as random]
                    [util :as util]]
            [jepsen.control.net :as cn]
            [dom-top.core :as dt]
            [wall.hack :as wh])
  (:import (java.net InetSocketAddress)
           (com.datastax.driver.core Cluster
                                     Cluster$Builder
                                     HostDistance
                                     NettyOptions
                                     NettyUtil
                                     PoolingOptions
                                     ProtocolVersion
                                     ResultSet
                                     Row
                                     Session
                                     SimpleStatement
                                     SocketOptions
                                     ThreadingOptions)
           (com.datastax.driver.core.policies ConstantReconnectionPolicy
                                              RoundRobinPolicy
                                              WhiteListPolicy)
           (com.yugabyte.driver.core.policies NoRetryOnClientTimeoutPolicy)
           (com.datastax.driver.core.exceptions DriverException
                                                InvalidQueryException
                                                UnavailableException
                                                OperationTimedOutException
                                                ReadTimeoutException
                                                WriteTimeoutException
                                                NoHostAvailableException
                                                TransportException)
           (io.netty.channel.nio NioEventLoopGroup)
           (java.util.concurrent LinkedBlockingQueue
                                 ThreadPoolExecutor
                                 TimeUnit)))

;; ---- Value & condition formatting ----

(defn format-value
  "Formats a Clojure value for inclusion in a CQL string."
  [v]
  (cond
    (nil? v)    "null"
    (number? v) (str v)
    (string? v) (str "'" (str/replace v "'" "''") "'")
    (keyword? v) (name v)
    :else       (str v)))

(defn format-condition
  "Formats a condition vector like [:= :id 5] into a CQL fragment."
  [[op col val]]
  (let [col-name (name col)]
    (case op
      :=  (str col-name " = " (format-value val))
      :in (str col-name " IN (" (str/join ", " (map format-value val)) ")"))))

(defn format-conditions
  "Joins condition vectors with AND."
  [conds]
  (str/join " AND " (map format-condition conds)))

;; ---- ResultSet → Clojure maps ----

(defn resultset->maps
  "Converts a ResultSet into a vector of Clojure maps with keyword keys."
  [^ResultSet rs]
  (let [col-defs (.getColumnDefinitions rs)
        n        (.size col-defs)
        col-names (mapv #(keyword (.getName col-defs (int %))) (range n))]
    (mapv (fn [^Row row]
            (into {} (map-indexed
                       (fn [i col-name]
                         [col-name (.getObject row (int i))])
                       col-names)))
          (.all rs))))

;; ---- Core execute ----

(defn execute!
  "Executes a CQL string on a session, returns a vector of maps."
  [^Session session ^String cql]
  (resultset->maps (.execute session (SimpleStatement. cql))))

;; ---- Connection management ----

(defn disconnect!
  "Closes a session and its associated cluster."
  [^Session session]
  (let [cluster (.getCluster session)]
    (try (.close session)
         (finally
           (.close cluster)))))

;; ---- Keyspace management ----

(defn create-keyspace!
  "Creates a keyspace with SimpleStrategy replication if it doesn't exist."
  [session ks-name replication-factor]
  (execute! session
            (str "CREATE KEYSPACE IF NOT EXISTS " ks-name
                 " WITH replication = {'class': 'SimpleStrategy',"
                 " 'replication_factor': " replication-factor "}")))

(defn use-keyspace!
  "Sets the session to use the given keyspace."
  [^Session session ks-name]
  (execute! session (str "USE " ks-name)))

;; ---- Timeout execution ----

(defn execute-with-timeout!
  "Executes a CQL string with a custom read timeout in milliseconds."
  [^Session session timeout ^String cql]
  (let [stmt (doto (SimpleStatement. cql)
               (.setReadTimeoutMillis (int timeout)))]
    (resultset->maps (.execute session stmt))))

;; ---- CQL builders: CREATE TABLE ----

(defn build-create-table
  "Builds a CREATE TABLE IF NOT EXISTS CQL string. col-defs is a map like
  {:id :int, :balance :bigint, :primary-key [:id]}."
  [table col-defs]
  (let [pk       (:primary-key col-defs)
        cols     (dissoc col-defs :primary-key)
        col-strs (map (fn [[col-name col-type]]
                        (str (name col-name) " " (name col-type)))
                      cols)
        pk-str   (str "PRIMARY KEY (" (str/join ", " (map name pk)) ")")]
    (str "CREATE TABLE IF NOT EXISTS " table
         " (" (str/join ", " (concat col-strs [pk-str])) ")")))

(defn create-table
  "Creates a table with a 30s timeout. col-defs is a map like
  {:id :int, :balance :bigint, :primary-key [:id]}."
  [session table col-defs]
  (execute-with-timeout! session 30000 (build-create-table table col-defs)))

(defn create-transactional-table
  "Like create-table, but enables YugaByte transactions."
  [session table col-defs]
  (execute-with-timeout! session 30000
                         (str (build-create-table table col-defs)
                              " WITH transactions = { 'enabled' : true }")))

(defn create-index
  "Creates an index with a 30s timeout. Takes a raw CQL string.
  Swallows 'already exists' errors."
  [session cql]
  (try (execute-with-timeout! session 30000 cql)
       (catch InvalidQueryException e
         (if (re-find #"already exists" (.getMessage e))
           :already-exists
           (throw e)))))

;; ---- CRUD ----

(defn select
  "Executes a SELECT query. Options:
    :keyspace  - qualify table with keyspace
    :columns   - vector of column keywords (default *)
    :where     - vector of condition vectors"
  [session table & {:keys [keyspace columns where]}]
  (let [table-ref (if keyspace (str keyspace "." table) table)
        cols      (if columns
                    (str/join ", " (map name columns))
                    "*")
        cql       (str "SELECT " cols " FROM " table-ref
                       (when where
                         (str " WHERE " (format-conditions where))))]
    (execute! session cql)))

(defn insert!
  "Executes an INSERT query. values is a map of column/value pairs."
  [session table values & {:keys [keyspace]}]
  (let [table-ref (if keyspace (str keyspace "." table) table)
        entries   (seq values)
        cols      (str/join ", " (map (comp name key) entries))
        vals      (str/join ", " (map (comp format-value val) entries))]
    (execute! session (str "INSERT INTO " table-ref
                           " (" cols ") VALUES (" vals ")"))))

(defn update!
  "Executes an UPDATE query. set-map is a map of column/value pairs to SET.
  Options:
    :keyspace  - qualify table with keyspace
    :where     - vector of condition vectors
    :only-if   - vector of condition vectors for lightweight transactions"
  [session table set-map & {:keys [keyspace where only-if]}]
  (let [table-ref (if keyspace (str keyspace "." table) table)
        set-str   (str/join ", " (map (fn [[col v]]
                                        (str (name col) " = " (format-value v)))
                                      set-map))]
    (execute! session (str "UPDATE " table-ref
                           " SET " set-str
                           (when where
                             (str " WHERE " (format-conditions where)))
                           (when only-if
                             (str " IF " (format-conditions only-if)))))))

(defn update-counter!
  "Executes an UPDATE for a counter column. Uses col = col + amount syntax."
  [session table col amount & {:keys [keyspace where]}]
  (let [table-ref (if keyspace (str keyspace "." table) table)
        col-name  (name col)]
    (execute! session (str "UPDATE " table-ref
                           " SET " col-name " = " col-name " + " amount
                           (when where
                             (str " WHERE " (format-conditions where)))))))

;; ---- Retry helper ----

(defmacro with-retry
  "Retries CQL unavailable/timeout errors for up to 120 seconds. Helpful for
  setting up initial data; YugaByte loves to throw 10+ second latencies at us
  early in the test."
  [& body]
  `(let [deadline# (+ (util/linear-time-nanos) (util/secs->nanos 120))
         sleep#    100] ; ms
     (dt/with-retry []
       ~@body
       (catch NoHostAvailableException e#
         (if (< deadline# (util/linear-time-nanos))
           (throw e#)
           (do (info "Timed out, retrying")
               (Thread/sleep (long (random/long sleep#)))
               (~'retry))))
       (catch OperationTimedOutException e#
         (if (< deadline# (util/linear-time-nanos))
           (throw e#)
           (do (info "Timed out, retrying")
               (Thread/sleep (long (random/long sleep#)))
               (~'retry)))))))

;; ---- Cluster & connect ----

(defn epoll-event-loop-group-constructor
  "Why is this not public?"
  []
  (wh/field NettyUtil :EPOLL_EVENT_LOOP_GROUP_CONSTRUCTOR NettyUtil))

(defn epoll-available?
  "Annnd security policy prevents us from calling this ??!!? so uhhh, wall-hack
  our way in there too, sigh"
  []
  ; Also this returns a boolean which is NOT the usual Boolean/FALSE, so we
  ; coerce it to a normal one with (boolean)... don't even ask
  (boolean (wh/method NettyUtil :isEpollAvailable [] nil)))

(defn ^Cluster cluster
  "Constructs a Cassandra client Cluster object with appropriate options."
  [node]
  (.. (Cluster/builder)
    (withProtocolVersion (ProtocolVersion/fromInt 3))
    (withPoolingOptions (doto (PoolingOptions.)
                          (.setCoreConnectionsPerHost HostDistance/LOCAL 1)
                          (.setMaxConnectionsPerHost  HostDistance/LOCAL 1)))
    ; This is sort of a hack; we're allowed to call cn/ip here without an SSH
    ; connection because it memoizes, and we already called it during setup.
    (addContactPoint (cn/ip node))
    (withRetryPolicy NoRetryOnClientTimeoutPolicy/INSTANCE)
    (withReconnectionPolicy (ConstantReconnectionPolicy. 1000))
    (withSocketOptions (.. (SocketOptions.)
                         (setConnectTimeoutMillis 1000)
                         (setReadTimeoutMillis 5000)))
    (withLoadBalancingPolicy (WhiteListPolicy.
                               (RoundRobinPolicy.)
                               ; Same story: memoized.
                               [(InetSocketAddress. (cn/ip node) 9042)]))
    (withThreadingOptions (proxy [ThreadingOptions] []
                            (createExecutor [cluster-name]
                              (doto (ThreadPoolExecutor.
                                      1 ; Core pool size
                                      1 ; Max pool size
                                      30 ; How long to keep threads alive
                                      TimeUnit/SECONDS
                                      (LinkedBlockingQueue.)
                                      (.createThreadFactory
                                        this cluster-name "worker"))
                                (.allowCoreThreadTimeOut true)))))
    (withNettyOptions (proxy [NettyOptions] []
                        (eventLoopGroup [thread-factory]
                          (if (epoll-available?)
                            (.newInstance (epoll-event-loop-group-constructor)
                                          1 thread-factory)
                            (NioEventLoopGroup. 1 thread-factory)))))
    (build)))

(defn connect
  "Opens a new client, with helpful defaults for YugaByte."
  [node]
  (with-retry
    (let [c (cluster node)]
      (try (.connect c)
           (catch DriverException e
             (.close c)
             (throw e))))))

;; ---- Keyspace setup ----

(defn ensure-keyspace!
  "Creates a keyspace using the given connection, if it doesn't already exist.
  Replication-factor is derived from the test."
  [conn keyspace-name test]
  (create-keyspace! conn keyspace-name (:replication-factor test)))

;; ---- Error handling ----

(defmacro with-errors
  "Takes an op, a set of idempotent operation :fs, and a body. Evalates body,
  and catches common errors, returning an appropriate completion for `op`."
  [op idempotent & body]
  `(let [crash# (if (~idempotent (:f ~op)) :fail :info)]
     (try
       ~@body
       (catch UnavailableException e#
         ; I think this was used back when we blocked on all nodes being online
         ; (info "Not enough replicas - failing")
         (assoc ~op :type :fail, :error [:unavailable (.getMessage e#)]))

       (catch WriteTimeoutException e#
         (assoc ~op :type crash#, :error :write-timed-out))

       (catch ReadTimeoutException e#
         (assoc ~op :type crash#, :error :read-timed-out))

       (catch OperationTimedOutException e#
         (assoc ~op :type crash#, :error :operation-timed-out))

       (catch TransportException e#
         (condp re-find (.getMessage e#)
           #"Connection has been closed"
           (assoc ~op :type crash#, :error :connection-closed)

           (throw e#)))

       (catch NoHostAvailableException e#
         (condp re-find (.getMessage e#)
           #"no host was tried"
           (do (info "All nodes are down - sleeping 2s")
               (Thread/sleep 2000)
               (assoc ~op :type :fail :error [:no-host-available (.getMessage e#)]))
           (assoc ~op :type crash#, :error [:no-host-available (.getMessage e#)])))

       (catch DriverException e#
         (if (re-find #"Value write after transaction start|Conflicts with higher priority transaction|Conflicts with committed transaction|Operation expired: Failed UpdateTransaction.* status: COMMITTED .*: Transaction expired|Error parsing schema for table"
                      (.getMessage e#))
           ; Definitely failed
           (assoc ~op :type :fail, :error (.getMessage e#))
           (throw e#)))

       (catch InvalidQueryException e#
         ; This can actually mean timeout
         (if (re-find #"RPC to .+ timed out after " (.getMessage e#))
           (assoc ~op :type crash#, :error [:rpc-timed-out (.getMessage e#)])
           (throw e#))))))

;; ---- Client macro ----

(defmacro defclient
  "Helper for defining CQL clients. Takes a class name, a string keyspace, a
  vector of state fields (as for defrecord), followed by protocols and
  functions, like defrecord. Appends two fields, `conn`, and `keyspace-created`
  to the state fields, which stores the cassandra client connection, provides
  default open! and close! functions, and passes the whole state to defrecord.

  Defines a constructor without conn or keyspace-created fields. Args are 1:1
  with your state fields.

    (->MyClient)

  Automatically creates keyspace during setup!, and ensures that keyspace is
  used on every conn thereafter. We do this because CQL assumes clients set the
  keyspace once, and we don't want to do multiple network trips to set the
  keyspace for every operation.

  Calls to setup! take a lock to prevent concurrent creation of tables, which
  hits a bug in Yugabyte.

  Example:

    (c/defclient CQLBank []
      (setup! [this test]
        (do-stuff-with conn))

      (invoke! [this test op]
        ...)

      (teardown! [this test]))"
  [name keyspace fields & exprs]
  (let [[interfaces methods opts] (#'clojure.core/parse-opts+specs
                                    (cons 'jepsen.client/Client exprs))
        ; We're going to rewrite the setup! fn to lock, create the keyspace,
        ; and use it before executing user code.
        setup-code (->> methods
                        (filter (comp #{'setup!} first))
                        first
                        (drop 2))

        ; Strip the original setup from the interface list. This is a hack, we
        ; should handle multiple setup! fns from diff protocols correctly.
        exprs (->> (remove (fn [expr]
                             (and (list? expr)
                                  (= 'setup! (first expr))))
                           exprs))]
    `(do (defrecord ~name ~(conj (vec fields) 'conn 'keyspace-created)
           jepsen.client/Client
           (open! [~'this ~'test ~'node]
             (let [conn# (connect ~'node)]
               (when (realized? ~'keyspace-created)
                 (use-keyspace! conn# ~keyspace))
               (assoc ~'this :conn conn#)))

           (setup! [~'this ~'test]
             (locking ~'keyspace-created
               (ensure-keyspace! ~'conn ~keyspace ~'test)
               (deliver ~'keyspace-created true)
               (use-keyspace! ~'conn ~keyspace)
               ~@setup-code))

           (close! [~'this ~'test]
             (disconnect! ~'conn))

           ~@exprs)

         ; Constructor
         (defn ~(symbol (str "->" name))
           ~(vec fields)
           ; Pass user fields, conn, keyspace-created
           (new ~name ~@fields nil (promise))))))

;; ---- Await setup ----

(defn await-setup
  "Used at the start of a test. Takes a node, opens a connection to it, and
  evalulates some basic commands to make sure the cluster is ready to accept
  requests. Retries when necessary."
  [node]
  (let [max-tries 1000]
    (dt/with-retry [tries max-tries]
      (when (< 0 tries max-tries)
        (Thread/sleep 1000))

      (when (zero? tries)
        (info "Zero?, tries " tries)
        (throw (RuntimeException.
                 "Client gave up waiting for cluster setup.")))

      (let [conn (connect node)]
        (try
          ; We need to do this serially to avoid a race in table creation
          (locking await-setup
            ; This... doesn't actually seem to guarantee that subsequent
            ; attempts to create keyspaces, tables, and rows will work. Grrr.
            (create-keyspace! conn "jepsen_setup" 3)

            (execute-with-timeout!
              conn 10000
              (str "CREATE TABLE IF NOT EXISTS jepsen_setup.waiting"
                   " (id INT PRIMARY KEY, balance BIGINT)"
                   " WITH transactions = { 'enabled': true }"))

            (insert! conn "waiting" {:id 0, :balance 5}
                     :keyspace "jepsen_setup"))
          (info "Cluster ready")

          (finally
            (disconnect! conn))))

      (catch com.datastax.driver.core.exceptions.InvalidQueryException e
        (condp re-find (.getMessage e)
          #"num_tablets should be greater than 0"
          (do (info "Waiting for cluster setup: num_tablets was 0")
              (retry (dec tries)))

          #"Not enough live tablet servers to create table with replication factor"
          (do (info "Waiting for cluster setup: Not enough live tablet servers")
              (retry (dec tries)))

          (throw e)))

      (catch OperationTimedOutException e
        (info "Waiting for cluster setup: Timed out")
        (retry (dec tries)))

      (catch NoHostAvailableException e
        (info "Waiting for cluster setup: No host available")
        (retry (dec tries))))))
