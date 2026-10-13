(ns yugabyte.ysql.append-table
  "Values are lists of integers. Each operation performs a transaction,
  comprised of micro-operations which are either reads of some value (returning
  the entire list) or appends (adding a single number to whatever the present
  value of the given list is). We detect cycles in these transactions using
  Jepsen's cycle-detection system.

  Lists are encoded as rows in a table; key names are table names, and the set
  of all rows determines the list contents.

  This test requires a way to order table contents, and as far as I can tell,
  there's no safe, transactional way to order inserts in YB. SERIAL columns
  aren't actually ordered; we can't use txn begin times (e.g. NOW()) because
  they might not reflect commit orders, and there's no way to get (presently)
  txn commit times. We can use COUNT(*), but that reads the whole table... Not
  sure what to do here."
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [jepsen.random :as random]
            [yugabyte.ysql.client :as c]))

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "append" i))

(defn insert-using-count!
  "Inserts a row with value v into a table, returning v. Key is derived from
  the count of rows in the table."
  [conn table v]
  (let [k (-> (c/query conn [(str "select count(*) from " table)])
              first
              :count)]
    (c/execute! conn [(str "insert into " table " (k, v) values (?, ?)") k v])
    v))

(defn insert-now!
  "Inserts a value v into a table, returning v. Key is computed using NOW()."
  [conn table v]
  (c/execute! conn [(str "insert into " table " (k, v) values (NOW(), ?)") v])
  v)

(defn insert-txn-timestamp!
  "Inserts a value v into a table, returning v. Key is derived from
  TRANSACTION_TIMESTAMP."
  [conn table v]
  (c/execute! conn [(str "insert into " table " (k, v) values (TRANSACTION_TIMESTAMP(), ?)") v])
  v)

(defn insert!
  "Inserts a row with value v into a table, returning v. Key is assigned
  automatically."
  [conn table v]
  (c/execute! conn [(str "insert into " table " (v) values (?)") v])
  v)

(defn read-ordered
  "Reads every value in table ordered by k."
  [conn table]
  (let [idx (str "idx_" table)
        use-index? (zero? (random/long 2))
        query-str (if use-index?
                    (str "/*+ IndexOnlyScan(" table " " idx ") */ select k, v from " table " order by k")
                    (str "select k, v from " table " order by k"))
        res (c/query conn [query-str])]
    (info table (if use-index? "IndexOnlyScan" "SeqScan") "→" (mapv :v res))
    (mapv :v res)))

(defn read-natural
  "Reads every value in table using natural ordering."
  [conn table]
  (->> (c/query conn [(str "select (v) from " table)])
       (mapv :v)))

(defn create-table!
  "Creates a table for the given relation. Swallows already-exists errors,
  because YB can't do `create ... if not exists` properly.
  Uses k INT PRIMARY KEY for deterministic ordering — concurrent inserts at
  the same position conflict on the PK, ensuring the ordering reflects the
  actual serialization order."
  [conn table-name]
  (info "Creating table" table-name)
  (try
    (c/execute! conn (j/create-table-ddl table-name
                                         [[:k :int "PRIMARY KEY"]
                                          [:v :int]]
                                         {:conditional? true}))
    (info "Created table" table-name)
    (catch com.yugabyte.util.PSQLException e
      (if (re-find #"already exists" (.getMessage e))
        (info "Table" table-name "already exists")
        (throw e))))
  (try
    (c/execute! conn (str "CREATE INDEX idx_" table-name " ON " table-name " (k, v)"))
    (info "Created index for" table-name)
    (catch com.yugabyte.util.PSQLException e
      (if (re-find #"already exists" (.getMessage e))
        (info "Index for" table-name "already exists")
        (throw e)))))

(defn catch-dne
  "Returns a form for catching a relation-does-not-exist exception of the given
  class. Binds the name of the missing relation to `table`, and evaluates
  body."
  [class table & body]
  `(~'catch ~class e#
     (if-let [~table (nth (re-find #"relation \"(.+?)\" does not exist"
                                   (.getMessage e#)) 1)]
       (do ~@body)
       (throw e#))))

(defmacro with-table
  "Evaluates body, catching \"relation does not exist\" exceptions, and
  evaluating retry, then body again, if that occurs."
  [conn & body]
  (let [table-sym (gensym 'table)]
    `(try (do ~@body)
          ~(apply catch-dne 'java.sql.BatchUpdateException table-sym
                  `(info "Creating table" ~table-sym "and retrying")
                  `(create-table! ~conn ~table-sym)
                  body)
          ~(apply catch-dne 'com.yugabyte.util.PSQLException table-sym
                  `(info "Creating table" ~table-sym "and retrying")
                  `(create-table! ~conn ~table-sym)
                  body)
        (catch Exception e#
          ; (info e# "with-table caught")
          (throw e#)))))

(defn mop!
  "Executes a transactional micro-op of the form [f k v] on a connection, where
  f is either :r for read or :append for list append. Returns the completed
  micro-op."
  [conn test [f k v]]
  (let [table (table-name k)]
      [f k (case f
             :r      (read-ordered conn table)
             :append (insert-using-count! conn table v))]))

(defrecord InternalClient [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper])

  (invoke-op! [this test op c conn-wrapper]
    (with-table c
      (let [txn  (:value op)
            txn' (j/with-db-transaction [c c {:isolation isolation}]
                   (mapv (partial mop! c test) txn))]
        (assoc op :type :ok, :value txn')))))

(c/defclient Client InternalClient)
