(ns yugabyte.ysql.append-columns
  "Values are lists of integers. Each operation performs a transaction,
  comprised of micro-operations which are either reads of some value (returning
  the entire list) or appends (adding a single number to whatever the present
  value of the given list is). We detect cycles in these transactions using
  Jepsen's cycle-detection system."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [yugabyte.ysql.client :as c]))

(defn read-columns
  "Read all columns from a table"
  [conn k]
  (let [read (c/query
               conn
               [(str "select column_name from information_schema.columns where table_schema = 'jepsen' and table_name = 'table_" k "'")])]
    (if (empty? read)
      nil
      (into (vector) (sort (map #(Integer/parseInt (str/replace (:column_name %) "c" "")) read))))))

(defn append-column
  "Creates a table if it's not exists. Otherwise add column to it."
  [conn k v]
  (let [read (c/query
               conn
               [(str "select column_name from information_schema.columns where table_schema = 'jepsen' and table_name = 'table_" k "'")])]
    (if (empty? read)
      (c/execute! conn [(str "create table jepsen.table_" k " (c" v " int)")])
      (c/execute! conn [(str "alter table jepsen.table_" k " add column c" v " int")]))
    v))

(defn mop!
  "Executes a transactional micro-op of the form [f k v] on a connection, where
  f is either :r for read or :append for list append. Returns the completed
  micro-op."
  [conn test [f k v]]
  [f k (case f
         :r (read-columns conn k)
         :append (append-column conn k v))])

(defrecord InternalClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c [(str "drop schema if exists jepsen cascade")])
    (c/execute! c [(str "create schema jepsen")]))

  (invoke-op! [this test op c conn-wrapper]
    (let [txn (:value op)
          use-txn? (< 1 (count txn))
          ; use-txn?  false ; Just for making sure the checker actually works
          txn' (if use-txn?
                 (c/with-txn c
                             (mapv (partial mop! c test) txn))
                 (mapv (partial mop! c test) txn))]
      (assoc op :type :ok, :value txn'))))

(c/defclient Client InternalClient)
