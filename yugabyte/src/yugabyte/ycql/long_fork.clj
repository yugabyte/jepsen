(ns yugabyte.ycql.long-fork
  (:refer-clojure :exclude [test])
  (:require [jepsen.tests.long-fork :as lf]
            [yugabyte.ycql.client :as c]))

(def keyspace "jepsen")
(def table "long_fork")

(c/defclient CQLLongForkIndexClient keyspace []
  (setup! [this test]
    (c/create-transactional-table
      conn table
      {:key   :int
       :key2  :int
       :val   :int
       :primary-key [:key]})
    (c/create-index conn
      "CREATE INDEX IF NOT EXISTS long_forks ON long_fork (key2) INCLUDE (val)"))

  (invoke! [this test op]
    (let [txn (:value op)]
      (c/with-errors op #{}
        (case (:f op)
          :read (let [ks (seq (lf/op-read-keys op))
                      ; Look up values by the value index
                      vs (->> (c/select conn table
                                        :columns [:key2 :val]
                                        :where [[:in :key2 ks]])
                              (map (juxt :key2 :val))
                              (into (sorted-map)))
                      ; Rewrite txn to use those values
                      txn' (reduce (fn [txn [f k _]]
                                     ; We already know these are all reads
                                     (conj txn [f k (get vs k)]))
                                   []
                                   txn)]
                  (assoc op :type :ok :value txn'))

          :write (let [[[_ k v]] txn]
                   (do (c/insert! conn table
                                  {:key k
                                   :key2 k
                                   :val v})
                       (assoc op :type :ok)))))))

  (teardown! [this test]))
