(ns yugabyte.ysql.g2
  "YSQL client for the Adya G2 predicate write-skew workload (yugabyte.g2).

  Two tables g2_a and g2_b, each with a secondary index on `key` that INCLUDEs
  `value`. An :insert op carries [key [a-id b-id]] with exactly one id set. In
  one transaction the client reads the predicate `value % 3 = 0` for this key
  over both tables *through the secondary index*; iff both are empty it inserts
  a matching row (value 30) into g2_a or g2_b. Driving the predicate read
  through the index is deliberate: Adya notes a DB may serialize on primary
  keys yet let an index observe stale data, which is exactly the write skew this
  detects. The checker flags any key for which more than one insert committed."
  (:require [clojure.java.jdbc :as j]
            [yugabyte.ysql.client :as c]))

(def table-a "g2_a")
(def table-b "g2_b")
(def index-a "idx_g2_a")
(def index-b "idx_g2_b")

(defn predicate-nonempty?
  "Does `table` have any row for this key matching value%3=0, read via `index`?"
  [conn table index k]
  (boolean (seq (c/query conn [(str "/*+ IndexScan(" table " " index ") */ "
                                    "select id from " table
                                    " where key = ? and value % 3 = 0") k]))))

(defrecord G2Client [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (doseq [[t idx] [[table-a index-a] [table-b index-b]]]
      (c/execute! c (j/create-table-ddl t [[:id :int "PRIMARY KEY"]
                                           [:key :int]
                                           [:value :int]]
                                        {:conditional? true}))
      (c/execute! c (str "CREATE INDEX " idx " ON " t " (key) INCLUDE (value)"))))

  (invoke-op! [this test op c conn-wrapper]
    (let [[k [a-id b-id]] (:value op)]
      (j/with-db-transaction [c c {:isolation isolation}]
        (if (or (predicate-nonempty? c table-a index-a k)
                (predicate-nonempty? c table-b index-b k))
          ; Anti-dependency observed: refuse to insert.
          (assoc op :type :fail)
          (do
            (if a-id
              (c/execute! c [(str "insert into " table-a " (id, key, value) values (?, ?, 30)") a-id k])
              (c/execute! c [(str "insert into " table-b " (id, key, value) values (?, ?, 30)") b-id k]))
            (assoc op :type :ok))))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-a)
    (c/drop-table c table-b)))

(c/defclient Client G2Client)
