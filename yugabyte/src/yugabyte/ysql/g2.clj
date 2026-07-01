(ns yugabyte.ysql.g2
  "YSQL client for the Adya G2 predicate write-skew workload (yugabyte.g2).

  Two tables g2_a and g2_b. An :insert op carries [key [a-id b-id]] where
  exactly one of a-id/b-id is set. In one transaction the client reads the
  predicate `value % 3 = 0` over both tables for this key; iff both are empty it
  inserts a row (value 30, which matches the predicate) into g2_a or g2_b. The
  checker flags any key for which more than one insert committed."
  (:require [clojure.java.jdbc :as j]
            [yugabyte.ysql.client :as c]))

(def table-a "g2_a")
(def table-b "g2_b")

(defn predicate-nonempty?
  "Does `table` have any row for this key matching the value%3=0 predicate?"
  [conn table k]
  (boolean (seq (c/query conn [(str "select id from " table
                                    " where key = ? and value % 3 = 0") k]))))

(defrecord G2Client [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (doseq [t [table-a table-b]]
      (c/execute! c (j/create-table-ddl t [[:id :int "PRIMARY KEY"]
                                           [:key :int]
                                           [:value :int]]
                                        {:conditional? true}))))

  (invoke-op! [this test op c conn-wrapper]
    (let [[k [a-id b-id]] (:value op)]
      (j/with-db-transaction [c c {:isolation isolation}]
        (if (or (predicate-nonempty? c table-a k)
                (predicate-nonempty? c table-b k))
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
