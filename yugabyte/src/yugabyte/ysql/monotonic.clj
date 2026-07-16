(ns yugabyte.ysql.monotonic
  "YSQL client for the monotonic-reads workload (yugabyte.monotonic).

  A single row `monotonic (k int primary key, v bigint)` seeded at v=0.
    :inc  -> UPDATE monotonic SET v = v + 1 WHERE k = 0
    :read -> SELECT v FROM monotonic WHERE k = 0   (coerced to Long)"
  (:require [clojure.java.jdbc :as j]
            [yugabyte.ysql.client :as c]))

(def table-name "monotonic")

(defrecord MonotonicClient [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name
                                      [[:k :int "PRIMARY KEY"]
                                       [:v :bigint]]
                                      {:conditional? true}))
    (c/execute! c [(str "insert into " table-name " (k, v) values (0, 0) "
                        "on conflict (k) do nothing")]))

  (invoke-op! [this test op c conn-wrapper]
    ; Run at the client's isolation (see note in ysql.types): without it the op
    ; uses the connection default (serializable) rather than si./rc.
    (j/with-db-transaction [c c {:isolation isolation}]
      (case (:f op)
        :inc
        (do (c/execute! c [(str "update " table-name " set v = v + 1 where k = 0")])
            (assoc op :type :ok))

        :read
        (let [v (-> (c/query c [(str "select v from " table-name " where k = 0")])
                    first :v)]
          (assoc op :type :ok, :value (some-> v long))))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))

(c/defclient Client MonotonicClient)
