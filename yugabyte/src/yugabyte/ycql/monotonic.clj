(ns yugabyte.ycql.monotonic
  "YCQL client for the monotonic-reads workload (yugabyte.monotonic). A single
  counter row is incremented by :inc and read by :read; the shared checker flags
  any per-process read that goes backwards."
  (:require [yugabyte.ycql.client :as c]))

(def keyspace "jepsen")
(def table "monotonic")

(c/defclient CQLMonotonic keyspace []
  (setup! [this test]
    (c/create-table conn table
                    {:k           :int
                     :v           :counter
                     :primary-key [:k]}))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :inc
        (do (c/update-counter! conn table :v 1 :where [[:= :k 0]])
            (assoc op :type :ok))

        :read
        (let [v (->> (c/select conn table :where [[:= :k 0]])
                     first :v)]
          (assoc op :type :ok, :value (or v 0))))))

  (teardown! [this test]))
