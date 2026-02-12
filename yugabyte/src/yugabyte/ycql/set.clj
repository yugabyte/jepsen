(ns yugabyte.ycql.set
  (:require [jepsen.random :as random]
            [yugabyte.ycql.client :as c]))

(def keyspace "jepsen")
(def table "elements")

(c/defclient CQLSetClient keyspace []
  (setup! [this test]
    (c/create-table conn table
                    {:val         :int
                     :count       :counter
                     :primary-key [:val]}))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :add (do (c/update-counter! conn table :count 1
                                    :where [[:= :val (:value op)]])
                 (assoc op :type :ok))

        :read (->> (c/select conn table :keyspace keyspace)
                   (mapcat (fn [row]
                             (repeat (:count row) (:val row))))
                   sort
                   (assoc op :type :ok, :value)))))

  (teardown! [this test]))

(def group-count
  "Number of distinct groups for indexing"
  8)

(c/defclient CQLSetIndexClient keyspace []
  (setup! [this test]
    (c/create-transactional-table conn table
                                  {:key         :int
                                   :val         :int
                                   :grp         :int
                                   :primary-key [:key]})
    (c/create-index conn
      "CREATE INDEX IF NOT EXISTS elements_by_group ON elements (grp) INCLUDE (val)"))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :add (do (c/insert! conn table
                            {:key (:value op)
                             :val (:value op)
                             :grp (random/long group-count)})
                 (assoc op :type :ok))

        :read (->> (c/select conn table
                             :columns [:val]
                             :where [[:in :grp (range group-count)]])
                   (map :val)
                   sort
                   (assoc op :type :ok, :value)))))

  (teardown! [this test]))
