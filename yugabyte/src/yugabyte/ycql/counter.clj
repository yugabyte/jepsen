(ns yugabyte.ycql.counter
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [yugabyte.ycql.client :as c]))

(def table-name "counter")
(def keyspace "jepsen")

(c/defclient CQLCounterClient keyspace []
  (setup! [this test]
    (c/create-table conn table-name
                    {:id          :int
                     :count       :counter
                     :primary-key [:id]})
    (c/update-counter! conn table-name :count 0
                       :where [[:= :id 0]]))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :add (do (c/update-counter! conn table-name :count (:value op)
                                    :keyspace keyspace
                                    :where [[:= :id 0]])
                 (assoc op :type :ok))

        :read (let [value (->> (c/select conn table-name
                                         :keyspace keyspace
                                         :where [[:= :id 0]])
                               first
                               :count)]
                (assoc op :type :ok :value value)))))

  (teardown! [this test]))
