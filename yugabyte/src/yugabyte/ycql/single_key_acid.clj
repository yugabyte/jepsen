(ns yugabyte.ycql.single-key-acid
  (:require [clojure [pprint :refer :all]]
            [jepsen.independent :as independent]
            [yugabyte.ycql.client :as c]))

(def keyspace "jepsen")
(def table-name "single_key_acid")

(c/defclient CQLSingleKey keyspace []
  (setup! [this test]
    (c/create-table conn table-name
                    {:id  :int
                     :val :int
                     :primary-key [:id]}))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (let [[id val] (:value op)]
        (case (:f op)
          :write
          (do (c/insert! conn table-name
                         {:id id, :val val}
                         :keyspace keyspace)
              (assoc op :type :ok))

          :cas
          (let [[expected-val new-val] val
                res (c/update! conn table-name
                               {:val new-val}
                               :keyspace keyspace
                               :where [[:= :id id]]
                               :only-if [[:= :val expected-val]])
                applied (get (first res) (keyword "[applied]"))]
            (assoc op :type (if applied :ok :fail)))

          :read
          (let [value (->> (c/select conn table-name
                                     :keyspace keyspace
                                     :where [[:= :id id]])
                           first
                           :val)]
            (assoc op :type :ok :value (independent/tuple id value)))))))

  (teardown! [this test]))
