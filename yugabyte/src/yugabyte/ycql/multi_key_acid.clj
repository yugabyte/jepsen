(ns yugabyte.ycql.multi-key-acid
  (:require [jepsen.independent :as independent]
            [jepsen.txn.micro-op :as mop]
            [clojure.string :as str]
            [yugabyte.ycql.client :as c]))

(def table-name "multi_key_acid")
(def keyspace "jepsen")

(c/defclient CQLMultiKey keyspace []
  (setup! [this test]
    (c/create-transactional-table
      conn table-name
      {:id          :int
       :ik          :int
       :val         :int
       :primary-key [:id :ik]}))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (let [[ik txn] (:value op)]
        (case (:f op)
          :read
          (let [ks (map mop/key txn)
                ; Look up values
                vs (->> (c/select conn table-name
                                  :columns [:id :val]
                                  :where [[:= :ik ik]
                                          [:in :id ks]])
                        (map (juxt :id :val))
                        (into {}))
                ; Rewrite txn to use those values
                txn' (mapv (fn [[f k _]] [f k (get vs k)]) txn)]
            (assoc op :type :ok, :value (independent/tuple ik txn')))

          :write
          (do (c/execute! conn
                          (str "BEGIN TRANSACTION "
                               (->> (for [[f k v] txn]
                                      (do
                                        ; We only support writes
                                        (assert (= :w f))
                                        (str "INSERT INTO "
                                             keyspace "." table-name
                                             " (id, ik, val) VALUES ("
                                             k ", " ik ", " v ");")))
                                    str/join)
                               "END TRANSACTION;"))
              (assoc op :type :ok))))))

  (teardown! [this test]))
