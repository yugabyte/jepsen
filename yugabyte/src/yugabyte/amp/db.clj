(ns yugabyte.amp.db
  "AMP endpoints are provisioned by ampqa, not by Jepsen. Setup only waits
  until each endpoint answers a query; teardown does nothing."
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [jepsen.db :as db]
            [slingshot.slingshot :refer [throw+]]
            [yugabyte.amp.client :as ac]
            [yugabyte.ysql.client :as c]))

(def ready-timeout-ms "How long setup waits for an endpoint" 120000)

(defn await-ready!
  "Blocks until the endpoint serves SELECT 1, retrying any SQLException until
  the deadline, then declares setup failed."
  [test node]
  (let [deadline (+ (System/currentTimeMillis) ready-timeout-ms)]
    (info "Waiting for endpoint" node)
    (loop []
      (let [ready? (try
                     (let [conn (ac/open-conn test node)]
                       (try
                         (j/query conn ["SELECT 1"])
                         true
                         (finally (c/close-conn conn))))
                     (catch java.sql.SQLException e
                       (when (< deadline (System/currentTimeMillis))
                         (throw+ {:type  :jepsen.db/setup-failed
                                  :node  node
                                  :cause (.getMessage e)}))
                       false))]
        (if ready?
          (info "Endpoint ready" node)
          (do (Thread/sleep 500) (recur)))))))

(defrecord AmpDB []
  db/DB
  (setup! [_ test node] (await-ready! test node))
  (teardown! [_ test node]))
