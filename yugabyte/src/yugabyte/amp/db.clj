(ns yugabyte.amp.db
  "The AMP stack is deployed before jepsen starts, so setup only gives each
  test a fresh database on the endpoint and teardown leaves the stack alone."
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.db :as db]
            [slingshot.slingshot :refer [throw+]]))

(def test-db "jepsen")

(defn password [] (System/getenv "AMP_PASSWORD"))

(defn admin-spec
  "Connection spec for the endpoint's admin role, on dbname."
  [test dbname]
  {:dbtype         "yugabytedb"
   :classname      "com.yugabyte.Driver"
   :dbname         dbname
   :host           (:amp-host test)
   :port           (:amp-port test)
   :user           (:amp-user test)
   :password       (password)
   :loginTimeout   30
   :connectTimeout 30
   :socketTimeout  120})

(defn- await-endpoint!
  "The endpoint may still be rolling from a previous test's faults."
  [test]
  (let [deadline (+ (System/currentTimeMillis) 900000)]
    (loop []
      (let [ok (try (j/query (admin-spec test (:amp-dbname test)) ["SELECT 1"]) true
                    (catch Exception e
                      (when (< deadline (System/currentTimeMillis))
                        (throw+ {:type :jepsen.db/setup-failed :cause (.getMessage e)}))
                      (info "Endpoint not ready:" (.getMessage e))
                      false))]
        (when-not ok (Thread/sleep 5000) (recur))))))

(defrecord AmpDB []
  db/DB
  (setup! [_ test node]
    (await-endpoint! test)
    (let [spec (admin-spec test (:amp-dbname test))]
      (info "Recreating database" test-db "on" (:amp-host test))
      (j/execute! spec [(str "DROP DATABASE IF EXISTS " test-db " WITH (FORCE)")] {:transaction? false})
      (j/execute! spec [(str "CREATE DATABASE " test-db)] {:transaction? false})))

  (teardown! [_ test node]))

(defn db [] (->AmpDB))
