(ns yugabyte.ysql.queue
  "YSQL client for the SKIP LOCKED work-queue workload (yugabyte.queue).

  Table `queue (id bigint primary key, payload bigint, claimer bigint, claimed
  boolean)`, plus a partial index over the unclaimed rows, which is the ordered
  access path a claim scans. `id` and `payload` are both the generated payload;
  `claimer` records the Jepsen process whose claim committed.

    :enqueue v  -> INSERT (id, payload) = (v, v); claimed takes its default.
    :dequeue    -> SELECT ... WHERE claimed = false ORDER BY id LIMIT 1 FOR
                   UPDATE SKIP LOCKED, briefly hold the lock so concurrent
                   claimers really do skip, then UPDATE claimed and claimer.
                   :ok with the payload, or :fail :empty for no row. Ops marked
                   :drain? true skip the hold.
    :read-table -> payload, claimer and claimed for every row.

  Every op runs in a transaction at the client's isolation. Both parts matter: the
  connection default is serializable, where YugabyteDB downgrades SKIP LOCKED to a
  blocking lock, and a locking read outside a transaction is rejected outright."
  (:require [clojure.java.jdbc :as j]
            [jepsen.random :as random]
            [yugabyte.ysql.client :as c]))

(def table-name "queue")
(def index-name "idx_queue_unclaimed")

(def unclaimed
  "Shared by the partial index and the claim, so the planner matches them."
  "claimed = false")

(def hold-ms
  "Upper bound on a dequeue's lock hold. Without an overlap window concurrent
  claimers would rarely meet a locked row and SKIP LOCKED would never skip."
  50)

(defrecord QueueClient [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name
                                      [[:id :bigint "PRIMARY KEY"]
                                       [:payload :bigint "NOT NULL"]
                                       [:claimer :bigint]
                                       [:claimed :boolean "NOT NULL DEFAULT false"]]
                                      {:conditional? true}))
    (c/execute! c (str "CREATE INDEX IF NOT EXISTS " index-name " ON " table-name
                       " (id ASC) WHERE " unclaimed)))

  (invoke-op! [this test op c conn-wrapper]
    ; Explicit isolation on every op, enqueues included, and the locking select is
    ; only legal inside a transaction at all. See the namespace docstring.
    (j/with-db-transaction [c c {:isolation isolation}]
      (case (:f op)
        :enqueue
        (let [v (:value op)]
          (c/execute! op c [(str "insert into " table-name " (id, payload) values (?, ?)") v v])
          (assoc op :type :ok))

        :dequeue
        (if-let [row (first (c/query op c [(str "select id, payload from " table-name
                                                " where " unclaimed
                                                " order by id limit 1 for update skip locked")]))]
          (do (when-not (:drain? op)
                (Thread/sleep (random/long hold-ms)))
              (let [n (first (c/execute! op c [(str "update " table-name
                                                    " set claimed = true, claimer = ? where id = ?")
                                               (:process op) (:id row)]))]
                (when (not= 1 n)
                  (throw (ex-info "claim updated wrong number of rows"
                                  {:op op, :row row, :updated n})))
                (assoc op :type :ok, :value (:payload row))))
          (assoc op :type :fail, :error :empty))

        :read-table
        (assoc op :type :ok
               :value (c/query op c [(str "select payload, claimer, claimed from "
                                          table-name)])))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))

(c/defclient Client QueueClient)
