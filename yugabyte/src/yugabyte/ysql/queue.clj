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
  blocking lock, and the row lock has to outlive the SELECT for concurrent
  claimers to have anything to skip."
  (:require [clojure.java.jdbc :as j]
            [clojure.string :as str]
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

(defn- assert-isolation!
  "Fails setup unless a transaction really runs at the client's isolation: a silent
  fallback to the connection default of serializable would downgrade SKIP LOCKED
  to a blocking lock and leave every assertion passing on a vacuous test. Reads
  the PostgreSQL-level setting, so it does not detect YugabyteDB mapping read
  committed onto snapshot when yb_enable_read_committed_isolation is off."
  [c isolation]
  (j/with-db-transaction [c c {:isolation isolation}]
    (let [want   (str/replace (name isolation) "-" " ")
          actual (:iso (first (c/query c ["select current_setting('transaction_isolation') as iso"])))]
      (when-not (= want actual)
        (throw (ex-info "transaction isolation is not what the client asked for"
                        {:want want, :actual actual}))))))

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
                       " (id ASC) WHERE " unclaimed))
    (assert-isolation! c isolation))

  (invoke-op! [this test op c conn-wrapper]
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
          ; Unclaimed rows below the one we got were not lockable, so counting them
          ; measures how many rows SKIP LOCKED stepped over. Exact at repeatable
          ; read, where the count shares the claim's snapshot; approximate at read
          ; committed, which re-snapshots per statement.
          (let [measure? (not (:drain? op))
                skipped  (when measure?
                           (:skipped (first (c/query op c [(str "select count(*) as skipped from "
                                                                table-name " where " unclaimed
                                                                " and id < ?")
                                                           (:id row)]))))]
            (when measure? (Thread/sleep (random/long hold-ms)))
            (c/execute! op c [(str "update " table-name
                                   " set claimed = true, claimer = ? where id = ?")
                              (:process op) (:id row)])
            (assoc op :type :ok, :value (:payload row), :skipped skipped))
          (assoc op :type :fail, :error :empty))

        :read-table
        (assoc op :type :ok
               :value (c/query op c [(str "select payload, claimer, claimed from "
                                          table-name)])))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))

(c/defclient Client QueueClient)
