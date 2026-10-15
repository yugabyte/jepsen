(ns yugabyte.amp.temporal
  "PostgreSQL 18 temporal primary key: PRIMARY KEY (room, during WITHOUT
  OVERLAPS). The integer room column needs btree_gist.

  A :book transaction inserts 1-2 bookings in distinct rooms, all-or-nothing.
  A conflicting booking is rejected with SQLSTATE 23P01. The workload never
  deletes, so a final read of the whole table is checked against the history:

  - no two bookings in one room overlap
  - every booking of an :ok transaction is present
  - no booking of a :fail transaction is present
  - every present booking matches what some invocation attempted
  - every transaction is present entirely or not at all
  - every 23P01-rejected transaction overlaps a present booking of another
    transaction"
  (:require [clojure.java.jdbc :as j]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.random :as random]
            [yugabyte.generator :as ygen]
            [yugabyte.ysql.client :as c]))

(def table-name "booking")
(def room-count 4)
(def horizon "Bookings start in [0, horizon)" 20000)
(def max-length 100)

(defrecord TemporalClient [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    ; CREATE EXTENSION is database-wide and IF NOT EXISTS does not guard
    ; against concurrent creation: the loser of a race gets a unique
    ; violation on pg_extension. Runs sharing the database serialize on a
    ; transaction-scoped advisory lock.
    (j/with-db-transaction [c c {:isolation :read-committed}]
      (c/query c ["SELECT pg_advisory_xact_lock(hashtext('jepsen.btree_gist'))"])
      (c/execute! c "CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public"))
    (c/execute! c (str "CREATE TABLE " table-name " ("
                       "id bigint NOT NULL, "
                       "room int NOT NULL, "
                       "during int4range NOT NULL, "
                       "PRIMARY KEY (room, during WITHOUT OVERLAPS))")))

  (invoke-op! [this test op c conn-wrapper]
    (case (:f op)
      :book
      (j/with-db-transaction [c c {:isolation isolation}]
        (doseq [{:keys [id room from to]} (:value op)]
          (c/execute! op c [(str "INSERT INTO " table-name " (id, room, during)"
                                 " VALUES (?, ?, int4range(?::int, ?::int))")
                            id room from to]))
        (assoc op :type :ok))

      :read
      (->> (c/query op c (str "SELECT id, room, lower(during) AS lo, upper(during) AS hi"
                              " FROM " table-name))
           (mapv (fn [r] {:id   (long (:id r))
                          :room (long (:room r))
                          :from (long (:lo r))
                          :to   (long (:hi r))}))
           (assoc op :type :ok, :value))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name true)))

(defn book-gen
  "Generator of :book transactions with globally unique booking ids."
  []
  (let [next-id (atom 0)]
    (fn [test ctx]
      {:f     :book
       :value (->> (random/shuffle (range room-count))
                   (take (inc (random/long 2)))
                   (mapv (fn [room]
                           (let [from (random/long horizon)]
                             {:id   (swap! next-id inc)
                              :room room
                              :from from
                              :to   (+ from 1 (random/long max-length))}))))})))

(defn overlap?
  [a b]
  (and (= (:room a) (:room b))
       (< (:from a) (:to b))
       (< (:from b) (:to a))))

(defn room-overlaps
  "Pairs of overlapping bookings. Within one room sorted by start, any overlap
  implies an overlap between neighbours."
  [rows]
  (->> rows
       (group-by :room)
       vals
       (mapcat (fn [rs]
                 (->> (sort-by :from rs)
                      (partition 2 1)
                      (filter (partial apply overlap?)))))
       vec))

(defn check-history
  [history]
  (let [books      (filter #(= :book (:f %)) history)
        attempted  (into {} (for [op books
                                  :when (= :invoke (:type op))
                                  b (:value op)]
                              [(:id b) b]))
        completed  (remove #(= :invoke (:type %)) books)
        final-read (->> history
                        (filter #(and (= :read (:f %)) (= :ok (:type %))))
                        last)]
    (if-not final-read
      {:valid? :unknown, :error "No successful final read"}
      (let [rows       (:value final-read)
            present?   (set (map :id rows))
            of-type    (fn [t] (filter #(= t (:type %)) completed))
            overlaps   (room-overlaps rows)
            lost       (vec (for [op (of-type :ok), b (:value op)
                                  :when (not (present? (:id b)))]
                              b))
            failed     (vec (for [op (of-type :fail), b (:value op)
                                  :when (present? (:id b))]
                              b))
            unexpected (vec (remove #(= % (attempted (:id %))) rows))
            partial-txns (vec (for [op completed
                                  :let [n (count (filter (comp present? :id) (:value op)))]
                                  :when (< 0 n (count (:value op)))]
                              op))
            rejected   (filter #(= :exclusion-violation (first (:error %)))
                               (of-type :fail))
            unexplained (vec (for [op rejected
                                   :let [ids (set (map :id (:value op)))
                                         others (remove (comp ids :id) rows)]
                                   :when (not-any? (fn [b] (some (partial overlap? b) others))
                                                   (:value op))]
                               op))
            errors     {:overlapping-bookings   overlaps
                        :lost-bookings          lost
                        :failed-but-present     failed
                        :unexpected-rows        unexpected
                        :partial-transactions   partial-txns
                        :unexplained-rejections unexplained}]
        (merge {:valid?          (every? empty? (vals errors))
                :row-count       (count rows)
                :ok-count        (count (of-type :ok))
                :rejected-count  (count rejected)
                :info-count      (count (of-type :info))}
               (into {} (remove (comp empty? val) errors)))))))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (check-history history))))

(defn workload
  [opts]
  (ygen/workload-with-op-index
    {:generator       (book-gen)
     ; One successful read is enough; retry through transient connection loss
     :final-generator (gen/until-ok (gen/delay 1 (gen/repeat 30 {:f :read})))
     :checker         (checker)}))
