(ns yugabyte.queue
  "Work-queue workload for SELECT ... FOR UPDATE SKIP LOCKED, YugabyteDB's
  documented job-queue primitive: a claimer takes the first unlocked matching row
  and skips rows another transaction holds, instead of blocking.

    :enqueue v  -> insert a row with a globally unique, increasing payload.
    :dequeue    -> claim the head of the queue. :ok with the claimed payload, or
                   :fail :empty when nothing was claimable, which is a legal
                   outcome and never an anomaly by itself. Ops marked
                   :drain? true are the final phase's hold-free drain.
    :read-table -> every row: the checker's ground truth, since a checker sees
                   only the history, never the database.

  Invariants: no payload is claimed twice (mutual exclusion, checked always), and
  every acked enqueue is still present and eventually claimed (completeness,
  asserted only once the drain has quiesced, because YugabyteDB also skips rows a
  committed transaction touched after the reader's read time, so mid-run a row can
  be briefly invisible to every claimer). Indeterminate claims are budgeted rather
  than assumed either way.

  Bespoke rather than jepsen.checker/total-queue: that checker does not fail on
  duplicates, and its drain handling throws on the :info ops a nemesis produces.

  No sz. variant: YugabyteDB downgrades SKIP LOCKED to a blocking lock at
  serializable, warning only (yugabyte-db#11761), so it would pass while testing
  nothing."
  (:require [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.history :as h]
            [yugabyte.generator :as ygen]))

(def ^:private terminal-errors
  "Enqueue errors meaning the insert certainly did not commit. A whitelist:
  exception-to-op also labels [:conn-closed ...] :fail, but that is what a backend
  reports when a kill/stop nemesis takes it down, which a commit that already
  landed reports too."
  #{:rollback :conflicting-transaction :try-again :restart-read-required
    :conn-not-ready})

(defn- error-tag
  "Leading keyword of an :error, unwrapping exception-to-op's [:batch ...]."
  [error]
  (cond (keyword? error)    error
        (sequential? error) (recur (if (= :batch (first error))
                                     (second error)
                                     (first error)))))

(defn enqueues
  "Infinite stream of :enqueue ops. Payloads are globally unique and increasing,
  so duplicates and losses are detectable by value alone."
  []
  (map (fn [v] {:type :invoke, :f :enqueue, :value v}) (range)))

; One-shot op maps; wrap in `repeat` to emit more than one.
(def dequeue    {:type :invoke, :f :dequeue, :value nil})
(def drain      (assoc dequeue :drain? true))
(def read-table {:type :invoke, :f :read-table, :value nil})

(def drains-per-thread
  "Hold-free, so a generous quota costs seconds."
  500)

(def final-reads
  "A few, so one :info read can't leave the checker without ground truth."
  5)

(defn main-generator
  "Half the threads only enqueue, so supply continues even when dequeues block
  under a nemesis. `stagger` is global in 0.3, hence dividing by threads. A bare
  op map is a one-shot generator, hence `repeat`."
  [threads]
  (->> (gen/reserve (quot threads 2) (enqueues) (repeat dequeue))
       (gen/stagger (/ 1 threads))))

(defn final-generator
  "Post-heal drain, then the reads that give the checker its ground truth.
  Multi-round because a single empty claim proves nothing: SKIP LOCKED also
  returns nothing for rows that are merely contended."
  []
  (gen/phases
    (gen/each-thread (gen/limit drains-per-thread (repeat drain)))
    (gen/limit final-reads (repeat read-table))))

(defn- final-read
  "The last :ok :read-table invoked after every :ok claim completed; an earlier
  read may predate a straggler claim's commit and misreport it as lost. Only :ok
  claims count: an :info claim may complete after the reads, and its commit may
  land after its own completion anyway, so waiting for it protects nothing.
  gen/phases synchronizes, so this already holds; the filter guards it."
  [history ops]
  (let [last-claim (->> ops
                        (filter #(and (= :dequeue (:f %)) (= :ok (:type %))))
                        (map :index)
                        (reduce max -1))]
    (->> ops
         (filter #(and (= :ok (:type %)) (= :read-table (:f %))))
         (filter #(< last-claim (:index (h/invocation history %))))
         last)))

(def ^:private max-reported
  "Entries kept per problem key. The table holds every row ever enqueued, so a
  systemic failure has thousands of payloads to report."
  32)

(defn- summarize
  "Bounded rendering of a problem list. Callers sort first, so a sample is the
  lowest few and is stable across runs."
  [xs]
  (let [v (vec xs)]
    (if (<= (count v) max-reported)
      v
      {:count (count v), :sample (subvec v 0 max-reported)})))

(defn- converged?
  "True if every draining thread ended on an empty claim, so the queue was
  quiescent and leftover rows are a real loss rather than transient contention. A
  thread whose final attempt failed for another reason, or was indeterminate,
  leaves the queue's state unknown there and blocks the conclusion. Grouped by
  thread, not process: jepsen retires a process after an :info and hands its
  thread `process + concurrency`, so a retired process alone never looks
  converged."
  [test ops]
  (let [by-thread (->> ops
                       (filter #(and (:drain? %) (not= :invoke (:type %))))
                       (group-by #(mod (:process %) (:concurrency test))))]
    (and (seq by-thread)
         (every? (fn [os]
                   (let [l (apply max-key :index os)]
                     (and (= :fail (:type l)) (= :empty (:error l)))))
                 (vals by-thread)))))

(defn checker
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [ops      (h/client-ops history)
            of       (fn [t f] (filter #(and (= t (:type %)) (= f (:f %))) ops))
            tried    (set (map :value (of :invoke :enqueue)))
            failed   (->> (of :fail :enqueue)
                          (filter (comp terminal-errors error-tag :error))
                          (map :value)
                          set)
            acked    (set (map :value (of :ok :enqueue)))
            claims   (map (juxt :value :process) (of :ok :dequeue))
            claimed  (set (map first claims))
            ; a process is retired after an indeterminate op, so each :info claim
            ; is one row that process may have claimed unseen
            unsure   (frequencies (map :process (of :info :dequeue)))
            read     (final-read history ops)
            rows     (:value read)
            row-of   (into {} (map (juxt :payload identity)) rows)
            drained? (converged? test ops)
            ; mutual exclusion is visible in the history alone
            dups     (->> claims (map first) frequencies
                          (keep (fn [[p n]] (when (< 1 n) p)))
                          sort)
            ; the rest needs the final read: without it row-of is empty, which
            ; would read as "every row lost" rather than "nothing to compare to"
            table-problems
            (when read
              (let [lost-claims (into (sorted-set)
                                      (for [[p] claims :let [r (row-of p)]
                                            :when (and r (not (:claimed r)))] p))
                    unexpected  (into (sorted-set)
                                      (->> rows (map :payload)
                                           (filter #(or (not (tried %))
                                                        (failed %)))))
                    ; each defect reported once, under its most specific key
                    explained   (into (set dups) lost-claims)]
                {:claimer-mismatches
                 (sort-by :payload
                          (for [[p c] claims :let [r (row-of p)]
                                :when (and r (not= c (:claimer r))
                                           (not (explained p)))]
                            {:payload p, :claimed-by c, :row-claimer (:claimer r)}))
                 ; a claim proves the row committed, so it must still be there
                 :lost-rows       (sort (remove row-of (into acked claimed)))
                 :lost-claims     lost-claims
                 :unexpected-rows unexpected
                 :excess-claims   (->> rows
                                       (filter :claimed)
                                       (remove #(claimed (:payload %)))
                                       (remove #(unexpected (:payload %)))
                                       (group-by :claimer)
                                       (mapcat (fn [[proc rs]]
                                                 (drop (get unsure proc 0)
                                                       (sort-by :payload rs))))
                                       (map :payload)
                                       sort)
                 :undrained       (when drained?
                                    (->> rows (remove :claimed) (map :payload)
                                         (remove unexpected)
                                         (remove lost-claims)
                                         sort))}))
            problems (into {} (comp (remove (comp empty? val))
                                    (map (fn [[k v]] [k (summarize v)])))
                           (assoc table-problems :duplicate-claims dups))]
        (cond-> {:valid?   (cond (seq problems) false
                                 ; nothing wrong in the history, but most
                                 ; invariants went unchecked
                                 (nil? read)    :unknown
                                 :else          true)
                 :problems problems
                 :stats    {:enqueued    (count acked)
                            :claimed     (count claims)
                            :rows        (count rows)
                            :unclaimed   (count (remove :claimed rows))
                            :final-read? (some? read)
                            :drain-converged? drained?}}
          (nil? read)
          (assoc :error (str "No :ok :read-table was invoked after the last claim "
                             "committed; only mutual exclusion could be checked.")))))))

(defn workload
  "Shared by si.queue and rc.queue; only the client's isolation differs."
  [opts]
  ; One :op-index counter across both phases: the client stamps it into every
  ; statement for server-log correlation, and per-phase counters would restart.
  (let [ctr (atom 0)]
    {:generator       (ygen/with-op-index ctr (main-generator (:concurrency opts)))
     :final-generator (ygen/with-op-index ctr (final-generator))
     :checker         (checker)}))
