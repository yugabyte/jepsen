(ns yugabyte.xor
  "XOR-based replica consistency check. Reads every tablet from every peer and
  compares (row_count, xor_hash), catching replica divergence that the history
  checkers cannot see: a client only ever talks to the tablet leader.

  Wraps the vendored yb-shastra script in resources/."
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [control :as c]
                    [util :as util]]
            [yugabyte.auto :as auto]))

(def script "follower_consistency_check_v2.py")
(def remote-path (str "/tmp/" script))
(def max-runtime 180)
(def timeout-ms 240000)

(def mismatch-marker
  "Matched instead of the exit code or the summary line: the script initialises
  exit_code = 1, and a per-table exception or a table with no tablets is also
  counted into \"Failure: Found inconsistencies\"."
  "MISMATCH DETECTED")

(def no-tables-marker
  "Discovery succeeded but found nothing to compare, e.g. sz.default-value drops
  its tables as a workload operation."
  "No tables")

(def status->valid
  "Run verdict per status. Only :ok and :no-tables pass: a run that checked less
  than everything must not look like one that checked everything and agreed."
  {:ok           true
   :no-tables    true
   :mismatch     false
   :unverifiable false
   :partial      :unknown
   :error        :unknown
   :not-run      :unknown})

(defn- marker?
  [s]
  (str/includes? s mismatch-marker))

(defn- clean
  "Strips the ANSI escapes the script emits unconditionally."
  [s]
  (str/trim (str/replace (str s) #"\u001b\[[0-9;]*m" "")))

(defn- lines-of
  [out]
  (remove str/blank? (str/split-lines out)))

(defn classify
  "Exit code and stdout -> {:status ... :summary ...}; see status->valid."
  [exit-code out]
  (cond
    (zero? exit-code)
    {:status :ok, :summary "all replicas consistent"}

    (and (= 1 exit-code) (marker? out))
    {:status  :mismatch
     :summary (->> (lines-of out) (filter marker?) (map clean) (str/join "; "))}

    (= 2 exit-code)
    {:status :unverifiable, :summary "exit 2: tablets unreadable on too many replicas"}

    (= 3 exit-code)
    {:status :partial, :summary "exit 3: stopped early, partial coverage"}

    (str/includes? out no-tables-marker)
    {:status :no-tables, :summary "no tables to compare"}

    :else
    {:status  :error
     :summary (str "exit " exit-code ": " (clean (last (lines-of out))))}))

(defn run-check!
  "Runs the check from the node this is invoked on. Never throws."
  [test]
  (util/timeout timeout-ms {:status :error, :summary "timed out"}
    (try
      ;; Output to a file: c/exec throws on a nonzero exit, so the code has to
      ;; come back separately.
      (let [log (str "/tmp/xor-check-" (System/currentTimeMillis) ".log")
            cmd (str "python3 -u " remote-path
                     ;; Passed straight to yb-admin --master_addresses, so give
                     ;; it every master and let yb-admin fail over.
                     " --master_leader_address " (auto/master-addresses test)
                     " --scan-all-databases"
                     " --tools-path " auto/dir "/bin"
                     " --max-runtime " max-runtime
                     ;; Retries stay at their default: the script reports a
                     ;; failed list_tablets as a mismatch.
                     " --admin-timeout 20"
                     " > " log " 2>&1; echo EXIT_CODE=$?")]
        (c/upload-resource! script remote-path)
        (let [res  (c/su (c/exec :bash :-c cmd))
              code (parse-long (second (re-find #"EXIT_CODE=(\d+)" res)))]
          (classify code (c/su (c/exec :cat log)))))
      (catch Exception e
        {:status :error, :summary (str "errored: " (.getMessage e))}))))

(defn checker
  "Reads the :xor-check op out of the history."
  []
  (reify checker/Checker
    (check [_ _ history _]
      (if-let [result (->> history
                           (filter #(and (= :xor-check (:f %)) (map? (:value %))))
                           first
                           :value)]
        (assoc result :valid? (status->valid (:status result) :unknown))
        ;; Missing only if the op never completed; stub workloads bypass this
        ;; checker entirely.
        {:valid?  :unknown
         :status  :not-run
         :summary "the check did not complete"}))))
