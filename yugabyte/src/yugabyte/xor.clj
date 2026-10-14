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
  "The only line the script prints exclusively where it actually compared peers.
  Neither the exit code nor the summary's \"Failure: Found inconsistencies\" can
  stand in for it: the script initialises exit_code = 1, and a per-table
  exception or a table with no tablets also returns 1 and is counted into that
  summary line."
  "MISMATCH DETECTED")

(defn- marker?
  [s]
  (str/includes? s mismatch-marker))

(defn- clean
  "Strips the ANSI escapes the script emits unconditionally."
  [s]
  (str/trim (str/replace (str s) #"\u001b\[[0-9;]*m" "")))

(def failing-statuses
  "Statuses that invalidate a run. :unverifiable fails because a replica we could
  not read is exactly where a loss would hide; the stress suite treats it the
  same way."
  #{:mismatch :unverifiable})

(defn classify
  "Exit code and stdout -> {:status :ok|:mismatch|:unverifiable|:warn}."
  [exit-code out]
  (let [lines (remove str/blank? (str/split-lines out))]
    (cond
      (zero? exit-code)
      {:status :ok, :summary "all replicas consistent"}

      (and (= 1 exit-code) (marker? out))
      {:status  :mismatch
       :summary (str/join "; " (map clean (filter marker? lines)))}

      (= 2 exit-code)
      {:status  :unverifiable
       :summary "exit 2: some tablets could not be read from enough replicas"}

      :else
      {:status  :warn
       :summary (str "exit " exit-code ": "
                     (if (= 3 exit-code)
                       "stopped early, partial coverage"
                       (clean (last lines))))})))

(defn run-check!
  "Runs the check from the node this is invoked on. Never throws."
  [test]
  (util/timeout timeout-ms {:status :warn, :summary "timed out"}
    (try
      ;; Output to a file: a scan prints a line per tablet, and c/exec throws on
      ;; a nonzero exit, so the code has to come back separately.
      ;; Despite the flag name, the script passes this straight to yb-admin
      ;; --master_addresses, so give it every master and let yb-admin fail over.
      (let [log (str "/tmp/xor-check-" (System/currentTimeMillis) ".log")
            cmd (str "python3 -u " remote-path
                     " --master_leader_address " (auto/master-addresses test)
                     " --scan-all-databases"
                     " --tools-path " auto/dir "/bin"
                     " --max-runtime " max-runtime
                     ;; Shorter than the 120s default so an unreachable master
                     ;; gives up quickly. Retries stay at their default: the
                     ;; script reports a failed list_tablets as a mismatch, so
                     ;; transients must be absorbed here rather than surfacing.
                     " --admin-timeout 20"
                     " > " log " 2>&1; echo EXIT_CODE=$?")]
        (c/upload-resource! script remote-path)
        (let [res  (c/su (c/exec :bash :-c cmd))
              code (parse-long (second (re-find #"EXIT_CODE=(\d+)" res)))]
          (classify code (c/su (c/exec :cat log)))))
      (catch Exception e
        {:status :warn, :summary (str "errored: " (.getMessage e))}))))

(defn checker
  "Reads the :xor-check op out of the history."
  []
  (reify checker/Checker
    (check [_ _ history _]
      (if-let [result (->> history
                           (filter #(and (= :xor-check (:f %)) (map? (:value %))))
                           first
                           :value)]
        (assoc result :valid? (not (contains? failing-statuses (:status result))))
        ;; Not a pass: the op is missing only if it never completed (e.g. the
        ;; nemesis invoke! threw), and a run we did not check must not look
        ;; like one we checked and found clean. Stub workloads do not reach
        ;; here - they bypass the composed checker entirely.
        {:valid? :unknown, :status :not-run
         :summary "no :xor-check result in history; the check did not complete"}))))
