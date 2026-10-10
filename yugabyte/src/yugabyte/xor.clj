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

(def mismatch-markers
  "Printed only on real divergence. Exit 1 alone is not enough to identify one:
  the script initialises exit_code = 1, so setup failures exit 1 as well."
  ["MISMATCH DETECTED" "Failure: Found inconsistencies"])

(defn- marker?
  [s]
  (some #(str/includes? s %) mismatch-markers))

(defn- clean
  "Strips the ANSI escapes the script emits unconditionally."
  [s]
  (str/trim (str/replace (str s) #"\u001b\[[0-9;]*m" "")))

(defn classify
  "Exit code and stdout -> {:status :ok|:mismatch|:warn}. Only :mismatch fails."
  [exit-code out]
  (let [lines (remove str/blank? (str/split-lines out))]
    (cond
      (zero? exit-code)
      {:status :ok, :summary "all replicas consistent"}

      (and (= 1 exit-code) (marker? out))
      {:status  :mismatch
       :summary (str/join "; " (map clean (filter marker? lines)))}

      :else
      {:status  :warn
       :summary (str "exit " exit-code ": "
                     (case exit-code
                       2 "some tables unverifiable"
                       3 "stopped early, partial coverage"
                       (clean (last lines))))})))

(defn run-check!
  "Runs the check from the node this is invoked on. Never throws."
  [test]
  (util/timeout timeout-ms {:status :warn, :summary "timed out"}
    (try
      ;; Output to a file: a scan prints a line per tablet, and c/exec throws on
      ;; a nonzero exit, so the code has to come back separately.
      ;; Despite the flag name, the script passes this straight to yb-admin
      ;; --master_addresses, so give it every master: no heal phase runs before
      ;; this on most workloads, and a single dead master would skip the check.
      (let [log (str "/tmp/xor-check-" (System/currentTimeMillis) ".log")
            cmd (str "python3 -u " remote-path
                     " --master_leader_address " (auto/master-addresses test)
                     " --scan-all-databases"
                     " --tools-path " auto/dir "/bin"
                     " --max-runtime " max-runtime
                     " > " log " 2>&1; echo EXIT_CODE=$?")]
        (c/upload-resource! script remote-path)
        (let [res  (c/su (c/exec :bash :-c cmd))
              code (parse-long (second (re-find #"EXIT_CODE=(\d+)" res)))]
          (classify code (c/su (c/exec :cat log)))))
      (catch Exception e
        {:status :warn, :summary (str "errored: " (.getMessage e))}))))

(defn checker
  "Reads the :xor-check op out of the history. Only :mismatch invalidates."
  []
  (reify checker/Checker
    (check [_ _ history _]
      (if-let [result (->> history
                           (filter #(and (= :xor-check (:f %)) (map? (:value %))))
                           first
                           :value)]
        (assoc result :valid? (not= :mismatch (:status result)))
        {:valid? true, :status :not-run}))))
