(ns yugabyte.amp.runner
  "Runs Jepsen workloads against AMP compute endpoints.

    PGPASSWORD=... lein run -m yugabyte.amp.runner test \\
      --workload amp/sz.append --endpoints host:5432 --time-limit 60

  Exit code: 0 valid, 1 invalid, 2 unknown, 255 crashed (jepsen.cli).
  gnuplot and graphviz (dot) must be on PATH: without them the checkers'
  plots fail and every verdict becomes unknown."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [clojure.java.shell :refer [sh]]
            [jepsen.cli :as cli]
            [yugabyte.amp.core :as core]
            [yugabyte.runner :as yr]))

(def cli-opts
  [[nil "--endpoints HOST:PORT,..." "Comma-separated AMP compute endpoints. Each becomes a Jepsen node."
    :parse-fn #(str/split % #",")
    :missing "--endpoints is required"
    :validate [(fn [eps] (every? #(re-matches #"[^:\s]+:\d+" %) eps))
               "Each endpoint must be host:port"]]

   [nil "--database NAME" "Database to connect to."
    :default "postgres"]

   [nil "--db-user NAME" "Database user. The password is read from PGPASSWORD."
    :default "cloud_admin"]

   [nil "--locking MODE" "Locking mode for append workloads: mixed (default), optimistic, or pessimistic"
    :default nil
    :parse-fn keyword
    :validate [#{:mixed :optimistic :pessimistic} "Must be one of: mixed, optimistic, pessimistic"]]

   [nil "--random-seed SEED" "Random seed for deterministic test execution. If not provided, a random seed is generated."
    :default nil
    :parse-fn parse-long]

   ["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :missing (str "--workload " (yr/one-of core/workloads))
    :validate [core/workloads (yr/one-of core/workloads)]]])

(defn opt-fn
  "Maps --endpoints onto Jepsen's node list, then applies the standard parsing."
  [parsed]
  (cli/test-opt-fn
    (if-let [eps (get-in parsed [:options :endpoints])]
      (assoc-in parsed [:options :nodes] eps)
      parsed)))

(def required-tools ["gnuplot" "dot"])

(defn check-tools!
  "Fails fast when a plotting tool is missing, instead of running the whole
  test and reporting unknown."
  []
  (let [missing (remove #(zero? (:exit (sh "which" %))) required-tools)]
    (when (seq missing)
      (throw (ex-info (str "Missing on PATH: " (vec missing)
                           " (install gnuplot and graphviz)")
                      {:missing missing})))))

(defn single-test-cmd
  []
  {"test" {:opt-spec (cli/merge-opt-specs cli/test-opt-spec cli-opts)
           :opt-fn   opt-fn
           :usage    "Runs a single AMP test"
           :run      (fn [{:keys [options]}]
                       (check-tools!)
                       (info "Test options:\n" (with-out-str (pprint options)))
                       (doseq [i (range (:test-count options))]
                         (let [test (yr/run-with-seed! #(core/amp-test options)
                                                       (:random-seed options))]
                           (case (:valid? (:results test))
                             false    (System/exit 1)
                             :unknown (System/exit 2)
                             nil))))}})

(defn -main
  [& args]
  (cli/run! (merge (cli/serve-cmd)
                   (single-test-cmd))
            args))
