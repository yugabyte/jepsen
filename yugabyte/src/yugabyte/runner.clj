(ns yugabyte.runner
  "Runs YugaByteDB tests."
  (:gen-class)
  (:require
            [clojure.tools.logging :refer :all]
            [jepsen.cli :as cli]
            [yugabyte [core]
                      [nemesis :as nemesis]
                      [single-row-inserts]
                      [single-key-acid]
             ]
  )
)

(def opt-spec
  "Additional command line options"
  [

   [nil "--nemesis NAME"
    (str "Nemesis to use, one of: " (clojure.string/join ", " (keys nemesis/nemeses)))
    :default "none"
    :validate [identity (cli/one-of nemesis/nemeses)]]
  ]
)

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for browsing results."
  [& args]
  (cli/run! (merge
      (cli/single-test-cmd {
          :opt-spec opt-spec
          :test-fn yugabyte.single-key-acid/test
      })
      (cli/serve-cmd)
      ) args))
