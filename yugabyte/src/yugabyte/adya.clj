(ns yugabyte.adya
  "Given a bunch of keys and two identical tables, insert a given key in the given table
  iff it isn't already present in either.

  Validate that each key only gets inserted once."
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.generator :as gen]
            [jepsen.tests.adya :as adya]))

(defn workload
  [opts]
  {:generator (adya/g2-gen)
   :checker   (adya/g2-checker)})
