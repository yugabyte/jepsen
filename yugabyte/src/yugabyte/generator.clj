(ns yugabyte.generator
  (:require [jepsen.generator :as gen]))

(defn with-op-index
  "Append :op-index integer to every operation emitted by the given generator.
  Value starts at 1 and increments by 1 for every subsequent emitted operation.
  Pass an explicit counter atom to share one numbering across several
  generators, e.g. a workload's main and final generators - each generator
  getting its own counter would restart at 1 and make the indices ambiguous in
  server-side logs."
  ([gen]
   (with-op-index (atom 0) gen))
  ([ctr gen]
   (gen/map (fn add-op-index [op]
              (assoc op :op-index (swap! ctr inc)))
            gen)))

(defn workload-with-op-index
  "Alters a workload map, wrapping generator in with-op-index"
  [workload]
  (assoc workload :generator (with-op-index (:generator workload))))
