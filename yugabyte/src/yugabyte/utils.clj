(ns yugabyte.utils
  "General helper utility functions"
  (:import (java.util Date)
           (java.text SimpleDateFormat)))

(defn map-values
  "Returns a map with values transformed by function f"
  [m f]
  (reduce-kv (fn [m k v] (assoc m k (f v)))
             {}
             m))

(defn pretty-datetime
  "Pretty-prints given datetime as yyyy-MM-dd_HH:mm:ss.SSS"
  [dt]
  (let [dtf (SimpleDateFormat. "yyyy-MM-dd_HH:mm:ss.SSS")]
    (.format dtf dt)))

(defn current-pretty-datetime
  []
  (pretty-datetime (Date.)))

(defn is-test-geo-partitioned?
  [test]
  (clojure.string/includes? (name (:workload test)) "geo."))

(defn is-test-read-committed?
  [test]
  (clojure.string/includes? (name (:workload test)) "rc."))

(defn is-test-serializable?
  [test]
  (clojure.string/includes? (name (:workload test)) "sz."))

(defn is-test-append-table?
  [test]
  (clojure.string/includes? (name (:workload test)) "append-table"))

(defn is-test-has-pessimistic-locs?
  "Returns true if the test may use pessimistic locking. With mixed locking
  (default), pessimistic is used randomly, so wait queues must be enabled.
  Only returns false when locking is explicitly :optimistic."
  [test]
  (not= :optimistic (:locking test)))
