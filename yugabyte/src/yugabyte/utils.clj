(ns yugabyte.utils
  "General helper utility functions"
  (:require [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [hiccup.core :as h]
            [jepsen.store :as store])
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

(defn html
  "Old code from timeline/html since regression introduced in jepsen-0.3.0"
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [pairs (->> history
                       timeline/sub-index
                       timeline/pairs)
            pair-count (count pairs)
            truncated? (< timeline/op-limit pair-count)
            pairs      (take timeline/op-limit pairs)]
        (->> (h/html [:html
                      [:head
                       [:style timeline/stylesheet]]
                      [:body
                       (timeline/breadcrumbs test (:history-key opts))
                       [:h1 (str (:name test) " key " (:history-key opts))]
                       (when truncated?
                         [:div {:class "truncation-warning"}
                          (str "Showing only " timeline/op-limit " of " pair-count " operations in this history.")])
                       [:div {:class "ops"}
                        (->> pairs
                             (map (partial timeline/pair->div
                                           history
                                           test
                                           (timeline/process-index history))))]]])
             (spit (store/path! test (:subdirectory opts) "timeline.html")))
        {:valid? true}))))

