(ns yugabyte.workaround
  (:require [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [hiccup.core :as h]
            [jepsen.store :as store]
            [knossos.history :as history]))

(defn process-index
  "Maps processes to columns"
  [history]
  (->> history
       history/processes
       history/sort-processes
       (reduce (fn [m p] (assoc m p (count m)))
               {})))

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
