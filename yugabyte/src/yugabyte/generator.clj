(ns yugabyte.generator
  (:require [jepsen.generator :as gen]))


(defn with-insert-deletes
  "Append random :operation-type in [:insert :update :delete] random range
   In case of :insert :to key will be taken from atomic which stated from end of the list
   In case of :delete :from will be changes to atomic that starts from beginning"
  [gen start-key end-key ops]
  (let [delete-ctr (atom start-key)
        insert-ctr (atom end-key)]
    (gen/map
     (fn add-op-type [op]
       (let [dice (rand-nth ops)]
         (cond
           (= dice :insert)
           (assoc op :operation-type dice :to (swap! insert-ctr inc))

           (= dice :update)
           (assoc op :operation-type dice)

           (= dice :delete)
           (assoc op :operation-type dice :from (swap! delete-ctr inc)))))
     gen)))

(defn with-op-index
  "Append :op-index integer to every operation emitted by the given generator.
  Value starts at 1 and increments by 1 for every subsequent emitted operation."
  [gen]
  (let [ctr (atom 0)]
    (gen/map (fn add-op-index [op]
               (assoc op :op-index (swap! ctr inc)))
             gen)))

(defn workload-with-op-index
  "Alters a workload map, wrapping generator in with-op-index"
  [workload]
  (assoc workload :generator (with-op-index (:generator workload))))
