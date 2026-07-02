(ns yugabyte.types
  "Writes numeric boundary values into a bigint column and reads them back,
  checking every value read for a key was actually written for that key. Catches
  overflow / truncation / precision corruption (e.g. a value silently narrowed
  to 32 bits). Isolation-independent, so meaningful at read-committed and
  snapshot as well as serializable."
  (:require [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [yugabyte.generator :as ygen]))

(def boundary-values
  "Edge-case 64-bit integers: zero/±1, 32-bit boundaries, and 64-bit extremes."
  [0 1 -1
   2147483647            ; Integer/MAX_VALUE
   2147483648            ; Integer/MAX_VALUE + 1
   -2147483648           ; Integer/MIN_VALUE
   -2147483649           ; Integer/MIN_VALUE - 1
   4294967296            ; 2^32
   9223372036854775807   ; Long/MAX_VALUE
   -9223372036854775808]) ; Long/MIN_VALUE

(def key-count 8)

(defn writes
  []
  (->> (range)
       (map (fn [i]
              {:type  :invoke
               :f     :write
               :value [(mod i key-count)
                       (nth boundary-values (mod i (count boundary-values)))]}))
       (map gen/once)))

(defn reads
  []
  {:type :invoke, :f :read, :value nil})

(defn checker
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [oks     (filter #(= :ok (:type %)) history)
            written (reduce (fn [m op]
                              (if (= :write (:f op))
                                (let [[k v] (:value op)]
                                  (update m k (fnil conj #{}) v))
                                m))
                            {} oks)
            errs    (for [op    oks
                          :when (= :read (:f op))
                          [k v] (:value op)
                          :when (not (contains? (get written k #{}) v))]
                      {:key k, :read v, :written (get written k #{})})]
        {:valid?     (empty? errs)
         :corruption (vec errs)}))))

(defn workload
  [opts]
  (let [threads (:concurrency opts)]
    ; reads must be an infinite generator (repeat), not a single op map: a lone
    ; map emits once then exhausts, starving reads to one op for the whole run.
    {:generator (->> (gen/reserve (quot threads 2) (writes) (repeat (reads)))
                     (gen/stagger (/ 1 threads))
                     (ygen/with-op-index))
     :checker   (checker)}))
