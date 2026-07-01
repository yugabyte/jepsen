(ns yugabyte.g2
  "Adya's G2: anti-dependency cycles / predicate write-skew. Two concurrent
  transactions each read a predicate over two tables and, only if it matches
  nothing, insert a row that would make the *other* transaction's predicate
  match. A correct serializable database lets at most one of the pair commit;
  under snapshot isolation or read-committed both may commit (write skew), so
  this workload is meaningful only at SERIALIZABLE.

  Uses Jepsen's stock G2 generator and checker; see jepsen.tests.adya."
  (:require [jepsen.tests.adya :as adya]
            [yugabyte.generator :as ygen]))

(defn workload
  [opts]
  (ygen/workload-with-op-index
    {:generator (adya/g2-gen)
     :checker   (adya/g2-checker)}))
