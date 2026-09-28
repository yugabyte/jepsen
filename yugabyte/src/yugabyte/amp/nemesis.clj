(ns yugabyte.amp.nemesis
  "Faults for an AMP stack on Kubernetes: the YugabyteDB universe underneath
  (YBA-managed master and tserver pods) and the AMP endpoint on top of it
  (endpoint-supervisor, safekeeper, pageserver and the Postgres compute, all in
  one pod). Every fault is a kubectl call, so it works on pods we cannot SSH
  into. Clock faults are absent on purpose: pods share their node's kernel
  clock.

  Partitions treat the stack as four logical nodes: yb0..yb2 (master-i plus
  tserver-i, like one YB VM) and ep (the endpoint pod)."
  (:require [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.util :as util]
            [yugabyte.amp.k8s :as k]
            [yugabyte.nemesis :as yn]))

(def yb-nodes ["yb0" "yb1" "yb2"])
(def all-nodes (conj yb-nodes "ep"))

(defn- yb-pods
  [test role]
  (->> (k/pods (:yb-namespace test) (str "app.kubernetes.io/name=yb-" role))
       (sort-by k/pod-name)))

(defn- yb-pod
  "The master or tserver pod of logical YB node i."
  [test role i]
  (first (filter #(str/ends-with? (k/pod-name %) (str "-yb-" role "-" i))
                 (yb-pods test role))))

(defn- endpoint-pods
  [test]
  (k/pods (:amp-namespace test) (:amp-endpoint-selector test)))

(defn- live-endpoint-pods
  [test]
  (filter k/running? (endpoint-pods test)))

(defn- node-pods
  "[ns pod-json] pairs making up a logical node."
  [test node]
  (if (= "ep" node)
    (map (fn [p] [(:amp-namespace test) p]) (live-endpoint-pods test))
    (let [i (subs node 2)]
      (->> ["master" "tserver"]
           (keep #(yb-pod test % i))
           (map (fn [p] [(:yb-namespace test) p]))))))

(defn- node-ips
  [test node]
  (keep (comp k/pod-ip second) (node-pods test node)))

(defn- all-ready?
  [pods]
  (and (seq pods)
       (every? (fn [p]
                 (and (k/running? p)
                      (every? :ready (get-in p [:status :containerStatuses]))))
               pods)))

(defn- await-ready!
  "Polls until every pod returned by pods-fn is running with all containers
  ready, up to timeout-s. Returns :ready or :timeout rather than throwing: a
  stack that never comes back is a finding, and the checker sees it in the
  history."
  [pods-fn timeout-s]
  (let [deadline (+ (System/currentTimeMillis) (* 1000 timeout-s))]
    (loop []
      (let [ok (try (all-ready? (pods-fn)) (catch Exception e (warn e "readiness poll") false))]
        (cond ok :ready
              (< deadline (System/currentTimeMillis)) :timeout
              :else (do (Thread/sleep 2000) (recur)))))))


(def ep-processes
  "Endpoint pod processes, by fault name, as pkill -f patterns."
  {:supervisor "/usr/local/bin/endpoint-supervisor"
   :safekeeper "/usr/local/bin/safekeeper"
   :pageserver "/usr/local/bin/pageserver"
   :compute    "/bin/postgres -D"})

(defn- yb-signal!
  [test role idxs sig]
  (->> idxs
       (keep #(yb-pod test role %))
       (mapv (fn [p]
               (k/signal! (:yb-namespace test) (k/pod-name p) (str "yb-" role) sig
                          (str "bin/yb-" role))
               (k/pod-name p)))))

(defn- ep-signal!
  [test procs sig]
  (vec (for [p (live-endpoint-pods test), proc procs]
         (do (k/signal! (:amp-namespace test) (k/pod-name p) "endpoint" sig
                        (ep-processes proc))
             [(k/pod-name p) proc]))))

(defn- random-idxs [] (util/random-nonempty-subset [0 1 2]))

(defn process-nemesis
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      ; Only the final heal waits: mid-test, another fault (a paused master, a
      ; partition) can keep a pod unready, and a blocking wait would hold that
      ; fault in place, since nemesis ops run one at a time.
      (let [wait     (if (:await op) identity (constantly 0))
            yb-ready #(await-ready! (fn [] (concat (yb-pods test "master") (yb-pods test "tserver")))
                                    (wait 600))
            ep-ready #(await-ready! (fn [] (endpoint-pods test)) (wait 900))]
        (assoc op :value
               (case (:f op)
                 :kill-tserver   (yb-signal! test "tserver" (random-idxs) "KILL")
                 :kill-master    (yb-signal! test "master" (random-idxs) "KILL")
                 :pause-tserver  (yb-signal! test "tserver" (random-idxs) "STOP")
                 :pause-master   (yb-signal! test "master" (random-idxs) "STOP")
                 :resume-tserver (yb-signal! test "tserver" [0 1 2] "CONT")
                 :resume-master  (yb-signal! test "master" [0 1 2] "CONT")
                 (:start-tserver :start-master) (yb-ready)

                 :kill-safekeeper (ep-signal! test [:safekeeper] "KILL")
                 :kill-pageserver (ep-signal! test [:pageserver] "KILL")
                 :kill-compute    (ep-signal! test [:compute] "KILL")
                 :kill-endpoint   (ep-signal! test [:supervisor] "KILL")
                 :delete-endpoint (mapv #(do (k/delete-pod! (:amp-namespace test) (k/pod-name %) true)
                                             (k/pod-name %))
                                        (live-endpoint-pods test))
                 :start-endpoint  (ep-ready)

                 :pause-safekeeper (ep-signal! test [:safekeeper] "STOP")
                 :pause-pageserver (ep-signal! test [:pageserver] "STOP")
                 :pause-endpoint   (ep-signal! test [:supervisor :safekeeper :pageserver :compute] "STOP")
                 :resume-endpoint  (ep-signal! test [:supervisor :safekeeper :pageserver :compute] "CONT")))))

    (teardown! [this test]
      (yb-signal! test "master" [0 1 2] "CONT")
      (yb-signal! test "tserver" [0 1 2] "CONT")
      (ep-signal! test [:supervisor :safekeeper :pageserver :compute] "CONT"))))


(defn- apply-grudge!
  "grudge: node -> nodes it refuses traffic from. Endpoint grudges against
  every YB node also drop the YB Services' ClusterIPs: a reply to a
  connection dialed through a ClusterIP arrives sourced from it."
  [test grudge]
  (let [ips (memoize (partial node-ips test))]
    (doall
      (for [[node enemies] grudge
            :when (seq enemies)
            [ns p] (node-pods test node)]
        (let [src (cond-> (vec (mapcat ips enemies))
                    (and (= "ep" node) (= (set enemies) (set yb-nodes)))
                    (into (k/service-ips (:yb-namespace test))))]
          (k/drop-from! ns (k/pod-name p) src)
          [(k/pod-name p) (sort enemies)])))))

(defn- heal-all!
  [test]
  (doall
    (for [node all-nodes, [ns p] (node-pods test node)]
      (do (k/heal! ns (k/pod-name p)) (k/pod-name p)))))

(defn partition-nemesis
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      ; Attach the NET_ADMIN containers up front so the first partition is not
      ; delayed by image pulls, and clear rules a crashed run left behind.
      (doseq [node all-nodes, [ns p] (node-pods test node)]
        (k/ensure-net-container! ns (k/pod-name p)))
      (heal-all! test)
      this)

    (invoke! [this test op]
      (case (:f op)
        :start-partition (assoc op :value (apply-grudge! test (:value op)))
        :stop-partition  (assoc op :value (heal-all! test))))

    (teardown! [this test] (heal-all! test))))


(defn- controller-url
  [test path]
  (str "http://amp-controller." (:amp-namespace test) ".svc.cluster.local:8080/api/v1" path))

(defn- await-task!
  "Polls a controller task to a terminal status, returning it with its result."
  [test task-id timeout-s]
  (let [deadline (+ (System/currentTimeMillis) (* 1000 timeout-s))]
    (loop []
      (let [t (try (json/read-str (:body (http/get (controller-url test (str "/tasks/" task-id))
                                                    {:socket-timeout 30000 :connection-timeout 5000}))
                                  :key-fn keyword)
                   (catch Exception e {:status "poll-error" :error (.getMessage e)}))]
        (cond (#{"succeeded" "failed"} (:status t)) (select-keys t [:status :result :error])
              (< deadline (System/currentTimeMillis)) {:status "timeout" :last t}
              :else (do (Thread/sleep 3000) (recur)))))))

(def ^:private resize-sizes
  "Alternated by resize-endpoint: a CPU step and a memory step each way."
  (cycle [{:cpu "3" :memory "6Gi"} {:cpu "2" :memory "4Gi"}]))

(defn resize-nemesis
  "Resizes the endpoint through the controller, the path an operator takes,
  which resizes the running pod in place."
  []
  (let [sizes (atom resize-sizes)]
    (reify nemesis/Nemesis
      (setup! [this test] this)
      (invoke! [this test op]
        (let [size (first (swap! sizes next))
              resp (try (http/post (controller-url test (str (:amp-endpoint-path test) "/resize"))
                              ; Bounded: nemesis ops run one at a time, so a call
                              ; blocked on an unavailable backing store would hold
                              ; every other active fault in place with it.
                              {:content-type       :json
                               :body               (json/write-str {:resources size})
                               :socket-timeout     60000
                               :connection-timeout 5000
                               :throw-exceptions   false})
                        (catch java.io.IOException e {:status :timeout :body (.getMessage e)}))
              task (try (json/read-str (:body resp) :key-fn keyword) (catch Exception _ nil))]
          (assoc op :value (if (and (= 202 (:status resp)) (:id task))
                             (assoc (await-task! test (:id task) 180) :size size)
                             {:http (:status resp) :body (:body resp) :size size}))))
      (teardown! [this test]))))

(defn controller-nemesis
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (let [pods (k/pods (:amp-namespace test) "app=amp-controller")]
        (case (:f op)
          :kill-controller  (assoc op :value (mapv #(do (k/delete-pod! (:amp-namespace test) (k/pod-name %) true)
                                                        (k/pod-name %))
                                                   pods))
          :start-controller (assoc op :value
                                   (await-ready! #(k/pods (:amp-namespace test)
                                                          "app=amp-controller")
                                                 (if (:await op) 300 0))))))
    (teardown! [this test])))

(defn full-nemesis
  []
  (nemesis/compose
    {#{:kill-tserver :kill-master :pause-tserver :pause-master
       :resume-tserver :resume-master :start-tserver :start-master
       :kill-safekeeper :kill-pageserver :kill-compute :kill-endpoint
       :delete-endpoint :start-endpoint
       :pause-safekeeper :pause-pageserver :pause-endpoint :resume-endpoint}
     (process-nemesis)

     #{:start-partition :stop-partition} (partition-nemesis)
     #{:kill-controller :start-controller} (controller-nemesis)
     #{:resize-endpoint} (resize-nemesis)}))


(def op yn/op)

(defn- grudge-op
  [kind grudge]
  (op :start-partition grudge :partition-type kind))

(defn- partition-gens
  []
  {:partition-yb-one  (fn [_ _] (grudge-op :yb-one (nemesis/complete-grudge (nemesis/split-one yb-nodes))))
   :partition-yb-ring (fn [_ _] (grudge-op :yb-ring (nemesis/majorities-ring yb-nodes)))
   :partition-ep-yb   (fn [_ _] (grudge-op :ep-yb (nemesis/complete-grudge [["ep"] yb-nodes])))
   :partition-ep-one  (fn [_ _] (let [n (rand-nth yb-nodes)]
                                  (grudge-op :ep-one {"ep" #{n} n #{"ep"}})))
   :partition-any     (fn [_ _] (grudge-op :any (nemesis/complete-grudge
                                                   (nemesis/bisect (shuffle all-nodes)))))})

(def specs
  #{:kill-tserver :kill-master :pause-tserver :pause-master
    :kill-safekeeper :kill-pageserver :kill-compute :kill-endpoint :delete-endpoint
    :pause-safekeeper :pause-pageserver :pause-endpoint
    :kill-controller :resize-endpoint
    :partition-yb-one :partition-yb-ring :partition-ep-yb :partition-ep-one :partition-any
    :yb-kill :yb-pause :ep-kill :ep-pause :yb-partition :ep-partition :all})

(defn expand-options
  [n]
  (let [groups {:yb-kill      [:kill-tserver :kill-master]
                :yb-pause     [:pause-tserver :pause-master]
                :ep-kill      [:kill-safekeeper :kill-pageserver :kill-compute :kill-endpoint :delete-endpoint]
                :ep-pause     [:pause-safekeeper :pause-pageserver :pause-endpoint]
                :yb-partition [:partition-yb-one :partition-yb-ring]
                :ep-partition [:partition-ep-yb :partition-ep-one]}
        groups (assoc groups :all (conj (vec (mapcat val groups)) :partition-any :kill-controller :resize-endpoint))]
    (reduce (fn [n [g faults]]
              (if (g n) (merge n (zipmap faults (repeat true))) n))
            n groups)))

(defn mixed-generator
  [n]
  (let [o (fn [gens recovery]
            (when-let [mix (yn/opt-mix n gens)]
              (yn/flip-flop mix recovery)))]
    (->> [(o {:kill-tserver (op :kill-tserver)} (op :start-tserver))
          (o {:kill-master (op :kill-master)} (op :start-master))
          (o {:pause-tserver (op :pause-tserver)} (op :resume-tserver))
          (o {:pause-master (op :pause-master)} (op :resume-master))
          (o {:kill-safekeeper (op :kill-safekeeper)
              :kill-pageserver (op :kill-pageserver)
              :kill-compute    (op :kill-compute)
              :kill-endpoint   (op :kill-endpoint)
              :delete-endpoint (op :delete-endpoint)}
             (op :start-endpoint))
          (o {:pause-safekeeper (op :pause-safekeeper)
              :pause-pageserver (op :pause-pageserver)
              :pause-endpoint   (op :pause-endpoint)}
             (op :resume-endpoint))
          (o {:kill-controller (op :kill-controller)} (op :start-controller))
          (yn/opt-mix n {:resize-endpoint (op :resize-endpoint)})
          (o (partition-gens) (op :stop-partition))]
         (remove nil?)
         gen/mix
         ((case (:schedule n)
            (nil :random) gen/stagger
            :fixed        gen/delay)
          (:interval n)))))

(defn final-generator
  [n]
  (->> (cond-> []
         (some n [:pause-tserver]) (conj :resume-tserver)
         (some n [:pause-master]) (conj :resume-master)
         (some n [:pause-safekeeper :pause-pageserver :pause-endpoint]) (conj :resume-endpoint)
         (some n (keys (partition-gens))) (conj :stop-partition)
         (some n [:kill-tserver]) (conj :start-tserver)
         (some n [:kill-master]) (conj :start-master)
         (some n [:kill-safekeeper :kill-pageserver :kill-compute :kill-endpoint
                  :delete-endpoint :pause-safekeeper :pause-pageserver :pause-endpoint])
         (conj :start-endpoint)
         (some n [:kill-controller]) (conj :start-controller))
       (map #(op % nil :await true))
       (map gen/once)))

(defn full-generator
  "Alternates two minutes of faults with a heal and a quiet minute, so the
  history also shows whether the stack recovers."
  [n]
  (if (:no-recovery n)
    (mixed-generator n)
    (interleave (repeatedly #(gen/time-limit 120 (mixed-generator n)))
                (repeatedly #(gen/phases (final-generator n) (gen/sleep 60))))))

(defn nemesis
  [opts]
  (let [n (expand-options (:nemesis opts))]
    {:nemesis         (full-nemesis)
     :generator       (when (seq (dissoc n :interval :schedule :no-recovery))
                        (full-generator n))
     :final-generator (final-generator n)}))

(def perf-nemeses
  #{{:name "kill yb" :start #{:kill-tserver :kill-master} :stop #{:start-tserver :start-master} :fill-color "#E9A4A0"}
    {:name "pause yb" :start #{:pause-tserver :pause-master} :stop #{:resume-tserver :resume-master} :fill-color "#A0B1E9"}
    {:name "kill endpoint" :start #{:kill-safekeeper :kill-pageserver :kill-compute :kill-endpoint :delete-endpoint}
     :stop #{:start-endpoint} :fill-color "#E9C3A0"}
    {:name "pause endpoint" :start #{:pause-safekeeper :pause-pageserver :pause-endpoint}
     :stop #{:resume-endpoint} :fill-color "#B8A0E9"}
    {:name "controller" :start #{:kill-controller} :stop #{:start-controller} :fill-color "#D2E9A0"}
    {:name "resize endpoint" :start #{:resize-endpoint} :stop #{} :fill-color "#A0E9D8"}
    {:name "partition" :start #{:start-partition} :stop #{:stop-partition} :fill-color "#888888"}})
