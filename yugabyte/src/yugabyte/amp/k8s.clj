(ns yugabyte.amp.k8s
  "kubectl helpers for driving an AMP stack from inside the cluster. The
  jepsen process runs in a pod whose ServiceAccount may exec into, patch and
  delete pods in the AMP and YugabyteDB namespaces; there is no SSH anywhere."
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.tools.logging :refer [info warn]]))

(def net-image
  "Image for the NET_ADMIN ephemeral container that owns a pod's iptables."
  "nicolaka/netshoot:v0.13")

(def ^:private net-spec
  "An ephemeral container inherits the pod's non-root user, and iptables needs
  root as well as NET_ADMIN."
  (delay
    (let [f (java.io.File/createTempFile "jepsen-net" ".json")]
      (spit f (json/write-str {:securityContext {:runAsUser    0
                                                 :runAsGroup   0
                                                 :runAsNonRoot false
                                                 :capabilities {:add ["NET_ADMIN" "NET_RAW"]}}}))
      (.getPath f))))

(defn kubectl
  "Runs kubectl with args, returning trimmed stdout. Throws on a non-zero exit
  unless :allow-fail? is set, in which case it returns nil."
  [& args]
  (let [[opts args] (if (map? (first args)) [(first args) (rest args)] [{} args])
        args        (map str args)
        {:keys [exit out err]} (apply sh/sh "kubectl" args)]
    (cond
      (zero? exit) (str/trim out)
      (:allow-fail? opts) (do (warn "kubectl" (str/join " " args) "exited" exit (str/trim err)) nil)
      :else (throw (ex-info (str "kubectl failed: " (str/trim err))
                            {:args args :exit exit :err err})))))

(defn get-json
  [& args]
  (json/read-str (apply kubectl (concat args ["-o" "json"])) :key-fn keyword))

(defn pods
  "Pods in ns matching a label selector, as parsed JSON items."
  [ns selector]
  (:items (get-json "-n" ns "get" "pods" "-l" selector)))

(defn pod-name [p] (get-in p [:metadata :name]))
(defn pod-ip [p] (get-in p [:status :podIP]))

(defn running?
  [p]
  (and (nil? (get-in p [:metadata :deletionTimestamp]))
       (= "Running" (get-in p [:status :phase]))))

(defn service-ips
  "ClusterIPs of every Service in ns."
  [ns]
  (->> (:items (get-json "-n" ns "get" "svc"))
       (map #(get-in % [:spec :clusterIP]))
       (remove #(or (nil? %) (= "None" %)))))

(defn exec!
  "kubectl exec into container of pod, running sh -c cmd."
  [opts ns pod container cmd]
  (kubectl opts "-n" ns "exec" pod "-c" container "--" "sh" "-c" cmd))

(defn signal!
  "Sends signal (e.g. \"KILL\", \"STOP\", \"CONT\") to every process in the
  container whose command line matches pattern. Missing processes are not an
  error: the fault may already have taken them down."
  [ns pod container sig pattern]
  ; [x]yz matches xyz but not the sh -c command line that carries it.
  (exec! {:allow-fail? true} ns pod container
         (str "pkill -" sig " -f '[" (subs pattern 0 1) "]" (subs pattern 1) "' || true")))

(defn delete-pod!
  [ns pod force?]
  (apply kubectl {:allow-fail? true} "-n" ns "delete" "pod" pod "--wait=false"
         (when force? ["--grace-period=0" "--force"])))

(defn net-container
  "Name of a running NET_ADMIN ephemeral container in pod, or nil."
  [ns pod]
  (let [p (get-json "-n" ns "get" "pod" pod)]
    (->> (get-in p [:status :ephemeralContainerStatuses])
         (filter #(and (str/starts-with? (:name %) "jepsen-net")
                       (get-in % [:state :running])))
         first
         :name)))

(defn ensure-net-container!
  "Attaches (once) a sleeping NET_ADMIN ephemeral container to pod, sharing
  the pod's network namespace, and returns its name. Ephemeral containers
  cannot be removed, so a pod gets a new one only when the old one is gone."
  [ns pod]
  (or (net-container ns pod)
      (let [n (str "jepsen-net-" (mod (System/currentTimeMillis) 100000))]
        (info "Attaching" n "to" pod)
        (kubectl "-n" ns "debug" pod "--profile=netadmin" "--custom" @net-spec "--image" net-image
                 "-c" n "--" "sleep" "infinity")
        (loop [i 0]
          (cond
            (net-container ns pod) n
            (< 90 i) (throw (ex-info "net container did not start" {:pod pod}))
            :else (do (Thread/sleep 1000) (recur (inc i))))))))

(defn iptables!
  "Runs a shell snippet of iptables commands in pod's network namespace."
  [ns pod cmd]
  (let [c (ensure-net-container! ns pod)]
    (exec! {} ns pod c cmd)))

(defn drop-from!
  "Drops every packet arriving at pod from the given source IPs, in a JEPSEN
  chain hooked into INPUT so heal! can flush exactly our rules."
  [ns pod ips]
  (when (seq ips)
    (iptables! ns pod
               (str "iptables -N JEPSEN 2>/dev/null; "
                    "iptables -C INPUT -j JEPSEN 2>/dev/null || iptables -I INPUT -j JEPSEN; "
                    (str/join "; " (map #(str "iptables -A JEPSEN -s " % " -j DROP") ips))))))

(defn heal!
  [ns pod]
  (when (net-container ns pod)
    (exec! {:allow-fail? true} ns pod (net-container ns pod)
           "iptables -F JEPSEN 2>/dev/null || true")))

(defn logs!
  "Writes the logs of every container of pod (current and previous) under
  dir. Best effort."
  [ns pod dir]
  (let [p     (get-json "-n" ns "get" "pod" pod)
        names (map :name (get-in p [:spec :containers]))]
    (doseq [c names, prev [false true]]
      (let [out (apply kubectl {:allow-fail? true} "-n" ns "logs" pod "-c" c
                       (when prev ["--previous"]))]
        (when (seq out)
          (spit (str dir "/" pod "." c (when prev ".previous") ".log") out))))))
