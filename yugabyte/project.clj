(defproject yugabyte "0.1.2-SNAPSHOT"
  :description "Jepsen testing for YugaByteDB"
  :url "http://yugabyte.com/"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [clj-http "3.12.3" :exclusions [commons-logging]]
                 [jepsen "0.3.11"]
                 [com.yugabyte/cassandra-driver-core "3.10.3-yb-3"]
                 [org.slf4j/slf4j-api "2.0.17"]
                 [org.clojure/java.jdbc "0.7.12"]
                 [org.clojure/data.json "2.4.0"]
                 [com.yugabyte/jdbc-yugabytedb "42.3.5-yb-3"]
                 [version-clj "2.0.2"]
                 [clj-wallhack "1.0.1"]]
  :main yugabyte.runner
  :jvm-opts ["-Djava.awt.headless=true" "-Xms4g" "-Xmx10g"])
;  :aot [yugabyte.runner
;        clojure.tools.logging.impl])
