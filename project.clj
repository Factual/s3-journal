(defproject factual/s3-journal "0.1.0-SNAPSHOT"
  :description "Reliable, high-throughput journalling to S3"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.5.1"]]}}
  :dependencies [[com.amazonaws/aws-java-sdk "1.6.10" :exclusions [commons-codec]]
                 [factual/durable-queue "0.1.1-SNAPSHOT"]
                 [org.clojure/tools.logging "0.2.6"]
                 [byte-transforms "0.1.2"]]
  :jvm-opts ^:replace ["-server" "-Xmx8g"]
  :global-vars {*warn-on-reflection* true})
