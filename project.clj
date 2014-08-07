(defproject factual/s3-journal "0.1.0"
  :description "Reliable, high-throughput journalling to S3"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.5.1"]]}}
  :dependencies [[com.amazonaws/aws-java-sdk "1.8.7" :exclusions [commons-codec]]
                 [factual/durable-queue "0.1.2"]
                 [org.clojure/tools.logging "0.3.0"]
                 [byte-transforms "0.1.3"]]
  :jvm-opts ^:replace ["-server" "-Xmx8g"]
  :global-vars {*warn-on-reflection* true})
