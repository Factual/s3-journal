(defproject factual/s3-journal "0.1.2"
  :description "Reliable, high-throughput journalling to S3"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.7.0-alpha2"]]}}
  :dependencies [[com.amazonaws/aws-java-sdk-core "1.9.28.1" :exclusions [commons-codec]]
                 [com.amazonaws/aws-java-sdk-s3 "1.9.28.1" :exclusions [commons-codec]]
                 [factual/durable-queue "0.1.4"]
                 [manifold "0.1.0-beta12"]
                 [org.clojure/tools.logging "0.3.1"]
                 [byte-streams "0.2.0-alpha9"]
                 [byte-transforms "0.1.4-alpha1"]]
  :jvm-opts ^:replace ["-server" "-Xmx8g"]
  :global-vars {*warn-on-reflection* true})
