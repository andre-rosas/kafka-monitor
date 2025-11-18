(defproject registry-processor "0.1.0-SNAPSHOT"
  :description "Kafka consumer/producer that validates orders and registers approved ones"
  :url "https://github.com/andre-rosas/kafka-monitor"

  :dependencies [[org.clojure/clojure "1.11.3"]

                 ;; Kafka Producer
                 [org.apache.kafka/kafka-clients "3.6.0"]

                 ;; Cassandra (state store)
                 [com.datastax.oss/java-driver-core "4.17.0"]
                 [com.datastax.oss/java-driver-query-builder "4.17.0"]

                 ;; JSON
                 [cheshire "5.12.0"]

                 ;; Config
                 [aero "1.1.6"]

                 ;; Logging
                 [org.clojure/tools.logging "1.2.4"]
                 [ch.qos.logback/logback-classic "1.2.11"]
                 [org.slf4j/slf4j-api "1.7.36"]

                 ;; Timbre  
                 [com.taoensso/timbre "6.3.0"]

                 ;; Spec
                 [org.clojure/spec.alpha "0.3.218"]

                 ;; Shared
                 [shared "0.1.0"]]

  :plugins [[lein-cljfmt "0.9.2"]
            [lein-cloverage "1.2.4"]
            [lein-kibit "0.1.8"]
            [lein-bikeshed "0.5.2"]
            [jonase/eastwood "1.4.2"]
            [lein-ancient "0.7.0"]]

  :source-paths ["src/clj"]
  :test-paths ["test/clj"]
  :resource-paths ["resources" "test/resources"]

  :main ^:skip-aot registry-processor.core

  :target-path "target/%s"
  :uberjar-name "registry-processor-standalone.jar"

  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[org.clojure/test.check "1.1.1"]
                                  [org.apache.kafka/kafka-streams-test-utils "3.6.0"]
                                  [org.testcontainers/testcontainers "1.19.1"]
                                  [org.testcontainers/cassandra "1.19.1"]
                                  [org.testcontainers/kafka "1.19.1"]]
                   :jvm-opts ["-Xmx2g"]}
             :test {:jvm-opts ["-Xmx2g"]}}

  :aliases {"test-all" ["do" ["clean"] ["test"]]
            "uberjar-build" ["do" ["clean"] ["uberjar"]]}

  :cloverage {:output "target/coverage"
              :codecov? true
              :html? true
              :emma-xml? true
              :lcov? true
              :text? true
              :summary? true
              :fail-threshold 80} ;; Fail if below 80%

  :cljfmt {:indents {defroute [[:block 2]]
                     defmethod [[:block 2]]
                     defrecord [[:block 1]]}})