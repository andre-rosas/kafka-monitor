(defproject query-processor "0.1.0-SNAPSHOT"
  :description "Kafka consumer que materializa views de consulta dos pedidos"
  :url "https://github.com/andre-rosas/kafka-monitor"

  :dependencies [[org.clojure/clojure "1.11.3"]

                 ;; Kafka Consumer
                 [org.apache.kafka/kafka-clients "3.6.0"]

                 ;; Cassandra (state store)
                 [com.datastax.oss/java-driver-core "4.17.0"]
                 [com.datastax.oss/java-driver-query-builder "4.17.0"]

                 ;; JSON/EDN
                 [cheshire "5.12.0"]
                 [org.clojure/data.json "2.4.0"]

                 ;; Config
                 [aero "1.1.6"]

                 ;; Logging
                 [org.clojure/tools.logging "1.2.4"]
                 [ch.qos.logback/logback-classic "1.2.11"]
                 [org.slf4j/slf4j-api "1.7.36"]

                 ;; Spec
                 [org.clojure/spec.alpha "0.3.218"]

                 ;; local dependence: shared
                 [shared "0.1.0"]]

  :plugins [[lein-cljfmt "0.9.2"]
            [lein-cloverage "1.2.4"]
            [lein-kibit "0.1.8"]
            [lein-bikeshed "0.5.2"]
            [jonase/eastwood "1.4.2"]
            [lein-ancient "0.7.0"]]

  ;; Dirs
  :source-paths ["src/clj"]
  :test-paths ["test/clj"]
  :resource-paths ["resources"]

  ;; Entry point
  :main ^:skip-aot query-processor.core

  ;; Build
  :target-path "target/%s"
  :uberjar-name "query-processor-standalone.jar"

  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}

             :dev {:dependencies [[org.clojure/test.check "1.1.1"]
                                  ;; Kafka embedded for tests
                                  [org.apache.kafka/kafka-streams-test-utils "3.6.0"]
                                  ;; Mock Cassandra
                                  [org.testcontainers/testcontainers "1.19.1"]
                                  [org.testcontainers/cassandra "1.19.1"]]
                   :jvm-opts ["-Xmx2g"]
                   :resource-paths ["test/resources"]}

             :test {:jvm-opts ["-Xmx2g"
                               "-Dlogback.configurationFile=logback-test.xml"]}}

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

  ;; Linting
  :cljfmt {:indents {defroute [[:block 2]]
                     defmethod [[:block 2]]
                     defrecord [[:block 1]]}})