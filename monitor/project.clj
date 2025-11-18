
(defproject monitor "0.1.0-SNAPSHOT"
  :description "HTTP API and Dashboard for Kafka Monitor"
  :url "https://github.com/andre-rosas/kafka-monitor"

  :dependencies [[org.clojure/clojure "1.11.3"]
                 [org.clojure/clojurescript "1.11.60"]

                 ;; Web server
                 [ring/ring-core "1.10.0"]
                 [ring/ring-jetty-adapter "1.10.0"]
                 [ring/ring-json "0.5.1"]
                 [ring/ring-defaults "0.3.4"]
                 [ring-cors "0.1.13"]
                 [compojure "1.7.0"]
                 [ring-mock "0.1.5"]

                 ;; Cassandra
                 [com.datastax.oss/java-driver-core "4.17.0"]
                 [com.datastax.oss/java-driver-query-builder "4.17.0"]

                 ;; JSON
                 [cheshire "5.12.0"]

                 ;; Config
                 [aero "1.1.6"]

                 ;; Logging
                 [org.clojure/tools.logging "1.2.4"]
                 [ch.qos.logback/logback-classic "1.2.12"]
                 [org.slf4j/slf4j-api "1.7.36"]

                 ;; ClojureScript + React (via CLJSJS)
                 [cljsjs/react "18.2.0-1"]
                 [cljsjs/react-dom "18.2.0-1"]
                 [reagent "1.2.0"]
                 [re-frame "1.3.0"]
                 [day8.re-frame/http-fx "0.2.4"]
                 [day8.re-frame/test "0.1.5"]
                 [clojure.java-time "1.3.0"]

                 ;; Async & HTTP
                 [org.clojure/core.async "1.6.681"]

                 ;; Shared
                 [shared "0.1.0"]]

  :repositories [["clojars" {:url "https://repo.clojars.org/"}]]

  :plugins [[lein-cljsbuild "1.1.8"]
            [lein-cloverage "1.2.4"]]

  :source-paths ["src/clj"]
  :test-paths ["test/clj"]
  :resource-paths ["resources"]

  :main monitor.server

  :target-path "target/%s"
  :uberjar-name "monitor-standalone.jar"

  :clean-targets ^{:protect false} ["resources/public/js/compiled" "target"]

  :cljsbuild {:builds
              [{:id "min"
                :source-paths ["src/cljs"]
                :compiler {:main monitor.core
                           :output-to "resources/public/js/compiled/app.js"
                           :optimizations :simple
                           :pretty-print false}}
               {:id "test"
                :source-paths ["src/cljs" "test/cljs"]
                :compiler {:main monitor.test-runner
                           :output-to "target/test/test.js"
                           :output-dir "target/test"
                           :target :nodejs
                           :optimizations :none
                           :pretty-print true}}]}

  :doo {:build "test"
        :alias {:default [:node]}}

  :aliases {"cassandra-test" ["test" "monitor.cassandra-test"]
            "config-test" ["test" "monitor.config-test"]
            "server-test" ["test" "monitor.server-test"]
            "test-backend" ["test"]
            "test-frontend" ["cljsbuild" "once" "test"]
            "test-cljs" ["do"
                         ["cljsbuild" "once" "test"]
                         ["shell" "node" "target/test/test.js"]]
            "test-all" ["do" ["test"] ["cljsbuild" "once" "test"]]}

  :test-selectors {:default (constantly true)
                   :cassandra #(= (:ns %) 'monitor.cassandra-test)
                   :config #(= (:ns %) 'monitor.config-test)
                   :server #(= (:ns %) 'monitor.server-test)}

  :cloverage {:output "target/coverage"
              :codecov? true
              :html? true
              :emma-xml? true
              :lcov? true
              :text? true
              :summary? true
              :fail-threshold 80} ;; Fail if below 80%

  :profiles {:uberjar {:aot :all
                       :prep-tasks ["compile" ["cljsbuild" "once" "min"]]}})