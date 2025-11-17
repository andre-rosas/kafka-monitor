(defproject shared "0.1.0"
  :description "Shared code for kafka-monitor microservices"
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [org.clojure/clojurescript "1.11.60"]]

  :source-paths ["src/cljc"]
  :test-paths ["test/cljc"]

  ;; Configuration for test selectors
  :test-selectors {:default (constantly true)
                   :verbose :verbose
                   :unit (constantly true)}

  ;; Plugins for test coverage and reporting
  :plugins [[lein-cloverage "1.2.4"]
            [lein-ancient "0.7.0"]]

  ;; Test coverage configuration
  :cloverage {:codecov? true
              :html? true
              :text? true
              :fail-threshold 80
              :ns-exclude-regex [#"^user$"]})