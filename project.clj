(defproject kafka-monitor "0.1.0-SNAPSHOT"
  :description "Kafka Monitor - Event-Driven Microservices"
  :url "https://github.com/your-user/kafka-monitor"

  :plugins [[lein-sub "0.3.0"]
            [lein-cljfmt "0.9.2"]
            [lein-kibit "0.1.8"]
            [lein-bikeshed "0.5.2"]
            [jonase/eastwood "1.4.2"]
            [lein-cloverage "1.2.4"]]

  ;; Define the sub-projects (microservices)
  :sub ["shared"
        "order-processor"
        "query-processor"
        "registry-processor"
        "monitor"]

  :aliases {;; Test - runs tests in all sub-projects
            "test" ["sub" "test"]
            "test-all" ["sub" "do" "clean," "test"]
            "coverage" ["sub" "cloverage"]

            ;; Lint
            "lint" ["sub" "do" "cljfmt" "check," "kibit"]
            "lint-fix" ["sub" "cljfmt" "fix"]

            ;; Build
            "build" ["sub" "do" "clean," "uberjar"]

            ;; Run (must use 'sub')
            "run-order" ["sub" "order-processor" "run"]
            "run-query" ["sub" "query-processor" "run"]
            "run-registry" ["sub" "registry-processor" "run"]
            "run-monitor" ["sub" "monitor" "run"]

            ;; Dev helpers
            "check-all" ["do" ["lint"] ["test"]]}

  :cljfmt {:indents {defroute [[:block 2]]
                     defmethod [[:block 2]]
                     defrecord [[:block 1]]}})
