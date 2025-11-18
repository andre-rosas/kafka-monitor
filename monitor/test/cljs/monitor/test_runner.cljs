(ns monitor.test-runner
  (:require [cljs.test :refer-macros [run-tests]]
            [monitor.subs-test]
            [monitor.events-test]
            [views.products-test]))

(enable-console-print!)

(defn ^:export run []
  (println "\n=== Running ClojureScript Tests ===\n")
  (run-tests
   'monitor.subs-test
   'monitor.events-test
   'views.products-test))
