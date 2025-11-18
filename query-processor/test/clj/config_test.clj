(ns config-test
  "Tests for configuration loading and access functions."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [query-processor.config :as config]
            [aero.core :as aero]))

;; Mock Configuration Map for testing
(def test-config-map
  {:kafka {:bootstrap-servers "test-kafka:9092"
           :group-id "qp-test-group"
           :topics ["orders"]
           :poll-timeout-ms 500
           :max-poll-records 100}
   :cassandra {:host "test-cassandra"
               :port 9042
               :datacenter "dc1"
               :keyspace "qp_store_test"}
   :processor {:processor-id "qp-test-1"
               :timeline-max-size 50
               :batch-size 50
               :commit-interval-ms 1000}})

(use-fixtures :once
  (fn [f]
    ;; Redefine config/load-config to return the mock map
    ;; and force the configuration atom to be initialized with this mock data.
    (with-redefs [config/load-config (constantly test-config-map)
                  config/config (delay (config/load-config :test))]
      (f))))

(deftest test-kafka-config-access
  (testing "Kafka configuration is loaded correctly"
    (let [cfg (config/kafka-config)]
      (is (= "test-kafka:9092" (:bootstrap-servers cfg)))
      (is (= "qp-test-group" (:group-id cfg)))
      (is (contains? (set (:topics cfg)) "orders")))))

(deftest test-cassandra-config-access
  (testing "Cassandra configuration is loaded correctly"
    (let [cfg (config/cassandra-config)]
      (is (= "test-cassandra" (:host cfg)))
      (is (= 9042 (:port cfg)))
      (is (= "qp_store_test" (:keyspace cfg))))))

(deftest test-processor-config-access
  (testing "Processor configuration is loaded correctly"
    (let [cfg (config/processor-config)]
      (is (= "qp-test-1" (:processor-id cfg)))
      (is (= 50 (:timeline-max-size cfg)))
      (is (= 1000 (:commit-interval-ms cfg))))))

(deftest test-get-processor-id
  (testing "get-processor-id returns the correct ID"
    (is (= "qp-test-1" (config/get-processor-id)))))