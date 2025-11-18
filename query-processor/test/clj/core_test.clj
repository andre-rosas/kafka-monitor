(ns core-test
  (:require [clojure.test :refer [deftest testing is]]
            [query-processor.core :as core]
            [query-processor.aggregator :as agg])
  (:import [java.util Properties]))

(deftest test-map-to-properties
  (testing "Map converts to Properties"
    (let [m {:key1 "val1" :key2 42}
          props (core/map->properties m)]
      (is (instance? Properties props))
      (is (= "val1" (.get props "key1")))
      (is (= "42" (.get props "key2"))))))

(deftest test-retry-success-first-try
  (testing "Retry succeeds immediately"
    (let [counter (atom 0)
          f (fn [] (swap! counter inc) "ok")]
      (is (= "ok" (core/retry-with-backoff f 3 10 100)))
      (is (= 1 @counter)))))

(deftest test-retry-success-after-failures
  (testing "Retry succeeds after some failures"
    (let [counter (atom 0)
          f (fn []
              (swap! counter inc)
              (if (< @counter 3)
                (throw (Exception. "fail"))
                "ok"))]
      (is (= "ok" (core/retry-with-backoff f 5 10 100)))
      (is (= 3 @counter)))))

(deftest test-retry-exhausts
  (testing "Retry throws after max attempts"
    (let [counter (atom 0)
          f (fn [] (swap! counter inc) (throw (Exception. "fail")))]
      (is (thrown? Exception (core/retry-with-backoff f 3 10 100)))
      (is (= 3 @counter)))))

(deftest test-record-to-map
  (testing "Record converts to map"
    (let [record (org.apache.kafka.clients.consumer.ConsumerRecord.
                  "topic" 0 100 "key" "value")
          m (core/record->map record)]
      (is (= "topic" (:topic m)))
      (is (= 0 (:partition m)))
      (is (= 100 (:offset m)))
      (is (= "value" (:value m))))))

(deftest test-process-batch-map
  (testing "Batch processing aggregates results"
    (let [context {:views-atom (atom (agg/init-views "test"))
                   :processor-config {:timeline-max-size 100}}
          records [{:value "{\"order-id\":\"O1\",\"customer-id\":1,\"product-id\":\"P1\",\"quantity\":1,\"unit-price\":10.0,\"total\":10.0,\"timestamp\":1000,\"status\":\"pending\"}" :topic "orders" :partition 0 :offset 0}
                   {:value "bad-json" :topic "orders" :partition 0 :offset 1}]
          result (core/process-batch-map context records)]

      (is (= 2 (:total result)))
      (is (= 1 (:success result)))
      (is (= 1 (:errors result))))))
