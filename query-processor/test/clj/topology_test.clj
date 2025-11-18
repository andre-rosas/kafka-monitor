(ns topology-test
  (:require [clojure.test :refer [deftest testing is]]
            [query-processor.aggregator :as agg]
            [cheshire.core :as json]))

;; Test orders
(def order-1 {:order-id "ORDER-001" :customer-id 42 :product-id "PROD-A"
              :quantity 5 :unit-price 30.0 :total 150.0 :timestamp 1000 :status "confirmed"})
(def order-2 {:order-id "ORDER-002" :customer-id 42 :product-id "PROD-B"
              :quantity 3 :unit-price 50.0 :total 150.0 :timestamp 2000 :status "shipped"})
(def order-3 {:order-id "ORDER-003" :customer-id 99 :product-id "PROD-A"
              :quantity 10 :unit-price 30.0 :total 300.0 :timestamp 3000 :status "delivered"})

(deftest test-aggregate-single-order
  (testing "Aggregate single order updates all views"
    (let [views (agg/init-views "test")
          config {:timeline-max-size 100}
          result (agg/aggregate-order views order-1 config)]

      (is (= 1 (count (:customer-stats result))))
      (is (= 1 (count (:product-stats result))))
      (is (= 1 (count (:timeline result))))
      (is (= 1 (get-in result [:processing-stats :processed-count]))))))

(deftest test-aggregate-multiple-orders
  (testing "Aggregate multiple orders correctly"
    (let [views (agg/init-views "test")
          config {:timeline-max-size 100}
          result (-> views
                     (agg/aggregate-order order-1 config)
                     (agg/aggregate-order order-2 config)
                     (agg/aggregate-order order-3 config))]

      (is (= 2 (count (:customer-stats result))))
      (is (= 2 (count (:product-stats result))))
      (is (= 3 (count (:timeline result)))))))

(deftest test-customer-aggregation
  (testing "Customer stats aggregate correctly"
    (let [views (agg/init-views "test")
          config {:timeline-max-size 100}
          result (-> views
                     (agg/aggregate-order order-1 config)
                     (agg/aggregate-order order-2 config))
          customer (get-in result [:customer-stats 42])]

      (is (= 2 (:total-orders customer)))
      (is (= 300.0 (:total-spent customer)))
      (is (= "ORDER-002" (:last-order-id customer))))))

(deftest test-product-aggregation
  (testing "Product stats aggregate correctly"
    (let [views (agg/init-views "test")
          config {:timeline-max-size 100}
          result (-> views
                     (agg/aggregate-order order-1 config)
                     (agg/aggregate-order order-3 config))
          product (get-in result [:product-stats "PROD-A"])]

      (is (= 15 (:total-quantity product)))
      (is (= 450.0 (:total-revenue product)))
      (is (= 2 (:order-count product)))
      (is (= 7.5 (:avg-quantity product))))))

(deftest test-timeline-ordering
  (testing "Timeline maintains order (newest first)"
    (let [views (agg/init-views "test")
          config {:timeline-max-size 100}
          result (-> views
                     (agg/aggregate-order order-1 config)
                     (agg/aggregate-order order-2 config)
                     (agg/aggregate-order order-3 config))
          timeline (:timeline result)]

      (is (= "ORDER-003" (:order-id (first timeline))))
      (is (= "ORDER-001" (:order-id (last timeline)))))))

(deftest test-idempotency
  (testing "Duplicate orders are handled idempotently"
    (let [views (agg/init-views "test")
          config {:timeline-max-size 100}
          result (-> views
                     (agg/aggregate-order order-1 config)
                     (agg/aggregate-order order-1 config))
          customer (get-in result [:customer-stats 42])]

      (is (= 1 (:total-orders customer)))
      (is (= 150.0 (:total-spent customer))))))

(deftest test-timeline-max-size
  (testing "Timeline respects max size"
    (let [views (agg/init-views "test")
          config {:timeline-max-size 2}
          result (-> views
                     (agg/aggregate-order order-1 config)
                     (agg/aggregate-order order-2 config)
                     (agg/aggregate-order order-3 config))
          timeline (:timeline result)]

      (is (= 2 (count timeline)))
      (is (= "ORDER-003" (:order-id (first timeline)))))))