(ns topology-test
  "Tests for order processing topology and aggregation logic."
  (:require [clojure.test :refer :all]
            [query-processor.aggregator :as agg]
            [query-processor.model :as model]))

;; =============================================================================
;; Test Data
;; =============================================================================

(def order-accepted
  {:order-id "ORDER-001"
   :customer-id 100
   :product-id "PROD-X"
   :quantity 2
   :unit-price 50.0
   :total 100.0
   :timestamp 5000
   :status "accepted"})

(def order-pending
  {:order-id "ORDER-002"
   :customer-id 100
   :product-id "PROD-Y"
   :quantity 1
   :unit-price 75.0
   :total 75.0
   :timestamp 6000
   :status "pending"})

(def order-denied
  {:order-id "ORDER-003"
   :customer-id 200
   :product-id "PROD-X"
   :quantity 5
   :unit-price 50.0
   :total 250.0
   :timestamp 7000
   :status "denied"})

;; =============================================================================
;; Test Fixture to Mock add-revenue
;; =============================================================================

(use-fixtures :each
  (fn [f]
    (with-redefs [agg/add-revenue (fn [stats _order] stats)]
      (f))))

;; =============================================================================
;; Aggregation Tests
;; =============================================================================

(deftest test-aggregate-single-order
  (testing "Aggregate single order creates correct views"
    (let [views (agg/init-views "test-processor")
          config {:timeline-max-size 50}
          result (agg/aggregate-order views order-accepted config)]

      ;; Verify customer stats
      (is (= 1 (count (:customer-stats result))))
      (let [customer (get-in result [:customer-stats 100])]
        (is (= 100 (:customer-id customer)))
        (is (= 1 (:total-orders customer)))
        (is (= 100.0 (:total-spent customer))))

      ;; Verify product stats
      (is (= 1 (count (:product-stats result))))
      (let [product (get-in result [:product-stats "PROD-X"])]
        (is (= "PROD-X" (:product-id product)))
        (is (= 2 (:total-quantity product)))
        (is (= 100.0 (:total-revenue product))))

      ;; Verify timeline
      (is (= 1 (count (:timeline result))))

      ;; Verify processing stats
      (is (= 1 (get-in result [:processing-stats :processed-count]))))))

(deftest test-aggregate-multiple-orders
  (testing "Aggregate multiple orders from different customers"
    (let [views (agg/init-views "test-processor")
          config {:timeline-max-size 50}
          result (-> views
                     (agg/aggregate-order order-accepted config)
                     (agg/aggregate-order order-pending config)
                     (agg/aggregate-order order-denied config))]

      ;; Two different customers
      (is (= 2 (count (:customer-stats result))))

      ;; Two different products
      (is (= 2 (count (:product-stats result))))

      ;; Timeline contains all orders
      (is (= 3 (count (:timeline result))))

      ;; Three orders processed
      (is (= 3 (get-in result [:processing-stats :processed-count]))))))

(deftest test-customer-aggregation
  (testing "Customer aggregation accumulates correctly"
    (let [views (agg/init-views "test-processor")
          config {:timeline-max-size 50}
          result (-> views
                     (agg/aggregate-order order-accepted config)
                     (agg/aggregate-order order-pending config))
          customer (get-in result [:customer-stats 100])]

      (is (= 2 (:total-orders customer)))
      (is (= 175.0 (:total-spent customer)))
      (is (= "ORDER-002" (:last-order-id customer)))
      (is (= 6000 (:last-order-timestamp customer)))
      (is (= 5000 (:first-order-timestamp customer))))))

(deftest test-product-aggregation
  (testing "Product aggregation accumulates correctly"
    (let [views (agg/init-views "test-processor")
          config {:timeline-max-size 50}
          result (-> views
                     (agg/aggregate-order order-accepted config)
                     (agg/aggregate-order order-denied config))
          product (get-in result [:product-stats "PROD-X"])]

      (is (= 7 (:total-quantity product)))
      (is (= 350.0 (:total-revenue product)))
      (is (= 2 (:order-count product)))
      (is (= 3.5 (:avg-quantity product))))))

;; =============================================================================
;; Idempotency Tests
;; =============================================================================

(deftest test-idempotency
  (testing "Processing same order twice doesn't duplicate"
    (let [views (agg/init-views "test-processor")
          config {:timeline-max-size 50}
          result-once (agg/aggregate-order views order-accepted config)
          result-twice (agg/aggregate-order result-once order-accepted config)

          ;; Customer stats should not change
          customer-once (get-in result-once [:customer-stats 100])
          customer-twice (get-in result-twice [:customer-stats 100])]

      (is (= (:total-orders customer-once) (:total-orders customer-twice)))
      (is (= (:total-spent customer-once) (:total-spent customer-twice)))

      ;; Timeline should not duplicate
      (is (= 1 (count (:timeline result-once))))
      (is (= 1 (count (:timeline result-twice))))

      ;; Processing count should still increment (we attempted to process)
      (is (= 1 (get-in result-once [:processing-stats :processed-count])))
      (is (= 2 (get-in result-twice [:processing-stats :processed-count]))))))

;; =============================================================================
;; Timeline Tests
;; =============================================================================

(deftest test-timeline-ordering
  (testing "Timeline maintains correct ordering (most recent first)"
    (let [views (agg/init-views "test-processor")
          config {:timeline-max-size 50}
          result (-> views
                     (agg/aggregate-order order-accepted config)
                     (agg/aggregate-order order-pending config)
                     (agg/aggregate-order order-denied config))
          timeline (:timeline result)]
      (is (= 3 (count timeline)))
      (is (= "ORDER-003" (:order-id (nth timeline 0))))
      (is (= "ORDER-002" (:order-id (nth timeline 1))))
      (is (= "ORDER-001" (:order-id (nth timeline 2)))))))

(deftest test-timeline-max-size
  (testing "Timeline respects maximum size limit"
    (let [views (agg/init-views "test-processor")
          config {:timeline-max-size 2}
          orders [(assoc order-accepted :order-id "O1" :timestamp 1000)
                  (assoc order-pending :order-id "O2" :timestamp 2000)
                  (assoc order-denied :order-id "O3" :timestamp 3000)]
          result (reduce #(agg/aggregate-order %1 %2 config) views orders)]

      (is (= 2 (count (:timeline result))))
      (is (= "O3" (:order-id (nth (:timeline result) 0))))
      (is (= "O2" (:order-id (nth (:timeline result) 1)))))))