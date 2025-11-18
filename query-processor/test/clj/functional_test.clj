(ns functional-test
  "Tests for pure functions (aggregator logic without I/O)."
  (:require [clojure.test :refer :all]
            [query-processor.aggregator :as agg]
            [query-processor.model :as model]))

;; =============================================================================
;; Test Data
;; =============================================================================

(def order-1
  {:order-id "ORDER-001"
   :customer-id 42
   :product-id "PROD-A"
   :quantity 5
   :unit-price 30.0
   :total 150.0
   :timestamp 1000
   :status "accepted"})

(def order-2
  {:order-id "ORDER-002"
   :customer-id 42
   :product-id "PROD-B"
   :quantity 3
   :unit-price 50.0
   :total 150.0
   :timestamp 2000
   :status "pending"})

(def order-3
  {:order-id "ORDER-003"
   :customer-id 99
   :product-id "PROD-A"
   :quantity 10
   :unit-price 30.0
   :total 300.0
   :timestamp 3000
   :status "denied"})

;; =============================================================================
;; Customer Stats Aggregation Tests
;; =============================================================================

(deftest test-update-customer-stats-first-order
  (testing "Update customer stats with first order"
    (let [result (agg/update-customer-stats nil order-1)]
      (is (= 42 (:customer-id result)))
      (is (= 1 (:total-orders result)))
      (is (= 150.0 (:total-spent result)))
      (is (= "ORDER-001" (:last-order-id result)))
      (is (= 1000 (:last-order-timestamp result)))
      (is (= 1000 (:first-order-timestamp result))))))

(deftest test-update-customer-stats-second-order
  (testing "Update customer stats with second order"
    (let [initial (agg/update-customer-stats nil order-1)
          result (agg/update-customer-stats initial order-2)]
      (is (= 2 (:total-orders result)))
      (is (= 300.0 (:total-spent result)))
      (is (= "ORDER-002" (:last-order-id result)))
      (is (= 2000 (:last-order-timestamp result)))
      (is (= 1000 (:first-order-timestamp result))))))

(deftest test-should-update-customer-nil-stats
  (testing "Should update when stats is nil"
    (is (true? (agg/should-update-customer? nil order-1)))))

(deftest test-should-update-customer-duplicate
  (testing "Should NOT update when duplicate order-id"
    (let [stats {:last-order-id "ORDER-001"}]
      (is (false? (agg/should-update-customer? stats order-1))))))

(deftest test-should-update-customer-new-order
  (testing "Should update when new order-id"
    (let [stats {:last-order-id "ORDER-001"}]
      (is (true? (agg/should-update-customer? stats order-2))))))

;; =============================================================================
;; Product Stats Aggregation Tests
;; =============================================================================

(deftest test-calculate-avg-quantity-with-orders
  (testing "Calculate average quantity"
    (is (= 5.0 (agg/calculate-avg-quantity 50 10)))
    (is (= 10.0 (agg/calculate-avg-quantity 100 10)))
    (is (= 2.5 (agg/calculate-avg-quantity 25 10)))))

(deftest test-calculate-avg-quantity-zero-orders
  (testing "Calculate average with zero orders"
    (is (= 0.0 (agg/calculate-avg-quantity 0 0)))))

(deftest test-update-product-stats-first-order
  (testing "Update product stats with first order"
    (let [result (agg/update-product-stats nil order-1)]
      (is (= "PROD-A" (:product-id result)))
      (is (= 5 (:total-quantity result)))
      (is (= 150.0 (:total-revenue result)))
      (is (= 1 (:order-count result)))
      (is (= 5.0 (:avg-quantity result)))
      (is (= 1000 (:last-order-timestamp result))))))

(deftest test-update-product-stats-multiple-orders
  (testing "Update product stats with multiple orders"
    (let [initial (agg/update-product-stats nil order-1)
          result (agg/update-product-stats initial order-3)]
      (is (= 15 (:total-quantity result)))
      (is (= 450.0 (:total-revenue result)))
      (is (= 2 (:order-count result)))
      (is (= 7.5 (:avg-quantity result)))
      (is (= 3000 (:last-order-timestamp result))))))

;; =============================================================================
;; Timeline Tests
;; =============================================================================

(deftest test-add-to-timeline-empty
  (testing "Add order to empty timeline"
    (let [result (agg/add-to-timeline nil order-1 100)]
      (is (= 1 (count result)))
      (is (= "ORDER-001" (:order-id (first result)))))))

(deftest test-add-to-timeline-maintains-order
  (testing "Timeline maintains most recent first"
    (let [timeline (-> nil
                       (agg/add-to-timeline order-1 100)
                       (agg/add-to-timeline order-2 100)
                       (agg/add-to-timeline order-3 100))]
      (is (= 3 (count timeline)))
      (is (= "ORDER-003" (:order-id (first timeline))))
      (is (= "ORDER-002" (:order-id (second timeline))))
      (is (= "ORDER-001" (:order-id (nth timeline 2)))))))

(deftest test-add-to-timeline-respects-max-size
  (testing "Timeline respects max size"
    (let [orders (for [i (range 150)]
                   (assoc order-1 :order-id (str "ORDER-" i)
                          :timestamp (+ 1000 i)))
          timeline (reduce #(agg/add-to-timeline %1 %2 100) nil orders)]
      (is (= 100 (count timeline)))
      (is (= "ORDER-149" (:order-id (first timeline)))))))

(deftest test-timeline-contains-order-true
  (testing "Timeline contains order returns true"
    (let [timeline [{:order-id "ORDER-001"} {:order-id "ORDER-002"}]]
      (is (true? (agg/timeline-contains-order? timeline "ORDER-001")))
      (is (true? (agg/timeline-contains-order? timeline "ORDER-002"))))))

(deftest test-timeline-contains-order-false
  (testing "Timeline contains order returns false"
    (let [timeline [{:order-id "ORDER-001"} {:order-id "ORDER-002"}]]
      (is (false? (agg/timeline-contains-order? timeline "ORDER-999"))))))

(deftest test-should-add-to-timeline-empty
  (testing "Should add to empty timeline"
    (is (true? (agg/should-add-to-timeline? [] order-1)))))

(deftest test-should-add-to-timeline-duplicate
  (testing "Should NOT add duplicate to timeline"
    (let [timeline [{:order-id "ORDER-001"}]]
      (is (false? (agg/should-add-to-timeline? timeline order-1))))))

;; =============================================================================
;; Processing Stats Tests
;; =============================================================================

(deftest test-increment-processed
  (testing "Increment processed count"
    (let [stats {:processed-count 10 :error-count 2}
          result (agg/increment-processed stats)]
      (is (= 11 (:processed-count result)))
      (is (= 2 (:error-count result)))
      (is (pos? (:last-processed-timestamp result))))))

(deftest test-increment-errors
  (testing "Increment error count"
    (let [stats {:processed-count 10 :error-count 2}
          result (agg/increment-errors stats)]
      (is (= 10 (:processed-count result)))
      (is (= 3 (:error-count result))))))

;; =============================================================================
;; Full Aggregation Tests
;; =============================================================================

(deftest test-aggregate-order-first-order
  (testing "Aggregate first order updates all views"
    (let [views (agg/init-views "processor-1")
          config {:timeline-max-size 100}
          result (agg/aggregate-order views order-1 config)]

      ;; Customer stats created
      (is (= 1 (count (:customer-stats result))))
      (is (some? (get-in result [:customer-stats 42])))

      ;; Product stats created
      (is (= 1 (count (:product-stats result))))
      (is (some? (get-in result [:product-stats "PROD-A"])))

      ;; Timeline updated
      (is (= 1 (count (:timeline result))))

      ;; Processing stats incremented
      (is (= 1 (get-in result [:processing-stats :processed-count]))))))

(deftest test-aggregate-order-multiple-orders
  (testing "Aggregate multiple orders"
    (let [views (agg/init-views "processor-1")
          config {:timeline-max-size 100}
          result (-> views
                     (agg/aggregate-order order-1 config)
                     (agg/aggregate-order order-2 config)
                     (agg/aggregate-order order-3 config))]

      ;; Two customers
      (is (= 2 (count (:customer-stats result))))

      ;; Two products
      (is (= 2 (count (:product-stats result))))

      ;; Three orders in timeline
      (is (= 3 (count (:timeline result))))

      ;; Three orders processed
      (is (= 3 (get-in result [:processing-stats :processed-count]))))))

(deftest test-aggregate-order-same-customer
  (testing "Aggregate multiple orders from same customer"
    (let [views (agg/init-views "processor-1")
          config {:timeline-max-size 100}
          result (-> views
                     (agg/aggregate-order order-1 config)
                     (agg/aggregate-order order-2 config))]

      (let [customer-stats (get-in result [:customer-stats 42])]
        (is (= 2 (:total-orders customer-stats)))
        (is (= 300.0 (:total-spent customer-stats)))
        (is (= "ORDER-002" (:last-order-id customer-stats)))))))

(deftest test-aggregate-order-batch
  (testing "Aggregate batch of orders"
    (let [views (agg/init-views "processor-1")
          config {:timeline-max-size 100}
          orders [order-1 order-2 order-3]
          result (agg/aggregate-order-batch views orders config)]

      (is (= 2 (count (:customer-stats result))))
      (is (= 2 (count (:product-stats result))))
      (is (= 3 (count (:timeline result))))
      (is (= 3 (get-in result [:processing-stats :processed-count]))))))

;; =============================================================================
;; Init Views Tests
;; =============================================================================

(deftest test-init-views
  (testing "Initialize views creates correct structure"
    (let [result (agg/init-views "processor-1")]
      (is (map? result))
      (is (map? (:customer-stats result)))
      (is (empty? (:customer-stats result)))
      (is (map? (:product-stats result)))
      (is (empty? (:product-stats result)))
      (is (vector? (:timeline result)))
      (is (empty? (:timeline result)))
      (is (map? (:processing-stats result)))
      (is (= "processor-1" (get-in result [:processing-stats :processor-id])))
      (is (= 0 (get-in result [:processing-stats :processed-count])))
      (is (= 0 (get-in result [:processing-stats :error-count]))))))