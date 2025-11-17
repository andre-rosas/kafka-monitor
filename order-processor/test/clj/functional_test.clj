(ns functional-test
  "Tests for pure functions (no side effects).
   
   These tests verify business logic in isolation:
   - Order generation
   - Data transformations
   - Calculations
   
   Why separate functional tests?
   - Pure functions are easiest to test
   - No mocks needed
   - Fast execution
   - High confidence in correctness"
  (:require [clojure.test :refer [deftest testing is]]
            [order-processor.model :as model]
            [order-processor.db :as db]))

;; =============================================================================
;; Order Generation Tests
;; =============================================================================

(deftest test-generate-order-structure
  (testing "Generated order has all required fields"
    (let [order (model/new-order {:customer-id 1
                                  :product-id "PROD-001"
                                  :quantity 5
                                  :unit-price 10.99})]
      (is (string? (:order-id order)))
      (is (number? (:customer-id order)))
      (is (string? (:product-id order)))
      (is (pos-int? (:quantity order)))
      (is (pos? (:unit-price order)))
      (is (pos? (:total order)))
      (is (number? (:timestamp order)))
      (is (= "pending" (:status order))))))

(deftest test-generate-order-validity
  (testing "Generated orders are always valid"
    (dotimes [_ 100]
      (let [order (model/new-order {:customer-id (inc (rand-int 100))
                                    :product-id (str "PROD-" (inc (rand-int 100)))
                                    :quantity (inc (rand-int 10))
                                    :unit-price (+ 1.0 (rand 100.0))})]
        (is (model/valid-order? order))))))

;; =============================================================================
;; Data Transformation Tests
;; =============================================================================

(deftest test-order-to-cassandra-row-transformation
  (testing "Order can be transformed to Cassandra row format"
    (let [order (model/new-order {:customer-id 42
                                  :product-id "PROD-001"
                                  :quantity 5
                                  :unit-price 19.99})
          row-params (db/order->cassandra-row order)]
      (is (vector? row-params))
      (is (= 8 (count row-params)))
      (is (instance? java.util.UUID (first row-params)))
      (is (= 42 (second row-params)))
      (is (= "PROD-001" (nth row-params 2))))))

;; =============================================================================
;; Calculation Tests
;; =============================================================================

(deftest test-total-calculation-precision
  (testing "Total calculation handles decimal precision correctly"
    (let [test-cases [{:quantity 3 :unit-price 10.99 :expected 32.97}
                      {:quantity 7 :unit-price 15.50 :expected 108.50}
                      {:quantity 1 :unit-price 99.99 :expected 99.99}]]
      (doseq [{:keys [quantity unit-price expected]} test-cases]
        (let [order (model/new-order {:customer-id 1
                                      :product-id "TEST"
                                      :quantity quantity
                                      :unit-price unit-price})]
          (is (= expected (:total order))))))))

;; =============================================================================
;; Business Rules Tests
;; =============================================================================

(deftest test-business-rules-pure
  (testing "Business rules validation is pure"
    (let [valid-order (model/new-order {:customer-id 1
                                        :product-id "PROD-001"
                                        :quantity 5
                                        :unit-price 10.00})
          invalid-order (assoc valid-order :total 99.99)]

      ;; Valid order
      (is (:valid? (model/validate-business-rules valid-order)))

      ;; Invalid order
      (is (not (:valid? (model/validate-business-rules invalid-order))))

      ;; Function is pure - calling twice gives same result
      (is (= (model/validate-business-rules valid-order)
             (model/validate-business-rules valid-order))))))

(deftest test-total-calculation-robustness
  (testing "Total calculation is robust against floating point errors"
    (dotimes [_ 50]
      (let [quantity (inc (rand-int 10))
            unit-price (+ 0.01 (rand 100.0))
            order (model/new-order {:customer-id (inc (rand-int 100))
                                    :product-id (str "PROD-" (inc (rand-int 100)))
                                    :quantity quantity
                                    :unit-price unit-price})
            expected-total (* quantity unit-price)
            calculated-total (:total order)
            diff (Math/abs (- expected-total calculated-total))]
        (is (< diff 0.02) (format "Difference too large: %.4f for quantity %d, unit-price %.2f"
                                  diff quantity unit-price))))))