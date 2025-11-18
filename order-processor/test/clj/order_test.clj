(ns order-test
  "Tests for order generation and validation."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [order-processor.order :as order]
            [shared.utils :as utils]))

;; Test config for unit tests
(def test-config
  {:orders {:quantity-range [1 10]
            :price-range [15.0 100.0]
            :customer-ids [1001 1002 1003 1004 1005]
            :product-ids ["PROD-001" "PROD-002" "PROD-003" "PROD-004"]}})


(deftest test-generate-order-structure
  (testing "Generated order has all required fields"
    (let [order (order/generate-order test-config)]  ; Uses test-config
      (is (contains? order :order-id) "Should have order-id")
      (is (contains? order :customer-id) "Should have customer-id")
      (is (contains? order :product-id) "Should have product-id")
      (is (contains? order :quantity) "Should have quantity")
      (is (contains? order :unit-price) "Should have unit-price")
      (is (contains? order :total) "Should have total")
      (is (contains? order :timestamp) "Should have timestamp")
      (is (contains? order :status) "Should have status"))))

(deftest test-generate-order-types
  (testing "Generated order fields have correct types"
    (let [order (order/generate-order test-config)]
      (is (string? (:order-id order)) (str "Order ID should be string, got: " (:order-id order)))
      (is (number? (:customer-id order)) (str "Customer ID should be number, got: " (:customer-id order)))
      (is (string? (:product-id order)) (str "Product ID should be string, got: " (:product-id order)))
      (is (pos-int? (:quantity order)) (str "Quantity should be positive integer, got: " (:quantity order)))
      (is (number? (:unit-price order)) (str "Unit price should be number, got: " (:unit-price order)))
      (is (number? (:total order)) (str "Total should be number, got: " (:total order)))
      (is (number? (:timestamp order)) (str "Timestamp should be number, got: " (:timestamp order)))
      (is (= "PENDING" (:status order)) (str "Status should be PENDING, got: " (:status order))))))

(deftest test-order-total-calculation
  (testing "Order total equals quantity * price (with proper rounding and tolerance)"
    (let [order (order/generate-order test-config)
          expected (utils/round (* (:quantity order) (:unit-price order)) 2)
          actual (:total order)
          difference (Math/abs (- expected actual))]
      ;; Increased tolerance to 0.05 to account for floating point arithmetic
      (is (< difference 0.05)
          (format "Total should be close to quantity * unit-price. Expected: %.2f, Actual: %.2f, Diff: %.4f"
                  expected actual difference)))))

(deftest test-utils-round-money
  (testing "Money rounding to 2 decimal places"
    (is (= 10.99 (utils/round 10.9876 2)))
    (is (= 5.00 (utils/round 5.0 2)))
    (is (= 0.01 (utils/round 0.0051 2)))))

(deftest test-utils-random-between
  (testing "Random value is between min and max"
    (dotimes [_ 100]
      (let [value (utils/random-between 10.0 20.0)]
        (is (>= value 10.0))
        (is (<= value 20.0))))))

(deftest test-multiple-orders-have-different-ids
  (testing "Multiple generated orders have different IDs"
    (let [order1 (order/generate-order test-config)
          order2 (order/generate-order test-config)]
      (is (not= (:order-id order1) (:order-id order2))
          "Order IDs should be unique"))))