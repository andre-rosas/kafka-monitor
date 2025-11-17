(ns model-test
  "Tests for order model, validation, and serialization.
   
   These tests verify:
   - Order structure conforms to spec
   - Validation catches invalid data
   - Serialization/deserialization works correctly
   - Business rules are enforced"
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.spec.test.alpha :as stest]
            [order-processor.model :as model]))

;; =============================================================================
;; Validation Tests
;; =============================================================================

(deftest test-valid-order
  (testing "Valid order passes validation"
    (let [order (model/new-order {:customer-id 1
                                  :product-id "PROD-001"
                                  :quantity 5
                                  :unit-price 10.99})]
      (is (model/valid-order? order))
      (is (:valid? (model/validate-order order))))))

(deftest test-invalid-order-missing-fields
  (testing "Order missing required fields fails validation"
    (let [invalid-order {:order-id "123"
                         :customer-id 1}]
      (is (not (model/valid-order? invalid-order)))
      (is (not (:valid? (model/validate-order invalid-order))))
      (is (thrown? Exception (model/validate-order! invalid-order))))))

(deftest test-invalid-order-wrong-types
  (testing "Order with wrong types fails validation"
    (let [invalid-order {:order-id "not-a-uuid"
                         :customer-id "not-a-number"
                         :product-id "PROD-001"
                         :quantity 5
                         :unit-price 10.99
                         :total 54.95
                         :timestamp 1234567890
                         :status "pending"}]
      (is (not (model/valid-order? invalid-order))))))

(deftest test-invalid-status
  (testing "Order with invalid status fails validation"
    (is (thrown? Exception
                 (model/new-order {:customer-id 1
                                   :product-id "PROD-001"
                                   :quantity 5
                                   :unit-price 10.99
                                   :status "INVALID_STATUS"})))))

;; =============================================================================
;; Business Rules Tests
;; =============================================================================

(deftest test-total-calculation
  (testing "Total is correctly calculated"
    (let [order (model/new-order {:customer-id 1
                                  :product-id "PROD-001"
                                  :quantity 5
                                  :unit-price 10.99})]
      (is (= 54.95 (:total order)))
      (is (model/total-matches? order)))))

(deftest test-business-rules-validation
  (testing "Business rules catch invalid total"
    (let [order {:order-id "123e4567-e89b-12d3-a456-426614174000"
                 :customer-id 1
                 :product-id "PROD-001"
                 :quantity 5
                 :unit-price 10.99
                 :total 999.99
                 :timestamp 1234567890
                 :status "pending"}]
      (let [result (model/validate-business-rules order)]
        (is (not (:valid? result)))
        (is (seq (:errors result)))))))

;; =============================================================================
;; Serialization Tests
;; =============================================================================

(deftest test-json-serialization
  (testing "Order can be serialized to JSON and back"
    (let [order (model/new-order {:customer-id 42
                                  :product-id "PROD-001"
                                  :quantity 3
                                  :unit-price 19.99})
          json (model/order->json order)
          restored (model/json->order json)]
      (is (string? json))
      (is (= (:order-id order) (:order-id restored)))
      (is (= (:customer-id order) (:customer-id restored)))
      (is (= (:product-id order) (:product-id restored)))
      (is (= (:quantity order) (:quantity restored)))
      (is (= (:unit-price order) (:unit-price restored)))
      (is (= (:total order) (:total restored)))
      (is (= (:status order) (:status restored))))))

(deftest test-json-deserialization-validation
  (testing "Invalid JSON fails validation on deserialization"
    (let [invalid-json "{\"order-id\": \"not-a-uuid\"}"]
      (is (thrown? Exception (model/json->order invalid-json))))))

;; =============================================================================
;; Constructor Tests
;; =============================================================================

(deftest test-new-order-defaults
  (testing "new-order sets correct defaults"
    (let [order (model/new-order {:customer-id 1
                                  :product-id "PROD-001"
                                  :quantity 5
                                  :unit-price 10.99})]
      (is (string? (:order-id order)))
      (is (= "pending" (:status order)))
      (is (number? (:timestamp order)))
      (is (pos? (:timestamp order))))))

;; =============================================================================
;; Property-Based Tests
;; =============================================================================

(deftest test-generated-orders-are-valid
  (testing "All generated orders are valid"
    (dotimes [_ 100]
      (let [order (first (stest/sample (model/gen-order) 1))]
        (is (model/valid-order? order))
        (is (model/total-matches? order))))))