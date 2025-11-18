(ns model-test
  "Tests for model validation and serialization."
  (:require [clojure.test :refer :all]
            [query-processor.model :as model]))

;; =============================================================================
;; Test Data
;; =============================================================================

(def valid-order
  {:order-id "ORDER-123"
   :customer-id 42
   :product-id "PROD-001"
   :quantity 5
   :unit-price 30.0
   :total 150.0
   :timestamp 1234567890
   :status "pending"})

(def valid-customer-stats
  {:customer-id 42
   :total-orders 5
   :total-spent 500.0
   :last-order-id "ORDER-123"
   :last-order-timestamp 1234567890
   :first-order-timestamp 1234560000})

(def valid-product-stats
  {:product-id "PROD-001"
   :total-quantity 50
   :total-revenue 1500.0
   :order-count 10
   :avg-quantity 5.0
   :last-order-timestamp 1234567890})

(def valid-timeline-entry
  {:order-id "ORDER-123"
   :customer-id 42
   :product-id "PROD-001"
   :total 150.0
   :status "accepted"
   :timestamp 1234567890})

;; =============================================================================
;; Validation Tests
;; =============================================================================

(deftest test-valid-order
  (testing "Valid order passes validation"
    (is (true? (model/valid-order? valid-order)))))

(deftest test-invalid-order-missing-field
  (testing "Order missing required field fails"
    (let [invalid (dissoc valid-order :order-id)]
      (is (false? (model/valid-order? invalid))))))

(deftest test-invalid-order-wrong-type
  (testing "Order with wrong type fails"
    (let [invalid (assoc valid-order :customer-id "not-a-number")]
      (is (false? (model/valid-order? invalid))))))

(deftest test-invalid-order-negative-total
  (testing "Order with negative total fails"
    (let [invalid (assoc valid-order :total -100.0)]
      (is (false? (model/valid-order? invalid))))))

(deftest test-valid-customer-stats
  (testing "Valid customer stats pass validation"
    (is (true? (model/valid-customer-stats? valid-customer-stats)))))

(deftest test-valid-product-stats
  (testing "Valid product stats pass validation"
    (is (true? (model/valid-product-stats? valid-product-stats)))))

(deftest test-valid-timeline-entry
  (testing "Valid timeline entry passes validation"
    (is (true? (model/valid-timeline-entry? valid-timeline-entry)))))

;; =============================================================================
;; Serialization Tests
;; =============================================================================

(deftest test-deserialize-order
  (testing "Deserialize order from JSON"
    (let [json "{\"order-id\":\"ORDER-123\",\"customer-id\":42,\"product-id\":\"PROD-001\",\"quantity\":5,\"unit-price\":30.0,\"total\":150.0,\"timestamp\":1234567890,\"status\":\"pending\"}"
          result (model/deserialize-order json)]
      (is (= "ORDER-123" (:order-id result)))
      (is (= 42 (:customer-id result)))
      (is (= 5 (:quantity result))))))

(deftest test-deserialize-invalid-json
  (testing "Deserialize invalid JSON throws exception"
    (is (thrown? Exception (model/deserialize-order "invalid json")))))

(deftest test-serialize-customer-stats
  (testing "Serialize customer stats to JSON"
    (let [result (model/serialize-customer-stats valid-customer-stats)]
      (is (string? result))
      (is (.contains result "customer_id"))
      (is (.contains result "42")))))

(deftest test-serialize-product-stats
  (testing "Serialize product stats to JSON"
    (let [result (model/serialize-product-stats valid-product-stats)]
      (is (string? result))
      (is (.contains result "product_id"))
      (is (.contains result "PROD-001")))))

;; =============================================================================
;; Constructor Tests
;; =============================================================================

(deftest test-new-customer-stats
  (testing "Create new customer stats"
    (let [result (model/new-customer-stats 42 valid-order)]
      (is (= 42 (:customer-id result)))
      (is (= 0 (:total-orders result)))
      (is (= 0.0 (:total-spent result)))
      (is (nil? (:last-order-id result)))
      (is (= 1234567890 (:first-order-timestamp result))))))

(deftest test-new-product-stats
  (testing "Create new product stats"
    (let [result (model/new-product-stats "PROD-001")]
      (is (= "PROD-001" (:product-id result)))
      (is (= 0 (:total-quantity result)))
      (is (= 0.0 (:total-revenue result)))
      (is (= 0 (:order-count result)))
      (is (= 0.0 (:avg-quantity result))))))

(deftest test-new-processing-stats
  (testing "Create new processing stats"
    (let [result (model/new-processing-stats "processor-1")]
      (is (= "processor-1" (:processor-id result)))
      (is (= 0 (:processed-count result)))
      (is (= 0 (:error-count result)))
      (is (pos? (:last-processed-timestamp result))))))

;; =============================================================================
;; Extraction Tests
;; =============================================================================

(deftest test-extract-timeline-entry
  (testing "Extract timeline entry from full order"
    (let [full-order (assoc valid-order :extra-field "not-needed")
          result (model/extract-timeline-entry full-order)]
      (is (= "ORDER-123" (:order-id result)))
      (is (= 42 (:customer-id result)))
      (is (= "PROD-001" (:product-id result)))
      (is (= 150.0 (:total result)))
      (is (= "pending" (:status result)))
      (is (= 1234567890 (:timestamp result)))
      (is (nil? (:extra-field result))))))

(deftest test-extract-timeline-entry-only-needed-fields
  (testing "Timeline entry contains only needed fields"
    (let [result (model/extract-timeline-entry valid-order)
          expected-keys #{:order-id :customer-id :product-id :total :status :timestamp}]
      (is (= expected-keys (set (keys result)))))))