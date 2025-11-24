(ns model-test
  "Comprehensive tests for data models and business logic in registry-processor."
  (:require [clojure.test :refer :all]
            [registry-processor.model :as model]
            [clojure.spec.alpha :as s]
            [cheshire.core :as json]))

(def valid-order
  "Sample valid order for model testing."
  {:order-id "ORDER-MODEL-001"
   :customer-id 42
   :product-id "PROD-MODEL-001"
   :quantity 5
   :unit-price 30.0
   :total 150.0
   :timestamp 1234567890
   :status "accepted"})

(def minimal-order
  "Minimal valid order for boundary testing."
  {:order-id "MIN"
   :customer-id 1
   :product-id "P"
   :quantity 1
   :unit-price 1.0
   :total 1.0
   :timestamp 1
   :status "pending"})

(deftest order-spec-validation-test
  (testing "Valid order conforms to specification"
    (is (model/valid-order? valid-order) "Valid order should pass spec validation")
    (is (s/valid? ::model/order valid-order) "Should conform to order spec"))

  (testing "Invalid order fails specification validation"
    (let [invalid-order (assoc valid-order :customer-id 0)]
      (is (not (model/valid-order? invalid-order)) "Invalid order should fail spec validation")
      (is (not (s/valid? ::model/order invalid-order)) "Should not conform to order spec")))

  (testing "Order validation with missing required fields"
    (let [incomplete-order (dissoc valid-order :order-id)]
      (is (not (model/valid-order? incomplete-order)) "Incomplete order should fail validation")))

  (testing "Order validation with wrong field types"
    (let [wrong-type-order (assoc valid-order :customer-id "not-a-number")]
      (is (not (model/valid-order? wrong-type-order)) "Wrong type order should fail validation"))))

(deftest registered-order-creation-test
  (testing "Create new registered order from validated order"
    (let [registered (model/new-registered-order valid-order true)]
      (is (model/valid-registered-order? registered) "Registered order should be valid")
      (is (= "ORDER-MODEL-001" (:order-id registered)) "Should preserve order ID")
      (is (= 42 (:customer-id registered)) "Should preserve customer ID")
      (is (= "PROD-MODEL-001" (:product-id registered)) "Should preserve product ID")
      (is (= 5 (:quantity registered)) "Should preserve quantity")
      (is (= 150.0 (:total registered)) "Should preserve total")
      (is (= "accepted" (:status registered)) "Should set status to accepted for validated orders")
      (is (= 1 (:version registered)) "New order should have version 1")
      (is (true? (:validation-passed registered)) "Should set validation-passed to true")
      (is (pos? (:registered-at registered)) "Should set registration timestamp")))

  (testing "Create registered order for failed validation"
    (let [registered (model/new-registered-order valid-order false)]
      (is (false? (:validation-passed registered)) "Should set validation-passed to false for failed validation")
      (is (= "denied" (:status registered)) "Should set status to denied for failed validation"))))

(deftest order-update-test
  (testing "Update order status increments version and updates timestamp"
    (let [original (model/new-registered-order valid-order true)
          updated (model/update-order-status original "denied")]

      (is (= "denied" (:status updated)) "Should update status")
      (is (= 2 (:version updated)) "Should increment version")
      (is (pos? (:updated-at updated)) "Should set update timestamp")
      (is (<= (:registered-at original) (:updated-at updated))
          "Update timestamp should be >= registration timestamp")))

  (testing "Multiple status updates"
    (let [original (model/new-registered-order valid-order true)
          first-update (model/update-order-status original "denied")
          second-update (model/update-order-status first-update "pending")]

      (is (= "pending" (:status second-update)) "Should reflect final status")
      (is (= 3 (:version second-update)) "Should increment version multiple times"))))

(deftest order-update-history-test
  (testing "Create order update record"
    (let [update-record (model/new-order-update
                         "ORDER-001"
                         2
                         "pending"
                         "accepted"
                         "Status transition")]

      (is (= "ORDER-001" (:order-id update-record)) "Should set order ID")
      (is (= 2 (:version update-record)) "Should set version")
      (is (= "pending" (:previous-status update-record)) "Should set previous status")
      (is (= "accepted" (:new-status update-record)) "Should set new status")
      (is (pos? (:updated-at update-record)) "Should set update timestamp")
      (is (= "Status transition" (:update-reason update-record)) "Should set update reason"))))

(deftest update-decision-logic-test
  (testing "Should update when no existing order"
    (is (model/should-update-order? nil valid-order) "Should always update when no existing order"))

  (testing "Should update when status changes"
    (let [existing {:status "pending"}
          new-order {:status "accepted"}]
      (is (model/should-update-order? existing new-order) "Should update when status changes")))

  (testing "Should update when quantity changes"
    (let [existing {:status "accepted" :quantity 5 :total 150.0}
          new-order {:status "accepted" :quantity 10 :total 300.0}]
      (is (model/should-update-order? existing new-order) "Should update when quantity changes")))

  (testing "Should update when total changes"
    (let [existing {:status "accepted" :quantity 5 :total 150.0}
          new-order {:status "accepted" :quantity 5 :total 200.0}]
      (is (model/should-update-order? existing new-order) "Should update when total changes")))

  (testing "Should not update when order is unchanged"
    (let [existing {:status "accepted" :quantity 5 :total 150.0}
          new-order {:status "accepted" :quantity 5 :total 150.0}]
      (is (not (model/should-update-order? existing new-order)) "Should not update unchanged order"))))

(deftest serialization-deserialization-test
  (testing "Deserialize order from JSON string"
    (let [json-order (json/generate-string valid-order)
          deserialized (model/deserialize-order json-order)]

      (is (map? deserialized) "Deserialized should be a map")
      (is (= (:order-id valid-order) (:order-id deserialized)) "Should preserve order ID")
      (is (= (:customer-id valid-order) (:customer-id deserialized)) "Should preserve customer ID")
      (is (int? (:customer-id deserialized)) "Customer ID should be integer")
      (is (int? (:quantity deserialized)) "Quantity should be integer")
      (is (int? (:timestamp deserialized)) "Timestamp should be long")))

  (testing "Handle deserialization errors gracefully"
    (is (thrown? Exception (model/deserialize-order "invalid-json"))
        "Should throw exception for invalid JSON"))

  (testing "Serialize registered order to JSON"
    (let [registered (model/new-registered-order valid-order true)
          serialized (model/serialize-registered-order registered)]

      (is (string? serialized) "Serialized should be string")
      (is (re-find #"ORDER-MODEL-001" serialized) "Should contain order ID in JSON"))))

(deftest validation-stats-test
  (testing "Create new validation statistics"
    (let [stats (model/new-validation-stats "test-processor")]
      (is (= "test-processor" (:processor-id stats)) "Should set processor ID")
      (is (zero? (:total-validated stats)) "Should initialize total validated to zero")
      (is (zero? (:total-approved stats)) "Should initialize total approved to zero")
      (is (zero? (:total-rejected stats)) "Should initialize total rejected to zero")
      (is (pos? (:timestamp stats)) "Should set creation timestamp")))

  (testing "Validation stats conform to specification"
    (let [stats (model/new-validation-stats "test-processor")]
      (is (s/valid? ::model/validation-stats stats) "Stats should conform to validation-stats spec"))))

(deftest error-explanation-test
  (testing "Get human-readable validation error explanation"
    (let [invalid-order (assoc valid-order :customer-id 0)
          explanation (model/explain-validation-error ::model/order invalid-order)]

      (is (string? explanation) "Explanation should be a string")
      (is (re-find #"customer-id" explanation) "Should mention the problematic field"))))

(deftest model-consistency-test
  (testing "Model functions maintain data integrity"
    (let [registered (model/new-registered-order valid-order true)
          updated (model/update-order-status registered "denied")]

      (is (model/valid-registered-order? registered) "Original should remain valid")
      (is (model/valid-registered-order? updated) "Updated should be valid")
      (is (= (:order-id registered) (:order-id updated)) "Order ID should not change")
      (is (= (:customer-id registered) (:customer-id updated)) "Customer ID should not change"))))

(deftest edge-case-handling-test
  (testing "Handle nil values in update decision logic"
    (is (model/should-update-order? nil {:status "pending"}) "Should handle nil existing order")
    (is (thrown? Exception (model/should-update-order? {:status "pending"} nil))
        "Should handle nil new order"))

  (testing "Handle boundary values in order creation"
    (let [registered (model/new-registered-order minimal-order true)]
      (is (model/valid-registered-order? registered) "Minimal order should be valid"))))