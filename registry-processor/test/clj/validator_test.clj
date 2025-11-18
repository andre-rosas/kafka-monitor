(ns validator-test
  "Comprehensive tests for business validation rules in registry-processor."
  (:require [clojure.test :refer :all]
            [registry-processor.validator :as validator]))

(def valid-order
  "Sample valid order meeting all business rules."
  {:order-id "ORDER-VALID-001"
   :customer-id 42
   :product-id "PROD-VALID-001"
   :quantity 5
   :unit-price 30.0
   :total 150.0
   :timestamp 1234567890
   :status "accepted"})

(deftest individual-validation-rules-test
  (testing "Minimum quantity validation rule"
    (let [valid-order-quantity (assoc valid-order :quantity 1)
          invalid-order-quantity (assoc valid-order :quantity 0)]

      (is (:passed (validator/validate-minimum-quantity valid-order-quantity))
          "Quantity of 1 should pass")
      (is (not (:passed (validator/validate-minimum-quantity invalid-order-quantity)))
          "Quantity of 0 should fail")
      (is (contains? (validator/validate-minimum-quantity invalid-order-quantity) :message)
          "Should include error message for failed validation")))

  (testing "Maximum quantity validation rule"
    (let [valid-order-quantity (assoc valid-order :quantity 1000)
          invalid-order-quantity (assoc valid-order :quantity 1001)]

      (is (:passed (validator/validate-maximum-quantity valid-order-quantity))
          "Quantity of 1000 should pass")
      (is (not (:passed (validator/validate-maximum-quantity invalid-order-quantity)))
          "Quantity of 1001 should fail")))

  (testing "Minimum total validation rule"
    (let [valid-order-total (assoc valid-order :total 1.0)
          invalid-order-total (assoc valid-order :total 0.99)]

      (is (:passed (validator/validate-minimum-total valid-order-total))
          "Total of 1.0 should pass")
      (is (not (:passed (validator/validate-minimum-total invalid-order-total)))
          "Total of 0.99 should fail")))

  (testing "Maximum total validation rule"
    (let [valid-order-total (assoc valid-order :total 100000.0)
          invalid-order-total (assoc valid-order :total 100000.01)]

      (is (:passed (validator/validate-maximum-total valid-order-total))
          "Total of 100000.0 should pass")
      (is (not (:passed (validator/validate-maximum-total invalid-order-total)))
          "Total of 100000.01 should fail")))

  (testing "Price consistency validation rule"
    (let [consistent-order valid-order
          inconsistent-order (assoc valid-order :total 999.0)]

      (is (:passed (validator/validate-price-consistency consistent-order))
          "Consistent price calculation should pass")
      (is (not (:passed (validator/validate-price-consistency inconsistent-order)))
          "Inconsistent price calculation should fail")))

  (testing "Status validation rule"
    (let [valid-statuses #{"pending" "accepted" "denied"}
          invalid-status-order (assoc valid-order :status "invalid_status")]

      (doseq [status valid-statuses]
        (let [test-order (assoc valid-order :status status)]
          (is (:passed (validator/validate-status test-order))
              (str "Status '" status "' should be valid"))))

      (is (not (:passed (validator/validate-status invalid-status-order)))
          "Invalid status should fail validation")))

  (testing "Customer ID validation rule"
    (let [valid-customer-order (assoc valid-order :customer-id 1)
          invalid-customer-order (assoc valid-order :customer-id 0)]

      (is (:passed (validator/validate-customer-id valid-customer-order))
          "Positive customer ID should pass")
      (is (not (:passed (validator/validate-customer-id invalid-customer-order)))
          "Zero customer ID should fail")
      ;; Test for non-numeric customer ID should throw exception
      (let [invalid-customer-type (assoc valid-order :customer-id "not-a-number")]
        (is (thrown? ClassCastException
                     (validator/validate-customer-id invalid-customer-type))
            "Non-numeric customer ID should throw ClassCastException"))))

  (testing "Product ID validation rule"
    (let [valid-product-order (assoc valid-order :product-id "PROD-123")
          empty-product-order (assoc valid-order :product-id "")
          nil-product-order (assoc valid-order :product-id nil)]

      (is (:passed (validator/validate-product-id valid-product-order))
          "Non-empty product ID should pass")
      (is (not (:passed (validator/validate-product-id empty-product-order)))
          "Empty product ID should fail")
      (is (not (:passed (validator/validate-product-id nil-product-order)))
          "Nil product ID should fail"))))

(deftest comprehensive-validation-test
  (testing "Complete order validation with all rules passing"
    (let [result (validator/validate-order valid-order)]
      (is (:passed result) "Valid order should pass all rules")
      (is (= 8 (count (:rules result))) "Should have exactly 8 validation rules")
      (is (every? :passed (:rules result)) "All individual rules should pass")
      (is (nil? (:message result)) "No error message should be present for valid order")))

  (testing "Complete order validation with multiple rule failures"
    (let [invalid-order (assoc valid-order :quantity 0 :total -10 :status "invalid_status")
          result (validator/validate-order invalid-order)]

      (is (not (:passed result)) "Invalid order should fail validation")
      (is (some? (:message result)) "Should include error message for failed validation")
      (is (> (count (filter (complement :passed) (:rules result))) 1)
          "Should have multiple rule failures")))

  (testing "Edge case: order with maximum allowed values"
    (let [edge-case-order (assoc valid-order
                                 :quantity 1000
                                 :total 100000.0
                                 :unit-price 100.0)
          result (validator/validate-order edge-case-order)]

      (is (:passed result) "Order with maximum values should pass")
      (is (every? :passed (:rules result)) "All rules should pass for edge case")))

  (testing "Edge case: order with minimum allowed values"
    (let [edge-case-order (assoc valid-order
                                 :quantity 1
                                 :total 1.0
                                 :unit-price 1.0)
          result (validator/validate-order edge-case-order)]

      (is (:passed result) "Order with minimum values should pass")
      (is (every? :passed (:rules result)) "All rules should pass for edge case"))))

(deftest validation-helper-functions-test
  (testing "Validation passed predicate function"
    (let [passed-result {:passed true}
          failed-result {:passed false}]

      (is (validator/validation-passed? passed-result) "Should return true for passed validation")
      (is (not (validator/validation-passed? failed-result)) "Should return false for failed validation")))

  (testing "Get failed rules function"
    (let [mixed-results {:rules [{:rule-name :rule1 :passed true}
                                 {:rule-name :rule2 :passed false}
                                 {:rule-name :rule3 :passed false}]}
          failed-rules (validator/get-failed-rules mixed-results)]

      (is (= 2 (count failed-rules)) "Should identify 2 failed rules")
      (is (every? keyword? failed-rules) "Failed rules should be keywords")
      (is (some #{:rule2} failed-rules) "Should include rule2")
      (is (some #{:rule3} failed-rules) "Should include rule3")))

  (testing "Get failure summary function"
    (let [failed-result {:passed false
                         :rules [{:rule-name :minimum-quantity :passed false}
                                 {:rule-name :maximum-total :passed false}]}
          summary (validator/get-failure-summary failed-result)]

      (is (string? summary) "Summary should be a string")
      (is (re-find #"minimum-quantity" summary) "Should mention failed rules")
      (is (re-find #"maximum-total" summary) "Should mention all failed rules")))

  (testing "Failure summary for passed validation"
    (let [passed-result {:passed true :rules []}
          summary (validator/get-failure-summary passed-result)]

      (is (nil? summary) "Should return nil for passed validation"))))

(deftest validation-rule-composition-test
  (testing "All validation rules are applied"
    (let [rules-count (count validator/validation-rules)]
      (is (= 8 rules-count) "Should have exactly 8 validation rules")
      (is (every? ifn? validator/validation-rules) "All rules should be functions")))

  (testing "Validation rules execution order consistency"
    (let [result1 (validator/validate-order valid-order)
          result2 (validator/validate-order valid-order)]

      (is (= (map :rule-name (:rules result1))
             (map :rule-name (:rules result2)))
          "Rule execution order should be consistent"))))

(deftest error-message-quality-test
  (testing "Error messages are informative and actionable"
    (let [zero-quantity-order (assoc valid-order :quantity 0)
          result (validator/validate-minimum-quantity zero-quantity-order)]

      (is (string? (:message result)) "Error message should be a string")
      (is (re-find #"Quantity" (:message result)) "Should mention the field name")
      (is (re-find #"at least" (:message result)) "Should mention the requirement")
      (is (re-find #"0" (:message result)) "Should mention the actual value"))))

(deftest type-safety-test
  (testing "Validator handles invalid data types gracefully"
    (let [order-with-string-customer-id (assoc valid-order :customer-id "not-a-number")]
      (is (thrown? ClassCastException
                   (validator/validate-order order-with-string-customer-id))
          "Should throw ClassCastException when customer-id is not a number"))))