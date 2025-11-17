(ns db-test
  "Tests for Cassandra database operations."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [order-processor.db :as db]
            [order-processor.model :as model]))

;; =============================================================================
;; Test Setup - Mock Cassandra Session
;; =============================================================================

(def mock-session
  (reify com.datastax.oss.driver.api.core.CqlSession
    (close [this] nil)
    (^java.util.concurrent.CompletionStage executeAsync [this ^com.datastax.oss.driver.api.core.cql.Statement statement]
      nil)
    (^com.datastax.oss.driver.api.core.cql.ResultSet execute [this ^com.datastax.oss.driver.api.core.cql.Statement statement]
      (reify com.datastax.oss.driver.api.core.cql.ResultSet
        (^java.util.Iterator iterator [this]
          (.iterator (java.util.ArrayList.)))))))

(use-fixtures :each
  (fn [f]
    ;; Mock the session to avoid real database connections
    (with-redefs [db/create-session (constantly mock-session)
                  db/get-session (constantly mock-session)]
      (f))))

;; =============================================================================
;; Data Transformation Tests
;; =============================================================================

(deftest test-order-to-cassandra-row-complete
  (testing "order->cassandra-row converts all order fields correctly"
    (let [order {:order-id "123e4567-e89b-12d3-a456-426614174000"
                 :customer-id 42
                 :product-id "PROD-001"
                 :quantity 5
                 :unit-price 19.99
                 :total 99.95
                 :timestamp 1234567890000
                 :status "pending"}
          row-params (db/order->cassandra-row order)]

      (is (vector? row-params))
      (is (= 8 (count row-params)) "Should have 8 parameters for Cassandra insert")

      ;; Test each parameter type and value
      (is (instance? java.util.UUID (nth row-params 0)) "First element should be UUID")
      (is (= 42 (nth row-params 1)) "Customer ID should be preserved")
      (is (= "PROD-001" (nth row-params 2)) "Product ID should be preserved")
      (is (= 5 (nth row-params 3)) "Quantity should be preserved")
      (is (instance? java.math.BigDecimal (nth row-params 4)) "Price should be BigDecimal")
      (is (instance? java.math.BigDecimal (nth row-params 5)) "Total should be BigDecimal")
      (is (instance? java.time.Instant (nth row-params 6)) "Timestamp should be Instant")
      (is (= "pending" (nth row-params 7)) "Status should be preserved"))))

(deftest test-order-to-cassandra-row-different-statuses
  (testing "order->cassandra-row handles different order statuses"
    (let [statuses ["pending" "confirmed" "cancelled" "shipped" "delivered"]]
      (doseq [status statuses]
        (let [order {:order-id "123e4567-e89b-12d3-a456-426614174000"
                     :customer-id 1
                     :product-id "TEST"
                     :quantity 1
                     :unit-price 10.0
                     :total 10.0
                     :timestamp 1234567890000
                     :status status}
              row-params (db/order->cassandra-row order)]
          (is (= status (nth row-params 7)) (str "Should preserve status: " status)))))))

(deftest test-order-to-cassandra-row-edge-cases
  (testing "order->cassandra-row handles edge cases"
    ;; Test with minimum values
    (let [min-order {:order-id "00000000-0000-0000-0000-000000000000"
                     :customer-id 1
                     :product-id "A"
                     :quantity 1
                     :unit-price 0.01
                     :total 0.01
                     :timestamp 1
                     :status "pending"}
          min-row (db/order->cassandra-row min-order)]
      (is (vector? min-row))
      (is (= 8 (count min-row))))))

;; =============================================================================
;; Database Operation Tests (Mocked)
;; =============================================================================

(deftest test-save-order-validation-strict
  (testing "save-order! validates order structure strictly"
    ;; Test various invalid orders
    (let [invalid-orders [{:order-id "invalid-uuid"}  ; Missing required fields
                          {:order-id "123e4567-e89b-12d3-a456-426614174000" :customer-id "not-number"}  ; Wrong type
                          {:order-id "123e4567-e89b-12d3-a456-426614174000" :customer-id -1}  ; Invalid customer ID
                          ]]

      (doseq [invalid-order invalid-orders]
        (try
          (db/save-order! invalid-order)
          (is false (str "Should throw exception for invalid order: " invalid-order))
          (catch Exception e
            (is true (str "Correctly threw exception for invalid order: " (.getMessage e)))))))))

(deftest test-save-order-valid-order
  (testing "save-order! accepts valid orders without database connection"
    (let [valid-order (model/new-order {:customer-id 1
                                        :product-id "PROD-001"
                                        :quantity 5
                                        :unit-price 10.99})]
      ;; This should not throw an exception even with mocked session
      (try
        (db/save-order! valid-order)
        (is true "Should not throw exception for valid order with mocked session")
        (catch Exception e
          (is false (str "Should not throw exception for valid order: " (.getMessage e))))))))

;; =============================================================================
;; Pure Function Tests (No Database Interaction)
;; =============================================================================

(deftest test-order-transformation-pure
  (testing "Order transformation functions are pure"
    (let [order1 {:order-id "123e4567-e89b-12d3-a456-426614174000"
                  :customer-id 1
                  :product-id "TEST"
                  :quantity 1
                  :unit-price 10.0
                  :total 10.0
                  :timestamp 1000
                  :status "pending"}]

      ;; Same input should always produce same output
      (let [result1 (db/order->cassandra-row order1)
            result2 (db/order->cassandra-row order1)]
        (is (= result1 result2) "order->cassandra-row should be pure")))))

(deftest test-order-transformation-boundary-values
  (testing "Order transformation handles boundary values correctly"
    ;; Test with maximum typical values
    (let [max-order {:order-id "ffffffff-ffff-ffff-ffff-ffffffffffff"
                     :customer-id Integer/MAX_VALUE
                     :product-id "PRODUCT-WITH-VERY-LONG-NAME-1234567890"
                     :quantity 1000
                     :unit-price 999999.99
                     :total 999999990.00
                     :timestamp Long/MAX_VALUE
                     :status "pending"}
          max-row (db/order->cassandra-row max-order)]

      (is (vector? max-row))
      (is (= 8 (count max-row)))

      ;; Test with zero and negative values (where applicable)
      (let [edge-order {:order-id "00000000-0000-0000-0000-000000000000"
                        :customer-id 1  ; Must be positive
                        :product-id "X"
                        :quantity 1    ; Must be positive
                        :unit-price 0.01
                        :total 0.01
                        :timestamp 0   ; Should handle zero timestamp
                        :status "pending"}
            edge-row (db/order->cassandra-row edge-order)]

        (is (vector? edge-row))
        (is (= 8 (count edge-row)))))))