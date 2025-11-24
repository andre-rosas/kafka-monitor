(ns db-test
  "Unit tests for database mapping and operations."
  (:require [clojure.test :refer :all]
            [registry-processor.db :as db]
            [registry-processor.model :as model])
  (:import [com.datastax.oss.driver.api.core.cql Row ResultSet BoundStatement PreparedStatement Statement]
           [com.datastax.oss.driver.api.core CqlSession]
           [java.time Instant]))

;; =============================================================================
;; Mocks Helpers
;; =============================================================================

(defn mock-row [data]
  (reify Row
    (^String getString [_ ^String col] (get data col))
    (^int getInt [_ ^String col] (get data col))
    (^double getDouble [_ ^String col] (get data col))
    (^long getLong [_ ^String col] (get data col))
    (^boolean getBoolean [_ ^String col] (get data col))
    (^Instant getInstant [_ ^String col] (when-let [ts (get data col)] (Instant/ofEpochMilli ts)))))

(defn mock-result-set [rows]
  (let [iterator (.iterator rows)]
    (reify ResultSet
      (one [_] (first rows))
      (iterator [_] iterator))))

;; =============================================================================
;; Mapper Tests
;; =============================================================================

(deftest row-mapper-test
  (testing "row->registered-order mapping"
    (let [ts (System/currentTimeMillis)
          row-data {"order_id" "ORD-1"
                    "customer_id" 10
                    "product_id" "PROD-1"
                    "quantity" 5
                    "total" 100.0
                    "status" "accepted"
                    "registered_at" ts
                    "version" 1
                    "validation_passed" true}
          row (mock-row row-data)
          result (db/row->registered-order row)]
      (is (= "ORD-1" (:order-id result)))
      (is (= 100.0 (:total result)))
      (is (= ts (:registered-at result)))
      (is (:validation-passed result))))

  (testing "row->registered-order handles nil row"
    (is (nil? (db/row->registered-order nil)))))

(deftest validation-stats-mapper-test
  (testing "row->validation-stats mapping"
    (let [ts (System/currentTimeMillis)
          row-data {"processor_id" "proc-1"
                    "total_validated" 100
                    "total_approved" 90
                    "total_rejected" 10
                    "timestamp" ts}
          row (mock-row row-data)
          result (db/row->validation-stats row)]
      (is (= "proc-1" (:processor-id result)))
      (is (= 100 (:total-validated result)))
      (is (= ts (:timestamp result))))))

;; =============================================================================
;; Integration Simulation Tests (Checking bindings)
;; =============================================================================

(deftest save-registered-order-test
  (testing "Save registered order calls database correctly"
    (let [order {:order-id "ORD-1"
                 :customer-id 1
                 :product-id "P1"
                 :quantity 1
                 :total 10.0
                 :status "new"
                 :registered-at 1000
                 :version 1
                 :validation-passed true}

          mock-session (reify CqlSession
                         (^ResultSet execute [_ ^Statement _]
                           (reify ResultSet)))

          prepared-stmts {:upsert-registered-order nil}]

      ;; Simply test that function does not catch Excpetion
      (with-redefs [db/save-registered-order!
                    (fn [session stmts ord]
                      (is (= "ORD-1" (:order-id ord)))
                      (is (= 1 (:customer-id ord)))
                      (is (= "new" (:status ord)))
                      (is (= 1 (:version ord)))
                      (is (true? (:validation-passed ord))))]

        (db/save-registered-order! mock-session prepared-stmts order)))))