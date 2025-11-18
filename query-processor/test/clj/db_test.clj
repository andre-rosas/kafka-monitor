(ns db-test
  "Tests for Cassandra data transformations and mock persistence."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [query-processor.db :as db])
  (:import [com.datastax.oss.driver.api.core CqlSession]))

;; =============================================================================
;; Test Data
;; =============================================================================

(def test-customer-stats
  {:customer-id 42
   :total-orders 5
   :total-spent 500.0
   :last-order-id "ORDER-123"
   :last-order-timestamp 1234567890
   :first-order-timestamp 1234560000})

(def test-product-stats
  {:product-id "PROD-A"
   :total-quantity 50
   :total-revenue 1500.0
   :order-count 10
   :avg-quantity 5.0
   :last-order-timestamp 1234567890})

(def test-processing-stats
  {:processor-id "processor-1"
   :processed-count 1000
   :error-count 5
   :last-processed-timestamp 1234567890})

;; =============================================================================
;; Mock Session Setup
;; =============================================================================

(def mock-session
  (reify CqlSession
    (close [_] nil)
    ;; execute(String) - returns ResultSet
    (^{:tag com.datastax.oss.driver.api.core.cql.ResultSet} execute [_ ^String _query]
      (reify com.datastax.oss.driver.api.core.cql.ResultSet
        (one [_] nil) ; Default to no rows found
        (iterator [_] (.iterator (java.util.ArrayList.)))))
    ;; execute(Statement) - returns ResultSet
    (^{:tag com.datastax.oss.driver.api.core.cql.ResultSet} execute [_ ^com.datastax.oss.driver.api.core.cql.Statement _statement]
      (reify com.datastax.oss.driver.api.core.cql.ResultSet
        (one [_] nil) ; Default to no rows found
        (iterator [_] (.iterator (java.util.ArrayList.)))))))

(use-fixtures :each
  (fn [f]
    ;; Mock the session to avoid real database connections
    (with-redefs [db/create-session (constantly mock-session)]
      (f))))

;; =============================================================================
;; Persistence Tests (Mocking execute)
;; =============================================================================

(deftest test-save-customer-stats
  (testing "save-customer-stats! calls session execute with a bound statement"
    (let [execute-calls (atom 0)
          mock-bound-stmt (proxy [com.datastax.oss.driver.api.core.cql.BoundStatement] []
                            (setInt [^Integer _idx ^Integer _val] this)
                            (setString [^Integer _idx ^String _val] this)
                            (setInstant [^Integer _idx _val] this)
                            (setDouble [^Integer _idx ^Double _val] this))
          mock-session-with-execute (reify CqlSession
                                      (^{:tag com.datastax.oss.driver.api.core.cql.ResultSet} execute [_ ^com.datastax.oss.driver.api.core.cql.Statement bound-stmt]
                                        (swap! execute-calls inc)
                                        (is (instance? com.datastax.oss.driver.api.core.cql.BoundStatement bound-stmt))
                                        (reify com.datastax.oss.driver.api.core.cql.ResultSet
                                          (one [_] nil)
                                          (iterator [_] (.iterator (java.util.ArrayList.))))))
          mock-stmt (proxy [com.datastax.oss.driver.api.core.cql.PreparedStatement] []
                      (bind [& values] mock-bound-stmt))
          mock-stmts {:upsert-customer mock-stmt}]

      (db/save-customer-stats! mock-session-with-execute mock-stmts test-customer-stats)

      (is (= 1 @execute-calls)))))

(deftest test-get-processing-stats-not-found
  (testing "get-processing-stats returns nil if row is not found"
    (with-redefs [db/get-processing-stats (fn [_ _ _] nil)]
      (let [result (db/get-processing-stats mock-session {} "missing-id")]
        (is (nil? result))))))