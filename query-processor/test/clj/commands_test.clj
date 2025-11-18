(ns commands-test
  "Tests for command system multimethod."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [query-processor.commands :as cmd]
            [query-processor.aggregator :as agg]
            [query-processor.db :as db]
            [clojure.tools.logging :as log])
  (:import [com.datastax.oss.driver.api.core CqlSession]))

;; =============================================================================
;; Test Setup
;; =============================================================================

(def mock-session
  (reify CqlSession
    (close [_] nil)
    ;; execute(String) - returns ResultSet
    (^{:tag com.datastax.oss.driver.api.core.cql.ResultSet} execute [_ ^String _query]
      (reify com.datastax.oss.driver.api.core.cql.ResultSet
        (one [_] nil)
        (iterator [_] (.iterator (java.util.ArrayList.)))))
    ;; execute(Statement) - returns ResultSet
    (^{:tag com.datastax.oss.driver.api.core.cql.ResultSet} execute [_ ^com.datastax.oss.driver.api.core.cql.Statement _statement]
      (reify com.datastax.oss.driver.api.core.cql.ResultSet
        (one [_] nil)
        (iterator [_] (.iterator (java.util.ArrayList.)))))))

(defn create-test-context
  []
  (let [views-atom (atom (agg/init-views "test-processor"))
        processor-config {:processor-id "test-processor"
                          :timeline-max-size 100}]
    {:session mock-session
     :prepared-stmts {}
     :views-atom views-atom
     :processor-config processor-config}))

(def test-order
  {:order-id "ORDER-123"
   :customer-id 42
   :product-id "PROD-A"
   :quantity 5
   :unit-price 30.0
   :total 150.0
   :timestamp 1234567890
   :status "accepted"})

(use-fixtures :each
  (fn [f]
    ;; Silence all logs during tests
    (with-redefs [query-processor.db/save-all-views! (constantly nil)
                  log/debug (constantly nil)
                  log/info (constantly nil)
                  log/warn (constantly nil)
                  log/error (constantly nil)]
      (f))))

;; =============================================================================
;; Command: Consume Tests
;; =============================================================================

(deftest test-consume-valid-order
  (testing "Consume command processes valid order"
    (let [context (create-test-context)
          result (cmd/execute context {:type :consume :data test-order})]

      (is (true? (:success result)))
      (is (= "ORDER-123" (:order-id result)))
      (is (some? (:views result)))

      (let [views @(:views-atom context)]
        (is (some? (get-in views [:customer-stats 42])))
        (is (some? (get-in views [:product-stats "PROD-A"])))
        (is (= 1 (count (:timeline views))))))))

(deftest test-consume-invalid-order
  (testing "Consume command rejects invalid order"
    (let [context (create-test-context)
          invalid-order {:order-id "INVALID"}
          result (cmd/execute context {:type :consume :data invalid-order})]

      (is (false? (:success result)))
      (is (some? (:error result))))))

(deftest test-consume-increments-stats
  (testing "Consume command increments processing stats"
    (let [context (create-test-context)]

      (cmd/execute context {:type :consume :data test-order})

      (let [views @(:views-atom context)
            stats (:processing-stats views)]
        (is (= 1 (:processed-count stats)))
        (is (= 0 (:error-count stats)))))))

(deftest test-consume-increments-errors
  (testing "Consume command increments error count on failure"
    (let [context (create-test-context)
          invalid-order {}]

      (cmd/execute context {:type :consume :data invalid-order})

      (let [views @(:views-atom context)
            stats (:processing-stats views)]
        (is (= 0 (:processed-count stats)))
        (is (= 1 (:error-count stats)))))))

;; =============================================================================
;; Command: Persist Tests
;; =============================================================================

(deftest test-persist-with-session
  (testing "Persist command succeeds with valid session"
    (let [context (create-test-context)
          result (cmd/execute context {:type :persist})]

      (is (true? (:success result))))))

(deftest test-persist-without-session
  (testing "Persist command handles missing session"
    (let [context (assoc (create-test-context) :session nil)
          result (cmd/execute context {:type :persist})]

      (is (false? (:success result)))
      (is (some? (:error result))))))

;; =============================================================================
;; Command: Health Check Tests
;; =============================================================================

(deftest test-health-check-healthy
  (testing "Health check returns healthy status"
    (let [context (create-test-context)
          result (cmd/execute context {:type :health-check})]

      (is (true? (:success result)))
      (is (= "healthy" (:status result)))
      (is (= "connected" (:cassandra result)))
      (is (some? (:stats result))))))

(deftest test-health-check-unhealthy
  (testing "Health check returns unhealthy on database error"
    (let [failing-session (reify CqlSession
                            (^{:tag com.datastax.oss.driver.api.core.cql.ResultSet} execute [_ ^String _query] (throw (Exception. "Connection failed")))
                            (^{:tag com.datastax.oss.driver.api.core.cql.ResultSet} execute [_ ^com.datastax.oss.driver.api.core.cql.Statement _stmt] (throw (Exception. "Connection failed"))))
          context (assoc (create-test-context) :session failing-session)
          result (cmd/execute context {:type :health-check})]

      (is (false? (:success result)))
      (is (= "unhealthy" (:status result)))
      (is (some? (:error result))))))

;; =============================================================================
;; Command: Get Stats Tests
;; =============================================================================

(deftest test-get-stats-empty-views
  (testing "Get stats returns zero counts for empty views"
    (let [context (create-test-context)
          result (cmd/execute context {:type :get-stats})]

      (is (true? (:success result)))
      (is (= 0 (get-in result [:stats :customer-count])))
      (is (= 0 (get-in result [:stats :product-count])))
      (is (= 0 (get-in result [:stats :timeline-size]))))))

(deftest test-get-stats-with-data
  (testing "Get stats returns correct counts after processing"
    (let [context (create-test-context)]

      (cmd/execute context {:type :consume :data test-order})
      (cmd/execute context {:type :consume :data (assoc test-order :order-id "ORDER-124")})

      (let [result (cmd/execute context {:type :get-stats})]
        (is (true? (:success result)))
        (is (= 1 (get-in result [:stats :customer-count])))
        (is (= 1 (get-in result [:stats :product-count])))
        (is (= 2 (get-in result [:stats :timeline-size])))
        (is (= 2 (get-in result [:stats :processing-stats :processed-count])))))))

;; =============================================================================
;; Command: Reset Tests
;; =============================================================================

(deftest test-reset-clears-views
  (testing "Reset command clears all views"
    (let [context (create-test-context)]

      (cmd/execute context {:type :consume :data test-order})

      (is (= 1 (count (:customer-stats @(:views-atom context)))))

      (let [result (cmd/execute context {:type :reset})]
        (is (true? (:success result)))

        (let [views @(:views-atom context)]
          (is (empty? (:customer-stats views)))
          (is (empty? (:product-stats views)))
          (is (empty? (:timeline views))))))))

;; =============================================================================
;; Command: Default Handler Tests
;; =============================================================================

(deftest test-unknown-command
  (testing "Unknown command returns error"
    (let [context (create-test-context)
          result (cmd/execute context {:type :unknown-command})]

      (is (false? (:success result)))
      (is (some? (:error result)))
      (is (.contains (:error result) "Unknown command type")))))