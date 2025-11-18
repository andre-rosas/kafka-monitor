(ns commands-test
  "Comprehensive tests for registry-processor command system."
  (:require [clojure.test :refer :all]
            [registry-processor.commands :as cmd]
            [registry-processor.model :as model]
            [registry-processor.validator :as validator]
            [registry-processor.db :as db]
            [clojure.tools.logging :as log])
  (:import [com.datastax.oss.driver.api.core.cql BoundStatement ResultSet Statement PreparedStatement]
           [com.datastax.oss.driver.api.core CqlSession]))

;; =============================================================================
;; Test Fixtures and Mocks
;; =============================================================================

(def valid-order
  "Sample valid order for testing."
  {:order-id "ORDER-TEST-123"
   :customer-id 42
   :product-id "PROD-TEST-456"
   :quantity 5
   :unit-price 30.0
   :total 150.0
   :timestamp 1234567890
   :status "approved"})

(def invalid-order
  "Sample invalid order for testing (exceeds quantity limit)."
  {:order-id "ORDER-TEST-123"
   :customer-id 42
   :product-id "PROD-TEST-456"
   :quantity 2000
   :unit-price 30.0
   :total 60000.0
   :timestamp 1234567890
   :status "approved"})

(def mock-session
  "Mock Cassandra session for testing."
  (reify CqlSession
    (close [this])

    (^ResultSet execute [this ^String query]
      (reify ResultSet
        (one [this] nil)
        (iterator [this]
          (.iterator (java.util.ArrayList.)))))

    (^ResultSet execute [this ^Statement statement]
      (reify ResultSet
        (one [this] nil)
        (iterator [this]
          (.iterator (java.util.ArrayList.)))))))

(defn create-mock-prepared-statement
  "Create a mock prepared statement that implements bind."
  []
  (proxy [PreparedStatement] []
    (bind [values]
      (reify BoundStatement))))

(def mock-prepared-stmts
  "Mock prepared statements for testing."
  {:get-registered-order (create-mock-prepared-statement)
   :upsert-registered-order (create-mock-prepared-statement)
   :insert-order-update (create-mock-prepared-statement)
   :upsert-validation-stats (create-mock-prepared-statement)})

;; Helper to silence logs during tests.
;; We redefine `log*` because the logging macros (info, error) are often
;; expanded at compile time in the source code, so redefining the macro
;; symbols here won't stop the output. Redefining the implementation function works.
(defmacro with-silent-logs [& body]
  `(with-redefs [clojure.tools.logging/log* (fn [_# _# _# _#] nil)]
     ~@body))

;; =============================================================================
;; Command Tests
;; =============================================================================

(deftest validate-command-test
  (testing "Validate command with valid order"
    (with-silent-logs
      (let [context {:stats-atom (atom (model/new-validation-stats "test"))}]
        (with-redefs [validator/validate-order (fn [order]
                                                 {:passed true :rules [] :order-id (:order-id order)})]
          (let [result (cmd/execute context {:type :validate :data valid-order})]
            (is (:success result))
            (is (get-in result [:validation-result :passed]))
            (is (= 1 (get @(:stats-atom context) :total-validated)))
            (is (= 1 (get @(:stats-atom context) :total-approved))))))))

  (testing "Validate command with invalid order"
    (with-silent-logs
      (let [context {:stats-atom (atom (model/new-validation-stats "test"))}]
        (with-redefs [validator/validate-order (fn [order]
                                                 {:passed false :rules [] :order-id (:order-id order)})]
          (let [result (cmd/execute context {:type :validate :data invalid-order})]
            (is (:success result))
            (is (not (get-in result [:validation-result :passed])))
            (is (= 1 (get @(:stats-atom context) :total-validated)))
            (is (= 1 (get @(:stats-atom context) :total-rejected))))))))

  (testing "Validate command with malformed order data"
    (with-silent-logs
      (let [context {:stats-atom (atom (model/new-validation-stats "test"))}
            malformed-order {:invalid "data"}]
        (let [result (cmd/execute context {:type :validate :data malformed-order})]
          (is (not (:success result)))
          (is (contains? result :error))
          (is (= 1 (get @(:stats-atom context) :total-rejected))))))))

(deftest register-command-test
  (testing "Register new valid order"
    (with-silent-logs
      (let [context {:session mock-session
                     :prepared-stmts mock-prepared-stmts
                     :stats-atom (atom (model/new-validation-stats "test"))}]

        (with-redefs [db/get-registered-order (fn [session stmts order-id] nil)
                      db/save-registered-order! (fn [session stmts registered-order] nil)
                      db/save-order-update! (fn [session stmts update-record] nil)]

          (let [result (cmd/execute context {:type :register
                                             :data {:order valid-order
                                                    :validation-passed true}})]
            (is (:success result))
            (is (= "ORDER-TEST-123" (:order-id result)))
            (is (contains? result :registered-order)))))))

  (testing "Register existing order with status update"
    (with-silent-logs
      (let [existing-order (model/new-registered-order valid-order true)
            context {:session mock-session
                     :prepared-stmts mock-prepared-stmts
                     :stats-atom (atom (model/new-validation-stats "test"))}]

        (with-redefs [db/get-registered-order (fn [session stmts order-id] existing-order)
                      db/save-registered-order! (fn [session stmts registered-order] nil)
                      db/save-order-update! (fn [session stmts update-record] nil)]

          (let [updated-order (assoc valid-order :status "shipped")
                result (cmd/execute context {:type :register
                                             :data {:order updated-order
                                                    :validation-passed true}})]
            (is (:success result))
            (is (= "ORDER-TEST-123" (:order-id result)))
            (is (contains? result :registered-order)))))))

  (testing "Skip unchanged order"
    (with-silent-logs
      (let [existing-order (model/new-registered-order valid-order true)
            context {:session mock-session
                     :prepared-stmts mock-prepared-stmts
                     :stats-atom (atom (model/new-validation-stats "test"))}]

        (with-redefs [db/get-registered-order (fn [session stmts order-id] existing-order)
                      db/save-registered-order! (fn [session stmts registered-order] nil)]

          (let [result (cmd/execute context {:type :register
                                             :data {:order valid-order
                                                    :validation-passed true}})]
            (is (:success result))
            (is (= "ORDER-TEST-123" (:order-id result)))
            (is (:skipped result)))))))

  (testing "Handle database error during registration"
    (with-silent-logs
      (let [context {:session mock-session
                     :prepared-stmts mock-prepared-stmts
                     :stats-atom (atom (model/new-validation-stats "test"))}]

        (with-redefs [db/get-registered-order (fn [session stmts order-id]
                                                (throw (Exception. "Database connection failed")))]

          (let [result (cmd/execute context {:type :register
                                             :data {:order valid-order
                                                    :validation-passed true}})]
            (is (not (:success result)))
            (is (contains? result :error))))))))

(deftest health-check-command-test
  (testing "Health check returns healthy status"
    (with-silent-logs
      (let [context {:session mock-session
                     :stats-atom (atom (model/new-validation-stats "test"))}]
        (let [result (cmd/execute context {:type :health-check})]
          (is (:success result))
          (is (= "healthy" (:status result)))
          (is (= "connected" (:cassandra result)))))))

  (testing "Health check returns unhealthy status on database error"
    (with-silent-logs
      (let [broken-session (reify CqlSession
                             (^ResultSet execute [_ ^String _]
                               (throw (Exception. "Connection failed")))
                             (^ResultSet execute [_ ^Statement _]
                               (throw (Exception. "Connection failed"))))
            context {:session broken-session
                     :stats-atom (atom (model/new-validation-stats "test"))}]

        (let [result (cmd/execute context {:type :health-check})]
          (is (not (:success result)))
          (is (= "unhealthy" (:status result)))
          (is (contains? result :error)))))))

(deftest get-stats-command-test
  (testing "Get stats returns current statistics"
    (with-silent-logs
      (let [context {:stats-atom (atom (model/new-validation-stats "test"))}
            result (cmd/execute context {:type :get-stats})]
        (is (:success result))
        (is (contains? result :stats))))))

(deftest default-command-test
  (testing "Unknown command type returns error"
    (with-silent-logs
      (let [context {}]
        (let [result (cmd/execute context {:type :unknown-command})]
          (is (not (:success result)))
          (is (contains? result :error)))))))