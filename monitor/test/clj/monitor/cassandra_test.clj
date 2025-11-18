(ns monitor.cassandra-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [monitor.cassandra :as cass]
            [monitor.config :as config]
            [clojure.tools.logging :as log])
  (:import [com.datastax.oss.driver.api.core CqlSession]
           [com.datastax.oss.driver.api.core.cql SimpleStatement ResultSet Row PreparedStatement Statement]
           [java.net InetSocketAddress]
           [java.util UUID]
           [java.time Instant]
           [java.util.concurrent CompletableFuture]
           [ch.qos.logback.classic Level]))

;; --- Mocks for Testing Cassandra Interaction ---

(def test-sessions (atom {}))

(defn create-mock-result-set
  "Creates a mock ResultSet with optional row data"
  [row-data]
  (reify ResultSet
    (iterator [_]
      (if row-data
        (.iterator (java.util.Collections/singletonList row-data))
        (.iterator (java.util.Collections/emptyList))))))

(def mock-session
  (reify CqlSession
    (^ResultSet execute [this ^Statement statement]
      (let [query (if (instance? SimpleStatement statement)
                    (.getQuery ^SimpleStatement statement)
                    (str statement))
            params (if (instance? SimpleStatement statement)
                     (into-array Object (.getPositionalValues ^SimpleStatement statement))
                     (into-array Object []))]

        (case query
          "SELECT now() FROM system.local"
          (create-mock-result-set
           (reify Row
             (^Instant getInstant [_ ^String col-name] (Instant/now))))

          "SELECT * FROM orders_by_customer WHERE customer_id = ?"
          (let [id (when (> (alength params) 0) (aget params 0))]
            (if (= 123 id)
              (create-mock-result-set
               (reify Row
                 (^int getInt [_ ^String s] (case s "customer_id" 123 "total_orders" 5 0))
                 (^double getDouble [_ ^String s] (case s "total_spent" 150.50 0.0))
                 (^String getString [_ ^String s] (case s "last_order_id" "A-1" nil))
                 (^Instant getInstant [_ ^String s] (case s "last_order_timestamp" (Instant/ofEpochMilli 1700000000000) nil))))
              (create-mock-result-set nil)))

          "SELECT * FROM registered_orders WHERE order_id = ?"
          (let [order-id (when (> (alength params) 0) (str (aget params 0)))]
            (if (= "11111111-1111-1111-1111-111111111111" order-id)
              (create-mock-result-set
               (reify Row
                 (^String getString [_ ^String s] (case s "order_id" order-id "product_id" "P-A" "status" "pending"))
                 (^int getInt [_ ^String s] (case s "customer_id" 1 "quantity" 1 "version" 1))
                 (^double getDouble [_ ^String s] (case s "total" 100.0))
                 (^boolean getBoolean [_ ^String s] (case s "validation_passed" false))
                 (^Instant getInstant [_ ^String s] (Instant/ofEpochMilli 1700000000000))))
              (create-mock-result-set nil)))

          "SELECT * FROM orders_timeline WHERE bucket_id = 0 ORDER BY timestamp DESC LIMIT ?"
          (create-mock-result-set
           (reify Row
             (^String getString [_ ^String s] (case s "order_id" "timeline-order-1" "product_id" "P-B" "status" "pending"))
             (^int getInt [_ ^String s] (case s "customer_id" 999))
             (^double getDouble [_ ^String s] (case s "total" 50.0))
             (^Instant getInstant [_ ^String s] (Instant/ofEpochMilli 1700000000000))))

          (cond
            (.startsWith query "UPDATE registered_orders SET status")
            (do (swap! test-sessions update :updated-order (fn [o] (assoc (or o {}) :status (aget params 0))))
                (create-mock-result-set nil))

            (.startsWith query "INSERT INTO registered_orders")
            (do (swap! test-sessions assoc :inserted-order {:order-id (str (aget params 0)) :status (aget params 5)})
                (create-mock-result-set nil))

            (.startsWith query "UPDATE validation_stats SET total_approved")
            (do (swap! test-sessions assoc :validation-approved true)
                (create-mock-result-set nil))

            (.startsWith query "UPDATE validation_stats SET total_rejected")
            (do (swap! test-sessions assoc :validation-rejected true)
                (create-mock-result-set nil))

            :else
            (create-mock-result-set nil)))))
    (closeAsync [_]
      (CompletableFuture/completedFuture nil))

    (close [_]
      nil)))

(defn get-mock-session-for-keyspace [keyspace]
  (if-let [s (get @test-sessions keyspace)]
    s
    (do (swap! test-sessions assoc keyspace mock-session) mock-session)))

(defmacro with-mock-cassandra [& body]
  `(with-redefs [cass/create-session (fn [keyspace#] (get-mock-session-for-keyspace keyspace#))
                 cass/sessions test-sessions
                 config/cassandra-config (fn [] {:host "mock" :port 9042 :datacenter "mockDC"})]
     (reset! test-sessions {})
     (try
       ~@body
       (finally
         (reset! test-sessions {})))))

;; --- Fixture for Resetting State ---
(defn session-cleanup-fixture [f]
  (f)
  (reset! cass/sessions {}))
(use-fixtures :once session-cleanup-fixture)

;; --- Tests ---

(deftest row-converters-test
  (let [mock-ts (Instant/ofEpochMilli 1700000000000)
        mock-uuid (UUID/fromString "11111111-1111-1111-1111-111111111111")]
    (testing "row->customer-stats"
      (let [mock-row (reify Row
                       (^int getInt [_ ^String s] (case s "customer_id" 1 "total_orders" 10))
                       (^double getDouble [_ ^String s] (case s "total_spent" 100.50))
                       (^String getString [_ ^String s] (case s "last_order_id" "ORD-1"))
                       (^Instant getInstant [_ ^String s] (case s "last_order_timestamp" mock-ts "first_order_timestamp" mock-ts)))]
        (is (= {:customer-id 1 :total-orders 10 :total-spent 100.50 :last-order-id "ORD-1"
                :last-order-timestamp 1700000000000 :first-order-timestamp 1700000000000}
               (cass/row->customer-stats mock-row)))))

    (testing "row->registered-order"
      (let [mock-row (reify Row
                       (^String getString [_ ^String s] (case s "order_id" (str mock-uuid) "product_id" "P-A" "status" "approved"))
                       (^int getInt [_ ^String s] (case s "customer_id" 2 "quantity" 5 "version" 1))
                       (^double getDouble [_ ^String s] (case s "total" 50.0))
                       (^boolean getBoolean [_ ^String s] (case s "validation_passed" true))
                       (^Instant getInstant [_ ^String s] (case s "registered_at" mock-ts "updated_at" mock-ts)))]
        (is (= {:order-id (str mock-uuid) :customer-id 2 :product-id "P-A" :quantity 5 :total 50.0 :status "approved"
                :registered-at 1700000000000 :updated-at 1700000000000 :version 1 :validation-passed true}
               (cass/row->registered-order mock-row)))))))

(deftest get-session-and-close-test
  (with-mock-cassandra
    (testing "get-session creates and caches a session"
      (is (false? (contains? @cass/sessions "test_keyspace")))
      (let [session1 (cass/get-session "test_keyspace")]
        (is (true? (contains? @cass/sessions "test_keyspace")))
        (is (= session1 (cass/get-session "test_keyspace")))))))

(deftest check-connection-test
  (with-mock-cassandra
    (testing "Successful connection"
      (is (true? (cass/check-connection))))

    (testing "Failed connection (Mock exception on execute)"
      ;; Silenciar logs de erro esperados durante este teste
      (let [original-level (.getLevel (org.slf4j.LoggerFactory/getLogger "monitor.cassandra"))]
        (try
          (.setLevel (org.slf4j.LoggerFactory/getLogger "monitor.cassandra")
                     ch.qos.logback.classic.Level/OFF)
          (with-redefs [cass/get-session (fn [_] (throw (Exception. "Connection failed")))]
            (is (false? (cass/check-connection))))
          (finally
            (.setLevel (org.slf4j.LoggerFactory/getLogger "monitor.cassandra")
                       original-level)))))))

(deftest query-processor-queries-test
  (with-mock-cassandra
    (testing "get-customer-stats - found"
      (let [stats (cass/get-customer-stats 123)]
        (is (= 123 (:customer-id stats)))
        (is (= 150.50 (:total-spent stats)))))

    (testing "get-customer-stats - not found"
      (is (nil? (cass/get-customer-stats 999))))))

(deftest registry-processor-queries-test
  (with-mock-cassandra
    (testing "update-order-status - existing order"
      (is (true? (cass/update-order-status "11111111-1111-1111-1111-111111111111" "approved")))
      (is (= "approved" (:status (:updated-order @test-sessions)))))

    (testing "update-validation-stats! - approved"
      (reset! test-sessions {:validation-approved false})
      (cass/update-validation-stats! "approved")
      (is (true? (:validation-approved @test-sessions))))

    (testing "update-validation-stats! - denied"
      (reset! test-sessions {:validation-rejected false})
      (cass/update-validation-stats! "denied")
      (is (true? (:validation-rejected @test-sessions))))))

(deftest aggregated-stats-test
  (with-mock-cassandra
    (testing "get-all-stats returns correct structure"
      (with-redefs [cass/get-all-customers (fn [] [{:customer-id 1 :total-spent 100.0} {:customer-id 2 :total-spent 50.0}])
                    cass/get-all-products (fn [] [{:product-id "P1" :total-revenue 200.0} {:product-id "P2" :total-revenue 10.0}])
                    cass/get-all-registered-orders (fn [] [{:status "approved"} {:status "denied"} {:status "pending"}])]
        (let [stats (cass/get-all-stats)]
          (is (contains? stats :query-processor))
          (is (= 2 (:customer-count (:query-processor stats))))
          (is (= 150.0 (:total-customers-spent (:query-processor stats))))
          (is (= 210.0 (:total-revenue (:query-processor stats))))
          (is (contains? stats :registry-processor))
          (is (= 3 (:registered-count (:registry-processor stats))))
          (is (= 1 (:approved-count (:registry-processor stats))))
          (is (= 1 (:denied-count (:registry-processor stats))))
          (is (= 1 (:pending-count (:registry-processor stats))))
          (is (contains? stats :timestamp)))))))