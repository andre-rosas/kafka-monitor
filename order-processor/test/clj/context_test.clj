(ns context-test
  "Tests for application context and dependency injection."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [order-processor.context :as context]))

;; =============================================================================
;; Test Setup
;; =============================================================================

(use-fixtures :each
  (fn [f]
    (f)))

;; =============================================================================
;; Mock Implementations Tests
;; =============================================================================

(deftest test-mock-producer-send-message
  (testing "MockProducer sends messages and stores them in atom"
    (let [producer (context/->MockProducer (atom []))]
      ;; Send multiple messages
      (context/send-message! producer "topic1" "key1" "value1" nil)
      (context/send-message! producer "topic2" "key2" "value2" nil)

      (let [sent-messages @(:sent-messages-atom producer)]
        (is (= 2 (count sent-messages)))
        (is (= "topic1" (:topic (first sent-messages))))
        (is (= "key2" (:key (second sent-messages))))
        (is (= "value2" (:value (second sent-messages))))))))

(deftest test-mock-producer-callback
  (testing "MockProducer calls callback when provided"
    (let [callback-called (atom false)
          callback (reify org.apache.kafka.clients.producer.Callback
                     (onCompletion [this metadata exception]
                       (reset! callback-called true)))
          producer (context/->MockProducer (atom []))]

      (context/send-message! producer "test-topic" "key" "value" callback)
      (is @callback-called "Callback should be called"))))

(deftest test-mock-state-store-operations
  (testing "MockStateStore basic CRUD operations"
    (let [store (context/->MockStateStore (atom {}))
          order1 {:order-id "order-1" :customer-id 1 :product-id "P1"}
          order2 {:order-id "order-2" :customer-id 2 :product-id "P2"}]

      ;; Test save and retrieve
      (context/save-order! store order1)
      (context/save-order! store order2)

      (is (= order1 (context/get-order store "order-1")))
      (is (= order2 (context/get-order store "order-2")))
      (is (nil? (context/get-order store "non-existent")))

      ;; Test stats
      (let [stats (context/get-stats store)]
        (is (= {:total-orders 2} stats))))))

(deftest test-mock-state-store-update
  (testing "MockStateStore overwrites existing orders"
    (let [store (context/->MockStateStore (atom {}))
          original-order {:order-id "order-1" :customer-id 1 :product-id "P1"}
          updated-order {:order-id "order-1" :customer-id 1 :product-id "P2"}]

      (context/save-order! store original-order)
      (context/save-order! store updated-order)

      (is (= updated-order (context/get-order store "order-1")))
      (let [stats (context/get-stats store)]
        (is (= {:total-orders 1} stats))))))

;; =============================================================================
;; Test Context Creation
;; =============================================================================

(deftest test-create-test-context-structure
  (testing "create-test-context returns proper structure with mock implementations"
    (let [ctx (context/create-test-context)]
      (is (map? ctx))
      (is (contains? ctx :producer))
      (is (contains? ctx :state-store))
      (is (contains? ctx :config))

      ;; Verify components are proper types
      (is (satisfies? context/IProducer (:producer ctx)))
      (is (satisfies? context/IStateStore (:state-store ctx)))

      ;; Verify config structure
      (is (map? (:config ctx)))
      (is (contains? (:config ctx) :kafka))
      (is (contains? (:config ctx) :producer))
      (is (contains? (:config ctx) :orders))

      ;; Verify config values
      (is (= "test-orders" (get-in ctx [:config :kafka :topic])))
      (is (= 100 (get-in ctx [:config :producer :rate-per-second])))
      (is (= [1 100] (get-in ctx [:config :orders :customer-ids]))))))

(deftest test-test-context-isolation
  (testing "Different test contexts have isolated state"
    (let [ctx1 (context/create-test-context)
          ctx2 (context/create-test-context)
          order {:order-id "test-order" :customer-id 1 :product-id "P1"}]

      ;; Save order in first context
      (context/save-order! (:state-store ctx1) order)

      ;; Should not be visible in second context
      (is (nil? (context/get-order (:state-store ctx2) "test-order"))))))

;; =============================================================================
;; Integration Tests with Test Context
;; =============================================================================

(deftest test-producer-state-store-integration
  (testing "Producer and StateStore work together in test context"
    (let [ctx (context/create-test-context)
          producer (:producer ctx)
          state-store (:state-store ctx)
          test-order {:order-id "test-order" :customer-id 42 :product-id "TEST-PROD"}]

      ;; Save order to state store
      (context/save-order! state-store test-order)

      ;; Verify order can be retrieved
      (is (= test-order (context/get-order state-store "test-order")))

      ;; Send message via producer
      (context/send-message! producer "test-topic" "order-key" "order-value" nil)

      ;; Verify message was sent
      (let [sent-messages @(:sent-messages-atom producer)]
        (is (= 1 (count sent-messages)))
        (is (= "test-topic" (:topic (first sent-messages))))
        (is (= "order-key" (:key (first sent-messages))))
        (is (= "order-value" (:value (first sent-messages))))))))

(deftest test-multiple-operations
  (testing "Multiple operations maintain consistency"
    (let [ctx (context/create-test-context)
          producer (:producer ctx)
          state-store (:state-store ctx)]

      ;; Perform multiple operations
      (dotimes [i 5]
        (let [order {:order-id (str "order-" i) :customer-id i :product-id (str "P" i)}]
          (context/save-order! state-store order)
          (context/send-message! producer "topic" (str "key-" i) (str "value-" i) nil)))

      ;; Verify all orders saved
      (dotimes [i 5]
        (is (not (nil? (context/get-order state-store (str "order-" i))))))

      ;; Verify all messages sent
      (let [sent-messages @(:sent-messages-atom producer)]
        (is (= 5 (count sent-messages)))
        (is (every? #(= "topic" (:topic %)) sent-messages))))))