(ns topology-test
  "Integration tests simulating the full topology flow."
  (:require [clojure.test :refer :all]
            [registry-processor.core :as core]
            [registry-processor.model :as model]
            [registry-processor.commands :as cmd]
            [registry-processor.validator :as validator]
            [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:import [org.apache.kafka.clients.consumer ConsumerRecord]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [java.util Properties]
           [java.util.concurrent Future TimeUnit]))

;; =============================================================================
;; Mocks
;; =============================================================================

(defn mock-producer-capture []
  (let [sent (atom [])
        props (doto (Properties.)
                (.put "bootstrap.servers" "localhost:9092")
                (.put "key.serializer" "org.apache.kafka.common.serialization.StringSerializer")
                (.put "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"))]
    {:producer (proxy [KafkaProducer] [props]
                 (send [^ProducerRecord record]
                   (swap! sent conj {:topic (.topic record)
                                     :key (.key record)
                                     :value (.value record)})
                   (reify Future
                     (get [_] nil)
                     (get [_ ^long t ^TimeUnit u] nil)
                     (isCancelled [_] false)
                     (isDone [_] true)
                     (cancel [_ _] false))))
     :sent sent}))

(def mock-session
  (reify com.datastax.oss.driver.api.core.CqlSession
    (^com.datastax.oss.driver.api.core.cql.ResultSet execute [_ ^com.datastax.oss.driver.api.core.cql.Statement _]
      (reify com.datastax.oss.driver.api.core.cql.ResultSet
        (one [_] nil)
        (iterator [_] (.iterator []))))))

(def mock-stmts {})

;; Helper to silence logs
(defmacro with-silent-logs [& body]
  `(with-redefs [log/info (fn [& _#] nil)
                 log/warn (fn [& _#] nil)
                 log/error (fn [& _#] nil)
                 log/debug (fn [& _#] nil)]
     ~@body))

;; =============================================================================
;; Topology Flow Tests
;; =============================================================================

(deftest full-flow-test
  (testing "Order Topic -> Validator -> Registry Topic -> Cassandra"
    (with-silent-logs
      (let [{:keys [producer sent]} (mock-producer-capture)
            stats-atom (atom (model/new-validation-stats "test-flow"))

            context {:producer producer
                     :session mock-session
                     :prepared-stmts mock-stmts
                     :registry-topic "registry"
                     :stats-atom stats-atom}

            valid-order {:order-id "ORD-FLOW-1"
                         :customer-id 100
                         :product-id "PROD-X"
                         :quantity 2
                         :unit-price 50.0
                         :total 100.0
                         :timestamp 1234567890
                         :status "pending"}

            order-json (json/generate-string valid-order)
            order-record (ConsumerRecord. "orders" 0 0 "ORD-FLOW-1" order-json)]

        ;; --- STEP 1: Consume from Orders ---
        (let [result (core/process-record context order-record)]
          (is (:success result))
          (is (get-in result [:validation-result :passed])))

        ;; Verify message sent to registry topic
        (is (= 1 (count @sent)))
        (let [registry-msg (first @sent)
              body (json/parse-string (:value registry-msg) true)]
          (is (= "registry" (:topic registry-msg)))
          (is (= "ORD-FLOW-1" (:key registry-msg)))
          (is (= "ORD-FLOW-1" (get-in body [:order :order-id])))
          (is (true? (get-in body [:validation-result :passed]))))

        ;; --- STEP 2: Consume from Registry ---
        (let [registry-json (:value (first @sent))
              registry-record (ConsumerRecord. "registry" 0 1 "ORD-FLOW-1" registry-json)]

          (with-redefs [cmd/execute (fn [_ cmd]
                                      (if (= :register (:type cmd))
                                        {:success true :registered-order "mocked"}
                                        {:success false}))]

            (let [result (core/process-record context registry-record)]
              (is (:success result))
              (is (= "mocked" (:registered-order result))))))))))

(deftest bad-data-flow-test
  (testing "Invalid Order -> Rejected -> Not sent to Registry"
    (with-silent-logs
      (let [{:keys [producer sent]} (mock-producer-capture)
            stats-atom (atom (model/new-validation-stats "test-flow"))
            context {:producer producer
                     :registry-topic "registry"
                     :stats-atom stats-atom}

            ;; initial model/valid-order? check, but we will mock the validator to fail.
            ;; Using positive total to avoid Schema Exception.
            invalid-order {:order-id "ORD-BAD-1"
                           :customer-id 100
                           :product-id "PROD-X"
                           :quantity 2000 ;; High quantity
                           :unit-price 50.0
                           :total 100000.0 ;; Valid positive total
                           :timestamp 1234567890
                           :status "pending"}

            order-json (json/generate-string invalid-order)
            order-record (ConsumerRecord. "orders" 0 0 "ORD-BAD-1" order-json)]

        ;; Mock validator to return false specifically for this test case
        (with-redefs [validator/validate-order (fn [order]
                                                 {:passed false
                                                  :rules []
                                                  :order-id (:order-id order)})]

          (let [result (core/process-record context order-record)]
            (is (:success result))
            (is (false? (get-in result [:validation-result :passed])))))

        ;; Verify NO message sent to registry topic
        (is (empty? @sent))

        ;; Verify stats updated (rejected count)
        (is (= 1 (:total-rejected @stats-atom)))))))