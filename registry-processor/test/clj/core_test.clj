(ns core-test
  "Comprehensive tests for registry-processor core functionality including Kafka integration."
  (:require [clojure.test :refer :all]
            [registry-processor.core :as core]
            [registry-processor.config :as config]
            [registry-processor.model :as model]
            [registry-processor.commands :as commands]
            [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:import [org.apache.kafka.clients.consumer ConsumerRecord]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord RecordMetadata]
           [org.apache.kafka.common TopicPartition]
           [java.time Duration]
           [java.util.concurrent Future TimeUnit]
           [java.util Properties]))

(def valid-order
  {:order-id "ORDER-TEST-123"
   :customer-id 42
   :product-id "PROD-TEST-456"
   :quantity 5
   :unit-price 30.0
   :total 150.0
   :timestamp 1234567890
   :status "accepted"})

(def valid-registry-message
  {:order valid-order
   :validation-result {:passed true
                       :rules []
                       :order-id "ORDER-TEST-123"}
   :timestamp 1234567890})

(defn create-mock-consumer-record [topic key value]
  (ConsumerRecord. topic 0 0 key value))

(defn create-mock-producer
  "Create a properly mocked Kafka producer."
  []
  (let [sent-records (atom [])
        ;; CORREÇÃO: Properties obrigatórias para o construtor do KafkaProducer
        props (doto (Properties.)
                (.put "bootstrap.servers" "localhost:9092")
                (.put "key.serializer" "org.apache.kafka.common.serialization.StringSerializer")
                (.put "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"))]
    (proxy [KafkaProducer] [props]
      (send
        ([^ProducerRecord record]
         (swap! sent-records conj record)
         (let [metadata (RecordMetadata.
                         (TopicPartition. (.topic record) 0)
                         0 0 0 (Long/valueOf 0) 0)]
           (reify Future
             (get [this] metadata)
             (get [this ^long timeout ^TimeUnit unit] metadata)
             (isCancelled [this] false)
             (isDone [this] true)
             (cancel [this interrupt?] false))))

        ([^ProducerRecord record callback]
         (swap! sent-records conj record)
         (let [metadata (RecordMetadata.
                         (TopicPartition. (.topic record) 0)
                         0 0 0 (Long/valueOf 0) 0)]
           (when callback
             (.onCompletion callback metadata nil))
           (reify Future
             (get [this] metadata)
             (get [this ^long timeout ^TimeUnit unit] metadata)
             (isCancelled [this] false)
             (isDone [this] true)
             (cancel [this interrupt?] false)))))

      (flush [] nil)
      (close
        ([] nil)
        ([^Duration timeout] nil))
      (partitionsFor [topic] (java.util.ArrayList.))
      (metrics [] (java.util.Collections/emptyMap)))))

(def mock-producer (create-mock-producer))

(def mock-session
  (reify com.datastax.oss.driver.api.core.CqlSession
    (close [this])
    (^com.datastax.oss.driver.api.core.cql.ResultSet execute [this ^String query]
      (reify com.datastax.oss.driver.api.core.cql.ResultSet
        (one [this] nil)
        (iterator [this] (.iterator (java.util.ArrayList.)))))
    (^com.datastax.oss.driver.api.core.cql.ResultSet execute [this ^com.datastax.oss.driver.api.core.cql.Statement statement]
      (reify com.datastax.oss.driver.api.core.cql.ResultSet
        (one [this] nil)
        (iterator [this] (.iterator (java.util.ArrayList.)))))))

(deftest process-order-record-test
  (testing "Process valid order record"
    (let [json-order (json/generate-string valid-order)
          record (create-mock-consumer-record "orders" "ORDER-TEST-123" json-order)
          context {:producer mock-producer
                   :registry-topic "registry"
                   :stats-atom (atom (model/new-validation-stats "test"))}]

      (with-redefs [commands/execute (fn [ctx cmd]
                                       (when (= :validate (:type cmd))
                                         {:success true
                                          :order-id "ORDER-TEST-123"
                                          :validation-result {:passed true}}))
                    core/send-to-registry! (fn [producer topic order validation-result] nil)]

        (let [result (core/process-order-record context record)]
          (is (:success result))
          (is (= "ORDER-TEST-123" (:order-id result))))))))

(deftest process-registry-record-test
  (testing "Process valid registry record"
    (let [json-message (json/generate-string valid-registry-message)
          record (create-mock-consumer-record "registry" "ORDER-TEST-123" json-message)
          context {:session mock-session
                   :prepared-stmts {}
                   :stats-atom (atom (model/new-validation-stats "test"))}]

      (with-redefs [commands/execute (fn [ctx cmd]
                                       (when (= :register (:type cmd))
                                         {:success true
                                          :order-id "ORDER-TEST-123"
                                          :registered-order (model/new-registered-order valid-order true)}))]

        (let [result (core/process-registry-record context record)]
          (is (:success result))
          (is (not (:skipped result))))))))

(deftest process-record-test
  (testing "Process record from orders topic"
    (let [json-order (json/generate-string valid-order)
          record (create-mock-consumer-record "orders" "ORDER-TEST-123" json-order)
          context {:producer mock-producer :registry-topic "registry"}]

      (with-redefs [core/process-order-record (fn [ctx rec] {:success true})]
        (let [result (core/process-record context record)]
          (is (:success result)))))))