(ns registry-processor.core
  "Main entry point for registry-processor.
  
  This processor:
  1. Consumes from 'orders' topic
  2. Validates orders against business rules
  3. Produces approved orders to 'registry' topic
  4. Consumes from 'registry' topic (its own output)
  5. Persists to Cassandra"
  (:gen-class)
  (:require [registry-processor.config :as config]
            [registry-processor.model :as model]
            [registry-processor.validator :as validator]
            [registry-processor.db :as db]
            [registry-processor.commands :as cmd]
            [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer ConsumerConfig ConsumerRecord]
           [org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerRecord]
           [org.apache.kafka.common.serialization StringDeserializer StringSerializer]
           [java.time Duration]
           [java.util Properties]))

;; =============================================================================
;; Retry Logic
;; =============================================================================

(defn retry-with-backoff
  "Execute function with retry and exponential backoff."
  [f max-retries initial-delay-ms max-delay-ms]
  (loop [attempt 1
         delay-ms initial-delay-ms]
    (let [result (try
                   {:success true :value (f)}
                   (catch Exception e
                     {:success false :error e}))]
      (if (:success result)
        (:value result)
        (if (>= attempt max-retries)
          (throw (:error result))
          (do
            (log/warn "Retry attempt failed"
                      {:attempt attempt
                       :max-retries max-retries
                       :delay-ms delay-ms
                       :error (.getMessage (:error result))})
            (Thread/sleep delay-ms)
            (recur (inc attempt) (min (* delay-ms 2) max-delay-ms))))))))

;; =============================================================================
;; Kafka Helpers
;; =============================================================================

(defn map->properties
  "Convert map to Java Properties."
  [m]
  (let [props (Properties.)]
    (doseq [[k v] m]
      (.put props (name k) (str v)))
    props))

;; =============================================================================
;; Kafka Consumer Setup
;; =============================================================================

(defn create-consumer
  "Create Kafka consumer."
  [kafka-config]
  (let [{:keys [bootstrap-servers group-id]} kafka-config
        props (map->properties
               {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers
                ConsumerConfig/GROUP_ID_CONFIG group-id
                ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG (.getName StringDeserializer)
                ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG (.getName StringDeserializer)
                ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG "false"
                ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"
                ConsumerConfig/MAX_POLL_RECORDS_CONFIG "500"})]

    (log/info "Creating Kafka consumer" {:group-id group-id})
    (KafkaConsumer. props)))

(defn subscribe-topics
  "Subscribe consumer to topics."
  [^KafkaConsumer consumer topics]
  (log/info "Subscribing to topics" {:topics topics})
  (.subscribe consumer topics))

;; =============================================================================
;; Kafka Producer Setup
;; =============================================================================

(defn create-producer
  "Create Kafka producer."
  [kafka-config]
  (let [{:keys [bootstrap-servers]} kafka-config
        props (map->properties
               {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers
                ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG (.getName StringSerializer)
                ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG (.getName StringSerializer)
                ProducerConfig/ACKS_CONFIG "all"
                ProducerConfig/RETRIES_CONFIG "3"
                ProducerConfig/COMPRESSION_TYPE_CONFIG "snappy"})]

    (log/info "Creating Kafka producer")
    (KafkaProducer. props)))

(defn send-to-registry!
  "Send approved order to registry topic."
  [^KafkaProducer producer topic order validation-result]
  (try
    (let [message {:order order
                   :validation-result validation-result
                   :timestamp (System/currentTimeMillis)}
          json (json/generate-string message)]
      (log/debug "Sending to registry" {:order-id (:order-id order) :message-keys (keys message)})
      (let [record (ProducerRecord. topic (:order-id order) json)]
        @(.send producer record))
      (log/info "Order sent to registry topic" {:order-id (:order-id order)}))
    (catch Exception e
      (log/error e "Failed to send order to registry" {:order-id (:order-id order)}))))

;; =============================================================================
;; Message Processing
;; =============================================================================

(defn process-order-record
  "Process order from 'orders' topic: validate and send to 'registry' if accepted."
  [context ^ConsumerRecord record]
  (try
    (let [{:keys [producer registry-topic]} context
          json-value (.value record)
          order (model/deserialize-order json-value)]

      (log/debug "Processing order"
                 {:topic (.topic record)
                  :partition (.partition record)
                  :offset (.offset record)
                  :order-id (:order-id order)})

      (let [validate-result (cmd/execute context {:type :validate :data order})
            validation-result (:validation-result validate-result)
            passed (validator/validation-passed? validation-result)]

        (when passed
          (send-to-registry! producer registry-topic order validation-result))

        validate-result))

    (catch Exception e
      (log/error e "Error processing order record"
                 {:topic (.topic record)
                  :partition (.partition record)
                  :offset (.offset record)})
      {:success false
       :error (.getMessage e)})))

(defn process-registry-record
  "Process record from 'registry' topic: persist to Cassandra."
  [context ^ConsumerRecord record]
  (log/info ">>> Processing registry record START"
            {:topic (.topic record) :partition (.partition record) :offset (.offset record)})
  (try
    (let [json-value (.value record)]
      (log/info ">>> JSON received" {:json-preview (subs json-value 0 (min 150 (count json-value)))})

      (let [message (json/parse-string json-value true)]
        (log/info ">>> Message parsed" {:keys (keys message)})

        (let [order (:order message)
              validation-passed (get-in message [:validation-result :passed])]

          (log/info ">>> Extracted data"
                    {:order-id (:order-id order)
                     :validation-passed validation-passed
                     :has-order (some? order)
                     :has-validation-result (some? (:validation-result message))})

          (if (and order validation-passed)
            (let [result (cmd/execute context {:type :register
                                               :data {:order order
                                                      :validation-passed validation-passed}})]
              (log/info ">>> Register command executed" {:success (:success result) :order-id (:order-id order)})
              result)
            (do
              (log/warn ">>> Skipping - invalid data" {:has-order (some? order) :validation-passed validation-passed})
              {:success true :skipped true})))))

    (catch Exception e
      (log/error e ">>> ERROR processing registry record"
                 {:error-message (.getMessage e)
                  :error-class (.getName (.getClass e))
                  :json (.value record)})
      {:success false :error (.getMessage e)})))

(defn process-record
  "Process a Kafka record (routes based on topic)."
  [context ^ConsumerRecord record]
  (let [topic (.topic record)]
    (cond
      (= topic "orders")
      (process-order-record context record)

      (= topic "registry")
      (process-registry-record context record)

      :else
      (do
        (log/warn "Unknown topic" {:topic topic})
        {:success false :error "Unknown topic"}))))

(defn process-batch
  "Process batch of Kafka records."
  [context records]
  (let [results (map #(process-record context %) records)
        success-count (count (filter :success results))
        error-count (count (filter (complement :success) results))]
    {:total (count results)
     :success success-count
     :errors error-count}))

;; =============================================================================
;; Consumer Loop
;; =============================================================================

(defn consumer-loop
  "Main consumer loop."
  [^KafkaConsumer consumer context running-atom]
  (let [poll-timeout-ms 1000
        commit-interval-ms (config/get-commit-interval-ms)
        last-commit (atom (System/currentTimeMillis))]

    (log/info "Starting consumer loop")

    (while @running-atom
      (try
        (let [records (.poll consumer (Duration/ofMillis poll-timeout-ms))]

          (when-not (.isEmpty records)
            (log/debug "Polled records" {:count (.count records)})

            (let [stats (process-batch context records)]
              (log/info "Batch processed" stats)

              (let [now (System/currentTimeMillis)]
                (when (> (- now @last-commit) commit-interval-ms)
                  (log/info "Persisting stats and committing offsets")
                  (cmd/execute context {:type :persist-stats})

                  (.commitSync consumer)
                  (log/debug "Kafka offsets committed")

                  (reset! last-commit now))))))

        (catch Exception e
          (log/error e "Error in consumer loop")
          (Thread/sleep 1000))

        (catch InterruptedException e
          (log/info "Consumer loop interrupted")
          (reset! running-atom false))))

    (log/info "Consumer loop stopped")))

;; =============================================================================
;; Lifecycle
;; =============================================================================

(defn start-processor
  "Start registry-processor."
  []
  (log/info "Starting registry-processor")

  (try
    (let [consumer-cfg (config/kafka-consumer-config)
          producer-cfg (config/kafka-producer-config)
          cassandra-cfg (config/cassandra-config)
          processor-cfg (config/processor-config)
          processor-id (:processor-id processor-cfg)]

      (log/info "Configuration loaded" {:processor-id processor-id})

      (log/info "Connecting to Cassandra (with retry)...")
      (let [session (retry-with-backoff
                     #(db/create-session cassandra-cfg)
                     10 2000 30000)]

        (log/info "Connected to Cassandra successfully!")

        (log/info "Creating schema if not exists...")
        (db/create-schema! session)
        (log/info "Schema verification complete")

        (let [prepared-stmts (db/prepare-statements session)]

          (log/info "Prepared statements ready")

          (let [consumer (create-consumer consumer-cfg)
                producer (create-producer producer-cfg)
                topics (:topics consumer-cfg)
                registry-topic (:registry-topic producer-cfg)]

            (subscribe-topics consumer topics)

            (let [stats-atom (atom (model/new-validation-stats processor-id))

                  context {:session session
                           :prepared-stmts prepared-stmts
                           :stats-atom stats-atom
                           :producer producer
                           :registry-topic registry-topic
                           :processor-config processor-cfg}

                  running-atom (atom true)
                  consumer-thread (Thread. #(consumer-loop consumer context running-atom))]

              (.start consumer-thread)

              (log/info "Registry-processor started" {:processor-id processor-id})

              {:consumer consumer
               :producer producer
               :session session
               :prepared-stmts prepared-stmts
               :stats-atom stats-atom
               :context context
               :running-atom running-atom
               :consumer-thread consumer-thread})))))

    (catch Exception e
      (log/error e "Failed to start registry-processor")
      (throw e))))

(defn stop-processor
  "Stop registry-processor gracefully."
  [system]
  (log/info "Stopping registry-processor")

  (try
    (let [{:keys [consumer producer session context running-atom consumer-thread]} system]

      (reset! running-atom false)
      (.join consumer-thread 5000)

      (log/info "Persisting final stats")
      (cmd/execute context {:type :persist-stats})

      (when producer
        (log/info "Closing Kafka producer")
        (.close producer))

      (when consumer
        (log/info "Closing Kafka consumer")
        (.close consumer))

      (when session
        (log/info "Closing Cassandra session")
        (db/close-session session))

      (log/info "Registry-processor stopped"))

    (catch Exception e
      (log/error e "Error stopping registry-processor"))))

;; =============================================================================
;; Main
;; =============================================================================

(defn -main
  "Main entry point."
  [& _args]
  (log/info "Registry-processor starting...")

  (let [system (start-processor)]

    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. #(stop-processor system)))

    (log/info "Registry-processor running. Press Ctrl+C to stop.")

    @(promise)))

(comment
  (def system (start-processor))
  (cmd/execute (:context system) {:type :health-check})
  (cmd/execute (:context system) {:type :get-stats})
  (stop-processor system))