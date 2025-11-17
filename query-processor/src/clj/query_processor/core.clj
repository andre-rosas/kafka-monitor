(ns query-processor.core
  "Main entry point for query-processor (Kafka consumer).
  
  This processor consumes orders from Kafka and materializes views
  into Cassandra for fast queries.
  
  Architecture:
  - Kafka Consumer (consumer group: query-processor)
  - In-memory views (atom for state)
  - Cassandra persistence (periodic commits)
  - Command system for operations"
  (:gen-class)
  (:require [query-processor.config :as config]
            [query-processor.model :as model]
            [query-processor.aggregator :as agg]
            [query-processor.db :as db]
            [query-processor.commands :as cmd]
            [clojure.tools.logging :as log])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer ConsumerConfig ConsumerRecord]
           [org.apache.kafka.common.serialization StringDeserializer]
           [java.time Duration]
           [java.util Properties]))

;; =============================================================================
;; Retry Logic
;; =============================================================================

(defn retry-with-backoff
  "Executes a function with exponential retry and backoff."
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
;; Kafka Consumer Setup
;; =============================================================================

(defn map->properties
  "Convert Clojure map to Java Properties (for Kafka config)."
  [m]
  (let [props (Properties.)]
    (doseq [[k v] m]
      (.put props (name k) (str v)))
    props))

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

    (log/info "Creating Kafka consumer" {:group-id group-id :bootstrap-servers bootstrap-servers})
    (KafkaConsumer. props)))

(defn subscribe-topics
  "Subscribe consumer to topics."
  [^KafkaConsumer consumer topics]
  (log/info "Subscribing to topics" {:topics topics})
  (.subscribe consumer topics))

;; =============================================================================
;; Message Processing
;; =============================================================================

(defn record->map
  "Convert ConsumerRecord to a map for testing."
  [^ConsumerRecord record]
  {:value (.value record)
   :topic (.topic record)
   :partition (.partition record)
   :offset (.offset record)})

(defn process-record-map
  "Process a record map (for testing)."
  [context record-map]
  (try
    (let [json-value (:value record-map)
          order (model/deserialize-order json-value)]

      (log/debug "Processing record" record-map)

      (cmd/execute context {:type :consume :data order}))

    (catch Exception e
      (log/error e "Error processing record" record-map)
      ;; Increment error counter when deserialization fails
      (let [{:keys [views-atom]} context]
        (when views-atom
          (swap! views-atom update :processing-stats agg/increment-errors)))
      {:success false
       :error (.getMessage e)})))

(defn process-record
  "Process a single Kafka record."
  [context ^ConsumerRecord record]
  (process-record-map context (record->map record)))

(defn process-batch-map
  "Process a batch of record maps (for testing)."
  [context records]
  (let [results (map #(process-record-map context %) records)
        success-count (count (filter :success results))
        error-count (count (filter (complement :success) results))]

    {:total (count results)
     :success success-count
     :errors error-count}))

(defn process-batch
  "Process a batch of Kafka records."
  [context records]
  (let [record-maps (map record->map records)]
    (process-batch-map context record-maps)))

;; =============================================================================
;; Consumer Loop
;; =============================================================================

(defn consumer-loop
  "Main consumer loop that polls Kafka and processes orders."
  [^KafkaConsumer consumer context running-atom]
  (let [{:keys [poll-timeout-ms]} (config/kafka-config)
        commit-interval-ms (config/get-commit-interval-ms)
        last-commit (atom (System/currentTimeMillis))]

    (log/info "Starting consumer loop" {:poll-timeout-ms poll-timeout-ms
                                        :commit-interval-ms commit-interval-ms})

    (while @running-atom
      (try
        (let [records (.poll consumer (Duration/ofMillis poll-timeout-ms))]

          (when-not (.isEmpty records)
            (log/debug "Polled records" {:count (.count records)})

            (let [stats (process-batch context records)]
              (log/info "Batch processed" stats)

              (let [now (System/currentTimeMillis)]
                (when (> (- now @last-commit) commit-interval-ms)
                  (log/info "Persisting views to Cassandra")
                  (cmd/execute context {:type :persist})

                  (.commitSync consumer)
                  (log/debug "Kafka offsets committed")

                  (reset! last-commit now))))))

        (catch Exception e
          (log/error e "Error in consumer loop")
          (Thread/sleep 1000))

        (catch InterruptedException _e
          (log/info "Consumer loop interrupted")
          (reset! running-atom false)))))

  (log/info "Consumer loop stopped"))
;; =============================================================================
;; Lifecycle Management
;; =============================================================================

(defn start-processor
  "Start the query-processor."
  []
  (log/info "Starting query-processor")

  (try
    (let [kafka-cfg (config/kafka-config)
          cassandra-cfg (config/cassandra-config)
          processor-cfg (config/processor-config)
          processor-id (:processor-id processor-cfg)]

      (log/info "Configuration loaded" {:processor-id processor-id})

      (log/info "Connecting to Cassandra (with retry)...")
      (let [session (retry-with-backoff
                     #(db/create-session cassandra-cfg)
                     10 2000 30000)

            prepared-stmts (db/prepare-statements session)]

        (log/info "Connected to Cassandra successfully!")

        (let [consumer (create-consumer kafka-cfg)
              topics (:topics kafka-cfg)]

          (subscribe-topics consumer topics)

          (let [views-atom (atom (agg/init-views processor-id))

                context {:session session
                         :prepared-stmts prepared-stmts
                         :views-atom views-atom
                         :processor-config processor-cfg}

                running-atom (atom true)

                consumer-thread (Thread. #(consumer-loop consumer context running-atom))]

            (.start consumer-thread)

            (log/info "Query-processor started successfully" {:processor-id processor-id})

            {:consumer consumer
             :session session
             :prepared-stmts prepared-stmts
             :views-atom views-atom
             :context context
             :running-atom running-atom
             :consumer-thread consumer-thread}))))

    (catch Exception e
      (log/error e "Failed to start query-processor")
      (throw e))))

(defn stop-processor
  "Stop the query-processor gracefully."
  [system]
  (log/info "Stopping query-processor")

  (try
    (let [{:keys [consumer session context running-atom consumer-thread]} system]

      (reset! running-atom false)
      (.join consumer-thread 5000)

      (log/info "Persisting final views")
      (cmd/execute context {:type :persist})

      (when consumer
        (log/info "Closing Kafka consumer")
        (.close consumer))

      (when session
        (log/info "Closing Cassandra session")
        (db/close-session session))

      (log/info "Query-processor stopped successfully"))

    (catch Exception e
      (log/error e "Error stopping query-processor"))))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(defn -main
  "Main entry point for query-processor."
  [& _args]
  (log/info "Query-processor starting...")

  (let [system (start-processor)]

    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. #(stop-processor system)))

    (log/info "Query-processor running. Press Ctrl+C to stop.")

    @(promise)))

(comment
  (def system (start-processor))
  (cmd/execute (:context system) {:type :health-check})
  (cmd/execute (:context system) {:type :get-stats})
  (cmd/execute (:context system) {:type :query-customer :data {:customer-id 42}})
  (cmd/execute (:context system) {:type :query-timeline :data {:limit 10}})
  (stop-processor system))