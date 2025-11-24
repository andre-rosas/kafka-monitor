(ns order-processor.core
  "Orders producer main application.
   
   This is the heart of the order-processor microservice. Everything is here:
   - Kafka producer setup and management
   - Order generation loop
   - Database persistence
   - Lifecycle management (start/stop)
   - State management (only what's needed)
   
   Why keep everything in one file?
   - Easier for junior devs to understand the full flow
   - No jumping between files to see what happens
   - Clear separation of concerns within the file
   - Easier to test as a unit
   
   Architecture:
   1. Generate order (pure function)
   2. Send to Kafka (side effect)
   3. Save to Cassandra (side effect)
   4. Loop with configurable rate"
  (:require [order-processor.config :as config]
            [order-processor.model :as model]
            [order-processor.db :as db]
            [taoensso.timbre :as log])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord Callback]
           [java.util Properties])
  (:gen-class))

;; =============================================================================
;; State Management (Minimal - Only What's Needed)
;; =============================================================================

(def app-state
  "Application state. Made public so commands.clj can access.
   
   Why use an atom here?
   - Need to track if producer is running (mutable state)
   - Need to hold Kafka producer instance (resource management)
   - Atoms provide thread-safe updates
   
   Could we avoid this? Not easily - we need coordinated state
   between the main thread and the production loop thread."
  (atom {:running? false
         :producer nil
         :stats {:orders-sent 0
                 :orders-failed 0
                 :last-order-time nil}}))

;; =============================================================================
;; Kafka Producer (Minimal Java Interop)
;; =============================================================================

(defn- props->Properties
  "Convert Clojure map to Java Properties.
   
   Why this helper?
   - Keeps Java interop isolated to one place
   - Makes calling code cleaner (pure Clojure maps)
   - Easier to test (can mock with regular maps)"
  [props-map]
  (doto (Properties.)
    (#(doseq [[k v] props-map]
        (when (and k v)
          (.put % k v))))))

(defn create-kafka-producer
  "Create Kafka producer with configuration.
   
   Configuration comes from config.edn and includes:
   - bootstrap.servers: Where Kafka is running
   - acks: Acknowledgment level (all = wait for all replicas)
   - compression: Reduces network usage
   - batch.size: Batches messages for efficiency
   - linger.ms: Wait time to fill batches
   
   Returns: KafkaProducer instance"
  []
  (let [kafka-cfg (config/kafka-config)
        producer-cfg (config/producer-config)
        props {"bootstrap.servers" (:bootstrap-servers kafka-cfg)
               "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
               "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
               "acks" (:acks producer-cfg)
               "compression.type" (:compression-type producer-cfg)
               "batch.size" (str (:batch-size producer-cfg))
               "linger.ms" (str (:linger-ms producer-cfg))}]
    (KafkaProducer. (props->Properties props))))

;; =============================================================================
;; Order Generation (Pure Function)
;; =============================================================================

(defn generate-order
  "Generate a random order based on configuration.
   
   This is a PURE function - no side effects:
   - Takes config as input
   - Returns order map as output
   - Same config produces predictable structure (except random values)
   
   Why pure?
   - Easy to test (no mocks needed)
   - Easy to reason about
   - Can be called from anywhere safely
   - Composable with other functions"
  []
  (let [orders-cfg (config/orders-config)
        ;; [min-cust max-cust] (:customer-id-range orders-cfg)
        customer-id (rand-nth (:customer-ids orders-cfg))
        product-id (rand-nth (:product-ids orders-cfg))
        quantity (+ (first (:quantity-range orders-cfg))
                    (rand-int (- (second (:quantity-range orders-cfg))
                                 (first (:quantity-range orders-cfg)))))
        unit-price (+ (first (:price-range orders-cfg))
                      (rand (- (second (:price-range orders-cfg))
                               (first (:price-range orders-cfg)))))]
    (model/new-order
     {:customer-id customer-id
      :product-id product-id
      :quantity quantity
      :unit-price unit-price
      :status "pending"})))

;; =============================================================================
;; Publishing (Side Effects Isolated)
;; =============================================================================

(defn send-to-kafka!
  "Send order to Kafka topic.
   
   The ! suffix is a Clojure convention meaning 'this has side effects'.
   
   What happens:
   1. Convert order to JSON
   2. Create ProducerRecord (topic, key, value)
   3. Send asynchronously
   4. Callback handles success/failure
   
   Why async?
   - Don't block waiting for Kafka
   - Higher throughput
   - Callback handles result when ready
   
   Returns: Nothing (async operation)"
  ([order]
   (send-to-kafka! (:producer @app-state) order))

  ([producer order]
   (let [topics (get-in (config/kafka-config) [:topics])
         key (:order-id order)
         value (model/order->json order)

         callback (reify Callback
                    (onCompletion [_ metadata exception]
                      (if exception
                        (do
                          (log/error exception "Failed to send order" {:order-id (:order-id order)})
                          (swap! app-state update-in [:stats :orders-failed] inc))
                        (do
                          (log/debug "Order sent successfully"
                                     {:order-id (:order-id order)
                                      :partition (.partition metadata)
                                      :offset (.offset metadata)})
                          (swap! app-state update-in [:stats :orders-sent] inc)
                          (swap! app-state assoc-in [:stats :last-order-time] (System/currentTimeMillis))

                          ;; Save to database after Kafka confirms
                          (try
                            (db/save-order! order)
                            (catch Exception e
                              (log/error e "Failed to save order to database" {:order-id (:order-id order)})))))))]

     ;; Send to all topics
     (doseq [topic topics]
       (let [record (ProducerRecord. topic key value)]
         (.send producer record callback))))))

;; =============================================================================
;; Production Loop
;; =============================================================================

(defn production-loop
  "Main loop that generates and sends orders continuously.
   
   How it works:
   1. Check if still running
   2. Generate order
   3. Send to Kafka (async)
   4. Sleep to maintain rate
   5. Repeat
   
   Why Thread/sleep instead of core.async?
   - Simpler for this use case
   - No need for complex channel coordination
   - Good enough for controlled rate limiting
   
   Rate control:
   - If rate = 10/sec, we sleep 100ms between sends
   - This is approximate (doesn't account for processing time)
   - For production, consider more sophisticated rate limiting"
  [producer rate-per-second]
  (let [sleep-ms (long (/ 1000 rate-per-second))]
    (log/info "Starting production loop" {:rate-per-second rate-per-second})

    (while (:running? @app-state)
      (try
        ;; Generate and send order
        (let [order (generate-order)]
          (send-to-kafka! producer order))

        ;; Rate limiting
        (Thread/sleep sleep-ms)

        (catch InterruptedException _
          (log/info "Production loop interrupted"))
        (catch Exception e
          (log/error e "Error in production loop"))))

    (log/info "Production loop stopped")))

;; =============================================================================
;; Lifecycle Management
;; =============================================================================

(defn start!
  "Start the orders producer.
   
   Steps:
   1. Load configuration
   2. Connect to Cassandra
   3. Create Kafka producer
   4. Start production loop in background thread
   
   Idempotent: Safe to call multiple times (checks if already running)"
  []
  (when-not (:running? @app-state)
    (log/info "Starting Orders Producer...")

    ;; Initialize config
    (config/init!)

    ;; Connect to database
    (db/connect!)

    ;; Create Kafka producer
    (let [producer (create-kafka-producer)
          rate (:rate-per-second (config/producer-config))]

      ;; Update state
      (swap! app-state assoc
             :running? true
             :producer producer)

      ;; Start production loop in background thread
      (future (production-loop producer rate))

      (log/info "Orders Producer started"
                {:rate-per-second rate
                 :topic (get-in (config/kafka-config) [:topics])}))))

(defn stop!
  "Stop the orders producer gracefully.
   
   Steps:
   1. Set running flag to false (stops loop)
   2. Close Kafka producer (flushes pending messages)
   3. Disconnect from Cassandra
   
   Graceful shutdown ensures:
   - No messages are lost (Kafka producer flushes)
   - Database connections are closed properly
   - Resources are released"
  []
  (when (:running? @app-state)
    (log/info "Stopping Orders Producer...")

    ;; Stop production loop
    (swap! app-state assoc :running? false)

    ;; Wait a bit for loop to finish current iteration
    (Thread/sleep 1000)

    ;; Close Kafka producer (this flushes pending messages)
    (when-let [producer (:producer @app-state)]
      (.close producer)
      (swap! app-state assoc :producer nil))

    ;; Disconnect from database
    (db/disconnect!)

    (log/info "Orders Producer stopped"
              {:stats (:stats @app-state)})))

(defn get-stats
  "Get current production statistics.
   
   Useful for:
   - Monitoring
   - Health checks
   - Debugging"
  []
  (:stats @app-state))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(defn -main
  "Application entry point.
   
   Sets up shutdown hook to ensure graceful shutdown on Ctrl+C or kill signal.
   Then starts the producer and blocks forever (until shutdown)."
  [& _args]
  (log/info "Orders Producer microservice starting...")

  ;; Register shutdown hook
  (.addShutdownHook
   (Runtime/getRuntime)
   (Thread. ^Runnable stop!))

  ;; Start producer
  (start!)

  ;; Block forever (until shutdown hook is called)
  @(promise))