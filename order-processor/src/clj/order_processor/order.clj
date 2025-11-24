(ns order-processor.order
  "Order generation and Kafka publishing."
  (:require [order-processor.config :as config]
            [shared.utils :as utils]
            [cheshire.core :as json]
            [taoensso.timbre :as log])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord Callback]))

;; =============================================================================
;; Pure Functions - Order Generation
;; =============================================================================

(defn generate-order
  "Generate random order from config (pure function)."
  [cfg]
  (let [orders-cfg (:orders cfg)
        quantity (+ (first (:quantity-range orders-cfg))
                    (rand-int (- (second (:quantity-range orders-cfg))
                                 (first (:quantity-range orders-cfg)))))
        price (utils/random-between (first (:price-range orders-cfg))
                                    (second (:price-range orders-cfg)))]
    {:order-id (utils/generate-uuid)
     :customer-id (rand-nth (:customer-ids orders-cfg))
     :product-id (rand-nth (:product-ids orders-cfg))
     :quantity quantity
     :unit-price (utils/round price 2)
     :total (utils/round (* quantity price) 2)
     :timestamp (System/currentTimeMillis)
     :status "pending"}))

;; =============================================================================
;; Kafka Producer Setup
;; =============================================================================

(defn- map->properties
  "Convert Clojure map to Java Properties."
  [m]
  (doto (java.util.Properties.)
    (.putAll (reduce-kv (fn [props k v] (doto props (.put (name k) (str v)))) {} m))))

(defn create-producer
  "Create Kafka producer with configuration."
  []
  (let [kafka-cfg (config/kafka-config)
        producer-cfg (config/producer-config)]
    (KafkaProducer.
     (map->properties
      {"bootstrap.servers" (:bootstrap-servers kafka-cfg)
       "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
       "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
       "acks" (:acks producer-cfg "all")
       "compression.type" (:compression-type producer-cfg "none")
       "batch.size" (str (:batch-size producer-cfg 16384))
       "linger.ms" (str (:linger-ms producer-cfg 0))}))))

;; =============================================================================
;; Production Control
;; =============================================================================

(defonce producer-state
  (atom {:producer nil
         :running? false
         :production-thread nil}))

(defn send-order!
  "Send order to Kafka (side-effecting function)."
  [producer order topic]
  (try
    (let [record (ProducerRecord. topic (:order-id order) (json/generate-string order))]
      (.send producer record
             (reify Callback
               (onCompletion [_ metadata ex]
                 (if ex
                   (log/error ex "Failed to send order" {:order-id (:order-id order)})
                   (log/debug "Order sent successfully"
                              {:order-id (:order-id order)
                               :partition (.partition metadata)
                               :offset (.offset metadata)}))))))
    (catch Exception e
      (log/error e "Error sending order to Kafka"))))

(defn produce-loop
  "Continuously generate and send orders at specified rate."
  [producer rate-per-second cfg topic]
  (let [delay-ms (max 10 (/ 1000 rate-per-second))] ;; Minimum 10ms delay
    (loop []
      (when (:running? @producer-state)
        (try
          (let [order (generate-order cfg)]
            (send-order! producer order topic)
            (Thread/sleep (long delay-ms)))
          (catch InterruptedException _
            (log/info "Producer interrupted - stopping"))
          (catch Exception e
            (log/error e "Error in production loop - continuing after delay")
            (Thread/sleep 1000))) ;; Wait 1s on error
        (recur)))))

(defn start!
  "Start order production."
  []
  (when-not (:running? @producer-state)
    (let [producer (create-producer)
          cfg @config/config
          rate (:rate-per-second (:producer cfg) 1)
          topic (:topic (config/kafka-config))]

      (swap! producer-state assoc
             :producer producer
             :running? true
             :production-thread (future (produce-loop producer rate cfg topic)))

      (log/info "🚀 Order production STARTED"
                {:rate-per-second rate
                 :topic topic
                 :customers (count (get-in cfg [:orders :customer-ids]))
                 :products (count (get-in cfg [:orders :product-ids]))}))))

(defn stop!
  "Stop order production gracefully."
  []
  (when (:running? @producer-state)
    (swap! producer-state assoc :running? false)

    ;; Wait a bit for thread to stop
    (when-let [thread (:production-thread @producer-state)]
      (future-cancel thread))

    (when-let [producer (:producer @producer-state)]
      (.close producer)
      (swap! producer-state assoc :producer nil))

    (log/info "🛑 Order production STOPPED")))

(defn init-order-production!
  "Initialize and start order production automatically"
  []
  (log/info "🚀 Initializing automatic order production...")
  (start!))

(defn status
  "Get production status."
  []
  @producer-state)