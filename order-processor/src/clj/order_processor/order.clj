(ns order-processor.order
  "Order generation and Kafka publishing."
  (:require [order-processor.config :as config]
            [shared.utils :as utils]
            [cheshire.core :as json]
            [taoensso.timbre :as log])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]))

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
     :timestamp (utils/now-millis)
     :status "PENDING"}))

;; =============================================================================
;; Kafka Producer Setup
;; =============================================================================

(defn- map->properties
  "Convert Clojure map to Java Properties."
  [m]
  (let [props (java.util.Properties.)]
    (doseq [[k v] m]
      (.put props k v))
    props))

(defn create-producer
  "Create Kafka producer with configuration."
  []
  (let [kafka-cfg (config/kafka-config)
        producer-cfg (config/producer-config)
        props (map->properties
               {"bootstrap.servers" (:bootstrap-servers kafka-cfg)
                "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                "acks" (:acks producer-cfg)
                "compression.type" (:compression-type producer-cfg)
                "batch.size" (str (:batch-size producer-cfg))
                "linger.ms" (str (:linger-ms producer-cfg))})]
    (KafkaProducer. props)))

;; =============================================================================
;; Publishing
;; =============================================================================

(defn send-order!
  "Send order to Kafka (side-effecting function)."
  [producer order]
  (let [topic (get-in (config/kafka-config) [:topic])
        record (ProducerRecord. topic (:order-id order) (json/generate-string order))]

    (log/debug "Sending order" {:order-id (:order-id order)})  ;; Keep the log for debugging.

    (.send producer record
           (reify org.apache.kafka.clients.producer.Callback
             (onCompletion [_ metadata exception]
               (if exception
                 (log/error exception "Failed to send order" {:order-id (:order-id order)})
                 (log/debug "Order sent"
                            {:order-id (:order-id order)
                             :partition (.partition metadata)
                             :offset (.offset metadata)})))))))

;; =============================================================================
;; Production Loop
;; =============================================================================

;; Atom holding producer state (compare-and-swap semantics)
(defonce producer-state
  (atom {:producer nil
         :running? false}))

(defn produce-loop
  "Continuously generate and send orders at specified rate."
  [producer rate-per-second cfg]
  (let [delay-ms (/ 1000 rate-per-second)]
    (while (:running? @producer-state)
      (try
        (let [order (generate-order cfg)]
          (send-order! producer order)
          (Thread/sleep (long delay-ms)))
        (catch InterruptedException _
          (log/info "Producer interrupted"))
        (catch Exception e
          (log/error e "Error in production loop"))))))

(defn start!
  "Start order production."
  []
  (when-not (:running? @producer-state)
    (let [producer (create-producer)
          rate (:rate-per-second (config/producer-config))
          cfg @config/config]

      (swap! producer-state assoc
             :producer producer
             :running? true)

      (future (produce-loop producer rate cfg))

      (log/info "Order production started" {:rate-per-second rate}))))

(defn stop!
  "Stop order production gracefully."
  []
  (when (:running? @producer-state)
    (swap! producer-state assoc :running? false)

    (when-let [producer (:producer @producer-state)]
      (.close producer)
      (swap! producer-state assoc :producer nil))

    (log/info "Order production stopped")))