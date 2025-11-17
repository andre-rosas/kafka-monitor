(ns order-processor.context
  "Application context for dependency injection.
   
   This demonstrates:
   - Component lifecycle management
   - Dependency injection pattern in Clojure
   - Resource management (producer, state-store)
   - Testability through protocol-based design"
  (:require [order-processor.config :as config]
            [order-processor.state-store :as state-store]
            [taoensso.timbre :as log])
  (:import [org.apache.kafka.clients.producer KafkaProducer]))

;; =============================================================================
;; Protocols (For Testability and Polymorphism)
;; =============================================================================

(defprotocol IProducer
  "Protocol for Kafka producer operations.
   Allows us to mock producer in tests."
  (send-message! [this topic key value callback]))

(defprotocol IStateStore
  "Protocol for state store operations.
   Tracks sent orders for monitoring and recovery."
  (save-order! [this order])
  (get-order [this order-id])
  (get-stats [this]))

;; =============================================================================
;; Real Implementations
;; =============================================================================

(defrecord KafkaProducerComponent [^KafkaProducer producer]
  IProducer
  (send-message! [_ topic key value callback]
    (let [record (org.apache.kafka.clients.producer.ProducerRecord. topic key value)]
      (.send producer record callback))))

(defrecord LocalStateStore [store-atom]
  IStateStore
  (save-order! [_ order]
    (swap! store-atom state-store/add-order order))

  (get-order [_ order-id]
    (get-in @store-atom [:orders order-id]))

  (get-stats [_]
    (state-store/get-statistics @store-atom)))

;; =============================================================================
;; Context Creation
;; =============================================================================

(defn- map->properties
  "Convert map to Java Properties."
  [m]
  (let [props (java.util.Properties.)]
    (doseq [[k v] m]
      (.put props k v))
    props))

(defn create-kafka-producer
  "Create real Kafka producer."
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
    (->KafkaProducerComponent (KafkaProducer. props))))

(defn create-state-store
  "Create local state store for tracking orders."
  []
  (->LocalStateStore (atom (state-store/new-store))))

(defn create-context
  "Create application context with all components.
   
   Returns map with:
   - :producer - Kafka producer component
   - :state-store - Local state tracking
   - :config - Application config"
  []
  (log/info "Creating application context...")
  {:producer (create-kafka-producer)
   :state-store (create-state-store)
   :config {:kafka (config/kafka-config)
            :producer (config/producer-config)
            :orders (config/orders-config)}})

(defn close-context!
  "Close all resources in context."
  [{:keys [producer]}]
  (log/info "Closing application context...")
  (when producer
    (.close (:producer producer)))
  (log/info "Context closed"))

;; =============================================================================
;; Mock Implementations (For Testing)
;; =============================================================================

(defrecord MockProducer [sent-messages-atom]
  IProducer
  (send-message! [_ topic key value callback]
    (swap! sent-messages-atom conj {:topic topic :key key :value value})
    (when callback
      (.onCompletion callback nil nil))))

(defrecord MockStateStore [store-atom]
  IStateStore
  (save-order! [_ order]
    (swap! store-atom assoc (:order-id order) order))

  (get-order [_ order-id]
    (get @store-atom order-id))

  (get-stats [_]
    {:total-orders (count @store-atom)}))

(defn create-test-context
  "Create context for testing with mock implementations."
  []
  {:producer (->MockProducer (atom []))
   :state-store (->MockStateStore (atom {}))
   :config {:kafka {:topic "test-orders"}
            :producer {:rate-per-second 100}
            :orders {:customer-ids [1 100]
                     :product-ids ["P1" "P2"]
                     :price-range [10.0 20.0]
                     :quantity-range [1 5]}}})