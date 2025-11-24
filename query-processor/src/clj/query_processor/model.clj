(ns query-processor.model
  "Schemas and validation for query-processor materialized views.
  
  This namespace defines the data structures that represent the views
  optimized for querying, generated from the orders stream.
  
  IMPORTANT: These are MATERIALIZED VIEWS, not the original orders.
  Each view is optimized for a specific type of query."
  (:require [clojure.spec.alpha :as s]
            [cheshire.core :as json]
            [clojure.string]))

;; =============================================================================
;; SPECS - Orders (input coming from Kafka)
;; =============================================================================

(s/def ::order-id string?)
(s/def ::customer-id pos-int?)
(s/def ::product-id string?)
(s/def ::quantity pos-int?)
(s/def ::unit-price (s/and number? pos?))
(s/def ::total (s/and number? pos?))
(s/def ::timestamp pos-int?)
(s/def ::status #{"pending" "accepted" "denied"})

(s/def ::order
  ;; "Schema of an order received from Kafka.
  ;; This is the INPUT that will be processed to generate the views."
  (s/keys :req-un [::order-id
                   ::customer-id
                   ::product-id
                   ::quantity
                   ::unit-price
                   ::total
                   ::timestamp
                   ::status]))

;; =============================================================================
;; SPECS - View: Orders by Customer (aggregation by customer)
;; =============================================================================

(s/def ::total-orders nat-int?)
(s/def ::total-spent (s/and number? (complement neg?)))
(s/def ::last-order-id (s/nilable string?))
(s/def ::last-order-timestamp (s/nilable pos-int?))
(s/def ::first-order-timestamp pos-int?)

(s/def ::customer-stats
  ;; "Materialized view: aggregated statistics by customer.

  ;; Example:
  ;; {:customer-id 42
  ;;  :total-orders 5
  ;;  :total-spent 750.00
  ;;  :last-order-id \"ORDER-123\"
  ;;  :last-order-timestamp 1234567890
  ;;  :first-order-timestamp 1234560000}"
  (s/keys :req-un [::customer-id
                   ::total-orders
                   ::total-spent
                   ::last-order-id
                   ::last-order-timestamp
                   ::first-order-timestamp]))

;; =============================================================================
;; SPECS - View: Orders by Product (aggregation by product)
;; =============================================================================

(s/def ::total-quantity nat-int?)
(s/def ::total-revenue (s/and number? (complement neg?)))
(s/def ::order-count nat-int?)
(s/def ::avg-quantity (s/and number? (complement neg?)))

(s/def ::product-stats
  ;; "Materialized view: aggregated statistics by product.

  ;; Example:
  ;; {:product-id \"PROD-001\"
  ;;  :total-quantity 50
  ;;  :total-revenue 1500.00
  ;;  :order-count 10
  ;;  :avg-quantity 5.0
  ;;  :last-order-timestamp 1234567890}"
  (s/keys :req-un [::product-id
                   ::total-quantity
                   ::total-revenue
                   ::order-count
                   ::avg-quantity
                   ::last-order-timestamp]))

;; =============================================================================
;; SPECS - View: Orders Timeline (last N orders)
;; =============================================================================

(s/def ::timeline-entry
  ;; "An entry in the orders timeline.
  ;; Maintains essential information for fast display."
  (s/keys :req-un [::order-id
                   ::customer-id
                   ::product-id
                   ::quantity
                   ::unit-price
                   ::total
                   ::status
                   ::timestamp]))

(s/def ::timeline
  ;; "Ordered list of the last N orders (most recent first).
  ;; Configurable maximum size (ex: 100 orders)."
  (s/coll-of ::timeline-entry :kind vector?))

;; =============================================================================
;; SPECS - Processing Stats (processor's own metrics)
;; =============================================================================

(s/def ::processed-count nat-int?)
(s/def ::error-count nat-int?)
(s/def ::last-processed-timestamp pos-int?)
(s/def ::processor-id string?)
(s/def ::total-revenue-accepted (s/and number? (complement neg?)))

(s/def ::processing-stats
  ;; "Query-processor internal metrics.
  ;; Useful for monitoring and troubleshooting."
  (s/keys :req-un [::processor-id
                   ::processed-count
                   ::error-count
                   ::total-revenue-accepted
                   ::last-processed-timestamp]))

;; =============================================================================
;; VALIDATION
;; =============================================================================

(defn valid-order?
  "Validates if an order received from Kafka is in the correct format.
  
  Args:
    order - Map with order data
    
  Returns:
    true if valid, false otherwise
    
  Example:
    (valid-order? {:order-id \"123\" :customer-id 42 ...}) ;; => true
    (valid-order? {:order-id 123}) ;; => false (order-id must be string)"
  [order]
  (s/valid? ::order order))

(defn valid-customer-stats?
  "Validates if customer statistics are in the correct format."
  [stats]
  (s/valid? ::customer-stats stats))

(defn valid-product-stats?
  "Validates if product statistics are in the correct format."
  [stats]
  (s/valid? ::product-stats stats))

(defn valid-timeline-entry?
  "Validates if a timeline entry is in the correct format."
  [entry]
  (s/valid? ::timeline-entry entry))

(defn explain-validation-error
  "Returns a readable explanation about validation error.
  
  Args:
    spec - Spec to be validated (ex: ::order)
    data - Data that failed validation
    
  Returns:
    String with error explanation
    
  Example usage in logs:
    (when-not (valid-order? order)
      (log/error (explain-validation-error ::order order)))"
  [spec data]
  (s/explain-str spec data))

;; =============================================================================
;; SERIALIZATION / DESERIALIZATION
;; =============================================================================

(defn deserialize-order
  "Deserializes an order coming from Kafka (JSON -> Clojure map).
  
  Args:
    json-str - JSON string of the order
    
  Returns:
    Clojure map with the deserialized order
    
  Throws:
    Exception if invalid JSON
    
  Example:
    (deserialize-order \"{\\\"order-id\\\":\\\"123\\\",...}\")
    ;; => {:order-id \"123\" :customer-id 42 ...}"
  [json-str]
  (try
    (-> json-str
        (json/parse-string true)
        (update :customer-id int)
        (update :quantity int)
        (update :unit-price double)
        (update :total double)
        (update :timestamp long))
    (catch Exception e
      (throw (ex-info "Failed to deserialize order"
                      {:json json-str
                       :error (.getMessage e)})))))

(defn serialize-customer-stats
  "Serializes customer statistics to JSON (to store in Cassandra).
  
  Args:
    stats - Map with ::customer-stats
    
  Returns:
    JSON string
    
  Example:
    (serialize-customer-stats {:customer-id 42 :total-orders 5 ...})
    ;; => \"{\\\"customer_id\\\":42,\\\"total_orders\\\":5,...}\""
  [stats]
  (json/generate-string stats {:key-fn (fn [k] (clojure.string/replace (name k) "-" "_"))}))

(defn serialize-product-stats
  "Serializes product statistics to JSON."
  [stats]
  (json/generate-string stats {:key-fn (fn [k] (clojure.string/replace (name k) "-" "_"))}))

(defn serialize-timeline-entry
  "Serializes a timeline entry to JSON."
  [entry]
  (json/generate-string entry {:key-fn (fn [k] (name k))}))

;; =============================================================================
;; CONSTRUCTORS (Helpers to create empty/initial views)
;; =============================================================================

(defn new-customer-stats
  "Creates initial statistics for a new customer.
  
  Args:
    customer-id - Customer ID
    first-order - Customer's first order (used for initial timestamp)
    
  Returns:
    Map with initial ::customer-stats
    
  Example:
    (new-customer-stats 42 {:order-id \"123\" :timestamp 1234567890 ...})
    ;; => {:customer-id 42 :total-orders 0 :total-spent 0.0 ...}"
  [customer-id first-order]
  {:customer-id customer-id
   :total-orders 0
   :total-spent 0.0
   :last-order-id nil
   :last-order-timestamp nil
   :first-order-timestamp (:timestamp first-order)})

(defn new-product-stats
  "Creates initial statistics for a new product.
  
  Args:
    product-id - Product ID
    
  Returns:
    Map with initial ::product-stats"
  [product-id]
  {:product-id product-id
   :total-quantity 0
   :total-revenue 0.0
   :order-count 0
   :avg-quantity 0.0
   :last-order-timestamp nil})

(defn new-processing-stats
  "Creates initial metrics for the processor.
  
  Args:
    processor-id - Unique identifier of the processor instance
    
  Returns:
    Map with initial ::processing-stats"
  [processor-id]
  {:processor-id processor-id
   :processed-count 0
   :error-count 0
   :last-processed-timestamp (System/currentTimeMillis)})

;; =============================================================================
;; DATA EXTRACTION (Helpers to get specific fields)
;; =============================================================================

(defn extract-timeline-entry
  "Extracts only the necessary fields from an order for the timeline.
  
  Args:
    order - Complete order
    
  Returns:
    Map with only the necessary fields for ::timeline-entry
    
  Example:
    (extract-timeline-entry full-order)
    ;; => {:order-id \"123\" :customer-id 42 :product-id \"PROD-001\" ...}"
  [order]
  (select-keys order [:order-id :customer-id :product-id :quantity :unit-price :total :status :timestamp]))

(comment
  ;; Usage examples (for development)

  ;; Order validation
  (valid-order? {:order-id "ORDER-123"
                 :customer-id 42
                 :product-id "PROD-001"
                 :quantity 5
                 :unit-price 30.0
                 :total 150.0
                 :timestamp 1234567890
                 :status "accepted"})
  ;; => true

  ;; Create initial stats
  (new-customer-stats 42 {:timestamp 1234567890})
  ;; => {:customer-id 42 :total-orders 0 ...}

  ;; Deserialize JSON
  (deserialize-order "{\"order-id\":\"123\",\"customer-id\":42,...}")
  ;; => {:order-id "123" :customer-id 42 ...}
  )