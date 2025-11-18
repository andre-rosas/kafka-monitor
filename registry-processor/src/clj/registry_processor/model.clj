(ns registry-processor.model
  "Schemas and validation for registry-processor.
  
  This processor validates orders and registers approved ones.
  It acts as both consumer (from 'orders') and producer (to 'registry')."
  (:require [clojure.spec.alpha :as s]
            [cheshire.core :as json]))

;; =============================================================================
;; SPECS - Order (input from Kafka)
;; =============================================================================

(s/def ::order-id string?)
(s/def ::customer-id pos-int?)
(s/def ::product-id string?)
(s/def ::quantity pos-int?)
(s/def ::unit-price (s/and number? pos?))
(s/def ::total (s/and number? pos?))
(s/def ::timestamp pos-int?)
(s/def ::status #{"pending" "denied" "approved"})

(s/def ::order
  ;; "Schema for incoming order from Kafka."
  (s/keys :req-un [::order-id
                   ::customer-id
                   ::product-id
                   ::quantity
                   ::unit-price
                   ::total
                   ::timestamp
                   ::status]))

;; =============================================================================
;; SPECS - Validation Result
;; =============================================================================

(s/def ::rule-name keyword?)
(s/def ::passed boolean?)
(s/def ::message (s/nilable string?))

(s/def ::validation-rule-result
  ;; "Result of a single validation rule."
  (s/keys :req-un [::rule-name ::passed]
          :opt-un [::message]))

(s/def ::validation-result
  ;; "Complete validation result for an order."
  (s/keys :req-un [::order-id ::passed ::rules]
          :opt-un [::message]))

;; =============================================================================
;; SPECS - Registered Order (stored in Cassandra)
;; =============================================================================

(s/def ::registered-at pos-int?)
(s/def ::updated-at pos-int?)
(s/def ::version pos-int?)
(s/def ::validation-passed boolean?)

(s/def ::registered-order
  ;; "Schema for registered order in Cassandra."
  (s/keys :req-un [::order-id
                   ::customer-id
                   ::product-id
                   ::quantity
                   ::total
                   ::status
                   ::registered-at
                   ::version
                   ::validation-passed]))

;; =============================================================================
;; SPECS - Order Update (when order changes)
;; =============================================================================

(s/def ::previous-status (s/nilable string?))
(s/def ::new-status string?)
(s/def ::update-reason (s/nilable string?))

(s/def ::order-update
  ;; "Schema for order update history."
  (s/keys :req-un [::order-id
                   ::version
                   ::previous-status
                   ::new-status
                   ::updated-at]
          :opt-un [::update-reason]))

;; =============================================================================
;; SPECS - Validation Stats
;; =============================================================================

(s/def ::total-validated nat-int?)
(s/def ::total-approved nat-int?)
(s/def ::total-rejected nat-int?)
(s/def ::processor-id string?)

(s/def ::validation-stats
  ;; "Statistics about validation operations."
  (s/keys :req-un [::processor-id
                   ::total-validated
                   ::total-approved
                   ::total-rejected
                   ::timestamp]))

;; =============================================================================
;; Validation Functions
;; =============================================================================

(defn valid-order?
  "Check if order is valid."
  [order]
  (s/valid? ::order order))

(defn valid-registered-order?
  "Check if registered order is valid."
  [registered-order]
  (s/valid? ::registered-order registered-order))

(defn explain-validation-error
  "Get human-readable validation error."
  [spec data]
  (s/explain-str spec data))

;; =============================================================================
;; Serialization / Deserialization
;; =============================================================================

(defn deserialize-order
  "Deserialize order from JSON string."
  [json-str]
  (try
    (-> json-str
        (json/parse-string true)
        (update :customer-id int)
        (update :quantity int)
        (update :timestamp long))
    (catch Exception e
      (throw (ex-info "Failed to deserialize order"
                      {:json json-str
                       :error (.getMessage e)})))))

(defn serialize-registered-order
  "Serialize registered order to JSON."
  [registered-order]
  (json/generate-string registered-order))


(defn serialize-validation-result
  "Serialize validation result to JSON."
  [validation-result]
  (json/generate-string validation-result {:key-fn name}))

;; =============================================================================
;; Constructors
;; =============================================================================

(defn new-registered-order
  "Create new registered order from validated order."
  [order validation-passed]
  {:order-id (:order-id order)
   :customer-id (:customer-id order)
   :product-id (:product-id order)
   :quantity (:quantity order)
   :total (:total order)
   :status (if validation-passed "approved" "denied")
   :registered-at (System/currentTimeMillis)
   :version 1
   :validation-passed validation-passed})

(defn new-order-update
  "Create order update record."
  [order-id version previous-status new-status reason]
  {:order-id order-id
   :version version
   :previous-status previous-status
   :new-status new-status
   :updated-at (System/currentTimeMillis)
   :update-reason reason})

(defn new-validation-stats
  "Create initial validation stats."
  [processor-id]
  {:processor-id processor-id
   :total-validated 0
   :total-approved 0
   :total-rejected 0
   :timestamp (System/currentTimeMillis)})

;; =============================================================================
;; Business Logic Helpers
;; =============================================================================

(defn increment-version
  "Increment order version."
  [registered-order]
  (update registered-order :version inc))

(defn update-order-status
  "Update order status and timestamp."
  [registered-order new-status]
  (-> registered-order
      (assoc :status new-status)
      (assoc :updated-at (System/currentTimeMillis))
      (increment-version)))

(defn should-update-order?
  "Check if order should be updated based on existing registration."
  [existing-order new-order]
  (when (nil? new-order)
    (throw (ex-info "new-order cannot be nil" {:existing-order existing-order})))
  (cond
    ;; No existing order, always register
    (nil? existing-order)
    true

    ;; Same order-id but different status
    (not= (:status existing-order) (:status new-order))
    true

    ;; Different quantity or total (order modified)
    (or (not= (:quantity existing-order) (:quantity new-order))
        (not= (:total existing-order) (:total new-order)))
    true

    ;; Otherwise, don't update (duplicate)
    :else
    false))

(comment
  ;; Example usage

  ;; Validate order
  (valid-order? {:order-id "ORDER-001"
                 :customer-id 42
                 :product-id "PROD-A"
                 :quantity 5
                 :unit-price 30.0
                 :total 150.0
                 :timestamp 1234567890
                 :status "accepted"})
  ;; => true

  ;; Create registered order
  (new-registered-order {:order-id "ORDER-001"
                         :customer-id 42
                         :product-id "PROD-A"
                         :quantity 5
                         :total 150.0
                         :status "accepted"}
                        true)
  ;; => {:order-id "ORDER-001" :version 1 ...}
  )