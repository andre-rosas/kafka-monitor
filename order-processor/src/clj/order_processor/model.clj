(ns order-processor.model
  "Order domain model with validation.
   
   This namespace defines:
   - The Order data structure (what an order looks like)
   - Validation rules (what makes an order valid)
   - Serialization/deserialization (how to convert to/from JSON)
   - Test data generators (for property-based testing)
   
   Why separate model from business logic?
   - Clear contract between services (other consumers know what to expect)
   - Reusable validation across different contexts
   - Single source of truth for data structure
   - Easier to test in isolation"
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [cheshire.core :as json]
            [shared.utils :as utils]))

;; =============================================================================
;; Specs - Define What an Order Looks Like
;; =============================================================================

(s/def ::order-id
  ;; "Unique identifier for an order. Must be a valid UUID string."
  (s/and string? #(re-matches #"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" %)))

(s/def ::customer-id
  ;; "Customer identifier. Positive integer."
  pos-int?)

(s/def ::product-id
  ;; "Product identifier. Non-empty string."
  (s/and string? seq))

(s/def ::quantity
  ;; "Quantity ordered. Positive integer."
  pos-int?)

(s/def ::unit-price  ; ← Mude de ::price para ::unit-price
  ;; "Price per unit. Must be a positive number."
  (s/and number? pos?))

(s/def ::total
  ;; "Total order value (quantity * price). Positive number."
  (s/and number? pos?))

(s/def ::timestamp
  ;; "Order creation timestamp in milliseconds since epoch."
  (s/and int? pos?))

(s/def ::status
  ;; "Order status. One of the allowed statuses."
  #{"pending" "accepted" "denied"})

(s/def ::order
  ;; "Complete order specification.

  ;;  An order represents a customer purchase request and must contain:
  ;;  - Unique ID for tracking
  ;;  - Customer and product references
  ;;  - Quantity and pricing information
  ;;  - Timestamp for ordering and audit
  ;;  - Current status in the order lifecycle"
  (s/keys :req-un [::order-id ::customer-id ::product-id
                   ::quantity ::unit-price ::total
                   ::timestamp ::status]))

;; =============================================================================
;; Validation Functions
;; =============================================================================

(defn valid-order?
  "Check if order conforms to spec.
   
   Returns true if valid, false otherwise.
   Use this for simple boolean checks."
  [order]
  (s/valid? ::order order))

(defn validate-order
  "Validate order and return explanation if invalid.
   
   Returns:
   - {:valid? true} if order is valid
   - {:valid? false :errors [...]} if order is invalid
   
   Use this when you need detailed error messages."
  [order]
  (if (s/valid? ::order order)
    {:valid? true}
    {:valid? false
     :errors (s/explain-data ::order order)}))

(defn validate-order!
  "Validate order and throw exception if invalid.
   
   Use this when you want to fail-fast on invalid data.
   Throws ex-info with validation errors."
  [order]
  (when-not (s/valid? ::order order)
    (throw (ex-info "Invalid order"
                    {:type ::validation-error
                     :order order
                     :errors (s/explain-data ::order order)})))
  order)

;; =============================================================================
;; Business Logic Validation
;; =============================================================================

(defn total-matches?
  "Verify that total equals quantity * price (within rounding).
   
   This is a business rule validation, separate from structural validation.
   We allow 1 cent difference due to floating point rounding."
  [order]
  (let [expected (* (:quantity order) (:unit-price order))
        actual (:total order)
        diff (Math/abs (- expected actual))]
    (< diff 0.02)))

(defn validate-business-rules
  "Validate business rules beyond structural validation.
   
   Returns:
   - {:valid? true} if all rules pass
   - {:valid? false :errors [...]} if any rule fails"
  [order]
  (let [errors (cond-> []
                 (not (total-matches? order))
                 (conj "Total must equal quantity * unit-price"))]
    (if (empty? errors)
      {:valid? true}
      {:valid? false :errors errors})))

;; =============================================================================
;; Constructors
;; =============================================================================

(defn new-order
  "Create a new valid order.
   
   Takes raw values and constructs a properly formatted order.
   Automatically calculates total and sets timestamp.
   
   Example:
   (new-order {:customer-id 42
               :product-id \"PROD-001\"
               :quantity 5
               :unit-price 19.99})
   
   => {:order-id \"123e4567-...\"
       :customer-id 42
       :product-id \"PROD-001\"
       :quantity 5
       :unit-price 19.99
       :total 99.95
       :timestamp 1234567890
       :status \"pending\"}"
  [{:keys [customer-id product-id quantity unit-price status]}]
  (let [order {:order-id (utils/generate-uuid)
               :customer-id customer-id
               :product-id product-id
               :quantity quantity
               :unit-price (utils/round unit-price 2)
               :total (utils/round (* quantity unit-price) 2)
               :timestamp (utils/now-millis)
               :status (or status "pending")}]
    (validate-order! order)
    order))

;; =============================================================================
;; Serialization
;; =============================================================================

(defn order->json
  "Convert order to JSON string.
   
   Use this when sending to Kafka or storing in external systems."
  [order]
  (json/generate-string order))

(defn json->order
  "Parse JSON string to order map.
   
   Use this when receiving from Kafka or external systems.
   Validates the parsed order."
  [json-str]
  (let [order (json/parse-string json-str true)]
    (validate-order! order)
    order))

;; =============================================================================
;; Generators (For Property-Based Testing)
;; =============================================================================

(defn gen-order
  "Generator for valid orders.
   
   Used in property-based tests to generate random valid orders.
   Ensures generated data always conforms to spec."
  []
  (gen/fmap
   (fn [[customer-id product-id quantity price]]
     (new-order {:customer-id customer-id
                 :product-id product-id
                 :quantity quantity
                 :unit-price price}))
   (gen/tuple
    (s/gen ::customer-id)
    (s/gen ::product-id)
    (s/gen ::quantity)
    (gen/double* {:min 1.0 :max 1000.0 :infinite? false :NaN? false}))))

;; =============================================================================
;; Helpers
;; =============================================================================

(defn order-summary
  "Get human-readable summary of order.
   
   Useful for logging and debugging."
  [order]
  (format "Order %s: Customer %d ordered %d x %s @ $%.2f = $%.2f [%s]"
          (subs (:order-id order) 0 8)
          (:customer-id order)
          (:quantity order)
          (:product-id order)
          (:unit-price order)
          (:total order)
          (:status order)))