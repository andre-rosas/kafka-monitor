(ns registry-processor.validator
  "Business validation rules for orders.
  
  Pure functions that validate orders against business rules.
  If all rules pass, order is approved for registration.")

;; =============================================================================
;; Validation Rules (Pure Functions)
;; =============================================================================

(defn validate-minimum-quantity
  "Order must have at least 1 item."
  [order]
  (let [quantity (:quantity order)
        passed (>= quantity 1)]
    {:rule-name :minimum-quantity
     :passed passed
     :message (when-not passed
                (str "Quantity must be at least 1, got: " quantity))}))

(defn validate-maximum-quantity
  "Order cannot exceed 1000 items (fraud prevention)."
  [order]
  (let [quantity (:quantity order)
        max-qty 1000
        passed (<= quantity max-qty)]
    {:rule-name :maximum-quantity
     :passed passed
     :message (when-not passed
                (str "Quantity exceeds maximum of " max-qty ", got: " quantity))}))

(defn validate-minimum-total
  "Order total must be at least $1.00."
  [order]
  (let [total (:total order)
        min-total 1.0
        passed (>= total min-total)]
    {:rule-name :minimum-total
     :passed passed
     :message (when-not passed
                (str "Total must be at least $" min-total ", got: $" total))}))

(defn validate-maximum-total
  "Order total cannot exceed $100,000 (fraud prevention)."
  [order]
  (let [total (:total order)
        max-total 100000.0
        passed (<= total max-total)]
    {:rule-name :maximum-total
     :passed passed
     :message (when-not passed
                (str "Total exceeds maximum of $" max-total ", got: $" total))}))

(defn validate-price-consistency
  "Total must equal quantity * unit-price (with small tolerance for rounding)."
  [order]
  (let [quantity (:quantity order)
        unit-price (:unit-price order)
        total (:total order)
        expected (* quantity unit-price)
        tolerance 0.01
        diff (Math/abs (- total expected))
        passed (<= diff tolerance)]
    {:rule-name :price-consistency
     :passed passed
     :message (when-not passed
                (str "Total mismatch. Expected: $" expected ", got: $" total))}))

(defn validate-status
  "Order status must be valid."
  [order]
  (let [status (:status order)
        valid-statuses #{"pending" "accepted" "denied"}
        passed (contains? valid-statuses status)]
    {:rule-name :valid-status
     :passed passed
     :message (when-not passed
                (str "Invalid status: " status))}))

(defn validate-customer-id
  "Customer ID must be positive."
  [order]
  (let [customer-id (:customer-id order)
        passed (pos? customer-id)]
    {:rule-name :valid-customer-id
     :passed passed
     :message (when-not passed
                (str "Invalid customer ID: " customer-id))}))

(defn validate-product-id
  "Product ID must not be empty."
  [order]
  (let [product-id (:product-id order)
        passed (and (string? product-id)
                    (not (empty? product-id)))]
    {:rule-name :valid-product-id
     :passed passed
     :message (when-not passed
                "Product ID cannot be empty")}))

;; =============================================================================
;; Validation Orchestration
;; =============================================================================

(def validation-rules
  "All validation rules to be applied."
  [validate-minimum-quantity
   validate-maximum-quantity
   validate-minimum-total
   validate-maximum-total
   validate-price-consistency
   validate-status
   validate-customer-id
   validate-product-id])

(defn validate-order
  "Validate order against all business rules.
  
  Args:
    order - Order map to validate
    
  Returns:
    Map with:
    - :order-id
    - :passed (boolean - true if ALL rules passed)
    - :rules (vector of individual rule results)
    - :message (optional - summary if failed)
    
  Example:
    (validate-order order)
    ;; => {:order-id \"ORDER-001\"
    ;;     :passed true
    ;;     :rules [{:rule-name :minimum-quantity :passed true} ...]}"
  [order]
  (let [results (mapv #(% order) validation-rules)
        all-passed (every? :passed results)
        failed-rules (filter (complement :passed) results)
        failure-messages (map :message failed-rules)]
    {:order-id (:order-id order)
     :passed all-passed
     :rules results
     :message (when-not all-passed
                (str "Validation failed: " (clojure.string/join ", " failure-messages)))}))

(defn validation-passed?
  "Check if validation result indicates all rules passed."
  [validation-result]
  (:passed validation-result))

(defn get-failed-rules
  "Get list of rules that failed validation."
  [validation-result]
  (->> (:rules validation-result)
       (filter (complement :passed))
       (mapv :rule-name)))

(defn get-failure-summary
  "Get human-readable summary of validation failures."
  [validation-result]
  (when-not (:passed validation-result)
    (let [failed-rules (get-failed-rules validation-result)]
      (str "Failed rules: " (clojure.string/join ", " (map name failed-rules))))))

(comment
  ;; Example usage

  ;; Valid order
  (def valid-order
    {:order-id "ORDER-001"
     :customer-id 42
     :product-id "PROD-A"
     :quantity 5
     :unit-price 30.0
     :total 150.0
     :timestamp 1234567890
     :status "accepted"})

  (validate-order valid-order)
  ;; => {:order-id "ORDER-001" :passed true :rules [...]}

  ;; Invalid order (quantity too high)
  (def invalid-order
    (assoc valid-order :quantity 2000))

  (validate-order invalid-order)
  ;; => {:order-id "ORDER-001" :passed false :rules [...] :message "..."}

  (get-failed-rules (validate-order invalid-order))
  ;; => [:maximum-quantity]
  )