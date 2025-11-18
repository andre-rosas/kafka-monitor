(ns query-processor.aggregator
  "PURE aggregation functions for materialized views of orders.
  
  IMPORTANT: This namespace contains ONLY pure functions (no side-effects).
  All functions receive data, process it, and return new data.
  
  Principles:
  - No I/O (no Kafka, no Cassandra, no logs)
  - Total immutability
  - Easily testable
  - Functions composed through threading macros
  
  For junior developers:
  A pure function is one that:
  1. Always returns the same result for the same inputs
  2. Doesn't cause side effects (doesn't modify external state)
  3. Doesn't depend on external state (besides its parameters)"
  (:require [query-processor.model :as model]))

;; =============================================================================
;; AGGREGATION: Customer Stats (statistics per customer)
;; =============================================================================

(defn update-customer-stats
  "Updates a customer's statistics based on a new order.
  
  This is a PURE function that receives the current statistics state
  and the new order, returning a new updated state.
  
  Args:
    current-stats - Map with current ::customer-stats (or nil for new customer)
    order - Map with ::order of the received order
    
  Returns:
    Map with updated ::customer-stats
    
  Logic:
    - Increments total-orders by +1
    - Sums total-spent with order value
    - Updates last-order-id and last-order-timestamp
    - Maintains first-order-timestamp (doesn't change)
    
  Example:
    (update-customer-stats
      {:customer-id 42 :total-orders 5 :total-spent 500.0 ...}
      {:order-id \"123\" :total 150.0 :timestamp 9999 ...})
    ;; => {:customer-id 42 :total-orders 6 :total-spent 650.0 
    ;;     :last-order-id \"123\" :last-order-timestamp 9999 ...}"
  [current-stats order]
  (let [customer-id (:customer-id order)
        ;; If stats don't exist, creates initial. Otherwise uses current.
        stats (or current-stats
                  (model/new-customer-stats customer-id order))]
    ;; Updates fields functionally (returns new map)
    (-> stats
        (update :total-orders inc)
        (update :total-spent + (:total order))
        (assoc :last-order-id (:order-id order))
        (assoc :last-order-timestamp (:timestamp order)))))

(defn should-update-customer?
  "Checks if an order should update the customer's statistics.
  
  Useful for idempotence: avoids processing the same order twice.
  
  Args:
    current-stats - Map with current ::customer-stats (or nil)
    order - Map with ::order of the order
    
  Returns:
    true if should update, false otherwise
    
  Logic:
    - If stats don't exist => always updates (first order)
    - If last-order-id == order-id => DOESN'T update (duplicate)
    - If order timestamp > last-timestamp => updates (more recent)
    - Otherwise => updates (old order arrived late)
    
  Example:
    (should-update-customer?
      {:last-order-id \"ORDER-123\" :last-order-timestamp 1000}
      {:order-id \"ORDER-123\" :timestamp 1000})
    ;; => false (same order, doesn't process again)"
  [current-stats order]
  (cond
    ;; Stats don't exist => first time, always updates
    (nil? current-stats)
    true

    ;; Same order-id => duplicate, DOESN'T update
    (= (:last-order-id current-stats) (:order-id order))
    false

    ;; Otherwise, updates
    :else
    true))

;; =============================================================================
;; AGGREGATION: Product Stats (statistics per product)
;; =============================================================================

(defn calculate-avg-quantity
  "Calculates the average quantity of a product per order.
  
  Args:
    total-quantity - Total quantity sold
    order-count - Number of orders with this product
    
  Returns:
    Average (double), or 0.0 if order-count is zero
    
  Example:
    (calculate-avg-quantity 50 10) ;; => 5.0
    (calculate-avg-quantity 0 0)   ;; => 0.0"
  [total-quantity order-count]
  (if (zero? order-count)
    0.0
    (double (/ total-quantity order-count))))

(defn update-product-stats
  "Updates a product's statistics based on a new order.
  
  This is a PURE function that receives the current statistics state
  and the new order, returning a new updated state.
  
  Args:
    current-stats - Map with current ::product-stats (or nil for new product)
    order - Map with ::order of the received order
    
  Returns:
    Map with updated ::product-stats
    
  Logic:
    - Increments total-quantity with order quantity
    - Sums total-revenue with order total
    - Increments order-count by +1
    - Recalculates avg-quantity
    - Updates last-order-timestamp
    
  Example:
    (update-product-stats
      {:product-id \"PROD-001\" :total-quantity 100 :order-count 20 ...}
      {:product-id \"PROD-001\" :quantity 5 :total 150.0 ...})
    ;; => {:product-id \"PROD-001\" :total-quantity 105 :order-count 21 
    ;;     :avg-quantity 5.0 :total-revenue (+ old-revenue 150.0) ...}"
  [current-stats order]
  (let [product-id (:product-id order)
        ;; If stats don't exist, creates initial. Otherwise uses current.
        stats (or current-stats
                  (model/new-product-stats product-id))
        ;; Calculates new totals
        new-total-quantity (+ (:total-quantity stats) (:quantity order))
        new-order-count (inc (:order-count stats))
        new-total-revenue (+ (:total-revenue stats) (:total order))]
    ;; Returns new map with updated values
    (-> stats
        (assoc :total-quantity new-total-quantity)
        (assoc :total-revenue new-total-revenue)
        (assoc :order-count new-order-count)
        (assoc :avg-quantity (calculate-avg-quantity new-total-quantity new-order-count))
        (assoc :last-order-timestamp (:timestamp order)))))

(defn should-update-product?
  "Checks if an order should update the product's statistics.
  
  Idempotence: avoids processing duplicates.
  
  For products, it's simpler than customers because we don't store order-id.
  We assume that if it reached here, it should process (duplicate filtering
  happens at a higher level, by global order-id).
  
  Args:
    current-stats - Map with current ::product-stats (or nil)
    order - Map with ::order of the order
    
  Returns:
    true (always updates, since idempotence is handled at another level)"
  [_current-stats _order]
  ;; For products, always updates because deduplication happens by global order-id
  ;; (not by specific product)
  true)

;; =============================================================================
;; AGGREGATION: Timeline (last N orders)
;; =============================================================================

(defn add-to-timeline
  "Adds an order to the timeline, keeping only the last N orders.
  
  The timeline is ordered from most recent to oldest.
  
  Args:
    timeline - Vector with current ::timeline (or nil if empty)
    order - Map with ::order of the received order
    max-size - Maximum timeline size (ex: 100)
    
  Returns:
    Updated vector with the order added (most recent at the beginning)
    
  Logic:
    1. Extracts only necessary fields from the order
    2. Adds at the BEGINNING of the timeline (most recent)
    3. Removes old orders if exceeds max-size
    
  Example:
    (add-to-timeline
      [{:order-id \"ORDER-2\" :timestamp 2000}]
      {:order-id \"ORDER-3\" :timestamp 3000 ...}
      100)
    ;; => [{:order-id \"ORDER-3\" :timestamp 3000}
    ;;     {:order-id \"ORDER-2\" :timestamp 2000}]"
  [timeline order max-size]
  (let [current-timeline (or timeline [])
        ;; Extracts only necessary fields (saves space)
        entry (model/extract-timeline-entry order)]
    ;; Adds at the beginning (most recent) and limits size
    (->> current-timeline
         (cons entry)              ; Adds at the beginning
         (take max-size)           ; Keeps only max-size elements
         (vec))))                  ; Converts back to vector

(defn timeline-contains-order?
  "Checks if an order-id already exists in the timeline.
  
  Useful for avoiding duplicates in the timeline.
  
  Args:
    timeline - Vector with ::timeline
    order-id - String with the order ID
    
  Returns:
    true if order-id already exists, false otherwise
    
  Example:
    (timeline-contains-order?
      [{:order-id \"ORDER-1\"} {:order-id \"ORDER-2\"}]
      \"ORDER-1\")
    ;; => true"
  [timeline order-id]
  (boolean (some #(= (:order-id %) order-id) timeline)))

(defn should-add-to-timeline?
  "Checks if an order should be added to the timeline.
  
  Args:
    timeline - Vector with current ::timeline
    order - Map with ::order of the order
    
  Returns:
    true if should add, false otherwise
    
  Logic:
    - If timeline empty => always adds
    - If order-id already exists => DOESN'T add (duplicate)
    - Otherwise => adds"
  [timeline order]
  (or (empty? timeline)
      (not (timeline-contains-order? timeline (:order-id order)))))

;; =============================================================================
;; AGGREGATION: Processing Stats (processor metrics)
;; =============================================================================

(defn increment-processed
  "Increments the counter of successfully processed orders.
  
  Args:
    stats - Map with ::processing-stats
    
  Returns:
    Map updated with processed-count +1 and updated timestamp
    
  Example:
    (increment-processed {:processed-count 100 ...})
    ;; => {:processed-count 101 :last-processed-timestamp <now> ...}"
  [stats]
  (-> stats
      (update :processed-count inc)
      (assoc :last-processed-timestamp (System/currentTimeMillis))))

(defn increment-errors
  "Increments the error counter during processing.
  
  Args:
    stats - Map with ::processing-stats
    
  Returns:
    Map updated with error-count +1
    
  Example:
    (increment-errors {:error-count 5 ...})
    ;; => {:error-count 6 ...}"
  [stats]
  (update stats :error-count inc))

;; =============================================================================
;; COMPOSITION: Complete Aggregation
;; =============================================================================

(defn aggregate-order
  "High-level function that aggregates an order in ALL views.
  
  This function composes all specific aggregations.
  Returns a map with all updated views.
  
  Args:
    views - Map with all current views:
            {:customer-stats {customer-id -> stats}
             :product-stats {product-id -> stats}
             :timeline [...]
             :processing-stats {...}}
    order - Map with ::order of the received order
    config - Map with configurations:
             {:timeline-max-size 100}
    
  Returns:
    Map with updated views
    
  Example:
    (aggregate-order
      {:customer-stats {42 existing-stats}
       :product-stats {\"PROD-001\" existing-stats}
       :timeline []
       :processing-stats {...}}
      {:order-id \"ORDER-123\" :customer-id 42 :product-id \"PROD-001\" ...}
      {:timeline-max-size 100})
    ;; => {:customer-stats {42 updated-stats}
    ;;     :product-stats {\"PROD-001\" updated-stats}
    ;;     :timeline [{:order-id \"ORDER-123\" ...}]
    ;;     :processing-stats {...}}"
  [views order config]
  (let [customer-id (:customer-id order)
        product-id (:product-id order)
        current-customer-stats (get-in views [:customer-stats customer-id])
        current-product-stats (get-in views [:product-stats product-id])
        current-timeline (:timeline views)
        timeline-max-size (:timeline-max-size config 100)]

    (cond-> views
      ;; Updates customer-stats if necessary
      (should-update-customer? current-customer-stats order)
      (assoc-in [:customer-stats customer-id]
                (update-customer-stats current-customer-stats order))

      ;; Updates product-stats if necessary
      (should-update-product? current-product-stats order)
      (assoc-in [:product-stats product-id]
                (update-product-stats current-product-stats order))

      ;; Adds to timeline if necessary
      (should-add-to-timeline? current-timeline order)
      (assoc :timeline
             (add-to-timeline current-timeline order timeline-max-size))

      ;; Always increments processed counter
      true
      (update :processing-stats increment-processed))))

(defn aggregate-order-batch
  "Aggregates a batch of orders at once.
  
  Useful for processing multiple orders in batch (more efficient).
  
  Args:
    views - Map with all current views
    orders - Sequence of orders (::order)
    config - Map with configurations
    
  Returns:
    Map with updated views after processing all orders
    
  Example:
    (aggregate-order-batch views [order1 order2 order3] config)
    ;; => views updated with the 3 processed orders"
  [views orders config]
  (reduce
   (fn [current-views order]
     (aggregate-order current-views order config))
   views
   orders))

;; =============================================================================
;; HELPERS: Initialization
;; =============================================================================

(defn init-views
  "Creates initial empty structure for all views.
  
  Args:
    processor-id - String identifying this processor instance
    
  Returns:
    Map with initial structure of views
    
  Example:
    (init-views \"processor-1\")
    ;; => {:customer-stats {}
    ;;     :product-stats {}
    ;;     :timeline []
    ;;     :processing-stats {:processor-id \"processor-1\" ...}}"
  [processor-id]
  {:customer-stats {}
   :product-stats {}
   :timeline []
   :processing-stats (model/new-processing-stats processor-id)})

(comment
  ;; Usage examples (for development/tests)

  ;; Initialize views
  (def views (init-views "processor-1"))

  ;; Aggregate an order
  (def order {:order-id "ORDER-123"
              :customer-id 42
              :product-id "PROD-001"
              :quantity 5
              :unit-price 30.0
              :total 150.0
              :timestamp 1234567890
              :status "accepted"})

  (def updated-views
    (aggregate-order views order {:timeline-max-size 100}))

  ;; Check customer statistics
  (get-in updated-views [:customer-stats 42])
  ;; => {:customer-id 42 :total-orders 1 :total-spent 150.0 ...}

  ;; Check product statistics
  (get-in updated-views [:product-stats "PROD-001"])
  ;; => {:product-id "PROD-001" :total-quantity 5 :total-revenue 150.0 ...}

  ;; Check timeline
  (:timeline updated-views)
  ;; => [{:order-id "ORDER-123" :customer-id 42 ...}]

  ;; Aggregate another order from the same customer
  (def order2 (assoc order :order-id "ORDER-124" :total 200.0))
  (def updated-views-2
    (aggregate-order updated-views order2 {:timeline-max-size 100}))

  (get-in updated-views-2 [:customer-stats 42])
  ;; => {:customer-id 42 :total-orders 2 :total-spent 350.0 ...}
  )