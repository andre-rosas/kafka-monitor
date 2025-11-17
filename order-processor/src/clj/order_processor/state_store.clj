(ns order-processor.state-store
  "Local state store for tracking produced orders.
   
   Demonstrates:
   - Immutable data structures
   - Pure functions for state transitions
   - Statistics aggregation
   - Time-based windowing")

;; =============================================================================
;; State Structure
;; =============================================================================

(defn new-store
  "Create new empty store.
   
   Structure:
   {:orders {order-id order-data}
    :stats {:total 0
            :by-customer {}
            :by-product {}
            :total-value 0.0
            :last-updated timestamp}}"
  []
  {:orders {}
   :stats {:total 0
           :by-customer {}
           :by-product {}
           :total-value 0.0
           :last-updated nil}})

;; =============================================================================
;; Pure State Transitions
;; =============================================================================

(defn add-order
  "Add order to store (pure function).
   
   Updates both orders map and aggregated statistics."
  [store order]
  (let [order-id (:order-id order)
        customer-id (:customer-id order)
        product-id (:product-id order)
        total (:total order)]
    (-> store
        (assoc-in [:orders order-id] order)
        (update-in [:stats :total] inc)
        (update-in [:stats :by-customer customer-id] (fnil inc 0))
        (update-in [:stats :by-product product-id] (fnil inc 0))
        (update-in [:stats :total-value] + total)
        (assoc-in [:stats :last-updated] (:timestamp order)))))

(defn get-order
  "Get order by ID (pure function)."
  [store order-id]
  (get-in store [:orders order-id]))

(defn get-statistics
  "Get aggregated statistics (pure function)."
  [store]
  (:stats store))

(defn get-recent-orders
  "Get N most recent orders (pure function)."
  [store n]
  (->> (:orders store)
       vals
       (sort-by :timestamp >)
       (take n)))

(defn get-orders-by-customer
  "Get all orders for a customer (pure function)."
  [store customer-id]
  (->> (:orders store)
       vals
       (filter #(= (:customer-id %) customer-id))))

(defn get-orders-by-product
  "Get all orders for a product (pure function)."
  [store product-id]
  (->> (:orders store)
       vals
       (filter #(= (:product-id %) product-id))))

;; =============================================================================
;; Analytics
;; =============================================================================

(defn get-top-customers
  "Get top N customers by order count."
  [store n]
  (->> (get-in store [:stats :by-customer])
       (sort-by val >)
       (take n)))

(defn get-top-products
  "Get top N products by order count."
  [store n]
  (->> (get-in store [:stats :by-product])
       (sort-by val >)
       (take n)))

(defn get-average-order-value
  "Calculate average order value."
  [store]
  (let [stats (:stats store)
        total (:total stats)
        total-value (:total-value stats)]
    (if (pos? total)
      (/ total-value total)
      0.0)))