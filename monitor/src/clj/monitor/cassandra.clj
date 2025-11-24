(ns monitor.cassandra
  "Cassandra queries for monitor API.
  
  Connects to all three keyspaces:
  - query_processor
  - registry_processor"
  (:require [monitor.config :as config]
            [clojure.tools.logging :as log])
  (:import [com.datastax.oss.driver.api.core CqlSession]
           [com.datastax.oss.driver.api.core.cql SimpleStatement ResultSet Row PreparedStatement]
           [java.net InetSocketAddress]))

;; =============================================================================
;; Connection Management
;; =============================================================================

(def sessions (atom {})) ;; Atom to store active Cassandra sessions by keyspace
(def prepared-statements (atom {})) ;; Atom to cache prepared statements by query string

(defn create-session
  "Create Cassandra session for a specific keyspace."
  [keyspace]
  (let [{:keys [host port datacenter]} (config/cassandra-config)
        port-int (if (string? port) (Integer/parseInt port) port)]
    (log/info "Connecting to Cassandra" {:keyspace keyspace})
    (-> (CqlSession/builder)
        (.addContactPoint (InetSocketAddress. host (int port-int)))
        (.withLocalDatacenter datacenter)
        (.withKeyspace keyspace)
        (.build))))

(defn get-session
  "Get or create session for given keyspace."
  [keyspace]
  (if-let [session (get @sessions keyspace)]
    session
    (let [session (create-session keyspace)]
      (swap! sessions assoc keyspace session)
      session)))

(defn prepare-statement
  "Prepare a statement and cache it for reuse."
  [session query]
  (if-let [stmt (get @prepared-statements query)]
    stmt
    (let [prepared (.prepare session query)]
      (swap! prepared-statements assoc query prepared)
      prepared)))

;; =============================================================================
;; Row Converters (Cassandra database row to Clojure Map)
;; =============================================================================

(defn row->customer-stats [^Row row]
  {:customer-id (.getInt row "customer_id")
   :total-orders (.getInt row "total_orders")
   :total-spent (.getDouble row "total_spent")
   :last-order-id (.getString row "last_order_id")
   :last-order-timestamp (when-let [ts (.getInstant row "last_order_timestamp")]
                           (.toEpochMilli ts))
   :first-order-timestamp (when-let [ts (.getInstant row "first_order_timestamp")]
                            (.toEpochMilli ts))})

(defn row->product-stats [^Row row]
  {:product-id (.getString row "product_id")
   :total-quantity (.getInt row "total_quantity")
   :total-revenue (.getDouble row "total_revenue")
   :order-count (.getInt row "order_count")
   :avg-quantity (.getDouble row "avg_quantity")
   :last-order-timestamp (when-let [ts (.getInstant row "last_order_timestamp")]
                           (.toEpochMilli ts))})

(defn row->timeline-entry [^Row row]
  {:order-id (.getString row "order_id")
   :customer-id (.getInt row "customer_id")
   :product-id (.getString row "product_id")
   :quantity (.getInt row "quantity")
   :unit-price (.getDouble row "unit_price")
   :total (.getDouble row "total")
   :status (.getString row "status")
   :timestamp (when-let [ts (.getInstant row "timestamp")]
                (.toEpochMilli ts))})

(defn row->registered-order [^Row row]
  {:order-id (.getString row "order_id")
   :customer-id (.getInt row "customer_id")
   :product-id (.getString row "product_id")
   :quantity (.getInt row "quantity")
   :unit-price (.getDouble row "unit_price")
   :total (.getDouble row "total")
   :status (.getString row "status")
   :registered-at (when-let [ts (.getInstant row "registered_at")]
                    (.toEpochMilli ts))
   :updated-at (when-let [ts (.getInstant row "updated_at")]
                 (.toEpochMilli ts))
   :version (.getInt row "version")
   :validation-passed (.getBoolean row "validation_passed")})

(defn row->order-update [^Row row]
  {:order-id (.getString row "order_id")
   :version (.getInt row "version")
   :previous-status (.getString row "previous_status")
   :new-status (.getString row "new_status")
   :updated-at (when-let [ts (.getInstant row "updated_at")]
                 (.toEpochMilli ts))
   :update-reason (.getString row "update_reason")})

;; =============================================================================
;; Query Processor Queries
;; =============================================================================

(defn get-customer-stats
  "Get customer statistics by ID from query_processor."
  [customer-id]
  (let [session (get-session "query_processor")
        query "SELECT * FROM orders_by_customer WHERE customer_id = ?"
        result (.execute session (SimpleStatement/newInstance query (into-array Object [(int customer-id)])))
        row (.one result)]
    (when row
      (row->customer-stats row))))

(defn get-product-stats
  "Get product statistics by ID from query_processor."
  [product-id]
  (let [session (get-session "query_processor")
        query "SELECT * FROM orders_by_product WHERE product_id = ?"
        result (.execute session (SimpleStatement/newInstance query (into-array Object [product-id])))
        row (.one result)]
    (when row
      (row->product-stats row))))

(defn get-timeline
  "Get orders timeline from query_processor."
  [limit]
  (let [session (get-session "query_processor")
        query "SELECT * FROM orders_timeline WHERE bucket_id = 0 ORDER BY timestamp DESC LIMIT ?"
        result (.execute session (SimpleStatement/newInstance query (into-array Object [(int limit)])))]
    (->> result
         (map row->timeline-entry)
         (vec))))

(defn get-all-customers
  "Get all customer statistics from query_processor."
  []
  (let [session (get-session "query_processor")
        result (.execute session (SimpleStatement/newInstance "SELECT * FROM orders_by_customer"))]
    (->> result
         (map row->customer-stats)
         (vec))))

(defn get-all-customers-limited
  "Get all customers, limited to N, sorted by total spent descending."
  [limit]
  (->> (get-all-customers)
       (sort-by :total-spent >)
       (take limit)
       (vec)))

(defn get-all-products
  "Get all product statistics from query_processor."
  []
  (let [session (get-session "query_processor")
        result (.execute session (SimpleStatement/newInstance "SELECT * FROM orders_by_product"))]
    (->> result
         (map row->product-stats)
         (vec))))

;; =============================================================================
;; Registry Processor Queries
;; =============================================================================

(defn get-registered-order
  "Get registered order by ID from registry_processor."
  [order-id]
  (try
    (let [session (get-session "registry_processor")
          query "SELECT * FROM registered_orders WHERE order_id = ?"
          result (.execute session (SimpleStatement/newInstance query (into-array Object [order-id])))]  ;; ← MUDANÇA: usar STRING direto
      (when-let [row (.one result)]
        (row->registered-order row)))
    (catch Exception e
      (log/error e "Error getting registered order" {:order-id order-id})
      nil)))

(defn get-order-history
  "Get order update history from registry_processor."
  [order-id]
  (let [session (get-session "registry_processor")
        order-uuid (java.util.UUID/fromString order-id)
        query "SELECT * FROM order_updates WHERE order_id = ?"
        result (.execute session (SimpleStatement/newInstance query (into-array Object [order-uuid])))]
    (->> result
         (map row->order-update)
         (vec))))

(defn get-all-registered-orders
  "Get all registered orders from registry_processor."
  []
  (let [;; session (get-session "registry_processor")
        session (get-session "registry_processor")
        result (.execute session (SimpleStatement/newInstance "SELECT * FROM registered_orders"))]
    (->> result
         (map row->registered-order)
         (vec))))

(defn update-order-status
  "Update status of an order. Creates in registered_orders if doesn't exist."
  [order-id new-status]
  (try
    (let [session-registry (get-session "registry_processor")
          session-query (get-session "query_processor")
          order-uuid (java.util.UUID/fromString order-id)

          ;; search order from timeline
          timeline (get-timeline 1000)
          order (first (filter #(= order-id (:order-id %)) timeline))]

      (if order
        (let [;; if does not exist in registered_orders, creates
              insert-query "INSERT INTO registered_orders (order_id, customer_id, product_id, quantity, unit_price, total, status, registered_at, updated_at, version, validation_passed) VALUES (?, ?, ?, ?, ?, ?, ?, toTimestamp(now()), toTimestamp(now()), 1, ?)"]
          (.execute session-registry
                    (SimpleStatement/newInstance insert-query
                                                 (into-array Object [order-id
                                                                     (:customer-id order)
                                                                     (:product-id order)
                                                                     (or (:quantity order) 0)
                                                                     (or (:unit-price order) 0.0)
                                                                     (:total order)
                                                                     new-status
                                                                     (= new-status "accepted")])))
          (log/info "Created/updated order in registered_orders" {:order-id order-id :status new-status})
          true)
        (do
          (log/warn "Order not found in timeline" {:order-id order-id})
          false)))
    (catch Exception e
      (log/error e "Error updating order status" {:order-id order-id :new-status new-status})
      false)))

(defn get-orders-by-status
  "Get registered orders by status from registry_processor."
  [status limit]
  (let [session (get-session "registry_processor")
        query "SELECT * FROM registered_orders WHERE status = ? LIMIT ?"
        result (.execute session (SimpleStatement/newInstance query (into-array Object [status (int limit)])))]
    (->> result
         (map row->registered-order)
         (vec))))

(defn update-validation-stats!
  "Increments the validation counters in registry_processor.validation_stats."
  [status]
  (let [session (get-session "registry_processor")
        processor-id "monitor-api"

        approved-query (if (= status "accepted")
                         "total_approved = total_approved + 1,"
                         "")
        rejected-query (if (= status "denied")
                         "total_rejected = total_rejected + 1,"
                         "")]
    (try
      (let [cql (str "UPDATE validation_stats SET "
                     approved-query
                     rejected-query
                     "total_validated = total_validated + 1 "
                     "WHERE processor_id = ?")]
        (log/info "Updating validation stats" {:cql cql :status status})
        (.execute session (SimpleStatement/newInstance cql (into-array [processor-id]))))
      (catch Exception e
        (log/error e "Error updating validation stats for status:" status)))))

;; =============================================================================
;; Aggregated Statistics
;; =============================================================================

(defn get-all-stats
  "Get aggregated statistics from all processors."
  []
  (try
    (let [customers (get-all-customers)
          products (get-all-products)
          ;; registered (get-all-registered-orders)
          registered (get-all-registered-orders)

          ;; Count by STATUS (accepted, denied, pending)
          accepted-orders (filter #(= "accepted" (:status %)) registered)
          denied-count (count (filter #(= "denied" (:status %)) registered))
          pending-count (count (filter #(= "pending" (:status %)) registered))

          accepted-count (count accepted-orders)
          total-revenue-accepted (reduce + 0.0 (map :total accepted-orders))]

      {:query-processor {:customer-count (count customers)
                         :product-count (count products)
                         :total-customers-spent (reduce + 0.0 (map :total-spent customers))
                         :total-revenue-accepted total-revenue-accepted}
       :registry-processor {:registered-count (count registered)
                            :accepted-count accepted-count
                            :denied-count denied-count
                            :pending-count pending-count}
       :timestamp (System/currentTimeMillis)})
    (catch Exception e
      (log/error e "Error fetching aggregated stats")
      {})))

(defn get-recent-orders
  "Fetch the last N orders from the orders table in order_processor.
   WARNING: Use only for dev/demo. Full scan on non-partition key is expensive."
  [limit]
  (let [session (get-session "order_processor")
        cql (str "SELECT order_id, customer_id, product_id, quantity, total, timestamp, status "
                 "FROM orders "
                 "LIMIT ?")
        rs (.execute session (SimpleStatement/newInstance cql (to-array [limit])))]
    (->> rs
         (.currentPage)
         (map #(hash-map
                :order-id (str (.getUuid % "order_id"))
                :customer-id (.getInt % "customer_id")
                :product-id (.getString % "product-id")
                :quantity (.getInt % "quantity")
                :unit-price (.getDecimal % "unit_price")
                :total (.getDecimal % "total")
                :timestamp (when-let [ts (.getInstant % "timestamp")]
                             (.toEpochMilli ts))
                :status (.getString % "status")))
         (vec))))

(defn get-top-customers
  "Get top N customers by total spent."
  [limit]
  (->> (get-all-customers)
       (sort-by :total-spent >)
       (take limit)
       (vec)))

(defn get-top-products
  "Get top N products by revenue."
  [limit]
  (->> (get-all-products)
       (sort-by :total-revenue >)
       (take limit)
       (vec)))

(defn close-all-sessions!
  "Close all Cassandra sessions."
  []
  (doseq [[keyspace session] @sessions]
    (log/info "Closing session" {:keyspace keyspace})
    (.close session))
  (reset! sessions {}))

(defn check-connection
  "Check if Cassandra is reachable by executing a simple query."
  []
  (try
    (let [session (get-session "query_processor")]
      (.execute session (SimpleStatement/newInstance "SELECT now() FROM system.local"))
      true)
    (catch Exception e
      (log/error e "Cassandra connection check failed")
      false)))

(defn get-timeline-with-status
  "Get timeline WITH status from registered_orders."
  [limit]
  (let [timeline (get-timeline limit)
        registered (get-all-registered-orders)
        ;; Crates a map for order-id -> registered-order to quick lookup
        registered-map (into {} (map (juxt :order-id identity) registered))]

    ;; For each order in timeline, updates with registered data, if exists
    (->> timeline
         (map (fn [order]
                (if-let [reg (get registered-map (:order-id order))]
                  ;; if data exists in registered, uses status, quantity e unit-price from tehre.
                  (merge order (select-keys reg [:status :quantity :unit-price]))
                  ;; if does not, keep it as 'pending'.
                  order)))
         (vec))))