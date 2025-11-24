(ns order-processor.db
  "Cassandra database operations for orders producer.
   
   This namespace handles:
   - Connection to Cassandra keyspace 'order_processor'
   - Saving sent orders (for tracking and recovery)
   - Querying order statistics
   - Pure functions for data transformation
   
   Why track sent orders?
   - Recovery: If producer crashes, we know what was sent
   - Monitoring: Track throughput, patterns
   - Debugging: Investigate issues with specific orders
   - Analytics: Business insights"
  (:require [order-processor.config :as config]
            [order-processor.model :as model]
            [taoensso.timbre :as log])
  (:import [com.datastax.oss.driver.api.core CqlSession]
           [com.datastax.oss.driver.api.core.cql SimpleStatement]
           [java.net InetSocketAddress]
           [java.util UUID]))

;; =============================================================================
;; Connection Management
;; =============================================================================

(defonce ^:private session (atom nil))

(defn- create-session
  "Create Cassandra session."
  []
  (let [cassandra-cfg (config/cassandra-config)
        host (:host cassandra-cfg)
        port-val (:port cassandra-cfg)
        port-int (if (string? port-val) (Integer/parseInt port-val) port-val)
        contact-point (InetSocketAddress. host port-int)]
    (log/info "Creating Cassandra session" {:host host :port port-int :keyspace (:keyspace cassandra-cfg)})
    (-> (CqlSession/builder)
        (.addContactPoint contact-point)
        (.withLocalDatacenter (:datacenter cassandra-cfg))
        (.withKeyspace (:keyspace cassandra-cfg))
        (.build))))

(defn- table-exists?
  "Check if table exists in keyspace."
  [session table-name]
  (try
    (let [result (.execute session
                           (str "SELECT table_name FROM system_schema.tables "
                                "WHERE keyspace_name = 'order_processor' "
                                "AND table_name = '" table-name "'"))]
      (-> result .one some?))
    (catch Exception e
      (log/error e "Error checking table existence" {:table table-name})
      false)))

(defn- create-schema!
  "Create database schema if it doesn't exist."
  [session]
  (log/info "Checking order processor schema...")

  (when-not (table-exists? session "orders")
    (log/info "Creating table: orders")
    (.execute session
              "CREATE TABLE IF NOT EXISTS orders (
                 order_id UUID PRIMARY KEY,
                 customer_id INT,
                 product_id TEXT,
                 quantity INT,
                 unit_price DECIMAL,
                 total DECIMAL,
                 timestamp TIMESTAMP,
                 status TEXT
               )"))

  (try
    (when-not (table-exists? session "orders_by_timestamp")
      (log/info "Creating materialized view: orders_by_timestamp")
      (.execute session
                "CREATE MATERIALIZED VIEW IF NOT EXISTS orders_by_timestamp AS
                     SELECT * FROM orders
                     WHERE timestamp IS NOT NULL AND order_id IS NOT NULL
                     PRIMARY KEY (timestamp, order_id)
                     WITH CLUSTERING ORDER BY (order_id ASC)"))
    (catch Exception e
      (log/warn "Materialized views disabled, continuing without them: " (.getMessage e))))

  (log/info "Order processor schema verification complete"))

(defn connect!
  "Initialize database connection."
  []
  (when-not @session
    (log/info "Connecting to Cassandra" (config/cassandra-config))
    (let [s (create-session)]
      (create-schema! s)
      (reset! session s))
    (log/info "Connected to Cassandra")))

(defn disconnect!
  "Close database connection."
  []
  (when-let [s @session]
    (log/info "Disconnecting from Cassandra")
    (.close s)
    (reset! session nil)
    (log/info "Disconnected from Cassandra")))

(defn get-session
  "Get current session, connecting if needed."
  []
  (or @session
      (do (connect!) @session)))

;; =============================================================================
;; Pure Functions - Data Transformation
;; =============================================================================

(defn order->cassandra-row
  "Convert order map to Cassandra insert parameters."
  [order]
  [(UUID/fromString (:order-id order))
   (int (:customer-id order))
   (:product-id order)
   (int (:quantity order))
   (bigdec (:unit-price order))
   (bigdec (:total order))
   (java.time.Instant/ofEpochMilli (:timestamp order))
   (:status order)])

(defn cassandra-row->order
  "Convert Cassandra row to order map."
  [row]
  {:order-id (str (.getUuid row "order_id"))
   :customer-id (.getInt row "customer_id")
   :product-id (.getString row "product_id")
   :quantity (.getInt row "quantity")
   :unit-price (double (.getBigDecimal row "unit_price"))
   :total (double (.getBigDecimal row "total"))
   :timestamp (.toEpochMilli (.getInstant row "timestamp"))
   :status (.getString row "status")})

;; =============================================================================
;; Database Operations (Side Effects)
;; =============================================================================

(def ^:private insert-order-cql
  "INSERT INTO orders (order_id, customer_id, product_id, quantity, unit_price, total, timestamp, status)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?)")

(defn save-order!
  "Save order to Cassandra."
  [order]
  (model/validate-order! order)
  (let [session (get-session)
        statement (-> (SimpleStatement/newInstance
                       insert-order-cql
                       (to-array (order->cassandra-row order))))]
    (.execute session statement)
    (log/debug "Order saved to DB" {:order-id (:order-id order)})
    order))

(def ^:private get-order-cql
  "SELECT * FROM orders WHERE order_id = ?")

(defn get-order
  "Retrieve order by ID."
  [order-id]
  (let [session (get-session)
        uuid (UUID/fromString order-id)
        statement (SimpleStatement/newInstance get-order-cql (to-array [uuid]))
        result (.execute session statement)
        row (first result)]
    (when row
      (cassandra-row->order row))))

(def ^:private count-orders-cql
  "SELECT COUNT(*) as total FROM orders")

(defn count-orders
  "Count total orders in database."
  []
  (let [session (get-session)
        statement (SimpleStatement/newInstance count-orders-cql)
        result (.execute session statement)
        row (first result)]
    (.getLong row "total")))

(def ^:private get-recent-orders-cql
  "SELECT * FROM orders_by_timestamp LIMIT ?")

(defn get-recent-orders
  "Get N most recent orders."
  [n]
  (let [session (get-session)
        statement (SimpleStatement/newInstance get-recent-orders-cql (to-array [n]))
        result (.execute session statement)]
    (map cassandra-row->order result)))

;; =============================================================================
;; Statistics (Read-Only Queries)
;; =============================================================================

(def ^:private stats-by-customer-cql
  "SELECT customer_id, COUNT(*) as order_count, SUM(total) as total_spent
   FROM orders
   GROUP BY customer_id
   ALLOW FILTERING")

(defn get-customer-stats
  "Get aggregated statistics per customer."
  []
  (let [session (get-session)
        statement (SimpleStatement/newInstance stats-by-customer-cql)
        result (.execute session statement)]
    (map (fn [row]
           {:customer-id (.getInt row "customer_id")
            :order-count (.getLong row "order_count")
            :total-spent (double (.getBigDecimal row "total_spent"))})
         result)))

;; =============================================================================
;; Health Check
;; =============================================================================

(defn healthy?
  "Check if database connection is working."
  []
  (try
    (when-let [s @session]
      (let [result (.execute s (SimpleStatement/newInstance "SELECT release_version FROM system.local"))]
        (boolean (first result))))
    (catch Exception e
      (log/error e "Database health check failed")
      false)))