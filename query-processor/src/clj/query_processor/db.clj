(ns query-processor.db
  "Functions for interacting with Cassandra (query-processor state store).
  
  IMPORTANT: Although it does I/O with Cassandra, we try to keep the functions
  as pure as possible, receiving the session as a parameter and returning data.
  
  Keyspace: query_processor_store
  
  Tables:
  - orders_by_customer:  Aggregated statistics by customer
  - orders_by_product:   Aggregated statistics by product
  - orders_timeline:     Last N orders (ordered by timestamp)
  - processing_stats:    Processor's own metrics
  
  For junior developers:
  Cassandra is a distributed NoSQL database. Unlike SQL, here we think
  'queries first' - we create tables optimized for each type of query."
  (:require [query-processor.model :as model]
            [query-processor.config :as config]
            [clojure.tools.logging :as log])
  (:import [com.datastax.oss.driver.api.core CqlSession]
           [com.datastax.oss.driver.api.core.cql SimpleStatement
            PreparedStatement
            BoundStatement
            ResultSet
            Row]
           [java.net InetSocketAddress]
           [java.time Duration]))

;; =============================================================================
;; SCHEMA MIGRATION
;; =============================================================================

(defn table-exists?
  "Checks if a table exists in the current keyspace."
  [^CqlSession session table-name]
  (try
    (let [result (.execute session
                           (str "SELECT table_name FROM system_schema.tables "
                                "WHERE keyspace_name = 'query_processor_store' "
                                "AND table_name = '" table-name "'"))]
      (-> result .one some?))
    (catch Exception e
      (log/error e "Error checking table existence" {:table table-name})
      false)))

(defn create-schema!
  "Creates all necessary tables if they don't exist."
  [^CqlSession session]
  (log/info "Checking database schema...")

  ;; Table orders_by_customer
  (when-not (table-exists? session "orders_by_customer")
    (log/info "Creating table: orders_by_customer")
    (.execute session
              "CREATE TABLE IF NOT EXISTS orders_by_customer (
         customer_id INT PRIMARY KEY,
         first_order_timestamp TIMESTAMP,
         last_order_id TEXT,
         last_order_timestamp TIMESTAMP,
         total_orders INT,
         total_spent DOUBLE
       )"))

  ;; Table orders_by_product
  (when-not (table-exists? session "orders_by_product")
    (log/info "Creating table: orders_by_product")
    (.execute session
              "CREATE TABLE IF NOT EXISTS orders_by_product (
         product_id TEXT PRIMARY KEY,
         total_quantity INT,
         total_revenue DOUBLE,
         order_count INT,
         avg_quantity DOUBLE,
         last_order_timestamp TIMESTAMP
       )"))

  ;; Table orders_timeline
  (when-not (table-exists? session "orders_timeline")
    (log/info "Creating table: orders_timeline")
    (.execute session
              "CREATE TABLE IF NOT EXISTS orders_timeline (
         bucket_id INT,
         timestamp TIMESTAMP,
         order_id TEXT,
         customer_id INT,
         product_id TEXT,
         total DOUBLE,
         status TEXT,
         PRIMARY KEY (bucket_id, timestamp, order_id)
       ) WITH CLUSTERING ORDER BY (timestamp DESC)"))

  ;; Table processing_stats
  (when-not (table-exists? session "processing_stats")
    (log/info "Creating table: processing_stats")
    (.execute session
              "CREATE TABLE IF NOT EXISTS processing_stats (
         processor_id TEXT PRIMARY KEY,
         processed_count BIGINT,
         error_count BIGINT,
         last_processed_timestamp TIMESTAMP
       )"))

  (log/info "Database schema check completed"))

;; =============================================================================
;; CONNECTION AND SETUP
;; =============================================================================

(defn create-session
  "Creates a session with Cassandra.
  
  Args:
    config - Map with configurations:
             {:host \"localhost\"
              :port 9042
              :datacenter \"datacenter1\"
              :keyspace \"query_processor_store\"}
    
  Returns:
    CqlSession (Java object from Cassandra driver)
    
  Note: This function has side-effect (creates database connection).
  But it's isolated and called only at application startup.
  
  Example:
    (def session (create-session {:host \"localhost\" :port 9042 ...}))"
  [config]
  (let [{:keys [host port datacenter keyspace]} config
        host-str (str host)
        port-val (if (string? port)
                   (Integer/parseInt port)
                   (int port))
        port-int (int port-val)]
    (log/info "Connecting to Cassandra" {:host host-str :port port-int :keyspace keyspace})
    (try
      (-> (CqlSession/builder)
          (.addContactPoint (InetSocketAddress. host-str (int port-int)))
          (.withLocalDatacenter datacenter)
          (.withKeyspace keyspace)
          (.build))
      (catch Exception e
        (log/error e "Error connecting to Cassandra")
        (throw (ex-info "Failed to create Cassandra session"
                        {:config config
                         :error (.getMessage e)}))))))

(defn close-session
  "Closes the session with Cassandra.
  
  Args:
    session - CqlSession to be closed
    
  Returns:
    nil
    
  Should be called at application shutdown."
  [^CqlSession session]
  (log/info "Closing Cassandra session")
  (.close session))

(defn init-keyspace!
  "Creates the keyspace if it doesn't exist (only for dev/tests).
  
  Args:
    session - CqlSession (connected without specific keyspace)
    keyspace - Keyspace name
    replication-factor - Replication factor (1 for dev, 3+ for prod)
    
  Returns:
    nil (side-effect: creates keyspace)
    
  WARNING: In production, the keyspace should be created via separate CQL script."
  [^CqlSession session keyspace replication-factor]
  (let [cql (str "CREATE KEYSPACE IF NOT EXISTS " keyspace
                 " WITH replication = {'class': 'SimpleStrategy', "
                 "'replication_factor': " replication-factor "}")]
    (log/info "Creating keyspace" {:keyspace keyspace})
    (.execute session (SimpleStatement/newInstance cql))))

;; =============================================================================
;; PREPARED STATEMENTS (performance + security)
;; =============================================================================

(defn prepare-statements
  [^CqlSession session]
  (log/info "Preparing CQL statements")
  {:upsert-customer
   (.prepare session
             "INSERT INTO orders_by_customer 
              (customer_id, first_order_timestamp,
              last_order_id, last_order_timestamp,
              total_orders, total_spent)
              VALUES (?, ?, ?, ?, ?, ?)")

   :get-customer
   (.prepare session
             "SELECT * FROM orders_by_customer WHERE customer_id = ?")

   :upsert-product
   (.prepare session
             "INSERT INTO orders_by_product
              (product_id, total_quantity, total_revenue, order_count,
               avg_quantity, last_order_timestamp)
              VALUES (?, ?, ?, ?, ?, ?)")

   :get-product
   (.prepare session
             "SELECT * FROM orders_by_product WHERE product_id = ?")

   :insert-timeline
   (.prepare session
             "INSERT INTO orders_timeline
                 (bucket_id, timestamp, order_id, customer_id, product_id, total, status)
                 VALUES (?, ?, ?, ?, ?, ?, ?)")

   :get-timeline
   (.prepare session
             "SELECT * FROM orders_timeline WHERE bucket_id = ? ORDER BY timestamp DESC LIMIT ?")

   :upsert-processing-stats
   (.prepare session
             "INSERT INTO processing_stats
              (processor_id, processed_count, error_count, last_processed_timestamp)
              VALUES (?, ?, ?, ?)")

   :get-processing-stats
   (.prepare session
             "SELECT * FROM processing_stats WHERE processor_id = ?")})

;; =============================================================================
;; CONVERSION: Clojure Map <-> Cassandra Row
;; =============================================================================

(defn row->customer-stats
  "Converts a Cassandra Row to ::customer-stats Clojure map.
  
  Args:
    row - Cassandra Row (or nil)
    
  Returns:
    Map with ::customer-stats (or nil if row is nil)
    
  Example:
    (row->customer-stats cassandra-row)
    ;; => {:customer-id 42 :total-orders 5 ...}"
  [^Row row]
  (when row
    {:customer-id (.getInt row "customer_id")
     :total-orders (.getInt row "total_orders")
     :total-spent (.getDouble row "total_spent")
     :last-order-id (.getString row "last_order_id")
     :last-order-timestamp (when-let [ts (.getInstant row "last_order_timestamp")]
                             (.toEpochMilli ts))
     :first-order-timestamp (when-let [ts (.getInstant row "first_order_timestamp")]
                              (.toEpochMilli ts))}))

(defn row->product-stats
  "Converts a Cassandra Row to ::product-stats Clojure map.
  
  Args:
    row - Cassandra Row (or nil)
    
  Returns:
    Map with ::product-stats (or nil if row is nil)"
  [^Row row]
  (when row
    {:product-id (.getString row "product_id")
     :total-quantity (.getInt row "total_quantity")
     :total-revenue (.getDouble row "total_revenue")
     :order-count (.getInt row "order_count")
     :avg-quantity (.getDouble row "avg_quantity")
     :last-order-timestamp (when-let [ts (.getInstant row "last_order_timestamp")]
                             (.toEpochMilli ts))}))

(defn row->timeline-entry
  "Converts a Cassandra Row to ::timeline-entry Clojure map.
  
  Args:
    row - Cassandra Row
    
  Returns:
    Map with ::timeline-entry"
  [^Row row]
  {:order-id (.getString row "order_id")
   :customer-id (.getInt row "customer_id")
   :product-id (.getString row "product_id")
   :total (.getDouble row "total")
   :status (.getString row "status")
   :timestamp (when-let [ts (.getInstant row "timestamp")]
                (.toEpochMilli ts))})

(defn row->processing-stats
  "Converts a Cassandra Row to ::processing-stats Clojure map.
  
  Args:
    row - Cassandra Row (or nil)
    
  Returns:
    Map with ::processing-stats (or nil if row is nil)"
  [^Row row]
  (when row
    {:processor-id (.getString row "processor_id")
     :processed-count (.getLong row "processed_count")
     :error-count (.getLong row "error_count")
     :last-processed-timestamp (when-let [ts (.getInstant row "last_processed_timestamp")]
                                 (.toEpochMilli ts))}))

;; =============================================================================
;; OPERATIONS: Customer Stats
;; =============================================================================

(defn save-customer-stats!
  "Saves/updates a customer's statistics in Cassandra.
  
  Args:
    session - CqlSession
    prepared-stmts - Map with prepared statements
    stats - Map with ::customer-stats
    
  Returns:
    nil (side-effect: saves to database)
    
  Example:
    (save-customer-stats! session stmts customer-stats)"
  [^CqlSession session prepared-stmts stats]
  (log/info "DEBUG-REBUILD-2024-10-29 save-customer-stats EXECUTING")
  (let [stmt ^PreparedStatement (:upsert-customer prepared-stmts)
        empty-array (make-array Object 0)
        bound (.bind stmt empty-array)]
    (-> bound
        (.setInt 0 (:customer-id stats))
        (.setInstant 1 (if-let [ts (:first-order-timestamp stats)]
                         (java.time.Instant/ofEpochMilli ts)
                         nil))
        (.setString 2 (:last-order-id stats))
        (.setInstant 3 (if-let [ts (:last-order-timestamp stats)]
                         (java.time.Instant/ofEpochMilli ts)
                         nil))
        (.setInt 4 (or (:total-orders stats) 0))
        (.setDouble 5 (or (:total-spent stats) 0.0))
        (->> (.execute session)))))

(defn get-customer-stats
  "Fetches a customer's statistics from Cassandra.
  
  Args:
    session - CqlSession
    prepared-stmts - Map with prepared statements
    customer-id - Customer ID (int)
    
  Returns:
    Map with ::customer-stats (or nil if not found)
    
  Example:
    (get-customer-stats session stmts 42)
    ;; => {:customer-id 42 :total-orders 5 ...}"
  [^CqlSession session prepared-stmts customer-id]
  (let [stmt ^PreparedStatement (:get-customer prepared-stmts)
        bound ^BoundStatement (.bind stmt (int customer-id))
        result ^ResultSet (.execute session bound)
        row (.one result)]
    (row->customer-stats row)))

;; =============================================================================
;; OPERATIONS: Product Stats
;; =============================================================================

(defn save-product-stats!
  "Saves/updates a product's statistics in Cassandra.
  
  Args:
    session - CqlSession
    prepared-stmts - Map with prepared statements
    stats - Map with ::product-stats
    
  Returns:
    nil (side-effect: saves to database)"
  [^CqlSession session prepared-stmts stats]
  (let [stmt ^PreparedStatement (:upsert-product prepared-stmts)
        empty-array (make-array Object 0)
        bound (.bind stmt empty-array)]
    (-> bound
        (.setString 0 (:product-id stats))
        (.setInt 1 (or (:total-quantity stats) 0))
        (.setDouble 2 (or (:total-revenue stats) 0.0))
        (.setInt 3 (or (:order-count stats) 0))
        (.setDouble 4 (or (:avg-quantity stats) 0.0))
        (.setInstant 5 (if-let [ts (:last-order-timestamp stats)]
                         (java.time.Instant/ofEpochMilli ts)
                         nil))
        (->> (.execute session)))))

(defn get-product-stats
  "Fetches a product's statistics from Cassandra.
  
  Args:
    session - CqlSession
    prepared-stmts - Map with prepared statements
    product-id - Product ID (string)
    
  Returns:
    Map with ::product-stats (or nil if not found)"
  [^CqlSession session prepared-stmts product-id]
  (let [stmt ^PreparedStatement (:get-product prepared-stmts)
        bound ^BoundStatement (.bind stmt product-id)
        result ^ResultSet (.execute session bound)
        row (.one result)]
    (row->product-stats row)))

;; =============================================================================
;; OPERATIONS: Timeline
;; =============================================================================

(defn save-timeline-entry!
  "Saves an entry to the orders timeline in Cassandra.
  
  Args:
    session - CqlSession
    prepared-stmts - Map with prepared statements
    entry - Map with ::timeline-entry
    
  Returns:
    nil (side-effect: saves to database)
    
  Note: Uses bucket_id = 0 (single bucket strategy)."
  [^CqlSession session prepared-stmts entry]
  (let [stmt ^PreparedStatement (:insert-timeline prepared-stmts)
        empty-array (make-array Object 0)
        bound (.bind stmt empty-array)]
    (-> bound
        (.setInt 0 0)
        (.setInstant 1 (if-let [ts (:timestamp entry)]
                         (java.time.Instant/ofEpochMilli ts)
                         nil))
        (.setString 2 (:order-id entry))
        (.setInt 3 (:customer-id entry))
        (.setString 4 (:product-id entry))
        (.setDouble 5 (:total entry))
        (.setString 6 (:status entry))
        (->> (.execute session)))))

(defn get-timeline
  "Fetches the last N orders from the timeline.
  
  Args:
    session - CqlSession
    prepared-stmts - Map with prepared statements
    limit - Maximum number of orders to fetch (int)
    
  Returns:
    Vector with ::timeline (list of ::timeline-entry)
    
  Example:
    (get-timeline session stmts 100)
    ;; => [{:order-id \"123\" ...} {:order-id \"124\" ...}]"
  [^CqlSession session prepared-stmts limit]
  (let [stmt ^PreparedStatement (:get-timeline prepared-stmts)
        bound ^BoundStatement (.bind stmt (int 0) (int limit))  ; bucket_id = 0, limit
        result ^ResultSet (.execute session bound)]
    (->> result
         (map row->timeline-entry)
         (vec))))

;; =============================================================================
;; OPERATIONS: Processing Stats
;; =============================================================================

(defn save-processing-stats!
  "Saves processing metrics in Cassandra.
  
  Args:
    session - CqlSession
    prepared-stmts - Map with prepared statements
    stats - Map with ::processing-stats
    
  Returns:
    nil (side-effect: saves to database)"
  [^CqlSession session prepared-stmts stats]
  (let [stmt ^PreparedStatement (:upsert-processing-stats prepared-stmts)
        empty-array (make-array Object 0)
        bound (.bind stmt empty-array)]
    (-> bound
        (.setString 0 (:processor-id stats))
        (.setLong 1 (:processed-count stats))
        (.setLong 2 (:error-count stats))
        (.setInstant 3 (if-let [ts (:last-processed-timestamp stats)]
                         (java.time.Instant/ofEpochMilli ts)
                         nil))
        (->> (.execute session)))))

(defn get-processing-stats
  "Fetches processing metrics from Cassandra.
  
  Args:
    session - CqlSession
    prepared-stmts - Map with prepared statements
    processor-id - Processor instance ID (string)
    
  Returns:
    Map with ::processing-stats (or nil if not found)"
  [^CqlSession session prepared-stmts processor-id]
  (let [stmt ^PreparedStatement (:get-processing-stats prepared-stmts)
        bound ^BoundStatement (.bind stmt processor-id)
        result ^ResultSet (.execute session bound)
        row (.one result)]
    (row->processing-stats row)))

;; =============================================================================
;; OPERATIONS: Batch Save (performance)
;; =============================================================================

(defn save-all-views!
  "Saves all views at once (more efficient than saving one by one).
  
  Args:
    session - CqlSession
    prepared-stmts - Map with prepared statements
    views - Map with all views:
            {:customer-stats {customer-id -> stats}
             :product-stats {product-id -> stats}
             :timeline [entries...]
             :processing-stats {...}}
    
  Returns:
    nil (side-effect: saves everything to database)
    
  Note: Doesn't use Cassandra BATCH because tables are different.
  But groups operations to reduce round-trips."
  [session prepared-stmts views]
  (try
    (log/info "START save-all-views - customer-stats count:" (count (:customer-stats views)))
    ;; Save customer stats
    (doseq [[customer-id stats] (:customer-stats views)]
      (log/info "BEFORE saving customer-id:" customer-id)
      (save-customer-stats! session prepared-stmts stats)
      (log/info "AFTER saving customer-id:" customer-id))

    (log/info "Customer stats saved - product-stats count:" (count (:product-stats views)))

    ;; Save product stats
    (doseq [[_product-id stats] (:product-stats views)]
      (save-product-stats! session prepared-stmts stats))

    (log/info "Product stats saved - timeline count:" (count (:timeline views)))

    ;; Save timeline (only last 100 to avoid overloading)
    (doseq [entry (take 100 (:timeline views))]
      (save-timeline-entry! session prepared-stmts entry))

    (log/info "Timeline saved - saving processing stats")

    ;; Save processing stats
    (when-let [stats (:processing-stats views)]
      (save-processing-stats! session prepared-stmts stats))

    (log/info "SUCCESS - Views saved successfully to Cassandra")

    (catch Exception e
      (log/error e "Error saving views to Cassandra")
      (throw (ex-info "Failed to save views"
                      {:views views
                       :error (.getMessage e)})))))

;; =============================================================================
;; OPERATIONS: Load All (for recovery/restart)
;; =============================================================================

(defn load-all-customer-stats
  "Loads ALL customer statistics from Cassandra.
  
  Useful for recovery after processor restart.
  
  Args:
    session - CqlSession
    
  Returns:
    Map {customer-id -> ::customer-stats}
    
  WARNING: Can be slow if there are many customers. Use with care."
  [^CqlSession session]
  (let [result (.execute session
                         (SimpleStatement/newInstance
                          "SELECT * FROM orders_by_customer"))]
    (->> result
         (map row->customer-stats)
         (map (juxt :customer-id identity))
         (into {}))))

(defn load-all-product-stats
  "Loads ALL product statistics from Cassandra.
  
  Returns:
    Map {product-id -> ::product-stats}"
  [^CqlSession session]
  (let [result (.execute session
                         (SimpleStatement/newInstance
                          "SELECT * FROM orders_by_product"))]
    (->> result
         (map row->product-stats)
         (map (juxt :product-id identity))
         (into {}))))

(comment
  ;; Usage examples (for development)

  ;; Create session
  (def session (create-session {:host "localhost"
                                :port 9042
                                :datacenter "datacenter1"
                                :keyspace "query_processor_store"}))

  ;; Prepare statements
  (def stmts (prepare-statements session))

  ;; Save customer stats
  (save-customer-stats! session stmts
                        {:customer-id 42
                         :total-orders 5
                         :total-spent 500.0
                         :last-order-id "ORDER-123"
                         :last-order-timestamp 1234567890
                         :first-order-timestamp 1234560000})

  ;; Fetch customer stats
  (get-customer-stats session stmts 42)
  ;; => {:customer-id 42 :total-orders 5 ...}

  ;; Close session
  (close-session session))