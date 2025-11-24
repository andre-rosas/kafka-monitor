(ns registry-processor.db
  "Cassandra database operations for registry-processor.
  
  Keyspace: registry_processor
  
  Tables:
  - registered_orders: Approved orders that passed validation
  - order_updates: History of order status changes
  - validation_stats: Statistics about validation operations"
  (:require [registry-processor.model :as model]
            [clojure.tools.logging :as log])
  (:import [com.datastax.oss.driver.api.core CqlSession]
           [com.datastax.oss.driver.api.core.cql SimpleStatement PreparedStatement BoundStatement ResultSet Row]
           [java.net InetSocketAddress]
           [java.time Duration Instant]))

;; =============================================================================
;; SCHEMA MIGRATIONN
;; =============================================================================

(defn table-exists?
  "Verifica se uma tabela existe no keyspace atual."
  [^CqlSession session table-name]
  (try
    (let [result (.execute session
                           (str "SELECT table_name FROM system_schema.tables "
                                "WHERE keyspace_name = 'registry_processor' "
                                "AND table_name = '" table-name "'"))]
      (-> result .one some?))
    (catch Exception e
      (log/error e "Erro ao verificar existência da tabela" {:table table-name})
      false)))

(defn create-schema!
  "Cria todas as tabelas necessárias se não existirem."
  [^CqlSession session]
  (log/info "Verificando schema do registry processor...")

  ;; Table registered_orders
  (when-not (table-exists? session "registered_orders")
    (log/info "Criando tabela: registered_orders")
    (.execute session
              "CREATE TABLE IF NOT EXISTS registered_orders (
         order_id TEXT PRIMARY KEY,
         customer_id INT,
         product_id TEXT,
         quantity INT,
         total DOUBLE,
         status TEXT,
         registered_at TIMESTAMP,
         updated_at TIMESTAMP,
         version INT,
         validation_passed BOOLEAN
       )"))

  ;; Table order_updates
  (when-not (table-exists? session "order_updates")
    (log/info "Criando tabela: order_updates")
    (.execute session
              "CREATE TABLE IF NOT EXISTS order_updates (
         order_id TEXT,
         version INT,
         previous_status TEXT,
         new_status TEXT,
         updated_at TIMESTAMP,
         update_reason TEXT,
         PRIMARY KEY (order_id, version)
       ) WITH CLUSTERING ORDER BY (version DESC)"))

  ;; Table validation_stats
  (when-not (table-exists? session "validation_stats")
    (log/info "Criando tabela: validation_stats")
    (.execute session
              "CREATE TABLE IF NOT EXISTS validation_stats (
         processor_id TEXT PRIMARY KEY,
         total_validated BIGINT,
         total_approved BIGINT,
         total_rejected BIGINT,
         timestamp TIMESTAMP
       )"))

  (log/info "Verificação do schema do registry processor concluída"))

;; =============================================================================
;; Connection
;; =============================================================================

(defn create-session
  "Create Cassandra session."
  [{:keys [host port datacenter keyspace]}]
  (let [host-str (str host)
        port-val (if (string? port)
                   (Integer/parseInt port)
                   (int port))
        port-int (int port-val)]
    (log/info "Connecting to Cassandra" {:host host-str :port port-int :keyspace keyspace})
    (try
      (-> (CqlSession/builder)
          (.addContactPoint (InetSocketAddress. host-str port-int))
          (.withLocalDatacenter datacenter)
          (.withKeyspace keyspace)
          (.build))
      (catch Exception e
        (log/error e "Failed to create Cassandra session")
        (throw (ex-info "Cassandra connection failed"
                        {:config {:host host :port port}
                         :error (.getMessage e)}))))))

(defn close-session
  "Close Cassandra session."
  [^CqlSession session]
  (log/info "Closing Cassandra session")
  (.close session))

;; =============================================================================
;; Prepared Statements
;; =============================================================================

(defn prepare-statements
  "Prepare all CQL statements."
  [^CqlSession session]
  (log/info "Preparing CQL statements")
  {:upsert-registered-order
   (.prepare session
             "INSERT INTO registered_orders 
              (order_id, customer_id, product_id, quantity, unit_price, total, status, 
              registered_at, updated_at, version, validation_passed)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

   :get-registered-order
   (.prepare session
             "SELECT * FROM registered_orders WHERE order_id = ?")

   :insert-order-update
   (.prepare session
             "INSERT INTO order_updates
              (order_id, version, previous_status, new_status, updated_at, update_reason)
              VALUES (?, ?, ?, ?, ?, ?)")

   :get-order-updates
   (.prepare session
             "SELECT * FROM order_updates WHERE order_id = ?")

   :upsert-validation-stats
   (.prepare session
             "INSERT INTO validation_stats
              (processor_id, total_validated, total_approved, total_rejected, timestamp)
              VALUES (?, ?, ?, ?, ?)")

   :get-validation-stats
   (.prepare session
             "SELECT * FROM validation_stats WHERE processor_id = ?")})

;; =============================================================================
;; Row Conversion
;; =============================================================================

(defn row->registered-order
  "Convert Cassandra Row to registered-order map."
  [^Row row]
  (when row
    {:order-id (.getString row "order_id")
     :customer-id (.getInt row "customer_id")
     :product-id (.getString row "product_id")
     :quantity (.getInt row "quantity")
     :total (.getDouble row "total")
     :status (.getString row "status")
     :registered-at (when-let [ts (.getInstant row "registered_at")]
                      (.toEpochMilli ts))
     :updated-at (when-let [ts (.getInstant row "updated_at")]
                   (.toEpochMilli ts))
     :version (.getInt row "version")
     :validation-passed (.getBoolean row "validation_passed")}))

(defn row->order-update
  "Convert Cassandra Row to order-update map."
  [^Row row]
  {:order-id (.getString row "order_id")
   :version (.getInt row "version")
   :previous-status (.getString row "previous_status")
   :new-status (.getString row "new_status")
   :updated-at (when-let [ts (.getInstant row "updated_at")]
                 (.toEpochMilli ts))
   :update-reason (.getString row "update_reason")})

(defn row->validation-stats
  "Convert Cassandra Row to validation-stats map."
  [^Row row]
  (when row
    {:processor-id (.getString row "processor_id")
     :total-validated (.getLong row "total_validated")
     :total-approved (.getLong row "total_approved")
     :total-rejected (.getLong row "total_rejected")
     :timestamp (when-let [ts (.getInstant row "timestamp")]
                  (.toEpochMilli ts))}))

;; =============================================================================
;; Registered Orders Operations
;; =============================================================================

(defn save-registered-order!
  [^CqlSession session prepared-stmts registered-order]
  (let [stmt ^PreparedStatement (:upsert-registered-order prepared-stmts)
        bound ^BoundStatement (.bind stmt
                                     (into-array Object
                                                 [(:order-id registered-order)
                                                  (int (:customer-id registered-order))
                                                  (:product-id registered-order)
                                                  (int (:quantity registered-order))
                                                  (double (:unit-price registered-order))
                                                  (double (:total registered-order))
                                                  (:status registered-order)
                                                  (Instant/ofEpochMilli (:registered-at registered-order))
                                                  (when-let [ts (:updated-at registered-order)]
                                                    (Instant/ofEpochMilli ts))
                                                  (int (:version registered-order))
                                                  (boolean (:validation-passed registered-order))]))]
    (.execute session bound)))

(defn get-registered-order
  "Get registered order by ID."
  [^CqlSession session prepared-stmts order-id]
  (let [stmt ^PreparedStatement (:get-registered-order prepared-stmts)
        bound ^BoundStatement (.bind stmt (into-array Object [order-id]))
        result ^ResultSet (.execute session bound)
        row (.one result)]
    (row->registered-order row)))

;; =============================================================================
;; Order Updates Operations
;; =============================================================================

(defn save-order-update!
  [^CqlSession session prepared-stmts order-update]
  (let [stmt ^PreparedStatement (:insert-order-update prepared-stmts)
        bound ^BoundStatement (.bind stmt
                                     (into-array Object
                                                 [(:order-id order-update)
                                                  (int (:version order-update))
                                                  (:previous-status order-update)
                                                  (:new-status order-update)
                                                  (Instant/ofEpochMilli (:updated-at order-update))
                                                  (:update-reason order-update)]))]
    (.execute session bound)))

(defn get-order-updates
  [^CqlSession session prepared-stmts order-id]
  (let [stmt ^PreparedStatement (:get-order-updates prepared-stmts)
        bound ^BoundStatement (.bind stmt (into-array Object [order-id]))
        result ^ResultSet (.execute session bound)]
    (->> result
         (map row->order-update)
         (vec))))

;; =============================================================================
;; Validation Stats Operations
;; =============================================================================

(defn save-validation-stats!
  "Save validation statistics."
  [^CqlSession session prepared-stmts stats]
  (let [stmt ^PreparedStatement (:upsert-validation-stats prepared-stmts)
        bound ^BoundStatement (.bind stmt
                                     (into-array Object
                                                 [(:processor-id stats)
                                                  (long (:total-validated stats))
                                                  (long (:total-approved stats))
                                                  (long (:total-rejected stats))
                                                  (Instant/ofEpochMilli (:timestamp stats))]))]
    (.execute session bound)))

(defn get-validation-stats
  [^CqlSession session prepared-stmts processor-id]
  (let [stmt ^PreparedStatement (:get-validation-stats prepared-stmts)
        bound ^BoundStatement (.bind stmt (into-array Object [processor-id]))
        result ^ResultSet (.execute session bound)
        row (.one result)]
    (row->validation-stats row)))

(comment
  ;; Example usage
  (def session (create-session {:host "localhost"
                                :port 9042
                                :datacenter "datacenter1"
                                :keyspace "registry_processor"}))

  (def stmts (prepare-statements session))

  ;; Save registered order
  (save-registered-order! session stmts
                          {:order-id "ORDER-001"
                           :customer-id 42
                           :product-id "PROD-A"
                           :quantity 5
                           :total 150.0
                           :status "accepted"
                           :registered-at (System/currentTimeMillis)
                           :version 1
                           :validation-passed true})

  ;; Get registered order
  (get-registered-order session stmts "ORDER-001")

  (close-session session))