(ns query-processor.commands
  "Command system using multimethods for query-processor operations.
  
  This namespace provides a clean interface for executing different
  processor operations (consume, query, health-check, etc)."
  (:require [query-processor.model :as model]
            [query-processor.aggregator :as agg]
            [query-processor.db :as db]
            [clojure.tools.logging :as log])
  (:import [com.datastax.oss.driver.api.core CqlSession]))

;; =============================================================================
;; Command Multimethod
;; =============================================================================

(defmulti execute
  "Execute a command based on its type.
  
  Args:
    context - Map with system dependencies (session, prepared-stmts, etc)
    command - Map with {:type :command-name :data {...}}
    
  Returns:
    Result of command execution (varies by command type)
    
  Example:
    (execute context {:type :consume :data order})"
  (fn [_context command] (:type command)))

;; =============================================================================
;; Command: Consume (process order from Kafka)
;; =============================================================================

(defmethod execute :consume
  [context {:keys [data]}]
  (let [{:keys [views-atom prepared-stmts session processor-config]} context
        order data]

    (try
      ;; Validate order
      (when-not (model/valid-order? order)
        (throw (ex-info "Invalid order format"
                        {:order order
                         :explanation (model/explain-validation-error ::model/order order)})))

      ;; Aggregate order into views (pure function)
      (let [updated-views (swap! views-atom
                                 (fn [current-views]
                                   (agg/aggregate-order current-views order processor-config)))]

        (log/debug "Order aggregated successfully" {:order-id (:order-id order)})

        ;; Persist to Cassandra immediately
        (try
          (db/save-all-views! session prepared-stmts updated-views)
          (log/debug "Views persisted" {:order-id (:order-id order)})
          (catch Exception e
            (log/error e "Failed to persist views after aggregation" {:order-id (:order-id order)})))

        ;; Return success
        {:success true
         :order-id (:order-id order)
         :views updated-views})

      (catch Exception e
        (log/error e "Error consuming order" {:order order})

        ;; Increment error counter
        (swap! views-atom update :processing-stats agg/increment-errors)

        {:success false
         :error (.getMessage e)
         :order-id (:order-id order)}))))

;; =============================================================================
;; Command: Persist (save views to Cassandra)
;; =============================================================================

(defmethod execute :persist
  [{:keys [session prepared-stmts views-atom] :as context} _]
  (try
    (log/info "Persisting views to Cassandra")

    ;; Verificar se session e prepared-stmts estão disponíveis
    (if (and session (instance? CqlSession session) prepared-stmts)
      (do
        (db/save-all-views! session prepared-stmts @views-atom)
        {:success true})
      (do
        (log/warn "Cannot persist views: session or prepared statements not available")
        {:success false :error "Session ou prepared statements não disponíveis"}))

    (catch Exception e
      (log/error e "Failed to persist views after aggregation" {:views (keys @views-atom)})
      {:success false :error "Falha ao salvar views"})))

;; =============================================================================
;; Command: Query Customer Stats
;; =============================================================================

(defmethod execute :query-customer
  [context {:keys [data]}]
  (let [{:keys [prepared-stmts session]} context
        customer-id (:customer-id data)]

    (try
      (let [stats (db/get-customer-stats session prepared-stmts customer-id)]
        {:success true
         :data stats})

      (catch Exception e
        (log/error e "Error querying customer stats" {:customer-id customer-id})
        {:success false
         :error (.getMessage e)}))))

;; =============================================================================
;; Command: Query Product Stats
;; =============================================================================

(defmethod execute :query-product
  [context {:keys [data]}]
  (let [{:keys [prepared-stmts session]} context
        product-id (:product-id data)]

    (try
      (let [stats (db/get-product-stats session prepared-stmts product-id)]
        {:success true
         :data stats})

      (catch Exception e
        (log/error e "Error querying product stats" {:product-id product-id})
        {:success false
         :error (.getMessage e)}))))

;; =============================================================================
;; Command: Query Timeline
;; =============================================================================

(defmethod execute :query-timeline
  [context {:keys [data]}]
  (let [{:keys [prepared-stmts session]} context
        limit (:limit data 100)]

    (try
      (let [timeline (db/get-timeline session prepared-stmts limit)]
        {:success true
         :data timeline
         :count (count timeline)})

      (catch Exception e
        (log/error e "Error querying timeline")
        {:success false
         :error (.getMessage e)}))))

;; =============================================================================
;; Command: Health Check
;; =============================================================================

(defmethod execute :health-check
  [context _command]
  (let [{:keys [views-atom session]} context
        views @views-atom
        stats (:processing-stats views)]

    (try
      ;; Check Cassandra connection
      (.execute session
                (com.datastax.oss.driver.api.core.cql.SimpleStatement/newInstance
                 "SELECT now() FROM system.local"))

      {:success true
       :status "healthy"
       :stats stats
       :cassandra "connected"}

      (catch Exception e
        (log/error e "Health check failed")
        {:success false
         :status "unhealthy"
         :error (.getMessage e)}))))

;; =============================================================================
;; Command: Get Stats (in-memory views)
;; =============================================================================

(defmethod execute :get-stats
  [context _command]
  (let [{:keys [views-atom]} context
        views @views-atom]

    {:success true
     :stats {:customer-count (count (:customer-stats views))
             :product-count (count (:product-stats views))
             :timeline-size (count (:timeline views))
             :processing-stats (:processing-stats views)}}))

;; =============================================================================
;; Command: Reset (for testing)
;; =============================================================================

(defmethod execute :reset
  [context _command]
  (let [{:keys [views-atom processor-config]} context
        processor-id (:processor-id processor-config)]

    (log/warn "Resetting views (testing only!)")
    (reset! views-atom (agg/init-views processor-id))

    {:success true
     :message "Views reset to initial state"}))

;; =============================================================================
;; Default Handler
;; =============================================================================

(defmethod execute :default
  [_context command]
  (log/warn "Unknown command type" {:command command})
  {:success false
   :error (str "Unknown command type: " (:type command))})
