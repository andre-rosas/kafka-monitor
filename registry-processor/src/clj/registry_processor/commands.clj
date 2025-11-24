(ns registry-processor.commands
  "Command system for registry-processor operations.
   
   Provides a multimethod-based command system for:
   - Order validation
   - Order registration
   - Statistics persistence
   - Health checks
   - Query operations"
  (:require [registry-processor.model :as model]
            [registry-processor.validator :as validator]
            [registry-processor.db :as db]
            [clojure.tools.logging :as log]))

;; =============================================================================
;; Command Multimethod
;; =============================================================================

(defmulti execute
  "Execute command based on type.
   
   Args:
     context - Map containing system components (session, stats-atom, etc.)
     command - Map with :type and command-specific data
   
   Returns:
     Map with :success boolean and command-specific results"
  (fn [_context command] (:type command)))

;; =============================================================================
;; Command: Validate (validate order and decide if should register)
;; =============================================================================

(defmethod execute :validate
  [context {:keys [data]}]
  (let [{:keys [stats-atom]} context
        order data]

    (try
      ;; Validate order schema using model specifications
      (when-not (model/valid-order? order)
        (throw (ex-info "Invalid order format"
                        {:order order
                         :explanation (model/explain-validation-error ::model/order order)})))

      ;; Execute business validation rules
      (let [validation-result (validator/validate-order order)
            passed (:passed validation-result)]

        ;; Update statistics atom with validation results
        (swap! stats-atom
               (fn [stats]
                 (-> stats
                     (update :total-validated inc)
                     (update (if passed :total-approved :total-rejected) inc)
                     (assoc :timestamp (System/currentTimeMillis)))))

        (log/info "Order validation completed"
                  {:order-id (:order-id order)
                   :passed passed
                   :failed-rules (when-not passed
                                   (validator/get-failed-rules validation-result))})

        {:success true
         :order-id (:order-id order)
         :validation-result validation-result})

      (catch Exception e
        (log/error e "Error during order validation" {:order order})
        (swap! stats-atom update :total-rejected inc)
        {:success false
         :error (.getMessage e)
         :order-id (:order-id order)}))))

;; =============================================================================
;; Command: Register (save accepted order to Cassandra)
;; =============================================================================

(defmethod execute :register
  [context {:keys [data]}]
  (let [{:keys [session prepared-stmts]} context
        {:keys [order validation-passed]} data]

    (when (or (nil? session) (nil? prepared-stmts))
      (log/error "Missing database session or prepared statements in context")
      {:success false
       :error "Database configuration missing"
       :order-id (:order-id order)})

    (log/info "===> REGISTER command started"
              {:order-id (:order-id order)
               :validation-passed validation-passed})

    (log/info "===> REGISTER command started"
              {:order-id (:order-id order)
               :validation-passed validation-passed})
    (if validation-passed
      (try
        (let [existing (db/get-registered-order session prepared-stmts (:order-id order))]

          (log/info "===> Checked existing order"
                    {:order-id (:order-id order)
                     :exists (some? existing)})

          (if (model/should-update-order? existing order)
            (let [registered-order (if existing
                                     (model/update-order-status existing (:status order))
                                     (model/new-registered-order order validation-passed))]

              (log/info "===> Created registered-order"
                        {:order-id (:order-id order)
                         :version (:version registered-order)
                         :status (:status registered-order)})

              ;; Save to Cassandra
              (db/save-registered-order! session prepared-stmts registered-order)

              (log/info "===> Saved to Cassandra" {:order-id (:order-id order)})

              ;; If updating, save update history
              (when existing
                (let [update-record (model/new-order-update
                                     (:order-id order)
                                     (:version registered-order)
                                     (:status existing)
                                     (:status order)
                                     "Status changed")]
                  (db/save-order-update! session prepared-stmts update-record)))

              (log/info "===> Order registered successfully"
                        {:order-id (:order-id order)
                         :version (:version registered-order)})

              {:success true
               :order-id (:order-id order)
               :registered-order registered-order})

            (do
              (log/debug "===> Order unchanged, skipping" {:order-id (:order-id order)})
              {:success true
               :order-id (:order-id order)
               :skipped true})))

        (catch Exception e
          (log/error e "===> ERROR in REGISTER command"
                     {:order-id (:order-id order)
                      :error-message (.getMessage e)
                      :error-class (.getName (.getClass e))
                      :stack-trace (take 10 (.getStackTrace e))})
          {:success false
           :error (.getMessage e)
           :order-id (:order-id order)}))

      (do
        (log/debug "===> Skipping rejected order" {:order-id (:order-id order)})
        {:success true :order-id (:order-id order) :skipped true}))))

;; =============================================================================
;; Command: Persist Stats
;; =============================================================================

(defmethod execute :persist-stats
  [context _command]
  (let [{:keys [session prepared-stmts stats-atom]} context
        stats @stats-atom]

    (try
      (db/save-validation-stats! session prepared-stmts stats)
      (log/info "Stats persisted" stats)
      {:success true}

      (catch Exception e
        (log/error e "Error persisting stats")
        {:success false
         :error (.getMessage e)}))))

;; =============================================================================
;; Command: Query Registered Order
;; =============================================================================

(defmethod execute :query-registered
  [context {:keys [data]}]
  (let [{:keys [session prepared-stmts]} context
        order-id (:order-id data)]

    (try
      (let [registered-order (db/get-registered-order session prepared-stmts order-id)]
        {:success true
         :data registered-order})

      (catch Exception e
        (log/error e "Error querying registered order" {:order-id order-id})
        {:success false
         :error (.getMessage e)}))))

;; =============================================================================
;; Command: Query Order History
;; =============================================================================

(defmethod execute :query-history
  [context {:keys [data]}]
  (let [{:keys [session prepared-stmts]} context
        order-id (:order-id data)]

    (try
      (let [updates (db/get-order-updates session prepared-stmts order-id)]
        {:success true
         :data updates
         :count (count updates)})

      (catch Exception e
        (log/error e "Error querying order history" {:order-id order-id})
        {:success false
         :error (.getMessage e)}))))

;; =============================================================================
;; Command: Health Check
;; =============================================================================

(defmethod execute :health-check
  [context _command]
  (let [{:keys [session stats-atom]} context
        stats @stats-atom]

    (try
      ;; Test Cassandra connection
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
;; Command: Get Stats
;; =============================================================================

(defmethod execute :get-stats
  [context _command]
  (let [{:keys [stats-atom]} context
        stats @stats-atom]
    {:success true
     :stats stats}))

;; =============================================================================
;; Default Handler
;; =============================================================================

(defmethod execute :default
  [_context command]
  (log/warn "Unknown command type" {:command command})
  {:success false
   :error (str "Unknown command type: " (:type command))})