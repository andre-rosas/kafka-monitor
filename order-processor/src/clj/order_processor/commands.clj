(ns order-processor.commands
  "Command execution using Clojure multimethods.
   
   This demonstrates:
   - Multimethod dispatch (polymorphism in Clojure)
   - Command pattern for executable operations
   - Extensibility (easy to add new commands)
   
   Why multimethods?
   - Open/closed principle: Add new commands without changing existing code
   - Type-based dispatch: Different behavior based on command type
   - Cleaner than giant case statements
   - Testable: Each command is isolated
   
   Usage:
   (execute :start-producer {})
   (execute :send-order {:customer-id 1 :product-id \"P1\" ...})
   (execute :get-stats {})"
  (:require [order-processor.core :as core]
            [order-processor.model :as model]
            [order-processor.db :as db]
            [taoensso.timbre :as log]))

;; =============================================================================
;; Multimethod Definition
;; =============================================================================

(defmulti execute
  "Execute a command.
   
   Dispatches on the :command key in the argument map.
   Each command is implemented as a separate defmethod.
   
   Example:
   (execute {:command :start-producer})
   (execute {:command :send-order :order order-data})"
  :command)

;; =============================================================================
;; Lifecycle Commands
;; =============================================================================

(defmethod execute :start-producer
  ;; "Start the producer.

  ;;  Usage: (execute {:command :start-producer})"
  [_]
  (log/info "Executing command: start-producer")
  (core/start!)
  {:status :ok
   :message "Producer started"})

(defmethod execute :stop-producer
  ;; "Stop the producer.

  ;;  Usage: (execute {:command :stop-producer})"
  [_]
  (log/info "Executing command: stop-producer")
  (core/stop!)
  {:status :ok
   :message "Producer stopped"
   :stats (core/get-stats)})

;; =============================================================================
;; Order Commands
;; =============================================================================

(defmethod execute :send-order
  ;; "Send a single order.

  ;;  Usage: (execute {:command :send-order
  ;;                   :order {:customer-id 1
  ;;                           :product-id \"PROD-001\"
  ;;                           :quantity 5
  ;;                           :price 19.99}})"
  [{:keys [order]}]
  (log/info "Executing command: send-order")
  (let [valid-order (model/new-order order)
        producer (:producer @core/app-state)]
    (if producer
      (do
        (core/send-to-kafka! producer valid-order)
        {:status :ok
         :order-id (:order-id valid-order)
         :message "Order sent"})
      {:status :error
       :message "Producer not running"})))

(defmethod execute :send-n-orders
  ;; "Send N random orders.

  ;;  Usage: (execute {:command :send-n-orders :n 100})"
  [{:keys [n]}]
  (log/info "Executing command: send-n-orders" {:n n})
  (let [producer (:producer @core/app-state)]
    (if producer
      (do
        (dotimes [i n]
          (let [order (core/generate-order)]
            (core/send-to-kafka! producer order))
          (when (zero? (mod i 100))
            (log/info "Sent orders" {:count i})))
        {:status :ok
         :message (str "Sent " n " orders")})
      {:status :error
       :message "Producer not running"})))

;; =============================================================================
;; Query Commands
;; =============================================================================

(defmethod execute :get-stats
  ;; "Get production statistics.

  ;;  Usage: (execute {:command :get-stats})"
  [_]
  (log/debug "Executing command: get-stats")
  {:status :ok
   :stats (core/get-stats)})

(defmethod execute :get-order
  ;; "Get order by ID from database.

  ;;  Usage: (execute {:command :get-order :order-id \"123e4567-...\"})"
  [{:keys [order-id]}]
  (log/debug "Executing command: get-order" {:order-id order-id})
  (if-let [order (db/get-order order-id)]
    {:status :ok
     :order order}
    {:status :not-found
     :message "Order not found"}))

(defmethod execute :get-recent-orders
  ;; "Get N most recent orders from database.

  ;;  Usage: (execute {:command :get-recent-orders :n 10})"
  [{:keys [n] :or {n 10}}]
  (log/debug "Executing command: get-recent-orders" {:n n})
  {:status :ok
   :orders (db/get-recent-orders n)})

(defmethod execute :count-orders
  ;; "Count total orders in database.

  ;;  Usage: (execute {:command :count-orders})"
  [_]
  (log/debug "Executing command: count-orders")
  {:status :ok
   :count (db/count-orders)})

;; =============================================================================
;; Health Commands
;; =============================================================================

(defmethod execute :health-check
  ;; "Check health of all components.

  ;;  Usage: (execute {:command :health-check})"
  [_]
  (log/debug "Executing command: health-check")
  (let [running? (:running? @core/app-state)
        db-healthy? (db/healthy?)]
    {:status (if (and running? db-healthy?) :ok :degraded)
     :producer-running running?
     :database-healthy db-healthy?
     :stats (core/get-stats)}))

;; =============================================================================
;; Default Handler
;; =============================================================================

(defmethod execute :default
  ;; "Handle unknown commands.

  ;;  This is called when no matching defmethod exists."
  [{:keys [command]}]
  (log/warn "Unknown command" {:command command})
  {:status :error
   :message (str "Unknown command: " command)
   :available-commands [:start-producer
                        :stop-producer
                        :send-order
                        :send-n-orders
                        :get-stats
                        :get-order
                        :get-recent-orders
                        :count-orders
                        :health-check]})