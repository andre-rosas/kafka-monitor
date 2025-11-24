(ns monitor.server
  "Monitor API server - Ring + Compojure REST endpoints."
  (:gen-class)
  (:require [compojure.core :refer [defroutes GET POST PUT]]
            [compojure.route :as route]
            [ring.middleware.json :refer [wrap-json-response wrap-json-body]]
            [ring.middleware.cors :refer [wrap-cors]]
            [ring.util.response :refer [response status]]
            [monitor.cassandra :as cass]
            [clojure.tools.logging :as log]
            [ring.adapter.jetty :as jetty])
  (:import [java.io File]))

;; =============================================================================
;; STATIC FILE SERVING
;; =============================================================================

(defn serve-static-file
  "Serve static files from resources/public directory."
  [filepath content-type]

  (let [file (File. (str "resources/public/" filepath))]
    (if (.exists file)
      {:status 200
       :headers {"Content-Type" content-type}
       :body (slurp file)}
      {:status 404
       :headers {"Content-Type" "text/plain"}
       :body "Not found"})))

;; =============================================================================
;; API HANDLERS
;; =============================================================================

(defn health-handler
  "Health check endpoint - verifies Cassandra connection."
  [_request]

  (try
    (let [cassandra-status (if (cass/check-connection) "connected" "disconnected")]
      (response {:status "healthy"
                 :cassandra cassandra-status
                 :timestamp (System/currentTimeMillis)}))
    (catch Exception e
      (log/error e "Health check failed")
      (-> (response {:status "unhealthy" :error (.getMessage e)})
          (status 500)))))

(defn get-stats-handler
  "Get aggregated statistics from all processors."
  [_request]

  (try
    (let [stats (cass/get-all-stats)]
      (response {:success true :data stats}))
    (catch Exception e
      (log/error e "Error fetching stats")
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

(defn get-customer-handler
  "Get customer statistics by ID."
  [{:keys [params]}]

  (try
    (let [customer-id (Integer/parseInt (:id params))
          stats (cass/get-customer-stats customer-id)]
      (if stats
        (response {:success true :data stats})
        (-> (response {:success false :message "Customer not found"})
            (status 404))))
    (catch NumberFormatException _
      (-> (response {:success false :error "Invalid customer ID"})
          (status 400)))
    (catch Exception e
      (log/error e "Error fetching customer" {:params params})
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

(defn get-top-customers-handler
  "Get all customers (limited to 100) sorted by total spent."
  [_request]

  (try
    (let [customers (cass/get-all-customers-limited 100)]
      (response {:success true :data customers :count (count customers)}))
    (catch Exception e
      (log/error e "Error fetching top customers")
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

(defn search-customers-handler
  "Search customers by ID or return top N."
  [{:keys [params]}]

  (try
    (let [query (get params :q "")
          limit (Integer/parseInt (get params :limit "50"))
          customers (if (empty? query)
                      (cass/get-top-customers limit)
                      (let [customer-id (try (Integer/parseInt query) (catch Exception _ nil))]
                        (if customer-id
                          (if-let [customer (cass/get-customer-stats customer-id)]
                            [customer]
                            [])
                          [])))]
      (response {:success true :data customers :count (count customers) :query query}))
    (catch Exception e
      (log/error e "Error searching customers" {:params params})
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

(defn get-product-handler
  "Get product statistics by ID."
  [{:keys [params]}]

  (try
    (let [product-id (:id params)
          stats (cass/get-product-stats product-id)]
      (if stats
        (response {:success true :data stats})
        (-> (response {:success false :message "Product not found"})
            (status 404))))
    (catch Exception e
      (log/error e "Error fetching product" {:params params})
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

(defn get-top-products-handler
  "Get top 10 products by revenue."
  [_request]

  (try
    (let [products (cass/get-top-products 10)]
      (response {:success true :data products :count (count products)}))
    (catch Exception e
      (log/error e "Error fetching top products")
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

(defn get-orders-handler
  "Fetch the list of recent orders."
  [_request]
  (try
    (let [orders (cass/get-recent-orders 100)]
      (response {:status "success" :data orders}))
    (catch Exception e
      (log/error e "Error fetching orders")
      (status (response {:status "error" :message "Failed to fetch orders."}) 500))))

(defn search-products-handler
  "Search products by ID or return top N."
  [{:keys [params]}]

  (try
    (let [query (get params :q "")
          limit (Integer/parseInt (get params :limit "50"))
          products (if (empty? query)
                     (cass/get-top-products limit)
                     (let [all-products (cass/get-top-products 100)
                           query-upper (clojure.string/upper-case query)]
                       (filter #(clojure.string/starts-with? (:product-id %) query-upper)
                               all-products)))]
      (response {:success true :data products :count (count products) :query query}))
    (catch Exception e
      (log/error e "Error searching products" {:params params})
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

(defn get-timeline-handler
  "Get orders timeline with real status from registered_orders."
  [{:keys [params]}]

  (try
    (let [limit (Integer/parseInt (get params :limit "100"))
          timeline (cass/get-timeline-with-status limit)]
      (response {:success true :data timeline :count (count timeline)}))
    (catch Exception e
      (log/error e "Error fetching timeline")
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

(defn get-registered-order-handler
  "Get registered order by ID."
  [{:keys [params]}]

  (try
    (let [order-id (:id params)
          order (cass/get-registered-order order-id)]
      (if order
        (response {:success true :data order})
        (-> (response {:success false :message "Order not found"})
            (status 404))))
    (catch Exception e
      (log/error e "Error fetching registered order" {:params params})
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

(defn get-order-history-handler
  "Get order update history."
  [{:keys [params]}]
  (try
    (let [order-id (:id params)
          history (cass/get-order-history order-id)]
      (response {:success true :data history :count (count history)}))
    (catch Exception e
      (log/error e "Error fetching order history" {:params params})
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

(defn update-order-status-handler
  "Update order status manually (approve/deny)."
  [{:keys [params body]}]
  (try
    (let [order-id (:id params)
          new-status (:status body)]
      (if (contains? #{"accepted" "denied"} new-status)
        (do
          (cass/update-order-status order-id new-status)
          (response {:success true
                     :message (str "Order " order-id " updated to " new-status)
                     :order-id order-id
                     :status new-status}))
        (-> (response {:success false :error "Invalid status. Use 'accepted' or 'denied'"})
            (status 400))))
    (catch Exception e
      (log/error e "Error updating order status" {:params params :body body})
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

(defn create-order-handler
  "Order creation endpoint (not implemented - auto-generated by order-processor)."
  [_request]

  (try
    (-> (response {:success false
                   :message "Order creation not implemented. Orders are auto-generated by order-processor."})
        (status 501))
    (catch Exception e
      (log/error e "Error creating order")
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

(defn get-processors-status-handler
  "Get status of all processors."
  [_request]
  (try
    (response {:success true
               :data {:processors [{:name "order-processor" :status "healthy" :uptime "99.9%"}
                                   {:name "query-processor" :status "healthy" :uptime "99.8%"}
                                   {:name "registry-processor" :status "healthy" :uptime "99.7%"}]}
               :timestamp (System/currentTimeMillis)})
    (catch Exception e
      (log/error e "Error fetching processors status")
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

(defn search-order-handler
  "Search order - PRIORIZES registered_orders sobre timeline."
  [{:keys [params]}]
  (try
    (let [order-id (:id params)

          ;; Validates UUID
          valid-uuid? (try
                        (java.util.UUID/fromString order-id)
                        true
                        (catch Exception _
                          false))]

      (if (not valid-uuid?)
        (-> (response {:success false :error "Invalid UUID format"})
            (status 400))

        (let [;; Tries to FIND it first in registered_orders
              registered (cass/get-registered-order order-id)

              ;; Only search in timeline if it was not found in registered_orders
              timeline-order (when-not registered
                               (let [timeline (cass/get-timeline 1000)]
                                 (first (filter #(= order-id (:order-id %)) timeline))))]

          (cond
            ;; Id found in registered_orders, always returns this
            registered
            (response {:success true
                       :data registered
                       :source "registered"
                       :message "Order found in registered_orders"})

            ;;Not found in registered, but found in timeline
            timeline-order
            (response {:success true
                       :data timeline-order
                       :source "timeline"
                       :message "Order found in timeline (not yet registered)"})

            ;; IT didn't not find anywhere
            :else
            (-> (response {:success false
                           :message (str "Order " order-id " not found")})
                (status 404))))))

    (catch Exception e
      (log/error e "Error searching order" {:params params})
      (-> (response {:success false :error "Internal server error while searching order"})
          (status 500)))))

(defn get-orders-by-status-handler
  "Get orders by status from registered_orders."
  [{:keys [params]}]
  (try
    (let [status (:status params)
          limit (Integer/parseInt (get params :limit "100"))
          orders (cass/get-orders-by-status status limit)]
      (response {:success true :data orders :count (count orders)}))
    (catch Exception e
      (log/error e "Error fetching orders by status" {:params params})
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

(defn debug-order-handler
  "DEBUG: Check where order exists."
  [{:keys [params]}]
  (try
    (let [order-id (:id params)
          in-registered (cass/get-registered-order order-id)
          timeline (cass/get-timeline 1000)
          in-timeline (first (filter #(= order-id (:order-id %)) timeline))]

      (response {:success true
                 :order-id order-id
                 :in-registered (boolean in-registered)
                 :in-timeline (boolean in-timeline)
                 :registered-data in-registered
                 :timeline-data in-timeline}))
    (catch Exception e
      (log/error e "Debug error")
      (-> (response {:success false :error (.getMessage e)})
          (status 500)))))

;; =============================================================================
;; ROUTES
;; =============================================================================

(defroutes api-routes
  ;; Health & Stats
  (GET "/api/health" [] health-handler)
  (GET "/api/stats" [] get-stats-handler)

  ;; Customers
  (GET "/api/customers/top" [] get-top-customers-handler)
  (GET "/api/customers/search" [] search-customers-handler)
  (GET "/api/customers/:id" [] get-customer-handler)
  (GET "/api/orders" [] get-orders-handler)

  ;; Products
  (GET "/api/products/top" [] get-top-products-handler)
  (GET "/api/products/search" [] search-products-handler)
  (GET "/api/products/:id" [] get-product-handler)

  ;; Orders
  (GET "/api/timeline" [] get-timeline-handler)
  (GET "/api/orders/search/:id" [] search-order-handler)
  (GET "/api/orders/by-status/:status" [] get-orders-by-status-handler)
  (GET "/api/registered/:id" [] get-registered-order-handler)
  (GET "/api/orders/:id/history" [] get-order-history-handler)
  (PUT "/api/orders/:id/status" [] update-order-status-handler)
  (POST "/api/orders" [] create-order-handler)
  (GET "/api/debug/order/:id" [] debug-order-handler)

  ;; Processors
  (GET "/api/processors/status" [] get-processors-status-handler)

  ;; Static files
  (GET "/" [] (serve-static-file "index.html" "text/html; charset=utf-8"))
  (GET "/css/style.css" [] (serve-static-file "css/style.css" "text/css; charset=utf-8"))
  (GET "/js/compiled/app.js" [] (serve-static-file "js/compiled/app.js" "text/javascript; charset=utf-8"))

  ;; 404
  (route/not-found
   {:status 404
    :headers {"Content-Type" "application/json"}
    :body "{\"success\": false, \"error\": \"Not found\"}"}))

;; =============================================================================
;; MIDDLEWARE
;; =============================================================================

(def app
  "Ring app with middleware stack."
  (-> api-routes
      (wrap-json-body {:keywords? true})
      wrap-json-response
      (wrap-cors :access-control-allow-origin [#".*"]
                 :access-control-allow-methods [:get :post :put :delete :options]
                 :access-control-allow-headers ["Content-Type" "Authorization"])))

;; =============================================================================
;; SERVER LIFECYCLE
;; =============================================================================

(defonce server (atom nil))  ;; Stores the Jetty server instance

(defn start-server!
  "Start Jetty HTTP server."
  []
  (let [port (Integer/parseInt (or (System/getenv "PORT") "8080"))]
    (log/info "Starting server on port" port)
    (reset! server (jetty/run-jetty #'app {:port port :join? false}))
    (log/info "Server started.")))

(defn stop-server!
  "Stop Jetty server."
  []
  (when @server
    (log/info "Stopping server...")
    (.stop @server)
    (reset! server nil)
    (log/info "Server stopped."))) ; Close Cassandra connections gracefully

(defn -main
  "Main entry point. Starts server and adds shutdown hook."
  [& _args]
  (.addShutdownHook (Runtime/getRuntime)
                    (Thread. ^Runnable stop-server!))
  (start-server!)
  @(promise)) ; Keep the main thread aliv