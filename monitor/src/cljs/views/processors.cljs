(ns views.processors
  "Processors view - monitor system health.")

(defn processor-card [{:keys [name status description]}]
  [:div.processor-card {:class (str "status-" (clojure.core/name status))}
   [:div.processor-header
    [:h4 name]
    [:span.status-badge {:class (str "status-" (clojure.core/name status))}
     (clojure.core/name status)]]
   [:div.processor-body
    [:p description]]])

(defn processors-view []
  [:div.processors-view
   [:div.view-header
    [:h1 "Processors"]
    [:p.subtitle "Monitor Kafka processors and system health"]]
   [:div.processors-grid
    [processor-card
     {:name "Order Processor"
      :status :healthy
      :description "Generating 10 orders/second with random data (customer 1-100, products PROD-001 to 005)"}]
    [processor-card
     {:name "Query Processor"
      :status :healthy
      :description "Aggregating data into materialized views: by customer, by product, and timeline"}]
    [processor-card
     {:name "Registry Processor"
      :status :healthy
      :description "Validating orders against business rules and registering approved orders"}]
    [processor-card
     {:name "Cassandra Database"
      :status :healthy
      :description "Distributed NoSQL database storing aggregated views and registered orders"}]]])