(ns views.dashboard
  (:require [re-frame.core :as rf]
            [reagent.core :as r]
            [monitor.subs :as subs]
            [monitor.events :as events]))

(defn stat-card [{:keys [title value icon color subtitle]}]
  [:div.stat-card {:class color}
   [:div.stat-icon icon]
   [:div.stat-content
    [:div.stat-title title]
    [:div.stat-value (or value "0")]
    (when subtitle
      [:div.stat-subtitle subtitle])]])

(defn dashboard-stats []
  (let [stats @(rf/subscribe [::subs/stats])
        qp (get stats :query-processor {})
        rp (get stats :registry-processor {})
        timeline @(rf/subscribe [::subs/timeline])

        accepted-orders (or (count (filter #(= "accepted" (:status %)) timeline)) 0)
        denied-orders (or (count (filter #(= "denied" (:status %)) timeline)) 0)
        pending-orders (or (count (filter #(= "pending" (:status %)) timeline)) 0)
        total-orders (+ accepted-orders denied-orders pending-orders)
        total-validated (+ accepted-orders denied-orders)]

    [:div
     ;; First row - Main metrics
     [:div.stats-grid
      [stat-card
       {:title "Total Customers"
        :value (or (:customer-count qp) 0)
        :icon "👥"
        :color "blue"
        :subtitle "Active customers"}]
      [stat-card
       {:title "Total Products"
        :value (or (:product-count qp) 0)
        :icon "📦"
        :color "green"
        :subtitle "In catalog"}]
      [stat-card
       {:title "Total Revenue"
        :value (str "$" (.toFixed (or (:total-revenue-accepted qp) 0) 2))
        :icon "💰"
        :color "purple"
        :subtitle "From accepted orders"}]
      [stat-card
       {:title "Total Orders"
        :value total-orders
        :icon "📋"
        :color "gray"
        :subtitle "All time"}]]

     [:div.stats-grid {:style {:margin-top "1.5rem"}}
      [stat-card
       {:title "Pending Orders"
        :value pending-orders
        :icon "⏳"
        :color "yellow"
        :subtitle "Awaiting validation"}]
      [stat-card
       {:title "accepted Orders"
        :value accepted-orders
        :icon "✓"
        :color "green"
        :subtitle "Validated & registered"}]
      [stat-card
       {:title "Denied Orders"
        :value denied-orders
        :icon "✗"
        :color "red"
        :subtitle "Failed validation"}]
      [stat-card
       {:title "Approval Rate"
        :value (if (> total-validated 0)
                 (str (.toFixed (* 100 (/ accepted-orders total-validated)) 1) "%")
                 "N/A")
        :icon "📊"
        :color "blue"
        :subtitle "Success rate"}]]]))

(defn get-current-order-status [timeline order-id]
  (let [order (first (filter #(= (:order-id %) order-id) timeline))]
    (:status order "pending")))

(defn update-order-status!
  "Update order status via API and optimistically update frontend."
  [order-id new-status current-timeline]
  (let [current-order (first (filter #(= (:order-id %) order-id) current-timeline))
        old-status (:status current-order "pending")]

    (rf/dispatch [::events/update-timeline-status order-id new-status])
    (rf/dispatch [::events/update-stats order-id old-status new-status])

    (-> (js/fetch (str "/api/orders/" order-id "/status")
                  #js {:method "PUT"
                       :headers #js {"Content-Type" "application/json"}
                       :body (js/JSON.stringify #js {:status new-status})})
        (.then (fn [response]
                 (if (.-ok response)
                   (.json response)
                   (throw (js/Error. (str "HTTP error: " (.-status response)))))))
        (.then (fn [data]
                 (js/console.log "Order updated successfully:" data)
                 ;;  (rf/dispatch [::events/fetch-stats])
                 ))
        (.catch (fn [error]
                  (rf/dispatch [::events/update-timeline-status order-id old-status])
                  (rf/dispatch [::events/update-stats order-id new-status old-status])
                  (js/console.error "Failed to update order:" error))))))

(defn recent-activity []
  (let [timeline @(rf/subscribe [::subs/timeline])]
    [:div.recent-activity
     [:h3 "Recent Activity"]
     [:div.activity-list
      (if (empty? timeline)
        [:p.empty-state "No recent activity"]
        (for [order (take 10 timeline)]
          ^{:key (:order-id order)}
          [:div.activity-item
           [:div.activity-icon
            (case (:status order)
              "accepted" "✓"
              "denied" "✗"
              "pending" "⏳"
              "📦")]
           [:div.activity-content
            [:div.activity-header
             [:span.order-id (:order-id order)]
             [:span.order-status {:class (:status order)}
              (:status order)]]
            [:div.activity-details
             (str "Customer " (:customer-id order)
                  " • Product " (:product-id order)
                  " • $" (.toFixed (:total order) 2))]]
           [:div.activity-time
            (when-let [ts (:timestamp order)]
              (.toLocaleString (js/Date. ts)))]
           [:div.activity-actions
            (when (= "pending" (:status order))
              [:div {:style {:display "flex" :gap "0.5rem" :margin-top "0.5rem"}}
               [:button.btn.primary
                {:on-click #(update-order-status! (:order-id order) "accepted" timeline)
                 :style {:padding "0.375rem 0.75rem"
                         :font-size "0.875rem"
                         :background "#10b981"}}
                "✓ Approve"]

               [:button.btn.secondary
                {:on-click #(update-order-status! (:order-id order) "denied" timeline)
                 :style {:padding "0.375rem 0.75rem"
                         :font-size "0.875rem"
                         :background "#ef4444"}}
                "✗ Deny"]])]]))]]))

(defonce dashboard-initialized? (atom false))

(defn dashboard-view []
  (when-not @dashboard-initialized?
    (reset! dashboard-initialized? true)
    (rf/dispatch [::events/fetch-all]))

  [:div.dashboard-view
   [:div.dashboard-header
    [:div
     [:h1 "📊 Kafka Monitor Dashboard"]
     [:p.subtitle "Real-time event-driven order processing system"]]
    [:button.btn.primary
     {:on-click #(rf/dispatch [::events/fetch-all])}
     "🔄 Refresh"]]
   [dashboard-stats]
   [:div.dashboard-content
    [:div.left-column
     [recent-activity]]]])