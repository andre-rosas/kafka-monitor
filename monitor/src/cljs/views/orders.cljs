(ns views.orders
  "Orders view - complete table with search."
  (:require [reagent.core :as r]
            [re-frame.core :as rf]
            [monitor.events :as events]
            [monitor.subs :as subs]))

;; =============================================================================
;; Helper Functions 
;; =============================================================================

(defn format-timestamp [ts]
  (when ts
    (let [date (js/Date. ts)]
      (.toLocaleString date "pt-BR"
                       #js {:year "numeric"
                            :month "2-digit"
                            :day "2-digit"
                            :hour "2-digit"
                            :minute "2-digit"
                            :second "2-digit"}))))

(defn format-currency [value]
  (when value
    (str "$" (.toFixed value 2))))

(defn status-badge [status]
  [:span.status-badge {:class status}
   (case status
     "pending" "⏳ Pending"
     "accepted" "✓ Accepted"
     "denied" "✗ Denied"
     status)])

;; =============================================================================
;; Search Component (ESTILO PADRONIZADO)
;; =============================================================================

(defn order-search []
  (let [search-term (r/atom "")
        search-result (r/atom nil)
        loading? (r/atom false)
        error (r/atom nil)]
    (fn []
      [:div.search-section
       [:h3 "Search Orders"]
       [:div.search-box
        [:input.form-control
         {:type "text"
          :placeholder "Search by Order ID (complete UUID)"
          :value @search-term
          :on-change #(reset! search-term (-> % .-target .-value))
          :disabled @loading?}]
        [:button.btn.primary
         {:on-click (fn []
                      (when-not (empty? @search-term)
                        (reset! loading? true)
                        (reset! error nil)
                        (-> (js/fetch (str "/api/orders/search/" @search-term))
                            (.then #(.json %))
                            (.then (fn [data]
                                     (let [parsed (js->clj data :keywordize-keys true)]
                                       (if (:success parsed)
                                         (reset! search-result parsed)
                                         (reset! error (:message parsed))))))
                            (.catch #(reset! error (.-message %)))
                            (.finally #(reset! loading? false)))))
          :disabled (or @loading? (empty? @search-term))}
         (if @loading? "🔍 Searching..." "🔍 Search")]
        (when-not (empty? @search-term)
          [:button.btn.secondary
           {:on-click #(do
                         (reset! search-term "")
                         (reset! search-result nil)
                         (reset! error nil))}
           "✕ Clear"])]

       (when @error
         [:div.error-alert "⚠️ " @error])

       (when @search-result
         (let [order (:data @search-result)]
           [:div.search-result
            [:h4 "Order Details"]
            [:table.details-table
             [:tbody
              [:tr [:td "Order ID"] [:td {:style {:font-family "monospace"}} (:order-id order)]]
              [:tr [:td "Customer ID"] [:td (:customer-id order)]]
              [:tr [:td "Product ID"] [:td (:product-id order)]]
              [:tr [:td "Quantity"] [:td (:quantity order)]]
              [:tr [:td "Unit Price"] [:td (format-currency (:unit-price order))]]
              [:tr [:td "Total"] [:td (format-currency (:total order))]]
              [:tr [:td "Status"] [:td [status-badge (:status order)]]]
              [:tr [:td "Timestamp"] [:td (format-timestamp (:timestamp order))]]]]
            (when (and (= "registered" (:source @search-result))
                       (= "accepted" (get-in @search-result [:data :status])))
              [:div.info-box
               [:p "✓ This order is validated and accepted in the registry."]])

            (when (and (= "registered" (:source @search-result))
                       (= "denied" (get-in @search-result [:data :status])))
              [:div.error-box
               [:p "✗ This order was validated, but denied."]])]))])))

;; =============================================================================
;; Orders Table Component
;; =============================================================================

(defn orders-table []
  (let [orders (rf/subscribe [::subs/timeline])
        loading? (rf/subscribe [::subs/loading?])]
    (fn []
      [:div.orders-main
       [:div.table-header
        [:h3 "Recent Orders"]
        [:button.btn.primary
         {:on-click #(rf/dispatch [::events/fetch-timeline])}
         "🔄 Refresh"]]

       (if @loading?
         [:div.loading "Loading orders..."]
         (if (empty? @orders)
           [:div.empty-state
            [:p "No orders found"]]
           [:div.table-container
            [:table.sortable-table
             [:thead
              [:tr
               [:th "Order ID"]
               [:th "Customer"]
               [:th "Product"]
               [:th "Qty"]
               [:th "Total"]
               [:th "Status"]
               [:th "Date/Time"]]]
             [:tbody
              (for [order @orders]
                ^{:key (:order-id order)}
                [:tr
                 [:td
                  [:code {:style {:font-size "0.8em" :wordBreak "break-all"}}
                   (:order-id order)]]
                 [:td (:customer-id order)]
                 [:td (:product-id order)]
                 [:td (:quantity order)]
                 [:td (format-currency (:total order))]
                 [:td [status-badge (:status order)]]
                 [:td.timestamp (format-timestamp (:timestamp order))]])]]]))])))

;; =============================================================================
;; Orders Info Sidebar
;; =============================================================================

(defn orders-info []
  [:div.orders-info
   [:h3 "About Orders"]
   [:div.info-box
    [:p "Real-time order processing and validation system."]
    [:h4 "📊 Features:"]
    [:ul
     [:li [:strong "Search:"] " Find orders by complete UUID"]
     [:li [:strong "Filter:"] " View orders by status"]
     [:li [:strong "Real-time:"] " Live updates from Kafka"]
     [:li [:strong "Validation:"] " See order approval status"]]
    [:h4 "🔄 Status Types:"]
    [:ul
     [:li "⏳ Pending - Awaiting validation"]
     [:li "✓ Accepted - Approved and registered"]
     [:li "✗ Denied - Failed validation"]]
    [:p [:strong "Note:"] " Table shows 100 most recent orders"]]])

;; =============================================================================
;; Main Orders View
;; =============================================================================

(defn orders-view []
  (r/create-class
   {:component-did-mount
    (fn []
      (rf/dispatch [::events/fetch-timeline]))
    :reagent-render
    (fn []
      [:div.orders-view
       [:div.view-header
        [:h1 "📦 Orders"]
        [:p.subtitle "View and search system orders"]]
       [:div.orders-content
        [:div.orders-main
         [order-search]
         [orders-table]]
        [:div.orders-sidebar
         [orders-info]]]])}))