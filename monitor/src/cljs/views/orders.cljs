(ns views.orders
  "Orders view - search registered orders by UUID."
  (:require [reagent.core :as r]))

(defn search-order!
  "Search for order by UUID via API."
  [order-id result-atom loading-atom error-atom]

  (reset! loading-atom true)
  (reset! error-atom nil)
  (reset! result-atom nil)

  (-> (js/fetch (str "/api/orders/search/" order-id))
      (.then (fn [response]
               (if (.-ok response)
                 (.json response)
                 (throw (js/Error. "Order not found")))))
      (.then (fn [data]
               (let [parsed (js->clj data :keywordize-keys true)]
                 (if (:success parsed)
                   (reset! result-atom parsed)
                   (reset! error-atom (:message parsed "Order not found"))))))
      (.catch (fn [error]
                (reset! error-atom (.-message error))))
      (.finally (fn []
                  (reset! loading-atom false)))))

(defn format-timestamp
  "Format timestamp to readable date."
  [ts]

  (when ts
    (.toLocaleString (js/Date. ts))))

(defn order-result-card [result]
  "Display order details."
  (let [order (:data result)
        source (:source result)
        message (:message result)]
    [:div.search-result
     [:div {:style {:display "flex" :justify-content "space-between" :align-items "center" :margin-bottom "1rem"}}
      [:h4 "Order Details"]
      [:span.status-badge {:class source}
       (if (= source "registered") "✓ Validated" "⏳ Pending Validation")]]

     (when message
       [:p {:style {:color "#94a3b8" :margin-bottom "1rem"}} message])

     [:table.details-table
      [:tbody
       [:tr [:td "Order ID"] [:td {:style {:font-family "monospace"}} (:order-id order)]]
       [:tr [:td "Customer ID"] [:td (:customer-id order)]]
       [:tr [:td "Product ID"] [:td (:product-id order)]]
       [:tr [:td "Quantity"] [:td (:quantity order)]]
       [:tr [:td "Total"] [:td (str "$" (.toFixed (:total order) 2))]]
       [:tr [:td "Status"]
        [:td [:span.status-badge {:class (:status order)} (:status order)]]]

       (when (:registered-at order)
         [:tr [:td "Registered At"] [:td (format-timestamp (:registered-at order))]])

       (when (:updated-at order)
         [:tr [:td "Updated At"] [:td (format-timestamp (:updated-at order))]])

       (when (:version order)
         [:tr [:td "Version"] [:td (:version order)]])

       (when (contains? order :validation-passed)
         [:tr [:td "Validation Passed"]
          [:td (if (:validation-passed order) "✓ Yes" "✗ No")]])]]]))

(defn order-search []
  (let [order-id (r/atom "")
        result (r/atom nil)
        loading? (r/atom false)
        error (r/atom nil)]
    (fn []
      [:div.order-search-container
       [:h3 "Search Registered Order"]
       [:p.description "Search for orders in registered_orders table or recent timeline."]

       [:div.form-group
        [:label {:for "order-id"} "Order ID (UUID)"]
        [:input.form-control
         {:id "order-id"
          :type "text"
          :placeholder "e.g., a63c52a3-a507-4b70-9578-7dd49c3a841c"
          :value @order-id
          :on-change #(reset! order-id (-> % .-target .-value))
          :disabled @loading?}]]

       [:div {:style {:display "flex" :gap "1rem" :margin-bottom "2rem"}}
        [:button.btn.primary
         {:on-click #(when-not (empty? @order-id)
                       (search-order! @order-id result loading? error))
          :disabled (or @loading? (empty? @order-id))}
         (if @loading? "🔍 Searching..." "🔍 Search Order")]

        (when-not (empty? @order-id)
          [:button.btn.secondary
           {:on-click #(do
                         (reset! order-id "")
                         (reset! result nil)
                         (reset! error nil))}
           "✕ Clear"])]

       ;; Error message
       (when @error
         [:div.error-alert
          [:span "⚠️ " @error]])

       ;; Result
       (when @result
         [order-result-card @result])

       ;; Help section
       [:div.help-section
        [:h4 "💡 How to use"]
        [:ul
         [:li "Copy an order ID from the Recent Activity section on the Dashboard"]
         [:li "Paste it in the search field above"]
         [:li "Click 'Search Order' to view full order details"]
         [:li "Searches both registered_orders table and recent timeline"]
         [:li "Orders approved/denied manually are now properly registered"]]]])))

(defn orders-view []
  [:div.orders-view
   [:div.view-header
    [:h1 "Orders"]
    [:p.subtitle "Search and view registered orders"]]
   [:div.orders-content
    [order-search]]])