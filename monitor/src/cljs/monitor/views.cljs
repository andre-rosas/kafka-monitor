(ns monitor.views
  (:require [re-frame.core :as rf]
            [monitor.events :as events]
            [monitor.subs :as subs]
            [views.dashboard :as dashboard]
            [views.orders :as orders]
            [views.customers :as customers]
            [views.products :as products]
            [views.processors :as processors]
            [views.about :as about]))

(defn navbar []
  (let [selected @(rf/subscribe [::subs/selected-view])
        auto-refresh? @(rf/subscribe [::subs/auto-refresh?])]
    [:nav.navbar
     [:div.navbar-brand
      [:h1 "🚀 Kafka Monitor"]]
     [:div.navbar-menu
      [:a.navbar-item
       {:class (when (= selected :dashboard) "active")
        :on-click #(rf/dispatch [::events/navigate :dashboard])}
       "Dashboard"]
      [:a.navbar-item
       {:class (when (= selected :customers) "active")
        :on-click #(rf/dispatch [::events/navigate :customers])}
       "Customers"]
      [:a.navbar-item
       {:class (when (= selected :products) "active")
        :on-click #(rf/dispatch [::events/navigate :products])}
       "Products"]
      [:a.navbar-item
       {:class (when (= selected :orders) "active")
        :on-click #(rf/dispatch [::events/navigate :orders])}
       "Orders"]
      [:a.navbar-item
       {:class (when (= selected :processors) "active")
        :on-click #(rf/dispatch [::events/navigate :processors])}
       "Processors"]
      [:a.navbar-item
       {:class (when (= selected :about) "active")
        :on-click #(rf/dispatch [::events/navigate :about])}
       "About"]]
     [:div.navbar-actions
      [:button.btn-refresh
       {:on-click #(rf/dispatch [::events/fetch-all])}
       "🔄 Refresh"]
      [:label.auto-refresh
       [:input {:type "checkbox"
                :checked auto-refresh?
                :on-change #(rf/dispatch [::events/toggle-auto-refresh])}]
       "Auto-refresh"]]]))

(defn loading-spinner []
  [:div.loading-spinner
   [:div.spinner]
   [:p "Loading..."]])

(defn error-alert [error]
  [:div.error-alert
   [:button.close-btn {:on-click #(rf/dispatch [::events/clear-error])} "×"]
   [:strong "Error: "]
   [:span error]])

(defn main-panel []
  (let [loading? @(rf/subscribe [::subs/loading?])
        error @(rf/subscribe [::subs/error])
        selected @(rf/subscribe [::subs/selected-view])]
    [:div.app
     [navbar]
     [:div.container
      (when error
        [error-alert error])
      (if loading?
        [loading-spinner]
        (case selected
          :dashboard [dashboard/dashboard-view]
          :customers [customers/customers-view]
          :products [products/products-view]
          :orders [orders/orders-view]
          :processors [processors/processors-view]
          :about [about/about-view]
          [dashboard/dashboard-view]))]]))