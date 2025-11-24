(ns views.customers
  (:require [re-frame.core :as rf]
            [reagent.core :as r]
            [monitor.subs :as subs]
            [monitor.events :as events]))

(defn sortable-table []
  (let [sort-key (r/atom :total-spent)
        sort-dir (r/atom :desc)
        search-filter (r/atom "")]
    (fn []
      (let [all-customers @(rf/subscribe [::subs/top-customers])

            filtered-customers (if (empty? @search-filter)
                                 all-customers
                                 (let [search-id (js/parseInt @search-filter)]
                                   (if (js/isNaN search-id)
                                     all-customers
                                     (filter #(= (:customer-id %) search-id) all-customers))))

            toggle-sort (fn [key]
                          (if (= @sort-key key)
                            (swap! sort-dir #(if (= % :asc) :desc :asc))
                            (do
                              (reset! sort-key key)
                              (reset! sort-dir :desc))))

            comparator (if (= @sort-dir :asc)
                         (fn [a b] (compare (get a @sort-key 0) (get b @sort-key 0)))
                         (fn [a b] (compare (get b @sort-key 0) (get a @sort-key 0))))

            sorted-customers (sort comparator filtered-customers)

            sort-indicator (fn [key]
                             (when (= @sort-key key)
                               (if (= @sort-dir :asc) " ↑" " ↓")))]

        [:div.top-customers
         [:div.table-header
          [:h3 "Top Customers by Spending"]
          [:div.table-search
           [:input.search-input
            {:type "number"
             :placeholder "Filter by ID (e.g., 68)..."
             :value @search-filter
             :on-change #(reset! search-filter (-> % .-target .-value))}]
           (when-not (empty? @search-filter)
             [:button.btn-clear
              {:on-click #(reset! search-filter "")}
              "✕ Clear"])]]

         [:p.description
          (if (empty? @search-filter)
            (str "Showing " (count all-customers) " customers. Sort: " (name @sort-key) (if (= @sort-dir :asc) " ↑" " ↓"))
            (str "Filtered: " (count filtered-customers) " of " (count all-customers) " customers"))]

         [:table.sortable-table
          [:thead
           [:tr
            [:th {:on-click #(toggle-sort :customer-id)
                  :class (when (= @sort-key :customer-id) "sorted")
                  :style {:cursor "pointer"}}
             "Customer ID" (sort-indicator :customer-id)]
            [:th {:on-click #(toggle-sort :total-orders)
                  :class (when (= @sort-key :total-orders) "sorted")
                  :style {:cursor "pointer"}}
             "Orders" (sort-indicator :total-orders)]
            [:th {:on-click #(toggle-sort :total-spent)
                  :class (when (= @sort-key :total-spent) "sorted")
                  :style {:cursor "pointer"}}
             "Total Spent" (sort-indicator :total-spent)]
            [:th {:on-click #(toggle-sort :last-order-timestamp)
                  :class (when (= @sort-key :last-order-timestamp) "sorted")
                  :style {:cursor "pointer"}}
             "Last Order" (sort-indicator :last-order-timestamp)]]]
          [:tbody
           (if (empty? sorted-customers)
             [:tr [:td {:col-span 4 :style {:text-align "center"}}
                   (if (empty? @search-filter)
                     "No customers yet"
                     (str "No customer with ID " @search-filter))]]
             (for [customer sorted-customers]
               ^{:key (:customer-id customer)}
               [:tr
                [:td (:customer-id customer)]
                [:td (:total-orders customer)]
                [:td (str "$" (.toFixed (or (:total-spent customer) 0) 2))]
                [:td (when-let [ts (:last-order-timestamp customer)]
                       (.toLocaleString (js/Date. ts)))]]))]]]))))

(defn customers-info []
  [:div.customers-info
   [:h3 "About Customer Analytics"]
   [:div.info-box
    [:p "Real-time customer data from Query Processor."]
    [:h4 "📊 Features:"]
    [:ul
     [:li [:strong "Filter:"] " Type customer ID to filter the table"]
     [:li [:strong "Sort:"] " Click column headers (↑↓)"]
     [:li [:strong "Client-side:"] " Filters data already loaded in the table"]]]])

(defn customers-view []
  (let [mounted? (r/atom false)]
    (fn []
      (when-not @mounted?
        (reset! mounted? true)
        (rf/dispatch [::events/fetch-top-customers]))

      [:div.customers-view
       [:div.view-header
        [:h1 "Customers"]
        [:p.subtitle "Filter and analyze customers"]]
       [:div.customers-content
        [:div.customers-main
         [sortable-table]]
        [:div.customers-sidebar
         [customers-info]]]])))