(ns views.products
  (:require [re-frame.core :as rf]
            [reagent.core :as r]
            [monitor.subs :as subs]
            [monitor.events :as events]))

(defn sortable-table []
  (let [sort-key (r/atom :total-revenue)
        sort-dir (r/atom :desc)
        search-filter (r/atom "")]
    (fn []
      (let [all-products @(rf/subscribe [::subs/top-products])

            filtered-products (if (empty? @search-filter)
                                all-products
                                (let [search-upper (clojure.string/upper-case @search-filter)]
                                  (filter #(clojure.string/includes? (:product-id %) search-upper)
                                          all-products)))

            toggle-sort (fn [key]
                          (if (= @sort-key key)
                            (swap! sort-dir #(if (= % :asc) :desc :asc))
                            (do
                              (reset! sort-key key)
                              (reset! sort-dir :desc))))

            comparator (if (= @sort-dir :asc)
                         (fn [a b] (compare (get a @sort-key 0) (get b @sort-key 0)))
                         (fn [a b] (compare (get b @sort-key 0) (get a @sort-key 0))))

            sorted-products (sort comparator filtered-products)

            sort-indicator (fn [key]
                             (when (= @sort-key key)
                               (if (= @sort-dir :asc) " ↑" " ↓")))]

        [:div.top-products
         [:div.table-header
          [:h3 "Top Products by Revenue"]
          [:div.table-search
           [:input.search-input
            {:type "text"
             :placeholder "Filter by ID (e.g., PROD-004)..."
             :value @search-filter
             :on-change #(reset! search-filter (-> % .-target .-value))}]
           (when-not (empty? @search-filter)
             [:button.btn-clear
              {:on-click #(reset! search-filter "")}
              "✕ Clear"])]]

         [:p.description
          (if (empty? @search-filter)
            (str "Showing " (count all-products) " products. Sort: " (name @sort-key) (if (= @sort-dir :asc) " ↑" " ↓"))
            (str "Filtered: " (count filtered-products) " of " (count all-products) " products"))]

         [:table.sortable-table
          [:thead
           [:tr
            [:th {:on-click #(toggle-sort :product-id)
                  :class (when (= @sort-key :product-id) "sorted")
                  :style {:cursor "pointer"}}
             "Product ID" (sort-indicator :product-id)]
            [:th {:on-click #(toggle-sort :total-quantity)
                  :class (when (= @sort-key :total-quantity) "sorted")
                  :style {:cursor "pointer"}}
             "Quantity Sold" (sort-indicator :total-quantity)]
            [:th {:on-click #(toggle-sort :total-revenue)
                  :class (when (= @sort-key :total-revenue) "sorted")
                  :style {:cursor "pointer"}}
             "Revenue" (sort-indicator :total-revenue)]
            [:th {:on-click #(toggle-sort :order-count)
                  :class (when (= @sort-key :order-count) "sorted")
                  :style {:cursor "pointer"}}
             "Orders" (sort-indicator :order-count)]
            [:th {:on-click #(toggle-sort :avg-quantity)
                  :class (when (= @sort-key :avg-quantity) "sorted")
                  :style {:cursor "pointer"}}
             "Avg Qty" (sort-indicator :avg-quantity)]]]
          [:tbody
           (if (empty? sorted-products)
             [:tr [:td {:col-span 5 :style {:text-align "center"}}
                   (if (empty? @search-filter)
                     "No products yet"
                     (str "No product matching '" @search-filter "'"))]]
             (for [product sorted-products]
               ^{:key (:product-id product)}
               [:tr
                [:td (:product-id product)]
                [:td (:total-quantity product)]
                [:td (str "$" (.toFixed (or (:total-revenue product) 0) 2))]
                [:td (:order-count product)]
                [:td (.toFixed (or (:avg-quantity product) 0) 1)]]))]]]))))

(defn products-info []
  [:div.products-info
   [:h3 "About Product Analytics"]
   [:div.info-box
    [:p "Real-time product data from Query Processor."]
    [:h4 "📊 Features:"]
    [:ul
     [:li [:strong "Filter:"] " Type product ID to filter"]
     [:li [:strong "Sort:"] " Click headers (↑↓)"]
     [:li [:strong "Client-side:"] " Filters loaded data"]]]])

(defn products-view []
  (let [mounted? (r/atom false)]
    (fn []
      (when-not @mounted?
        (reset! mounted? true)
        (rf/dispatch [::events/fetch-top-products]))

      [:div.products-view
       [:div.view-header
        [:h1 "Products"]
        [:p.subtitle "Filter and analyze products"]]
       [:div.products-content
        [:div.products-main
         [sortable-table]]
        [:div.products-sidebar
         [products-info]]]])))