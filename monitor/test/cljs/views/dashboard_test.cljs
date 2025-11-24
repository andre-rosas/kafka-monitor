(ns dashboard-test
  (:require [cljs.test :refer-macros [deftest testing is]]
            [reagent.core :as r]
            [re-frame.core :as rf]
            [views.dashboard :as dashboard]
            [monitor.events :as events]))

(deftest stat-card-test
  (testing "Stat card renders correctly"
    (let [component (r/as-element
                     [dashboard/stat-card
                      {:title "Test"
                       :value "100"
                       :icon "🎯"
                       :color "blue"}])
          container (js/document.createElement "div")]
      (r/render component container)
      (is (.includes (.-innerHTML container) "Test"))
      (is (.includes (.-innerHTML container) "100"))
      (is (.includes (.-innerHTML container) "🎯")))))

(deftest dashboard-stats-test
  (testing "Dashboard stats renders with proper data structure"
    (rf/dispatch-sync [::events/initialize])
    (rf/dispatch-sync [::events/fetch-stats-success
                       {:data {:query-processor {:customer-count 50
                                                 :product-count 10
                                                 :total-revenue 1000.0}}}])
    (rf/dispatch-sync [::events/fetch-timeline-success
                       {:data [{:status "accepted"}
                               {:status "denied"}
                               {:status "pending"}]}])

    (let [component (r/as-element [dashboard/dashboard-stats])
          container (js/document.createElement "div")]
      (r/render component container)
      (is (.includes (.-innerHTML container) "50")) ;; customer-count
      (is (.includes (.-innerHTML container) "10")) ;; product-count
      )))
