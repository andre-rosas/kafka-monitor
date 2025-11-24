(ns monitor.events-test
  (:require [cljs.test :refer-macros [deftest is testing]]
            [events :as events]))

(deftest initialize-event-creates-initial-state
  (testing "Event handler ::initialize creates correct db structure"
    (let [result (events/initialize nil nil)]
      (is (map? result))
      (is (false? (:loading? result)))
      (is (nil? (:error result)))
      (is (= {} (:stats result)))
      (is (= [] (:timeline result)))
      (is (= [] (:top-customers result)))
      (is (= [] (:top-products result)))
      (is (= {} (:processors-status result)))
      (is (= :dashboard (:selected-view result)))
      (is (false? (:auto-refresh? result))))))

(deftest navigate-event-changes-view
  (testing "Event handler ::navigate updates selected-view"
    (let [initial-db {:selected-view :dashboard}
          result (events/navigate initial-db [::events/navigate :customers])]
      (is (= :customers (:selected-view result)))
      (is (map? result)))))

(deftest fetch-stats-success-updates-db
  (testing "Event handler ::fetch-stats-success updates stats and loading"
    (let [initial-db {:loading? true :stats {} :error "old error"}
          response {:data {:query-processor {:customer-count 100}
                           :registry-processor {:accepted-count 50}}}
          result (events/fetch-stats-success initial-db [::events/fetch-stats-success response])]
      (is (false? (:loading? result)))
      (is (nil? (:error result)))
      (is (= 100 (get-in result [:stats :query-processor :customer-count])))
      (is (= 50 (get-in result [:stats :registry-processor :accepted-count]))))))

(deftest fetch-stats-failure-sets-error
  (testing "Event handler ::fetch-stats-failure sets error and stops loading"
    (let [initial-db {:loading? true :error nil}
          result (events/fetch-stats-failure initial-db [::events/fetch-stats-failure {}])]
      (is (false? (:loading? result)))
      (is (= "Failed to load stats" (:error result))))))

(deftest fetch-timeline-success-updates-timeline
  (testing "Event handler ::fetch-timeline-success updates timeline"
    (let [initial-db {:timeline []}
          response {:data [{:order-id "order-1" :status "pending"}
                           {:order-id "order-2" :status "accepted"}]}
          result (events/fetch-timeline-success initial-db [::events/fetch-timeline-success response])]
      (is (= 2 (count (:timeline result))))
      (is (= "order-1" (:order-id (first (:timeline result))))))))

(deftest fetch-top-customers-success-updates-customers
  (testing "Event handler ::fetch-top-customers-success updates top-customers"
    (let [initial-db {:top-customers []}
          response {:data [{:customer-id 1 :total-spent 100.0}
                           {:customer-id 2 :total-spent 200.0}]}
          result (events/fetch-top-customers-success initial-db [::events/fetch-top-customers-success response])]
      (is (= 2 (count (:top-customers result))))
      (is (= 1 (:customer-id (first (:top-customers result))))))))

(deftest fetch-top-products-success-updates-products
  (testing "Event handler ::fetch-top-products-success updates top-products"
    (let [initial-db {:top-products []}
          response {:data [{:product-id "PROD-001" :total-revenue 500.0}
                           {:product-id "PROD-002" :total-revenue 300.0}]}
          result (events/fetch-top-products-success initial-db [::events/fetch-top-products-success response])]
      (is (= 2 (count (:top-products result))))
      (is (= "PROD-001" (:product-id (first (:top-products result))))))))

(deftest update-timeline-status-modifies-order
  (testing "Event handler ::update-timeline-status changes order status in timeline"
    (let [initial-db {:timeline [{:order-id "order-1" :status "pending"}
                                 {:order-id "order-2" :status "pending"}]}
          result (events/update-timeline-status initial-db [::events/update-timeline-status "order-1" "accepted"])
          updated-order (first (filter #(= "order-1" (:order-id %)) (:timeline result)))]
      (is (= "accepted" (:status updated-order)))
      (is (= "pending" (:status (second (:timeline result))))))))

(deftest update-stats-increments-counts-correctly
  (testing "Event handler ::update-stats correctly updates approval counts"
    (let [initial-db {:stats {:registry-processor {:accepted-count 10
                                                   :denied-count 5
                                                   :pending-count 20}}}
          result (events/update-stats initial-db [::events/update-stats "order-1" "pending" "accepted"])
          rp (get-in result [:stats :registry-processor])]
      (is (= 11 (:accepted-count rp)))
      (is (= 19 (:pending-count rp)))
      (is (= 5 (:denied-count rp)))))

  (testing "Event handler ::update-stats handles pending to denied"
    (let [initial-db {:stats {:registry-processor {:accepted-count 10
                                                   :denied-count 5
                                                   :pending-count 20}}}
          result (events/update-stats initial-db [::events/update-stats "order-1" "pending" "denied"])
          rp (get-in result [:stats :registry-processor])]
      (is (= 10 (:accepted-count rp)))
      (is (= 6 (:denied-count rp)))
      (is (= 19 (:pending-count rp))))))

(deftest toggle-auto-refresh-toggles-boolean
  (testing "Event handler ::toggle-auto-refresh toggles auto-refresh state"
    (let [db-false {:auto-refresh? false}
          result-true (events/toggle-auto-refresh db-false [::events/toggle-auto-refresh])]
      (is (true? (:auto-refresh? result-true))))

    (let [db-true {:auto-refresh? true}
          result-false (events/toggle-auto-refresh db-true [::events/toggle-auto-refresh])]
      (is (false? (:auto-refresh? result-false))))))

(deftest clear-error-removes-error
  (testing "Event handler ::clear-error sets error to nil"
    (let [initial-db {:error "Some error"}
          result (events/clear-error initial-db [::events/clear-error])]
      (is (nil? (:error result))))))