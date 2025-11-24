(ns monitor.subs-test
  (:require [cljs.test :refer-macros [deftest is testing]]
            [monitor.subs :as subs]
            [re-frame.core :as rf]))

(deftest selected-view-subscription-test
  (testing "::selected-view returns selected view from db"
    (let [test-db {:selected-view :dashboard}
          result (rf/subscribe [::subs/selected-view])]
      (with-redefs [rf/subscribe (fn [[sub-id]]
                                   (atom (get test-db (keyword (name sub-id)))))]
        (is (= :dashboard @(rf/subscribe [::subs/selected-view])))))))

(deftest loading-subscription-test
  (testing "::loading? returns loading state from db"
    (let [test-db {:loading? true}]
      (with-redefs [rf/subscribe (fn [[sub-id]]
                                   (atom (get test-db (keyword (name sub-id)))))]
        (is (true? @(rf/subscribe [::subs/loading?])))))))

(deftest error-subscription-test
  (testing "::error returns error message from db"
    (let [test-db {:error "Test error"}]
      (with-redefs [rf/subscribe (fn [[sub-id]]
                                   (atom (get test-db (keyword (name sub-id)))))]
        (is (= "Test error" @(rf/subscribe [::subs/error])))))))

(deftest auto-refresh-subscription-test
  (testing "::auto-refresh? returns auto-refresh state from db"
    (let [test-db {:auto-refresh? true}]
      (with-redefs [rf/subscribe (fn [[sub-id]]
                                   (atom (get test-db (keyword (name sub-id)))))]
        (is (true? @(rf/subscribe [::subs/auto-refresh?])))))))

(deftest stats-subscription-test
  (testing "::stats returns stats object from db"
    (let [test-db {:stats {:query-processor {:customer-count 10}
                           :registry-processor {:accepted-count 5}}}]
      (with-redefs [rf/subscribe (fn [[sub-id]]
                                   (atom (get test-db (keyword (name sub-id)))))]
        (let [result @(rf/subscribe [::subs/stats])]
          (is (= 10 (get-in result [:query-processor :customer-count])))
          (is (= 5 (get-in result [:registry-processor :accepted-count]))))))))

(deftest timeline-subscription-test
  (testing "::timeline returns timeline array from db"
    (let [test-db {:timeline [{:order-id "order-1" :status "pending"}
                              {:order-id "order-2" :status "accepted"}]}]
      (with-redefs [rf/subscribe (fn [[sub-id]]
                                   (atom (get test-db (keyword (name sub-id)))))]
        (let [result @(rf/subscribe [::subs/timeline])]
          (is (= 2 (count result)))
          (is (= "order-1" (:order-id (first result))))
          (is (= "pending" (:status (first result)))))))))

(deftest top-customers-subscription-test
  (testing "::top-customers returns customers array from db"
    (let [test-db {:top-customers [{:customer-id 1 :total-spent 100.0}
                                   {:customer-id 2 :total-spent 200.0}]}]
      (with-redefs [rf/subscribe (fn [[sub-id]]
                                   (atom (get test-db (keyword (name sub-id)))))]
        (let [result @(rf/subscribe [::subs/top-customers])]
          (is (= 2 (count result)))
          (is (= 1 (:customer-id (first result))))
          (is (= 100.0 (:total-spent (first result)))))))))

(deftest top-products-subscription-test
  (testing "::top-products returns products array from db"
    (let [test-db {:top-products [{:product-id "PROD-001" :total-revenue 500.0}
                                  {:product-id "PROD-002" :total-revenue 300.0}]}]
      (with-redefs [rf/subscribe (fn [[sub-id]]
                                   (atom (get test-db (keyword (name sub-id)))))]
        (let [result @(rf/subscribe [::subs/top-products])]
          (is (= 2 (count result)))
          (is (= "PROD-001" (:product-id (first result))))
          (is (= 500.0 (:total-revenue (first result)))))))))

(deftest processors-status-subscription-test
  (testing "::processors-status returns processors status from db"
    (let [test-db {:processors-status {:order-processor {:status "healthy"}
                                       :query-processor {:status "healthy"}}}]
      (with-redefs [rf/subscribe (fn [[sub-id]]
                                   (atom (get test-db (keyword (name sub-id)))))]
        (let [result @(rf/subscribe [::subs/processors-status])]
          (is (map? result))
          (is (contains? result :order-processor))
          (is (= "healthy" (get-in result [:order-processor :status]))))))))