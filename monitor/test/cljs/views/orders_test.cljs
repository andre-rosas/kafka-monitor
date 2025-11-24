(ns orders-test
  (:require [cljs.test :refer-macros [deftest testing is async]]
            [views.orders :as orders]))

(deftest format-timestamp-test
  (testing "Format timestamp converts epoch to readable date"
    (let [ts 1700000000000
          formatted (orders/format-timestamp ts)]
      (is (string? formatted))
      (is (not (boolean (empty? formatted)))))))

(deftest search-order-test
  (testing "Search order updates atoms correctly"
    (async done
           (let [result (atom nil)
                 loading (atom false)
                 error (atom nil)]

             ;; Mock fetch
             (set! js/fetch
                   (fn [url]
                     (js/Promise.resolve
                      #js {:ok true
                           :json (fn [] (js/Promise.resolve
                                         #js {:success true
                                              :data #js {:order-id "test-123"
                                                         :status "accepted"}}))})))

             (orders/search-order! "test-123" result loading error)

             ;; Wait for async operation
             (js/setTimeout
              (fn []
                (is (not @loading))
                (is (nil? @error))
                (is (= "test-123" (get-in @result [:data :order-id])))
                (done))
              100)))))