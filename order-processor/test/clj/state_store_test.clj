(ns state-store-test
  "Tests for local state store operations."
  (:require [clojure.test :refer [deftest testing is]]
            [order-processor.state-store :as state-store]))

;; =============================================================================
;; Store Initialization Tests
;; =============================================================================

(deftest test-new-store
  (testing "new-store creates empty store with correct structure"
    (let [store (state-store/new-store)]
      (is (map? store))
      (is (contains? store :orders))
      (is (contains? store :stats))
      (is (map? (:orders store)))
      (is (map? (:stats store)))
      (is (= 0 (get-in store [:stats :total])))
      (is (= 0.0 (get-in store [:stats :total-value])))
      (is (nil? (get-in store [:stats :last-updated]))))))

;; =============================================================================
;; Order Management Tests
;; =============================================================================

(deftest test-add-order
  (testing "add-order stores order and updates statistics"
    (let [initial-store (state-store/new-store)
          test-order {:order-id "order-1"
                      :customer-id 42
                      :product-id "PROD-001"
                      :quantity 3
                      :total 29.97
                      :timestamp 1234567890}
          updated-store (state-store/add-order initial-store test-order)]
      (is (= test-order (get-in updated-store [:orders "order-1"])))
      (is (= 1 (get-in updated-store [:stats :total])))
      (is (= 1 (get-in updated-store [:stats :by-customer 42])))
      (is (= 1 (get-in updated-store [:stats :by-product "PROD-001"])))
      (is (= 29.97 (get-in updated-store [:stats :total-value])))
      (is (= 1234567890 (get-in updated-store [:stats :last-updated]))))))

(deftest test-add-multiple-orders
  (testing "add-order handles multiple orders correctly"
    (let [store (state-store/new-store)
          order1 {:order-id "order-1" :customer-id 1 :product-id "P1" :quantity 1 :total 10.0 :timestamp 1000}
          order2 {:order-id "order-2" :customer-id 1 :product-id "P2" :quantity 2 :total 20.0 :timestamp 2000}
          order3 {:order-id "order-3" :customer-id 2 :product-id "P1" :quantity 1 :total 10.0 :timestamp 3000}
          final-store (-> store
                          (state-store/add-order order1)
                          (state-store/add-order order2)
                          (state-store/add-order order3))]
      (is (= 3 (count (:orders final-store))))
      (is (= 2 (get-in final-store [:stats :by-customer 1])))
      (is (= 1 (get-in final-store [:stats :by-customer 2])))
      (is (= 2 (get-in final-store [:stats :by-product "P1"])))
      (is (= 1 (get-in final-store [:stats :by-product "P2"])))
      (is (= 40.0 (get-in final-store [:stats :total-value])))
      (is (= 3000 (get-in final-store [:stats :last-updated]))))))

;; =============================================================================
;; Order Retrieval Tests
;; =============================================================================

(deftest test-get-order
  (testing "get-order retrieves specific order by ID"
    (let [store (state-store/new-store)
          test-order {:order-id "test-order" :customer-id 1 :product-id "P1" :quantity 1 :total 10.0 :timestamp 1000}
          store-with-order (state-store/add-order store test-order)]
      (is (= test-order (state-store/get-order store-with-order "test-order")))
      (is (nil? (state-store/get-order store-with-order "non-existent-order"))))))

(deftest test-get-recent-orders
  (testing "get-recent-orders returns orders sorted by timestamp"
    (let [store (state-store/new-store)
          order1 {:order-id "order-1" :customer-id 1 :product-id "P1" :quantity 1 :total 10.0 :timestamp 1000}
          order2 {:order-id "order-2" :customer-id 1 :product-id "P2" :quantity 2 :total 20.0 :timestamp 2000}
          order3 {:order-id "order-3" :customer-id 2 :product-id "P1" :quantity 1 :total 10.0 :timestamp 3000}
          store-with-orders (-> store
                                (state-store/add-order order1)
                                (state-store/add-order order2)
                                (state-store/add-order order3))
          recent-orders (state-store/get-recent-orders store-with-orders 2)]
      (is (= 2 (count recent-orders)))
      (is (= "order-3" (:order-id (first recent-orders))))
      (is (= "order-2" (:order-id (second recent-orders)))))))

;; =============================================================================
;; Query Tests
;; =============================================================================

(deftest test-get-orders-by-customer
  (testing "get-orders-by-customer filters orders by customer ID"
    (let [store (state-store/new-store)
          order1 {:order-id "order-1" :customer-id 1 :product-id "P1" :quantity 1 :total 10.0 :timestamp 1000}
          order2 {:order-id "order-2" :customer-id 1 :product-id "P2" :quantity 2 :total 20.0 :timestamp 2000}
          order3 {:order-id "order-3" :customer-id 2 :product-id "P1" :quantity 1 :total 10.0 :timestamp 3000}
          store-with-orders (-> store
                                (state-store/add-order order1)
                                (state-store/add-order order2)
                                (state-store/add-order order3))
          customer-1-orders (state-store/get-orders-by-customer store-with-orders 1)
          customer-2-orders (state-store/get-orders-by-customer store-with-orders 2)]
      (is (= 2 (count customer-1-orders)))
      (is (= 1 (count customer-2-orders)))
      (is (every? #(= 1 (:customer-id %)) customer-1-orders))
      (is (every? #(= 2 (:customer-id %)) customer-2-orders)))))

(deftest test-get-orders-by-product
  (testing "get-orders-by-product filters orders by product ID"
    (let [store (state-store/new-store)
          order1 {:order-id "order-1" :customer-id 1 :product-id "P1" :quantity 1 :total 10.0 :timestamp 1000}
          order2 {:order-id "order-2" :customer-id 1 :product-id "P2" :quantity 2 :total 20.0 :timestamp 2000}
          order3 {:order-id "order-3" :customer-id 2 :product-id "P1" :quantity 1 :total 10.0 :timestamp 3000}
          store-with-orders (-> store
                                (state-store/add-order order1)
                                (state-store/add-order order2)
                                (state-store/add-order order3))
          product-p1-orders (state-store/get-orders-by-product store-with-orders "P1")
          product-p2-orders (state-store/get-orders-by-product store-with-orders "P2")]
      (is (= 2 (count product-p1-orders)))
      (is (= 1 (count product-p2-orders)))
      (is (every? #(= "P1" (:product-id %)) product-p1-orders))
      (is (every? #(= "P2" (:product-id %)) product-p2-orders)))))

;; =============================================================================
;; Analytics Tests
;; =============================================================================

(deftest test-get-statistics
  (testing "get-statistics returns aggregated statistics"
    (let [store (state-store/new-store)
          order1 {:order-id "order-1" :customer-id 1 :product-id "P1" :quantity 1 :total 10.0 :timestamp 1000}
          store-with-order (state-store/add-order store order1)
          stats (state-store/get-statistics store-with-order)]
      (is (map? stats))
      (is (contains? stats :total))
      (is (contains? stats :by-customer))
      (is (contains? stats :by-product))
      (is (contains? stats :total-value))
      (is (contains? stats :last-updated)))))

(deftest test-get-top-customers
  (testing "get-top-customers returns customers sorted by order count"
    (let [store (state-store/new-store)
          order1 {:order-id "order-1" :customer-id 1 :product-id "P1" :quantity 1 :total 10.0 :timestamp 1000}
          order2 {:order-id "order-2" :customer-id 1 :product-id "P2" :quantity 2 :total 20.0 :timestamp 2000}
          order3 {:order-id "order-3" :customer-id 2 :product-id "P1" :quantity 1 :total 10.0 :timestamp 3000}
          order4 {:order-id "order-4" :customer-id 3 :product-id "P1" :quantity 1 :total 10.0 :timestamp 4000}
          store-with-orders (-> store
                                (state-store/add-order order1)
                                (state-store/add-order order2)
                                (state-store/add-order order3)
                                (state-store/add-order order4))
          top-customers (state-store/get-top-customers store-with-orders 2)]
      (is (= 2 (count top-customers)))
      (is (= [1 2] (map first top-customers)))
      (is (= [2 1] (map second top-customers))))))

(deftest test-get-top-products
  (testing "get-top-products returns products sorted by order count"
    (let [store (state-store/new-store)
          order1 {:order-id "order-1" :customer-id 1 :product-id "P1" :quantity 1 :total 10.0 :timestamp 1000}
          order2 {:order-id "order-2" :customer-id 1 :product-id "P2" :quantity 2 :total 20.0 :timestamp 2000}
          order3 {:order-id "order-3" :customer-id 2 :product-id "P1" :quantity 1 :total 10.0 :timestamp 3000}
          store-with-orders (-> store
                                (state-store/add-order order1)
                                (state-store/add-order order2)
                                (state-store/add-order order3))
          top-products (state-store/get-top-products store-with-orders 2)]
      (is (= 2 (count top-products)))
      (is (= ["P1" "P2"] (map first top-products)))
      (is (= [2 1] (map second top-products))))))

(deftest test-get-average-order-value
  (testing "get-average-order-value calculates correct average"
    (let [store (state-store/new-store)
          order1 {:order-id "order-1" :customer-id 1 :product-id "P1" :quantity 1 :total 10.0 :timestamp 1000}
          order2 {:order-id "order-2" :customer-id 1 :product-id "P2" :quantity 2 :total 30.0 :timestamp 2000}
          store-with-orders (-> store
                                (state-store/add-order order1)
                                (state-store/add-order order2))]
      (is (= 20.0 (state-store/get-average-order-value store-with-orders)))))
  (testing "get-average-order-value returns 0 for empty store"
    (let [store (state-store/new-store)]
      (is (= 0.0 (state-store/get-average-order-value store))))))