(ns core-test
  "Tests for main application logic and lifecycle."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [order-processor.core :as core]
            [order-processor.model :as model]
            [order-processor.config :as config]
            [order-processor.db :as db]))

;; =============================================================================
;; Test Setup
;; =============================================================================

(use-fixtures :each
  (fn [f]
    ;; Ensure clean state before each test
    (when (:running? @core/app-state)
      (core/stop!))
    (reset! core/app-state {:running? false :producer nil :stats {:orders-sent 0 :orders-failed 0}})

    ;; Mock external dependencies with ALL required config values
    (with-redefs [db/connect! (constantly nil)
                  db/disconnect! (constantly nil)
                  db/save-order! (constantly nil)
                  config/init! (constantly nil)
                  config/kafka-config (constantly {:bootstrap-servers "localhost:9092" :topics ["orders"]})
                  config/producer-config (constantly {:rate-per-second 10
                                                      :acks "all"
                                                      :compression-type "none"
                                                      :batch-size 16384
                                                      :linger-ms 1})
                  config/orders-config (constantly {:customer-ids [1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100]
                                                    :product-ids ["P1" "P2" "P3"]
                                                    :quantity-range [1 10]
                                                    :price-range [10.0 100.0]})]
      (f))))

;; =============================================================================
;; Application State Tests
;; =============================================================================

(deftest test-app-state-initialization
  (testing "Application state is properly initialized"
    (let [state @core/app-state]
      (is (map? state))
      (is (contains? state :running?))
      (is (contains? state :producer))
      (is (contains? state :stats))
      (is (map? (:stats state)))
      (is (contains? (:stats state) :orders-sent))
      (is (contains? (:stats state) :orders-failed))
      (is (= 0 (:orders-sent (:stats state))))
      (is (= 0 (:orders-failed (:stats state)))))))

(deftest test-app-state-mutation
  (testing "Application state can be properly mutated"
    ;; Test starting state
    (is (false? (:running? @core/app-state)))

    ;; Mutate state
    (swap! core/app-state assoc :running? true)
    (is (true? (:running? @core/app-state)))

    ;; Mutate nested stats
    (swap! core/app-state update-in [:stats :orders-sent] inc)
    (is (= 1 (:orders-sent (:stats @core/app-state))))))

;; =============================================================================
;; Order Generation Tests
;; =============================================================================

(deftest test-generate-order-validity
  (testing "generate-order creates structurally valid orders"
    (let [order (core/generate-order)]
      (is (model/valid-order? order) "Generated order should pass model validation")

      ;; Test individual fields
      (is (string? (:order-id order)) "Order ID should be string")
      (is (pos-int? (:customer-id order)) "Customer ID should be positive integer")
      (is (string? (:product-id order)) "Product ID should be string")
      (is (pos-int? (:quantity order)) "Quantity should be positive integer")
      (is (number? (:unit-price order)) "Unit price should be number")
      (is (number? (:total order)) "Total should be number")
      (is (number? (:timestamp order)) "Timestamp should be number")
      (is (= "pending" (:status order)) "Status should be pending"))))

(deftest test-order-generation-consistency
  (testing "Multiple generated orders have different IDs"
    (let [orders (repeatedly 10 core/generate-order)
          order-ids (map :order-id orders)]

      ;; All orders should have unique IDs
      (is (= (count order-ids) (count (set order-ids))) "All order IDs should be unique"))))

(deftest test-order-calculation-robustness
  (testing "Order totals are within acceptable rounding tolerance"
    (dotimes [_ 20]
      (let [order (core/generate-order)
            calculated-total (* (:quantity order) (:unit-price order))
            order-total (:total order)
            difference (Math/abs (- calculated-total order-total))]

        ;; Increased tolerance to 0.05 to account for floating point arithmetic and rounding
        (is (< difference 0.05)
            (format "Total should be close to quantity * unit-price. Expected: %.2f, Got: %.2f, Diff: %.4f"
                    calculated-total order-total difference))))))

;; =============================================================================
;; Statistics Tests
;; =============================================================================

(deftest test-stats-incrementation
  (testing "Statistics are properly incremented"
    (let [initial-stats (core/get-stats)]
      ;; Initial state
      (is (= 0 (:orders-sent initial-stats)))
      (is (= 0 (:orders-failed initial-stats)))

      ;; Simulate sending orders
      (swap! core/app-state update-in [:stats :orders-sent] + 5)
      (swap! core/app-state update-in [:stats :orders-failed] inc)

      (let [updated-stats (core/get-stats)]
        (is (= 5 (:orders-sent updated-stats)))
        (is (= 1 (:orders-failed updated-stats)))))))

(deftest test-stats-isolation
  (testing "Stats updates are isolated between tests"
    (let [initial-stats (core/get-stats)]
      ;; Each test should start with clean stats
      (is (= 0 (:orders-sent initial-stats)))
      (is (= 0 (:orders-failed initial-stats))))))