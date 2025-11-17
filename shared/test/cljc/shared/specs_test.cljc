(ns shared.specs-test
  "Comprehensive tests for shared specifications across Clojure and ClojureScript."
  #?(:clj (:require [clojure.test :refer [deftest testing is]]
                    [clojure.spec.alpha :as s]
                    [shared.specs :as specs])
     :cljs (:require [cljs.test :refer-macros [deftest testing is]]
                     [cljs.spec.alpha :as s]
                     [shared.specs :as specs])))

;; =============================================================================
;; Metadata for test selectors
;; =============================================================================

(def ^:dynamic *verbose-testing* false)

;; =============================================================================
;; Test Data Fixtures
;; =============================================================================

(def valid-order-base
  "Base valid order data for testing"
  {:order-id "order-123"
   :customer-id 1
   :product-id "product-456"
   :quantity 2
   :unit-price 10.0
   :total 20.0
   :timestamp 1234567890
   :status "pending"})

(def valid-customer-stats-base
  "Base valid customer stats for testing"
  {:customer-id 1
   :total-orders 5
   :total-spent 500.0
   :last-order-id "order-123"
   :last-order-timestamp 1234567890
   :first-order-timestamp 1234567890})

(def valid-product-stats-base
  "Base valid product stats for testing"
  {:product-id "product-456"
   :total-quantity 100
   :total-revenue 1000.0
   :order-count 10
   :avg-quantity 10.0
   :last-order-timestamp 1234567890})

(def valid-registered-order-base
  "Base valid registered order for testing"
  {:order-id "order-123"
   :customer-id 1
   :product-id "product-456"
   :quantity 2
   :total 20.0
   :status "pending"
   :registered-at 1234567890
   :version 1
   :validation-passed true})

;; =============================================================================
;; Helper Functions for Verbose Testing
;; =============================================================================

(defn- log-verbose [message]
  (when *verbose-testing*
    (println message)))

;; =============================================================================
;; Primitive Spec Tests
;; =============================================================================

(deftest ^:verbose order-id-spec-test
  (testing "Valid order IDs"
    (log-verbose "Testing valid order IDs")
    (is (s/valid? ::specs/order-id "order-123"))
    (is (s/valid? ::specs/order-id "123"))
    (is (s/valid? ::specs/order-id "a")))

  (testing "Invalid order IDs"
    (log-verbose "Testing invalid order IDs")
    (is (not (s/valid? ::specs/order-id "")))
    (is (not (s/valid? ::specs/order-id nil)))
    (is (not (s/valid? ::specs/order-id 123)))))

(deftest ^:verbose customer-id-spec-test
  (testing "Valid customer IDs"
    (log-verbose "Testing valid customer IDs")
    (is (s/valid? ::specs/customer-id 1))
    (is (s/valid? ::specs/customer-id 100))
    (is (s/valid? ::specs/customer-id 999999)))

  (testing "Invalid customer IDs"
    (log-verbose "Testing invalid customer IDs")
    (is (not (s/valid? ::specs/customer-id 0)))
    (is (not (s/valid? ::specs/customer-id -1)))
    (is (not (s/valid? ::specs/customer-id 1.5)))
    (is (not (s/valid? ::specs/customer-id "1")))))

(deftest ^:verbose product-id-spec-test
  (testing "Valid product IDs"
    (log-verbose "Testing valid product IDs")
    (is (s/valid? ::specs/product-id "product-456"))
    (is (s/valid? ::specs/product-id "123"))
    (is (s/valid? ::specs/product-id "a")))

  (testing "Invalid product IDs"
    (log-verbose "Testing invalid product IDs")
    (is (not (s/valid? ::specs/product-id "")))
    (is (not (s/valid? ::specs/product-id nil)))
    (is (not (s/valid? ::specs/product-id 456)))))

(deftest ^:verbose quantity-spec-test
  (testing "Valid quantities"
    (log-verbose "Testing valid quantities")
    (is (s/valid? ::specs/quantity 1))
    (is (s/valid? ::specs/quantity 100))
    (is (s/valid? ::specs/quantity 999)))

  (testing "Invalid quantities"
    (log-verbose "Testing invalid quantities")
    (is (not (s/valid? ::specs/quantity 0)))
    (is (not (s/valid? ::specs/quantity -1)))
    (is (not (s/valid? ::specs/quantity 1.5)))
    (is (not (s/valid? ::specs/quantity "1")))))

(deftest ^:verbose unit-price-spec-test
  (testing "Valid unit prices"
    (log-verbose "Testing valid unit prices")
    (is (s/valid? ::specs/unit-price 1.0))
    (is (s/valid? ::specs/unit-price 1))
    (is (s/valid? ::specs/unit-price 0.01))
    (is (s/valid? ::specs/unit-price 99999.99)))

  (testing "Invalid unit prices"
    (log-verbose "Testing invalid unit prices")
    (is (not (s/valid? ::specs/unit-price 0)))
    (is (not (s/valid? ::specs/unit-price -1.0)))
    (is (not (s/valid? ::specs/unit-price "10.0")))))

(deftest ^:verbose total-spec-test
  (testing "Valid totals"
    (log-verbose "Testing valid totals")
    (is (s/valid? ::specs/total 1.0))
    (is (s/valid? ::specs/total 1))
    (is (s/valid? ::specs/total 0.01))
    (is (s/valid? ::specs/total 99999.99)))

  (testing "Invalid totals"
    (log-verbose "Testing invalid totals")
    (is (not (s/valid? ::specs/total 0)))
    (is (not (s/valid? ::specs/total -1.0)))
    (is (not (s/valid? ::specs/total "10.0")))))

(deftest ^:verbose timestamp-spec-test
  (testing "Valid timestamps"
    (log-verbose "Testing valid timestamps")
    (is (s/valid? ::specs/timestamp 1))
    (is (s/valid? ::specs/timestamp 100))
    (is (s/valid? ::specs/timestamp 1234567890)))

  (testing "Invalid timestamps"
    (log-verbose "Testing invalid timestamps")
    (is (not (s/valid? ::specs/timestamp 0)))
    (is (not (s/valid? ::specs/timestamp -1)))
    (is (not (s/valid? ::specs/timestamp "1234567890")))))

(deftest ^:verbose status-spec-test
  (testing "Valid statuses"
    (log-verbose "Testing valid statuses")
    (is (s/valid? ::specs/status "pending"))
    (is (s/valid? ::specs/status "approved"))
    (is (s/valid? ::specs/status "denied")))

  (testing "Invalid statuses"
    (log-verbose "Testing invalid statuses")
    (is (not (s/valid? ::specs/status "confirmed")))
    (is (not (s/valid? ::specs/status "shipped")))
    (is (not (s/valid? ::specs/status "delivered")))
    (is (not (s/valid? ::specs/status "cancelled")))
    (is (not (s/valid? ::specs/status "accepted")))
    (is (not (s/valid? ::specs/status "")))
    (is (not (s/valid? ::specs/status nil)))
    (is (not (s/valid? ::specs/status "invalid")))))

;; =============================================================================
;; Composite Spec Tests
;; =============================================================================

(deftest ^:verbose order-spec-test
  (testing "Valid complete order"
    (log-verbose "Testing valid complete order")
    (is (s/valid? ::specs/order valid-order-base)))

  (testing "Valid orders with different statuses"
    (log-verbose "Testing orders with different statuses")
    (doseq [status ["pending" "approved" "denied"]]
      (is (s/valid? ::specs/order (assoc valid-order-base :status status))
          (str "Order with status: " status))))

  (testing "Invalid orders - missing required fields"
    (log-verbose "Testing invalid orders - missing required fields")
    (doseq [field [:order-id :customer-id :product-id :quantity :unit-price :total :timestamp :status]]
      (let [invalid-order (dissoc valid-order-base field)]
        (is (not (s/valid? ::specs/order invalid-order))
            (str "Order missing field: " field)))))

  (testing "Invalid orders - wrong field types"
    (log-verbose "Testing invalid orders - wrong field types")
    (let [invalid-cases [{:field :order-id :value 123}
                         {:field :customer-id :value "1"}
                         {:field :product-id :value 456}
                         {:field :quantity :value "2"}
                         {:field :unit-price :value "10.0"}
                         {:field :total :value "20.0"}
                         {:field :timestamp :value "1234567890"}
                         {:field :status :value "invalid-status"}]]
      (doseq [{:keys [field value]} invalid-cases]
        (let [invalid-order (assoc valid-order-base field value)]
          (is (not (s/valid? ::specs/order invalid-order))
              (str "Order with invalid " field ": " value)))))))

(deftest ^:verbose customer-stats-spec-test
  (testing "Valid customer stats"
    (log-verbose "Testing valid customer stats")
    (is (s/valid? ::specs/customer-stats valid-customer-stats-base)))

  (testing "Customer stats with nil last order (new customers)"
    (log-verbose "Testing customer stats with nil last order")
    (let [new-customer-stats (assoc valid-customer-stats-base
                                    :last-order-id nil
                                    :last-order-timestamp nil)]
      (is (s/valid? ::specs/customer-stats new-customer-stats))))

  (testing "Invalid customer stats"
    (log-verbose "Testing invalid customer stats")
    (let [invalid-cases [{:desc "negative total orders" :field :total-orders :value -1}
                         {:desc "negative total spent" :field :total-spent :value -100.0}
                         {:desc "zero first timestamp" :field :first-order-timestamp :value 0}]]
      (doseq [{:keys [desc field value]} invalid-cases]
        (let [invalid-stats (assoc valid-customer-stats-base field value)]
          (is (not (s/valid? ::specs/customer-stats invalid-stats))
              (str "Customer stats with " desc)))))))

(deftest ^:verbose product-stats-spec-test
  (testing "Valid product stats"
    (log-verbose "Testing valid product stats")
    (is (s/valid? ::specs/product-stats valid-product-stats-base)))

  (testing "Product stats with zero values"
    (log-verbose "Testing product stats with zero values")
    (let [zero-stats (assoc valid-product-stats-base
                            :total-quantity 0
                            :total-revenue 0.0
                            :order-count 0
                            :avg-quantity 0.0)]
      (is (s/valid? ::specs/product-stats zero-stats))))

  (testing "Invalid product stats"
    (log-verbose "Testing invalid product stats")
    (let [invalid-cases [{:desc "negative total quantity" :field :total-quantity :value -1}
                         {:desc "negative revenue" :field :total-revenue :value -100.0}
                         {:desc "negative order count" :field :order-count :value -1}
                         {:desc "negative avg quantity" :field :avg-quantity :value -1.0}]]
      (doseq [{:keys [desc field value]} invalid-cases]
        (let [invalid-stats (assoc valid-product-stats-base field value)]
          (is (not (s/valid? ::specs/product-stats invalid-stats))
              (str "Product stats with " desc)))))))

(deftest ^:verbose registered-order-spec-test
  (testing "Valid registered order"
    (log-verbose "Testing valid registered order")
    (is (s/valid? ::specs/registered-order valid-registered-order-base)))

  (testing "Registered order validation states"
    (log-verbose "Testing registered order validation states")
    (doseq [validation-state [true false]]
      (let [order (assoc valid-registered-order-base :validation-passed validation-state)]
        (is (s/valid? ::specs/registered-order order)
            (str "Registered order with validation-passed: " validation-state)))))

  (testing "Invalid registered orders"
    (log-verbose "Testing invalid registered orders")
    (let [invalid-cases [{:desc "empty order id" :field :order-id :value ""}
                         {:desc "zero customer id" :field :customer-id :value 0}
                         {:desc "empty product id" :field :product-id :value ""}
                         {:desc "zero quantity" :field :quantity :value 0}
                         {:desc "negative total" :field :total :value -1.0}
                         {:desc "invalid status" :field :status :value "invalid"}
                         {:desc "zero registered at" :field :registered-at :value 0}
                         {:desc "zero version" :field :version :value 0}
                         {:desc "non-boolean validation" :field :validation-passed :value "true"}]]
      (doseq [{:keys [desc field value]} invalid-cases]
        (let [invalid-order (assoc valid-registered-order-base field value)]
          (is (not (s/valid? ::specs/registered-order invalid-order))
              (str "Registered order with " desc)))))))

;; =============================================================================
;; Validation Function Tests
;; =============================================================================

(deftest ^:verbose validation-functions-test
  (testing "valid-order? function"
    (log-verbose "Testing valid-order? function")
    (is (true? (specs/valid-order? valid-order-base)))
    (is (false? (specs/valid-order? (dissoc valid-order-base :order-id)))))

  (testing "valid-customer-stats? function"
    (log-verbose "Testing valid-customer-stats? function")
    (is (true? (specs/valid-customer-stats? valid-customer-stats-base)))
    (is (false? (specs/valid-customer-stats? (dissoc valid-customer-stats-base :customer-id)))))

  (testing "valid-product-stats? function"
    (log-verbose "Testing valid-product-stats? function")
    (is (true? (specs/valid-product-stats? valid-product-stats-base)))
    (is (false? (specs/valid-product-stats? (dissoc valid-product-stats-base :product-id)))))

  (testing "valid-registered-order? function"
    (log-verbose "Testing valid-registered-order? function")
    (is (true? (specs/valid-registered-order? valid-registered-order-base)))
    (is (false? (specs/valid-registered-order? (dissoc valid-registered-order-base :order-id))))))

(deftest ^:verbose explain-error-test
  (testing "explain-error returns string for invalid data"
    (log-verbose "Testing explain-error function")
    (let [invalid-order (dissoc valid-order-base :order-id)
          explanation (specs/explain-error ::specs/order invalid-order)]
      (is (string? explanation))
      (is (not (empty? explanation)))
      (is (re-find #"order-id" explanation)))))

;; =============================================================================
;; Business Rules Tests
;; =============================================================================

(deftest ^:verbose business-rules-test
  (testing "Business rules structure"
    (log-verbose "Testing business rules structure")
    (is (map? specs/business-rules))
    (is (contains? specs/business-rules :min-quantity))
    (is (contains? specs/business-rules :max-quantity))
    (is (contains? specs/business-rules :min-total))
    (is (contains? specs/business-rules :max-total))
    (is (contains? specs/business-rules :price-tolerance))
    (is (contains? specs/business-rules :valid-statuses)))

  (testing "Business rules values"
    (log-verbose "Testing business rules values")
    (is (= 1 (:min-quantity specs/business-rules)))
    (is (= 1000 (:max-quantity specs/business-rules)))
    (is (= 1.0 (:min-total specs/business-rules)))
    (is (= 100000.0 (:max-total specs/business-rules)))
    (is (= 0.01 (:price-tolerance specs/business-rules)))
    (is (= #{"pending" "approved" "denied"} (:valid-statuses specs/business-rules))))

  (testing "Business rules align with specs"
    (log-verbose "Testing business rules alignment with specs")
    ;; Test that all statuses in business rules are valid according to the spec
    (doseq [status (:valid-statuses specs/business-rules)]
      (is (s/valid? ::specs/status status)
          (str "Status from business rules should be valid: " status)))

    ;; Test that the spec validates the same set of statuses
    (is (s/valid? ::specs/status "pending"))
    (is (s/valid? ::specs/status "approved"))
    (is (s/valid? ::specs/status "denied"))
    (is (not (s/valid? ::specs/status "invalid-status")))))

;; =============================================================================
;; Integration Tests
;; =============================================================================

(deftest ^:verbose spec-coherence-test
  (testing "All required specs are defined"
    (log-verbose "Testing spec coherence - all required specs defined")
    (let [required-specs [::specs/order-id ::specs/customer-id ::specs/product-id
                          ::specs/quantity ::specs/unit-price ::specs/total
                          ::specs/timestamp ::specs/status ::specs/order
                          ::specs/total-orders ::specs/total-spent ::specs/last-order-id
                          ::specs/last-order-timestamp ::specs/first-order-timestamp
                          ::specs/customer-stats ::specs/total-quantity
                          ::specs/total-revenue ::specs/order-count ::specs/avg-quantity
                          ::specs/product-stats ::specs/registered-at ::specs/updated-at
                          ::specs/version ::specs/validation-passed ::specs/registered-order]]
      (doseq [spec required-specs]
        (is (s/spec? (s/get-spec spec)) (str "Spec should be defined: " spec))))))

(deftest ^:verbose cross-platform-test
  (testing "Specs work in both Clojure and ClojureScript"
    (log-verbose "Testing cross-platform compatibility")
    (is (specs/valid-order? valid-order-base))
    (is (specs/valid-customer-stats? valid-customer-stats-base))
    (is (specs/valid-product-stats? valid-product-stats-base))
    (is (specs/valid-registered-order? valid-registered-order-base))))

;; =============================================================================
;; Test Runner Configuration
;; =============================================================================

#?(:clj
   (defn test-verbose
     "Run tests with verbose output"
     []
     (binding [*verbose-testing* true]
       (clojure.test/run-tests 'shared.specs-test))))