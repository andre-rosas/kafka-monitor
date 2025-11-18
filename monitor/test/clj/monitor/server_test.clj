(ns monitor.server-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [monitor.server :as server]
            [ring.mock.request :as mock]
            [cheshire.core :as json]
            [monitor.cassandra :as cass]
            [clojure.string :as str]))

;; --- Mocks for Testing Handlers ---

(defn parse-body [response]
  (json/parse-string (:body response) true))

(def mock-customer-stats {:customer-id 1 :total-spent 150.0})
(def mock-product-stats {:product-id "P1" :total-revenue 500.0})
(def mock-registered-order {:order-id (str (java.util.UUID/randomUUID)) :status "approved"})
(def mock-timeline [{:order-id "TL-1"}])
(def mock-order-history [{:version 1 :new-status "pending"}])
(def mock-stats {:query-processor {:customer-count 1} :registry-processor {:registered-count 1}})

(defmacro with-mock-cass-functions [& body]
  `(with-redefs [cass/check-connection (constantly true)
                 cass/get-all-stats (constantly mock-stats)
                 cass/get-customer-stats #(if (= % 1) mock-customer-stats nil)
                 cass/get-top-customers (constantly [mock-customer-stats mock-customer-stats])
                 cass/get-product-stats #(if (= % "P1") mock-product-stats nil)
                 cass/get-top-products (constantly [mock-product-stats mock-product-stats])
                 cass/get-registered-order #(if (= % (str (:order-id mock-registered-order))) mock-registered-order nil)
                 cass/get-timeline (constantly mock-timeline)
                 cass/update-order-status (constantly true)]
     ~@body))

(defn json-request [method uri json-data]
  (-> (mock/request method uri)
      (mock/content-type "application/json")
      (mock/body (json/generate-string json-data))))

(deftest api-routes-test
  (with-mock-cass-functions
    (testing "GET /api/health"
      (let [response (server/app (mock/request :get "/api/health"))
            body (parse-body response)]
        (is (= 200 (:status response)))
        (is (= "healthy" (:status body)))
        (is (= "connected" (:cassandra body)))))

    (testing "GET /api/stats"
      (let [response (server/app (mock/request :get "/api/stats"))
            body (parse-body response)]
        (is (= 200 (:status response)))
        (is (true? (:success body)))
        (is (= 1 (:customer-count (:query-processor (:data body)))))))

    (testing "GET /api/customers/:id - found"
      (let [response (server/app (mock/request :get "/api/customers/1"))
            body (parse-body response)]
        (is (= 200 (:status response)))
        (is (= 1 (:customer-id (:data body))))))

    (testing "GET /api/customers/:id - not found"
      (let [response (server/app (mock/request :get "/api/customers/999"))]
        (is (= 404 (:status response)))))

    (testing "GET /api/customers/:id - invalid ID"
      (let [response (server/app (mock/request :get "/api/customers/abc"))]
        (is (= 400 (:status response)))))

    (testing "PUT /api/orders/:id/status - success"
      (let [order-id "11111111-1111-1111-1111-111111111111"
            response (server/app (json-request :put
                                               (str "/api/orders/" order-id "/status")
                                               {:status "approved"}))
            body (parse-body response)]
        (is (= 200 (:status response)))
        (is (true? (:success body)))))

    (testing "PUT /api/orders/:id/status - invalid status"
      (let [response (server/app (json-request :put
                                               "/api/orders/123/status"
                                               {:status "invalid"}))]
        (is (= 400 (:status response)))))

    (testing "GET /"
      (let [response (server/app (mock/request :get "/"))]
        (is (= 200 (:status response)))
        (is (str/starts-with? (get-in response [:headers "Content-Type"]) "text/html"))))

    (testing "GET /non-existent-route - 404"
      (let [response (server/app (mock/request :get "/invalid"))]
        (is (= 404 (:status response)))))))