(ns monitor.config-test
  (:require [clojure.test :refer [deftest is testing]]
            [monitor.config :as config])
  (:import [java.lang System]))
;; --- Mocks for Testing Configuration ---

(def mock-config {:server {:port 9999} :cassandra {:host "mockhost"}})

(defn mock-load-config [profile]
  (case profile
    :dev mock-config
    :test (assoc-in mock-config [:server :port] 3001)
    :prod (assoc-in mock-config [:server :port] 8080)
    mock-config))

(deftest get-profile-test
  (testing "Defaults to 'dev' when no system property or environment variable is set"
    (with-redefs [config/get-system-property (constantly nil)
                  config/get-env-var (constantly nil)]
      (is (= :dev (config/get-profile)))))

  (testing "Uses system property 'profile' if set"
    (with-redefs [config/get-system-property (fn [k] (if (= k "profile") "test" nil))
                  config/get-env-var (constantly "prod")]
      (is (= :test (config/get-profile)))))

  (testing "Uses environment variable 'PROFILE' if system property is absent"
    (with-redefs [config/get-system-property (constantly nil)
                  config/get-env-var (fn [k] (if (= k "PROFILE") "prod" nil))]
      (is (= :prod (config/get-profile))))))

(deftest config-accessors-test
  (with-redefs [config/load-config mock-load-config
                config/get-profile (constantly :dev)]
    (alter-var-root #'config/config (constantly (delay (mock-load-config :dev))))

    (testing "cassandra-config returns the correct map"
      (is (= {:host "mockhost"} (config/cassandra-config))))

    (testing "server-config returns the correct map"
      (is (= {:port 9999} (config/server-config))))))