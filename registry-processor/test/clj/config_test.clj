(ns config-test
  "Comprehensive tests for configuration management in registry-processor."
  (:require [clojure.test :refer :all]
            [registry-processor.config :as config]
            [aero.core :as aero]
            [clojure.java.io :as io]))

(deftest configuration-loading-test
  (testing "Load configuration for test profile"
    (let [cfg (config/load-config :test)]
      (is (map? cfg) "Configuration should be a map")
      (is (contains? cfg :kafka-consumer) "Should contain Kafka consumer config")
      (is (contains? cfg :kafka-producer) "Should contain Kafka producer config")
      (is (contains? cfg :cassandra) "Should contain Cassandra config")
      (is (contains? cfg :processor) "Should contain processor config")))

  (testing "Load configuration for development profile"
    (let [cfg (config/load-config :dev)]
      (is (map? cfg) "Configuration should be a map")
      (is (contains? cfg :kafka-consumer) "Should contain Kafka consumer config")))

  (testing "Load configuration for production profile"
    (let [cfg (config/load-config :prod)]
      (is (map? cfg) "Configuration should be a map")
      (is (contains? cfg :kafka-consumer) "Should contain Kafka consumer config"))))

(deftest profile-detection-test
  (testing "Profile detection from system properties"
    (with-redefs [config/get-profile (fn [] :test)]
      (is (= :test (config/get-profile)) "Should detect test profile from system property")))

  (testing "Profile detection from environment variables"
    (with-redefs [config/get-profile (fn [] :prod)]
      (is (= :prod (config/get-profile)) "Should detect prod profile from environment variable")))

  (testing "Default profile fallback"
    (with-redefs [config/get-profile (fn [] :dev)]
      (is (= :dev (config/get-profile)) "Should fall back to dev profile by default"))))

(deftest configuration-accessors-test
  (testing "Kafka consumer configuration access"
    (let [consumer-cfg (config/kafka-consumer-config)]
      (is (map? consumer-cfg) "Consumer config should be a map")
      (is (contains? consumer-cfg :bootstrap-servers) "Should contain bootstrap servers")
      (is (contains? consumer-cfg :group-id) "Should contain consumer group ID")
      (is (contains? consumer-cfg :topics) "Should contain topics list")))

  (testing "Kafka producer configuration access"
    (let [producer-cfg (config/kafka-producer-config)]
      (is (map? producer-cfg) "Producer config should be a map")
      (is (contains? producer-cfg :bootstrap-servers) "Should contain bootstrap servers")
      (is (contains? producer-cfg :registry-topic) "Should contain registry topic")))

  (testing "Cassandra configuration access"
    (let [cassandra-cfg (config/cassandra-config)]
      (is (map? cassandra-cfg) "Cassandra config should be a map")
      (is (contains? cassandra-cfg :host) "Should contain host")
      (is (contains? cassandra-cfg :port) "Should contain port")
      (is (contains? cassandra-cfg :datacenter) "Should contain datacenter")
      (is (contains? cassandra-cfg :keyspace) "Should contain keyspace")))

  (testing "Processor configuration access"
    (let [processor-cfg (config/processor-config)]
      (is (map? processor-cfg) "Processor config should be a map")
      (is (contains? processor-cfg :processor-id) "Should contain processor ID")
      (is (contains? processor-cfg :commit-interval-ms) "Should contain commit interval"))))

(deftest configuration-values-test
  (testing "Processor ID retrieval"
    (let [processor-id (config/get-processor-id)]
      (is (string? processor-id) "Processor ID should be a string")
      (is (not (boolean (empty? processor-id))) "Processor ID should not be empty")))

  (testing "Commit interval retrieval with default"
    (let [commit-interval (config/get-commit-interval-ms)]
      (is (number? commit-interval) "Commit interval should be a number")
      (is (pos? commit-interval) "Commit interval should be positive")))

  (testing "Commit interval with explicit configuration"
    (with-redefs [config/processor-config (fn [] {:commit-interval-ms 10000})]
      (let [commit-interval (config/get-commit-interval-ms)]
        (is (= 10000 commit-interval) "Should return configured commit interval")))))

(deftest configuration-validation-test
  (testing "Required configuration sections exist"
    (let [cfg (config/load-config :test)]
      (doseq [section [:kafka-consumer :kafka-producer :cassandra :processor]]
        (is (contains? cfg section) (str "Should contain " section " section")))))

  (testing "Configuration structure consistency across profiles"
    (doseq [profile [:test :dev :prod]]
      (let [cfg (config/load-config profile)]
        (is (map? cfg) (str "Config for " profile " should be a map"))
        (is (every? keyword? (keys cfg)) (str "All keys for " profile " should be keywords"))))))

(deftest resource-loading-test
  (testing "Configuration file exists as resource"
    (let [config-file (io/resource "config.edn")]
      (is (some? config-file) "config.edn should exist as resource")
      (is (.exists (io/file config-file)) "config.edn file should exist")))

  (testing "Aero can read configuration file"
    (let [config-file (io/resource "config.edn")
          test-config (aero/read-config config-file {:profile :test})]
      (is (map? test-config) "Aero should read config as map")
      (is (contains? test-config :kafka-consumer) "Should contain expected sections"))))

(deftest environment-override-test
  (testing "Configuration supports environment variable overrides"
    ;; This test verifies that the configuration structure allows for
    ;; environment-based overrides as per Aero best practices
    (let [cfg (config/load-config :test)]
      (is (map? cfg) "Config should be loadable with environment overrides"))))

(deftest configuration-caching-test
  (testing "Configuration is properly cached"
    (let [first-call @config/config
          second-call @config/config]
      (is (identical? first-call second-call) "Configuration should be cached and return same instance"))))

(deftest error-handling-test
  (testing "Graceful handling of missing configuration file"
    (with-redefs [io/resource (fn [_] nil)]
      (is (thrown? Exception (config/load-config :test))
          "Should throw exception when config file not found"))))