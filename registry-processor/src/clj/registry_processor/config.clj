(ns registry-processor.config
  "Configuration management for registry-processor."
  (:require [aero.core :as aero]
            [clojure.java.io :as io]))

(defn get-profile
  "Get current profile from system property or environment variable.
  
  Priority:
  1. -Dprofile=prod (system property)
  2. PROFILE env var
  3. :dev (default)
  
  Returns:
    Keyword (:dev, :test, or :prod)"
  []
  (keyword (or (System/getProperty "profile")
               (System/getenv "PROFILE")
               "dev")))

(defn load-config
  "Load configuration from resources/config.edn."
  [profile]
  (aero/read-config
   (io/resource "config.edn")
   {:profile profile}))

(def config
  "Application configuration (delay)."
  (delay (load-config (get-profile))))

(defn kafka-consumer-config
  "Get Kafka consumer configuration."
  []
  (:kafka-consumer @config))

(defn kafka-producer-config
  "Get Kafka producer configuration."
  []
  (:kafka-producer @config))

(defn cassandra-config
  "Get Cassandra configuration."
  []
  (:cassandra @config))

(defn processor-config
  "Get processor-specific configuration."
  []
  (:processor @config))

(defn get-processor-id
  "Get processor instance ID."
  []
  (:processor-id (processor-config)))

(defn get-commit-interval-ms
  "Get commit interval in milliseconds."
  []
  (:commit-interval-ms (processor-config) 5000))
