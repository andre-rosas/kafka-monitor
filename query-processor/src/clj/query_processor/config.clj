(ns query-processor.config
  "Configuration loading and management for query-processor.
  
  Uses delay for lazy initialization (no atoms needed).
  Configuration is loaded once from config.edn and cached."
  (:require [aero.core :as aero]
            [clojure.java.io :as io]))

;; =============================================================================
;; Configuration Loading
;; =============================================================================

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
  "Load configuration from resources/config.edn.
  
  Args:
    profile - Keyword profile (:dev, :test, :prod)
    
  Returns:
    Map with all configuration
    
  Example:
    (load-config :dev)
    ;; => {:kafka {...} :cassandra {...} :processor {...}}"
  [profile]
  (aero/read-config
   (io/resource "config.edn")
   {:profile profile}))

;; Delayed config - lazy initialization (loaded only when first accessed)
(def config
  "Application configuration (delay).
  
  Usage:
    @config ;; force and get config
    (:kafka @config) ;; get kafka config"
  (delay (load-config (get-profile))))

;; =============================================================================
;; Configuration Getters
;; =============================================================================

(defn kafka-config
  "Get Kafka consumer configuration.
  
  Returns:
    Map with Kafka settings:
    {:bootstrap-servers \"localhost:9092\"
     :group-id \"query-processor\"
     :topics [\"orders\"]
     :poll-timeout-ms 1000
     :max-poll-records 500}"
  []
  (:kafka @config))

(defn cassandra-config
  "Get Cassandra configuration.
  
  Returns:
    Map with Cassandra settings:
    {:host \"localhost\"
     :port 9042
     :datacenter \"datacenter1\"
     :keyspace \"query_processor\"}"
  []
  (:cassandra @config))

(defn processor-config
  "Get processor-specific configuration.
  
  Returns:
    Map with processor settings:
    {:processor-id \"query-processor-1\"
     :timeline-max-size 100
     :batch-size 100
     :commit-interval-ms 5000}"
  []
  (:processor @config))

(defn get-processor-id
  "Get unique processor instance ID.
  
  Returns:
    String with processor ID (ex: \"query-processor-1\")"
  []
  (:processor-id (processor-config)))

(defn get-timeline-max-size
  "Get maximum size for timeline (number of orders to keep).
  
  Returns:
    Integer (default: 100)"
  []
  (:timeline-max-size (processor-config) 100))

(defn get-batch-size
  "Get batch size for processing orders.
  
  Returns:
    Integer (default: 100)"
  []
  (:batch-size (processor-config) 100))

(defn get-commit-interval-ms
  "Get interval for committing Kafka offsets (milliseconds).
  
  Returns:
    Integer (default: 5000ms = 5 seconds)"
  []
  (:commit-interval-ms (processor-config) 5000))