(ns monitor.config
  "Configuration for monitor."
  (:require [aero.core :as aero]
            [clojure.java.io :as io]))

(defn- get-system-property
  "Wrapper for System/getProperty to allow mocking in tests."
  [key]
  (System/getProperty key))

(defn- get-env-var
  "Wrapper for System/getenv to allow mocking in tests."
  [key]
  (System/getenv key))

(defn get-profile
  "Get current profile from system property or environment variable."
  []
  (keyword (or (get-system-property "profile")
               (get-env-var "PROFILE")
               "dev")))

(defn load-config
  "Reads and loads the configuration from config.edn based on the profile."
  [profile]
  (aero/read-config (io/resource "config.edn") {:profile profile}))

(def config (delay (load-config (get-profile)))) ; Lazy load the configuration

(defn cassandra-config
  "Returns the Cassandra configuration map, prioritizing ENV vars."
  []
  (let [config-map (:cassandra @config)
        env-host (get-env-var "CASSANDRA_HOST")
        env-port (get-env-var "CASSANDRA_PORT")
        env-datacenter (get-env-var "CASSANDRA_DATACENTER")]

    (-> config-map
        (assoc :host (or env-host (:host config-map)))
        (assoc :port (or env-port (:port config-map)))
        (assoc :datacenter (or env-datacenter (:datacenter config-map))))))

(defn server-config
  "Returns the server configuration map."
  []
  (:server @config))