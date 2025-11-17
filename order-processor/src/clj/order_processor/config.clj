(ns order-processor.config
  "Configuration management for orders producer."
  (:require [aero.core :as aero]
            [clojure.java.io :as io]))

(defmethod aero/reader 'range
  [_opts _tag [start end]]
  (vec (range start (inc end))))

(def config (delay (aero/read-config (io/resource "config.edn"))))

(defn get-config [& path]
  (get-in @config path))

(defn kafka-config []
  (get-config :kafka))

(defn producer-config []
  (get-config :producer))

(defn orders-config []
  (get-config :orders))

(defn cassandra-config []
  (get-config :cassandra))

(defn init! []
  @config
  nil)