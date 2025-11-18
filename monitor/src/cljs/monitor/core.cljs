(ns monitor.core
  (:require [reagent.dom :as rdom]
            [re-frame.core :as rf]
            [monitor.views :as views]
            [monitor.events :as events]))

(defn mount-root []
  (rf/clear-subscription-cache!)
  (let [root-el (.getElementById js/document "app")]
    (rdom/render [views/main-panel] root-el)))

(defn ^:dev/after-load re-render []
  (mount-root))

(defn ^:export init []
  (println "Initializing Monitor...")
  (rf/dispatch-sync [::events/initialize])
  (mount-root)
  (println "Monitor initialized!"))