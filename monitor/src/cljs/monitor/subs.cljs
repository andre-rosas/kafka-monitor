(ns monitor.subs
  (:require [re-frame.core :as rf]))

(rf/reg-sub
 ::selected-view
 (fn [db _]
   (:selected-view db)))

(rf/reg-sub
 ::loading?
 (fn [db _]
   (:loading? db)))

(rf/reg-sub
 ::error
 (fn [db _]
   (:error db)))

(rf/reg-sub
 ::auto-refresh?
 (fn [db _]
   (:auto-refresh? db)))

(rf/reg-sub
 ::stats
 (fn [db _]
   (:stats db)))

(rf/reg-sub
 ::timeline
 (fn [db _]
   (:timeline db)))

(rf/reg-sub
 ::top-customers
 (fn [db _]
   (:top-customers db)))

(rf/reg-sub
 ::top-products
 (fn [db _]
   (:top-products db)))

(rf/reg-sub
 ::processors-status
 (fn [db _]
   (:processors-status db)))