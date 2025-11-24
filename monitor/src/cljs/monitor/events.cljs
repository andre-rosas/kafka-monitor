
(ns monitor.events
  (:require [re-frame.core :as rf]
            [day8.re-frame.http-fx]
            [ajax.core :as ajax]
            [ajax.interceptors :as i]))

;; Beginning
(rf/reg-event-db
 ::initialize
 (fn [_ _]
   {:loading? false
    :error nil
    :stats {}
    :timeline []
    :top-customers []
    :top-products []
    :processors-status {}
    :selected-view :dashboard
    :auto-refresh? false}))

;; Navigation
(rf/reg-event-db
 ::navigate
 (fn [db [_ view]]
   (assoc db :selected-view view)))

(rf/reg-event-fx
 ::fetch-stats
 (fn [{:keys [db]} _]
   (let [cache-bust (str "?cb=" (.now js/Date))]
     {:db (assoc db :loading? true)
      :http-xhrio {:method :get
                   :uri (str "/api/stats" cache-bust)
                   :timeout 8000
                   :response-format (ajax/json-response-format {:keywords? true})
                   :on-success [::fetch-stats-success]
                   :on-failure [::fetch-stats-failure]}})))

(rf/reg-event-db
 ::fetch-stats-success
 (fn [db [_ response]]
   (-> db
       (assoc :loading? false)
       (assoc :stats (:data response))
       (assoc :error nil))))

(rf/reg-event-db
 ::fetch-stats-failure
 (fn [db [_ error]]
   (-> db
       (assoc :loading? false)
       (assoc :error "Failed to load stats"))))

(rf/reg-event-fx
 ::fetch-timeline
 (fn [_ _]
   (let [cache-bust (str "?cb=" (.now js/Date))]
     {:http-xhrio {:method :get
                   :uri (str "/api/timeline" cache-bust)
                   :timeout 8000
                   :response-format (ajax/json-response-format {:keywords? true})
                   :on-success [::fetch-timeline-success]
                   :on-failure [::fetch-timeline-failure]}})))
(rf/reg-event-db
 ::fetch-timeline-success
 (fn [db [_ response]]
   (assoc db :timeline (:data response))))

(rf/reg-event-db
 ::fetch-timeline-failure
 (fn [db [_ _]]
   db))

;; Fetch Top Customers
(rf/reg-event-fx
 ::fetch-top-customers
 (fn [_ _]
   {:http-xhrio {:method :get
                 :uri "/api/customers/top"
                 :timeout 8000
                 :response-format (ajax/json-response-format {:keywords? true})
                 :on-success [::fetch-top-customers-success]
                 :on-failure [::fetch-top-customers-failure]}}))

(rf/reg-event-db
 ::fetch-top-customers-success
 (fn [db [_ response]]
   (assoc db :top-customers (:data response))))

(rf/reg-event-db
 ::fetch-top-customers-failure
 (fn [db [_ _]]
   db))

(rf/reg-event-db
 ::update-timeline-status
 (fn [db [_ order-id new-status]]
   (update db :timeline
           (fn [timeline]
             (mapv (fn [order]
                     (if (= (:order-id order) order-id)
                       (assoc order :status new-status)
                       order))
                   timeline)))))

(rf/reg-event-db
 ::update-stats
 (fn [db [_ order-id old-status new-status]]
   (let [stats (:stats db)
         rp (:registry-processor stats)

         current-accepted (:accepted-count rp 0)
         current-denied (:denied-count rp 0)
         current-pending (:pending-count rp 0)

         new-accepted (cond
                        (and (= old-status "pending") (= new-status "accepted")) (inc current-accepted)
                        (and (= old-status "accepted") (= new-status "pending")) (dec current-accepted)
                        :else current-accepted)

         new-denied (cond
                      (and (= old-status "pending") (= new-status "denied")) (inc current-denied)
                      (and (= old-status "denied") (= new-status "pending")) (dec current-denied)
                      :else current-denied)

         new-pending (cond
                       (and (= old-status "pending") (not= new-status "pending")) (dec current-pending)
                       (and (not= old-status "pending") (= new-status "pending")) (inc current-pending)
                       :else current-pending)

         new-rp (assoc rp
                       :accepted-count new-accepted
                       :denied-count new-denied
                       :pending-count new-pending)

         new-stats (assoc stats :registry-processor new-rp)]

     (assoc db :stats new-stats))))

;; Fetch Top Products
(rf/reg-event-fx
 ::fetch-top-products
 (fn [_ _]
   {:http-xhrio {:method :get
                 :uri "/api/products/top"
                 :timeout 8000
                 :response-format (ajax/json-response-format {:keywords? true})
                 :on-success [::fetch-top-products-success]
                 :on-failure [::fetch-top-products-failure]}}))

(rf/reg-event-db
 ::fetch-top-products-success
 (fn [db [_ response]]
   (assoc db :top-products (:data response))))

(rf/reg-event-db
 ::fetch-top-products-failure
 (fn [db [_ _]]
   db))

;; Fetch All
(rf/reg-event-fx
 ::fetch-all
 (fn [_ _]
   {:dispatch-n [[::fetch-stats]
                 [::fetch-timeline]
                 [::fetch-top-customers]
                 [::fetch-top-products]]}))

;; Toggle Auto Refresh
(rf/reg-event-db
 ::toggle-auto-refresh
 (fn [db _]
   (update db :auto-refresh? not)))

;; Clear Error
(rf/reg-event-db
 ::clear-error
 (fn [db _]
   (assoc db :error nil)))