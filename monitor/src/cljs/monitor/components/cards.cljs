(ns monitor.components.cards
  "Reusable card components for dashboard.")

(defn stat-card
  "Display a statistic card with icon, title, and value."
  [{:keys [title value icon color subtitle loading?]}]
  [:div.stat-card {:class color}
   (when loading?
     [:div.card-loading])
   [:div.stat-icon icon]
   [:div.stat-content
    [:div.stat-title title]
    [:div.stat-value value]
    (when subtitle
      [:div.stat-subtitle subtitle])]])

(defn info-card
  "Display an information card with header and body."
  [{:keys [header body footer actions class]}]
  [:div.info-card {:class class}
   (when header
     [:div.card-header header])
   [:div.card-body body]
   (when footer
     [:div.card-footer footer])
   (when actions
     [:div.card-actions actions])])

(defn metric-card
  "Display a metric with trend indicator."
  [{:keys [label value trend unit icon]}]
  [:div.metric-card
   [:div.metric-icon icon]
   [:div.metric-content
    [:div.metric-label label]
    [:div.metric-value
     [:span.value value]
     (when unit
       [:span.unit unit])]
    (when trend
      [:div.metric-trend {:class (if (pos? trend) "up" "down")}
       [:span (if (pos? trend) "↑" "↓")]
       [:span (str (Math/abs trend) "%")]])]])

(defn status-card
  "Display service status card."
  [{:keys [name status uptime requests errors]}]
  (let [status-color (case status
                       :healthy "green"
                       :degraded "yellow"
                       :unhealthy "red"
                       "gray")]
    [:div.status-card
     [:div.status-header
      [:h3 name]
      [:span.status-badge {:class status-color}
       (name status)]]
     [:div.status-metrics
      [:div.status-metric
       [:span.label "Uptime"]
       [:span.value uptime]]
      [:div.status-metric
       [:span.label "Requests"]
       [:span.value requests]]
      [:div.status-metric
       [:span.label "Errors"]
       [:span.value errors]]]]))