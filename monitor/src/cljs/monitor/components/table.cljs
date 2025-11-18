(ns monitor.components.table
  "Reusable table and badge components.")

(defn badge
  "Display a simple badge with a color class.
   Useful for status indicators within tables.
   Ex: (badge {:label \"Healthy\" :color :green})"
  [{:keys [label color]}]
  [:span.badge {:class (name color)}
   label])

(defn- render-cell
  "Internal helper to render a table cell.
   It can either render data directly via :key
   or use a custom :render function."
  [row column]
  (let [data-key (:key column)
        render-fn (:render column)
        cell-data (get row data-key)
        class-name (:class column)]
    [:td {:class class-name}
     (if render-fn
       (render-fn row)
       (str cell-data))]))

(defn data-table
  "Renders a standard data table from columns and data.
   - :columns - A vector of maps, e.g., [{:key :id :label \"ID\"}
                                        {:key :name :label \"Name\"}
                                        {:key :status :label \"Status\" :render (fn [row] [badge ...])}]
   - :data - A vector of maps, where each map is a row.
   - :empty-message - (Optional) Message to show when data is empty."
  [{:keys [columns data empty-message class]}]
  [:div.table-container {:class class}
   [:table.table
    [:thead
     [:tr
      (for [col columns]
        [:th {:key (:key col)} (:label col)])]]
    [:tbody
     (if (empty? data)
       [:tr.empty-row
        [:td {:col-span (count columns)}
         (or empty-message "No data to display.")]]
       (for [row data]
         [:tr {:key (or (:id row) (hash row))}
          (for [col columns]
            ^{:key (:key col)}
            [render-cell row col])]))]]])