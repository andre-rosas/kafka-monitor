(ns monitor.components.forms
  "Reusable form components.")

(defn text-input
  "Text input field with label."
  [{:keys [id label value placeholder on-change required? disabled? error]}]
  [:div.form-group
   (when label
     [:label {:for id} label
      (when required? [:span.required " *"])])
   [:input.form-control
    {:id id
     :type "text"
     :value (or value "")
     :placeholder placeholder
     :on-change on-change
     :disabled disabled?
     :class (when error "error")}]
   (when error
     [:span.error-message error])])

(defn number-input
  "Number input field with label."
  [{:keys [id label value placeholder on-change min max step required? disabled? error]}]
  [:div.form-group
   (when label
     [:label {:for id} label
      (when required? [:span.required " *"])])
   [:input.form-control
    {:id id
     :type "number"
     :value (or value "")
     :placeholder placeholder
     :on-change on-change
     :min min
     :max max
     :step step
     :disabled disabled?
     :class (when error "error")}]
   (when error
     [:span.error-message error])])

(defn select-input
  "Select dropdown with options."
  [{:keys [id label value options on-change required? disabled? error]}]
  [:div.form-group
   (when label
     [:label {:for id} label
      (when required? [:span.required " *"])])
   [:select.form-control
    {:id id
     :value (or value "")
     :on-change on-change
     :disabled disabled?
     :class (when error "error")}
    [:option {:value ""} "-- Select --"]
    (for [opt options]
      (if (map? opt)
        [:option {:key (:value opt) :value (:value opt)} (:label opt)]
        [:option {:key opt :value opt} opt]))]
   (when error
     [:span.error-message error])])

(defn textarea-input
  "Textarea field with label."
  [{:keys [id label value placeholder on-change rows required? disabled? error]}]
  [:div.form-group
   (when label
     [:label {:for id} label
      (when required? [:span.required " *"])])
   [:textarea.form-control
    {:id id
     :value (or value "")
     :placeholder placeholder
     :on-change on-change
     :rows (or rows 4)
     :disabled disabled?
     :class (when error "error")}]
   (when error
     [:span.error-message error])])

(defn button
  "Button component with variants."
  [{:keys [label on-click type variant disabled? loading? icon class]}]
  [:button.btn
   {:type (or type "button")
    :class (str (name (or variant :primary)) " " class)
    :on-click on-click
    :disabled (or disabled? loading?)}
   (when loading?
     [:span.spinner])
   (when icon
     [:span.icon icon])
   [:span label]])

(defn form
  "Form wrapper with submit handler."
  [{:keys [on-submit children class]}]
  [:form.form
   {:on-submit (fn [e]
                 (.preventDefault e)
                 (when on-submit (on-submit)))
    :class class}
   children])

(defn search-input
  "Search input with icon."
  [{:keys [value on-change on-search placeholder]}]
  [:div.search-box
   [:input.search-input
    {:type "text"
     :value (or value "")
     :placeholder (or placeholder "Search...")
     :on-change on-change
     :on-key-press (fn [e]
                     (when (and on-search (= (.-key e) "Enter"))
                       (on-search value)))}]
   [:button.search-btn
    {:type "button"
     :on-click #(when on-search (on-search value))}
    "🔍"]])