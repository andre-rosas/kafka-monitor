(ns shared.utils
  "Shared utility functions across all microservices.
   
   Provides cross-platform utilities for:
   - Date/time operations
   - ID generation
   - Data conversion
   - Number manipulation
   - Collection operations
   - String processing
   - Validation
   - Error handling
   - Logging helpers"
  #?(:clj (:import [java.util Properties UUID Date]
                   [java.time Instant]))
  #?(:cljs (:require [goog.string :as gstring]
                     [goog.string.format])))
;; =============================================================================
;; Date/Time Utilities
;; =============================================================================

(defn now-millis
  "Get current timestamp in milliseconds since epoch.
   Cross-platform implementation for both JVM and JavaScript."
  []
  #?(:clj (System/currentTimeMillis)
     :cljs (.now js/Date)))

(defn millis->date
  "Convert milliseconds to Date object.
   Returns java.util.Date in CLJ and js/Date in CLJS."
  [millis]
  #?(:clj (Date. (long millis))
     :cljs (js/Date. millis)))

(defn format-timestamp
  "Format timestamp to human-readable string.
   Uses ISO format in CLJ and locale string in CLJS."
  [millis]
  #?(:clj (.toString (Instant/ofEpochMilli millis))
     :cljs (.toLocaleString (js/Date. millis))))

;; =============================================================================
;; ID Generation
;; =============================================================================

(defn generate-uuid
  "Generate a random UUID string.
   Cross-platform UUID generation using platform-specific implementations."
  []
  #?(:clj (str (UUID/randomUUID))
     :cljs (str (random-uuid))))

(defn generate-order-id
  "Generate unique order ID with timestamp and random component.
   Format: ORDER-{timestamp}-{random-number}"
  []
  (str "ORDER-" (now-millis) "-" (rand-int 10000)))

;; =============================================================================
;; Map/Properties Conversion
;; =============================================================================

(defn map->properties
  "Convert Clojure map to Java Properties (CLJ) or JavaScript object (CLJS).
   Primarily used for Kafka configuration across platforms."
  [m]
  #?(:clj
     (let [props (Properties.)]
       (doseq [[k v] m]
         (.put props (name k) (str v)))
       props)
     :cljs
     (clj->js m)))

(defn properties->map
  "Convert Java Properties to Clojure map (CLJ) or JavaScript object to map (CLJS).
   Returns map with keywordized keys."
  [^Properties props]
  #?(:clj
     (into {} (for [[k v] props]
                [(keyword k) v]))
     :cljs
     (js->clj props :keywordize-keys true)))

;; =============================================================================
;; Number Manipulation Utilities
;; =============================================================================

(defn round
  "Round number to specified decimal places.
   Uses mathematical rounding in CLJ and string formatting in CLJS."
  [num decimals]
  #?(:clj
     (let [factor (Math/pow 10 decimals)]
       (/ (Math/round (* num factor)) factor))
     :cljs
     (.toFixed num decimals)))

(defn random-between
  "Generate random number between min and max (inclusive).
   Returns floating-point number within specified range."
  [min-val max-val]
  (+ min-val (rand (- max-val min-val))))

(defn percentage
  "Calculate percentage (part/total * 100).
   Handles division by zero and rounds to 2 decimal places."
  [part total]
  (if (zero? total)
    0.0
    (round (* (/ part total) 100) 2)))

;; =============================================================================
;; Collection Utilities
;; =============================================================================

(defn safe-get
  "Safe map lookup with default value.
   Returns default value if key is not found in map."
  [m k default]
  (get m k default))

(defn filter-nil-values
  "Remove entries with nil values from map.
   Returns new map containing only non-nil values."
  [m]
  (into {} (filter (comp some? val) m)))

(defn group-by-key
  "Group collection by key function into map.
   Returns map where keys are results of key-fn and values are collections of items."
  [key-fn coll]
  (group-by key-fn coll))

;; =============================================================================
;; String Utilities
;; =============================================================================

(defn truncate
  "Truncate string to maximum length with ellipsis indicator.
   If string is shorter than max-length, returns original string unchanged.
   Handles edge cases where max-length is too small for ellipsis."
  [s max-length]
  (let [s-len (count s)]
    (cond
      ;; String fits within max-length
      (<= s-len max-length) s

      ;; Max-length too small for ellipsis, just truncate
      (<= max-length 3) (subs s 0 max-length)

      ;; Normal case: truncate and add ellipsis
      :else (let [chars-to-take (- max-length 3)]
              (str (subs s 0 chars-to-take) "...")))))

(defn kebab->snake
  "Convert kebab-case string to snake_case.
   Example: 'hello-world' becomes 'hello_world'"
  [s]
  (clojure.string/replace s #"-" "_"))

(defn snake->kebab
  "Convert snake_case string to kebab-case.
   Example: 'hello_world' becomes 'hello-world'"
  [s]
  (clojure.string/replace s #"_" "-"))

;; =============================================================================
;; Validation Utilities
;; =============================================================================

(defn valid-email?
  "Check if string matches basic email format pattern.
   Uses simple regex pattern for email validation."
  [email]
  (boolean (re-matches #".+@.+\..+" email)))

(defn valid-positive?
  "Check if value is a positive number.
   Returns true for numbers greater than zero."
  [n]
  (and (number? n) (pos? n)))

(defn within-range?
  "Check if number is within specified range (inclusive).
   Returns true if min-val <= n <= max-val."
  [n min-val max-val]
  (and (>= n min-val) (<= n max-val)))


;; =============================================================================
;; Error Handling Utilities
;; =============================================================================

(defn try-parse-int
  "Safely parse string to integer, returning nil on failure.
   Handles NumberFormatException in CLJ and NaN in CLJS."
  [s]
  #?(:clj
     (try
       (Integer/parseInt s)
       (catch Exception _ nil))
     :cljs
     (let [n (js/parseInt s)]
       (when-not (js/isNaN n) n))))

(defn try-parse-double
  "Safely parse string to double, returning nil on failure.
   Handles NumberFormatException in CLJ and NaN in CLJS."
  [s]
  #?(:clj
     (try
       (Double/parseDouble s)
       (catch Exception _ nil))
     :cljs
     (let [n (js/parseFloat s)]
       (when-not (js/isNaN n) n))))

;; =============================================================================
;; Logging Helpers Utilities
;; =============================================================================

(defn log-context
  "Create standardized logging context map.
   Merges event name and timestamp with provided data."
  [event data]
  (merge {:event event
          :timestamp (now-millis)}
         data))

(defn redact-sensitive
  "Redact sensitive fields from map for safe logging.
   Replaces values of specified keys with '***REDACTED***'."
  [m sensitive-keys]
  (reduce
   (fn [acc k]
     (if (contains? acc k)
       (assoc acc k "***REDACTED***")
       acc))
   m
   sensitive-keys))