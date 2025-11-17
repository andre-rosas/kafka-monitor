(ns shared.utils-test
  "Comprehensive tests for shared utility functions across Clojure and ClojureScript."
  #?(:clj (:require [clojure.test :refer [deftest testing is]]
                    [shared.utils :as utils])
     :cljs (:require [cljs.test :refer-macros [deftest testing is]]
                     [shared.utils :as utils])))

;; =============================================================================
;; Test Configuration
;; =============================================================================

(def ^:dynamic *verbose-testing* false)

(defn- log-verbose [message]
  (when *verbose-testing*
    (println message)))

;; =============================================================================
;; Date/Time Utilities Tests
;; =============================================================================

(deftest ^:verbose now-millis-test
  (testing "now-millis returns a number representing current time"
    (log-verbose "Testing now-millis function")
    (let [result (utils/now-millis)]
      (is (number? result))
      (is (pos? result)))))

(deftest ^:verbose millis->date-test
  (testing "millis->date converts milliseconds to date object"
    (log-verbose "Testing millis->date function")
    (let [millis (utils/now-millis)
          date (utils/millis->date millis)]
      #?(:clj (is (instance? java.util.Date date))
         :cljs (is (instance? js/Date date))))))

(deftest ^:verbose format-timestamp-test
  (testing "format-timestamp returns string representation"
    (log-verbose "Testing format-timestamp function")
    (let [millis (utils/now-millis)
          formatted (utils/format-timestamp millis)]
      (is (string? formatted))
      (is (not (empty? formatted))))))

;; =============================================================================
;; ID Generation Utilities Tests
;; =============================================================================

(deftest ^:verbose generate-uuid-test
  (testing "generate-uuid returns valid UUID string"
    (log-verbose "Testing generate-uuid function")
    (let [uuid (utils/generate-uuid)]
      (is (string? uuid))
      (is (re-matches #"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" uuid)))))

(deftest ^:verbose generate-order-id-test
  (testing "generate-order-id returns formatted order ID"
    (log-verbose "Testing generate-order-id function")
    (let [order-id (utils/generate-order-id)]
      (is (string? order-id))
      (is (re-matches #"^ORDER-\d+-\d+$" order-id)))))

;; =============================================================================
;; Map/Properties Conversion Tests
;; =============================================================================

(deftest ^:verbose map->properties-test
  (testing "map->properties converts map to platform-specific properties object"
    (log-verbose "Testing map->properties function")
    (let [test-map {:key1 "value1" :key2 123 :key3 true}
          props (utils/map->properties test-map)]
      #?(:clj (is (instance? java.util.Properties props))
         :cljs (is (object? props))))))

(deftest ^:verbose properties->map-test
  (testing "properties->map converts properties object back to map"
    (log-verbose "Testing properties->map function")
    #?(:clj
       (let [props (doto (java.util.Properties.)
                     (.setProperty "key1" "value1")
                     (.setProperty "key2" "123"))
             result (utils/properties->map props)]
         (is (map? result))
         (is (= "value1" (:key1 result)))
         (is (= "123" (:key2 result))))
       :cljs
       (let [props #js {"key1" "value1" "key2" "123"}
             result (utils/properties->map props)]
         (is (map? result))
         (is (= "value1" (:key1 result)))
         (is (= "123" (:key2 result)))))))

;; =============================================================================
;; Number Manipulation Tests
;; =============================================================================

(deftest ^:verbose round-test
  (testing "round function correctly rounds numbers to specified decimal places"
    (log-verbose "Testing round function")
    (is (= 3.14 (utils/round 3.14159 2)))
    (is (= 3.142 (utils/round 3.14159 3)))
    (is (= 3.0 (utils/round 3.14159 0)))
    (is (= 100.0 (utils/round 99.999 0)))))

(deftest ^:verbose random-between-test
  (testing "random-between generates numbers within specified range"
    (log-verbose "Testing random-between function")
    (let [min-val 5
          max-val 10
          result (utils/random-between min-val max-val)]
      (is (>= result min-val))
      (is (<= result max-val))
      (is (number? result)))))

(deftest ^:verbose percentage-test
  (testing "percentage calculates correct percentages"
    (log-verbose "Testing percentage function")
    (is (= 50.0 (utils/percentage 1 2)))
    (is (= 0.0 (utils/percentage 0 10)))
    (is (= 100.0 (utils/percentage 10 10)))
    (is (= 33.33 (utils/percentage 1 3)))
    (is (= 0.0 (utils/percentage 5 0)))))

;; =============================================================================
;; Collection Operation Tests
;; =============================================================================

(deftest ^:verbose safe-get-test
  (testing "safe-get returns values or defaults appropriately"
    (log-verbose "Testing safe-get function")
    (let [test-map {:a 1 :b "hello"}]
      (is (= 1 (utils/safe-get test-map :a :not-found)))
      (is (= "hello" (utils/safe-get test-map :b :not-found)))
      (is (= :not-found (utils/safe-get test-map :c :not-found)))
      (is (nil? (utils/safe-get test-map :c nil))))))

(deftest ^:verbose filter-nil-values-test
  (testing "filter-nil-values removes nil entries from map"
    (log-verbose "Testing filter-nil-values function")
    (let [test-map {:a 1 :b nil :c "hello" :d nil :e 42}
          filtered (utils/filter-nil-values test-map)]
      (is (= {:a 1 :c "hello" :e 42} filtered))
      (is (not (contains? filtered :b)))
      (is (not (contains? filtered :d))))))

(deftest ^:verbose group-by-key-test
  (testing "group-by-key groups collection items by key function"
    (log-verbose "Testing group-by-key function")
    (let [items [{:type "A" :value 1}
                 {:type "B" :value 2}
                 {:type "A" :value 3}
                 {:type "C" :value 4}]
          grouped (utils/group-by-key :type items)]
      (is (map? grouped))
      (is (= 2 (count (get grouped "A"))))
      (is (= 1 (count (get grouped "B"))))
      (is (= 1 (count (get grouped "C")))))))

;; =============================================================================
;; String Processing Tests
;; =============================================================================

(deftest ^:verbose truncate-test
  (log-verbose "Testing truncate function")
  ;; Normal cases - leave short strings unchanged
  (is (= "short" (utils/truncate "short" 10)))
  (is (= "hello" (utils/truncate "hello" 5)))

  ;; Truncation cases - ensure total length equals max-length
  (is (= "long st..." (utils/truncate "long string here" 10)))
  (is (= "exact l..." (utils/truncate "exact length" 10)))
  (is (= "abc..." (utils/truncate "abcdefghijklmn" 6)))

  ;; Edge cases with small max-length
  (is (= "a..." (utils/truncate "abcde" 4)))
  (is (= "ab" (utils/truncate "abcdef" 2)))
  (is (= "a" (utils/truncate "abcdef" 1)))

  ;; Exact boundary cases
  (is (= "hello world" (utils/truncate "hello world" 11)))
  (is (= "hello w..." (utils/truncate "hello world" 10)))
  (is (= "abc" (utils/truncate "abc" 3)))
  (is (= "ab" (utils/truncate "abc" 2)))
  (is (= "a" (utils/truncate "abc" 1)))
  (is (= "" (utils/truncate "abc" 0))))

(deftest ^:verbose kebab->snake-test
  (testing "kebab->snake converts kebab-case to snake_case"
    (log-verbose "Testing kebab->snake function")
    (is (= "hello_world" (utils/kebab->snake "hello-world")))
    (is (= "multiple_words_here" (utils/kebab->snake "multiple-words-here")))
    (is (= "no_change" (utils/kebab->snake "no_change")))))

(deftest ^:verbose snake->kebab-test
  (testing "snake->kebab converts snake_case to kebab-case"
    (log-verbose "Testing snake->kebab function")
    (is (= "hello-world" (utils/snake->kebab "hello_world")))
    (is (= "multiple-words-here" (utils/snake->kebab "multiple_words_here")))
    (is (= "no-change" (utils/snake->kebab "no-change")))))

;; =============================================================================
;; Validation Utilities Tests
;; =============================================================================

(deftest ^:verbose valid-email?-test
  (testing "valid-email? correctly identifies valid and invalid email formats"
    (log-verbose "Testing valid-email? function")
    (is (utils/valid-email? "test@example.com"))
    (is (utils/valid-email? "user.name@domain.co.uk"))
    (is (not (utils/valid-email? "invalid")))
    (is (not (utils/valid-email? "invalid@"))
        (is (not (utils/valid-email? "@domain.com"))))))

(deftest ^:verbose valid-positive?-test
  (testing "valid-positive? identifies positive numbers correctly"
    (log-verbose "Testing valid-positive? function")
    (is (utils/valid-positive? 1))
    (is (utils/valid-positive? 0.1))
    (is (utils/valid-positive? 1000))
    (is (not (utils/valid-positive? 0)))
    (is (not (utils/valid-positive? -1)))
    (is (not (utils/valid-positive? "1")))
    (is (not (utils/valid-positive? nil)))))

(deftest ^:verbose within-range?-test
  (testing "within-range? checks if numbers are within specified bounds"
    (log-verbose "Testing within-range? function")
    (is (utils/within-range? 5 1 10))
    (is (utils/within-range? 1 1 10))
    (is (utils/within-range? 10 1 10))
    (is (not (utils/within-range? 0 1 10)))
    (is (not (utils/within-range? 11 1 10)))
    (is (not (utils/within-range? -5 0 10)))))

;; =============================================================================
;; Error Handling Tests
;; =============================================================================

(deftest ^:verbose try-parse-int-test
  (testing "try-parse-int safely parses integers and returns nil on failure"
    (log-verbose "Testing try-parse-int function")
    (is (= 123 (utils/try-parse-int "123")))
    (is (= -456 (utils/try-parse-int "-456")))
    (is (nil? (utils/try-parse-int "not-a-number")))
    (is (nil? (utils/try-parse-int "")))
    (is (nil? (utils/try-parse-int "123.45")))))

(deftest ^:verbose try-parse-double-test
  (testing "try-parse-double safely parses doubles and returns nil on failure"
    (log-verbose "Testing try-parse-double function")
    (is (= 123.45 (utils/try-parse-double "123.45")))
    (is (= -67.89 (utils/try-parse-double "-67.89")))
    (is (= 100.0 (utils/try-parse-double "100")))
    (is (nil? (utils/try-parse-double "not-a-number")))
    (is (nil? (utils/try-parse-double "")))))

;; =============================================================================
;; Logging Helper Tests
;; =============================================================================

(deftest ^:verbose log-context-test
  (testing "log-context creates standardized logging context with timestamp"
    (log-verbose "Testing log-context function")
    (let [context (utils/log-context "user-login" {:user-id 123 :ip "192.168.1.1"})]
      (is (map? context))
      (is (= "user-login" (:event context)))
      (is (number? (:timestamp context)))
      (is (= 123 (:user-id context)))
      (is (= "192.168.1.1" (:ip context))))))

(deftest ^:verbose redact-sensitive-test
  (testing "redact-sensitive replaces sensitive field values with redaction marker"
    (log-verbose "Testing redact-sensitive function")
    (let [data {:username "john_doe"
                :password "secret123"
                :token "abc123"
                :email "john@example.com"
                :public-data "safe to log"}
          redacted (utils/redact-sensitive data [:password :token :api-key])]
      (is (= "***REDACTED***" (:password redacted)))
      (is (= "***REDACTED***" (:token redacted)))
      (is (= "john_doe" (:username redacted)))
      (is (= "john@example.com" (:email redacted)))
      (is (= "safe to log" (:public-data redacted)))
      (is (not (contains? redacted :api-key))))))

;; =============================================================================
;; Cross-Platform Compatibility Tests
;; =============================================================================

(deftest ^:verbose cross-platform-test
  (testing "All utility functions work in both Clojure and ClojureScript"
    (log-verbose "Testing cross-platform compatibility")
    ;; Test basic functionality that should work on both platforms
    (is (string? (utils/generate-uuid)))
    (is (number? (utils/now-millis)))
    (is (map? (utils/filter-nil-values {:a 1 :b nil})))
    (is (string? (utils/truncate "test string" 5)))))

;; =============================================================================
;; Test Runner Configuration
;; =============================================================================

#?(:clj
   (defn test-verbose
     "Run tests with verbose output"
     []
     (binding [*verbose-testing* true]
       (clojure.test/run-tests 'shared.utils-test))))