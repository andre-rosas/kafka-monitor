(ns views.products-test
  (:require [cljs.test :refer-macros [deftest is testing]]
            [views.products :as products]
            [reagent.core :as r]))

(deftest products-view-test
  (testing "products-view is a function component"
    (is (fn? products/products-view))))

(deftest products-info-test
  (testing "products-info renders correctly"
    (let [component (products/products-info)]
      (is (vector? component))
      (is (= :div.products-info (first component))))))

(deftest sortable-table-test
  (testing "sortable-table is a function component"
    (let [component (products/sortable-table)]
      (is (fn? component)))))