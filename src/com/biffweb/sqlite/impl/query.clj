(ns com.biffweb.sqlite.impl.query
  "Internal query execution: result set building, namespaced alias support."
  (:require [camel-snake-kebab.core :as csk]
            [clojure.string :as str]
            [next.jdbc.result-set :as rs])
  (:import [java.sql ResultSet ResultSetMetaData]))

(defn preprocess-select
  "Walk a HoneySQL :select clause and encode namespaced keyword aliases
   as strings so HoneySQL quotes them correctly."
  [select]
  (if (vector? select)
    (mapv (fn [item]
            (if (and (vector? item)
                     (= 2 (count item))
                     (keyword? (second item))
                     (namespace (second item)))
              (let [kw (second item)]
                [(first item)
                 (str/replace (str (namespace kw) "/" (name kw)) "-" "_")])
              item))
          select)
    select))

(defn- smart-column-names [^ResultSetMetaData rsmeta]
  (mapv (fn [^Integer i]
          (let [label (.getColumnLabel rsmeta i)]
            (if-let [slash-idx (str/index-of label "/")]
              (keyword (csk/->kebab-case-string (subs label 0 slash-idx))
                       (csk/->kebab-case-string (subs label (inc slash-idx))))
              (let [table (.getTableName rsmeta i)]
                (if (and table (not= table ""))
                  (keyword (csk/->kebab-case-string table)
                           (csk/->kebab-case-string label))
                  (keyword (csk/->kebab-case-string label)))))))
        (range 1 (inc (.getColumnCount rsmeta)))))

(defn smart-kebab-maps
  "Builder function that produces kebab-case keyword maps with proper handling
   of namespaced aliases."
  [^ResultSet rs _opts]
  (let [rsmeta (.getMetaData rs)
        cols (smart-column-names rsmeta)]
    (rs/->MapResultSetBuilder rs rsmeta cols)))

(defn make-column-reader
  "Create a column reader fn that applies read coercions."
  [read-coercions]
  (fn [builder ^ResultSet rs ^Integer i]
    (let [col-kw (nth (:cols builder) (dec i))
          value (.getObject rs i)
          coerce-fn (when (namespace col-kw)
                      (let [tbl (str/replace (namespace col-kw) "-" "_")
                            col (str/replace (name col-kw) "-" "_")]
                        (or (get read-coercions (str tbl "." col))
                            (get read-coercions col))))
          coerced-value (if (and coerce-fn (some? value))
                          (coerce-fn value)
                          value)]
      (rs/read-column-by-index coerced-value (:rsmeta builder) i))))
