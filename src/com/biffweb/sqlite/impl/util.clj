(ns com.biffweb.sqlite.impl.util
  "Internal helpers for SQL name conversion and type mapping."
  (:require [clojure.string :as str]))

(defn sql-name
  "Convert a kebab-case keyword name to snake_case for SQL."
  [s]
  (str/replace (name s) "-" "_"))

(defn col-table
  "Get the table keyword from a column :id (its namespace as a keyword)."
  [col-id]
  (keyword (namespace col-id)))

(defn col-table-name
  "Get the SQL table name from a column :id."
  [col-id]
  (sql-name (namespace col-id)))

(defn col-col-name
  "Get the SQL column name from a column :id."
  [col-id]
  (sql-name (name col-id)))

(defn sqlite-type
  "Map column :type to SQLite storage type."
  [col-type]
  (case col-type
    :uuid    "BLOB"
    :text    "TEXT"
    :int     "INT"
    :real    "REAL"
    :boolean "INT"
    :inst    "INT"
    :enum    "INT"
    :edn     "BLOB"
    :string  "TEXT"))
