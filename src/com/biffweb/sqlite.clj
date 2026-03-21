(ns com.biffweb.sqlite
  "SQLite utilities: schema generation from column definitions, type coercion,
   connection pooling, and data validation."
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.java.io :as io]
   [clojure.java.process :as process]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [com.biffweb.sqlite.litestream :as litestream]
   [com.biffweb.sqlite.sqlite3def :as sqlite3def]
   [honey.sql :as hsql]
   [malli.core :as malli]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [taoensso.nippy :as nippy])
  (:import
   [com.zaxxer.hikari HikariConfig HikariDataSource]
   [java.nio ByteBuffer]
   [java.sql ResultSet ResultSetMetaData]
   [java.time Instant]
   [java.util UUID]))

;; ============================================================================
;; Column Helpers
;; ============================================================================

(defn- sql-name
  "Convert a kebab-case string to snake_case for SQL."
  [s]
  (str/replace (name s) "-" "_"))

(defn- col-table
  "Get the table keyword from a column :id (its namespace as a keyword)."
  [col-id]
  (keyword (namespace col-id)))

(defn- col-table-name
  "Get the SQL table name from a column :id."
  [col-id]
  (sql-name (namespace col-id)))

(defn- col-col-name
  "Get the SQL column name from a column :id."
  [col-id]
  (sql-name (name col-id)))

;; ============================================================================
;; Type Mapping
;; ============================================================================

(defn- sqlite-type
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

(defn- coercion-type
  "Determine the coercion type for a column definition."
  [col]
  (case (:type col)
    :uuid    :uuid
    :boolean :bool
    :inst    :inst
    :edn     :nippy
    :enum    {:enum (:enum-values col)}
    nil))

;; ============================================================================
;; Type Coercion
;; ============================================================================

(defn- bytes->uuid
  "Convert a 16-byte array back to a UUID."
  [^bytes byte-array]
  (when byte-array
    (let [bb (ByteBuffer/wrap byte-array)]
      (UUID. (.getLong bb) (.getLong bb)))))

(defn- epoch-ms->inst
  "Convert epoch milliseconds to an Instant."
  [ms]
  (when ms
    (Instant/ofEpochMilli ms)))

(defn- int->bool
  "Convert 0/1 integer to boolean."
  [n]
  (when (some? n)
    (case n
      0 false
      1 true
      (throw (ex-info "Invalid boolean value, expected 0 or 1"
                      {:value n})))))

(defn- fast-thaw [blob]
  (when blob
    (nippy/fast-thaw blob)))

(defn- make-enum-reader [enum-map]
  (fn [db-val]
    (when (some? db-val)
      (or (get enum-map db-val)
          (throw (ex-info "Unknown enum value"
                          {:value db-val :available-values enum-map}))))))

(defn- uuid->bytes
  "Convert a UUID to a 16-byte array for SQLite BLOB storage."
  [^UUID uuid]
  (let [bb (ByteBuffer/allocate 16)]
    (.putLong bb (.getMostSignificantBits uuid))
    (.putLong bb (.getLeastSignificantBits uuid))
    (.array bb)))

(defn- inst->epoch-ms [x]
  (when x
    (.toEpochMilli ^Instant x)))

(defn- bool->int [b]
  (if b 1 0))

(defn- fast-freeze [v]
  (when v
    (nippy/fast-freeze v)))

(defn- make-enum-writer [enum-map]
  (let [reverse-map (into {} (map (fn [[k v]] [v k]) enum-map))]
    (fn [clj-val]
      (when (some? clj-val)
        (or (get reverse-map clj-val)
            (throw (ex-info "Unknown enum value for write"
                            {:value clj-val :available-values reverse-map})))))))

(defn- get-coerce-read-fn [ct]
  (case ct
    :uuid bytes->uuid
    :inst epoch-ms->inst
    :bool int->bool
    :nippy fast-thaw
    (when (map? ct)
      (when-let [enum-map (:enum ct)]
        (make-enum-reader enum-map)))))

(defn- get-coerce-write-fn [ct]
  (case ct
    :uuid uuid->bytes
    :inst inst->epoch-ms
    :bool bool->int
    :nippy fast-freeze
    (when (map? ct)
      (when-let [enum-map (:enum ct)]
        (make-enum-writer enum-map)))))

(defn build-coercions
  "Build coercion maps from a vector of column definitions.
   Returns {:read {col-id coerce-fn} :write {col-id coerce-fn}}"
  [columns]
  (reduce
   (fn [acc col]
     (let [ct (coercion-type col)]
       (if ct
         (let [read-fn (get-coerce-read-fn ct)
               write-fn (get-coerce-write-fn ct)]
           (cond-> acc
             read-fn (assoc-in [:read (:id col)] read-fn)
             write-fn (assoc-in [:write (:id col)] write-fn)))
         acc)))
   {:read {} :write {}}
   columns))

(defn build-all-read-coercions
  "Build read coercions from column definitions.
   Returns a map from SQL column name (string) to coerce-fn.
   Includes both table-qualified ('table.column') and unqualified ('column') entries."
  [columns]
  (let [{:keys [read]} (build-coercions columns)]
    (into {}
          (mapcat (fn [[col-id coerce-fn]]
                    (let [tbl (col-table-name col-id)
                          col (col-col-name col-id)]
                      [[(str tbl "." col) coerce-fn]
                       [col coerce-fn]])))
          read)))

(defn build-enum-val->int
  "Build a map from namespaced enum keywords to their integer DB values."
  [columns]
  (into {}
        (mapcat (fn [col]
                  (when-let [enum-map (:enum-values col)]
                    (let [col-ns (str (namespace (:id col)) "." (name (:id col)))]
                      (map (fn [[idx kw]]
                             (when-not (and (keyword? kw)
                                            (= col-ns (namespace kw)))
                               (throw (ex-info (str "Enum values must be namespaced keywords "
                                                    "with namespace matching the column. "
                                                    "Expected namespace: " col-ns
                                                    ", got: " (pr-str kw))
                                               {:column (:id col) :value kw})))
                             [kw idx])
                           enum-map)))))
        columns))

;; ============================================================================
;; Namespaced Alias Support
;; ============================================================================

(defn- preprocess-select
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

(defn- smart-column-names
  "Compute column keywords from ResultSetMetaData with proper handling of
   namespaced aliases."
  [^ResultSetMetaData rsmeta]
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

(defn- smart-kebab-maps
  "Builder function that produces kebab-case keyword maps with proper handling
   of namespaced aliases."
  [^ResultSet rs _opts]
  (let [rsmeta (.getMetaData rs)
        cols (smart-column-names rsmeta)]
    (rs/->MapResultSetBuilder rs rsmeta cols)))

(defn- make-column-reader
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

(defn- coerce-params
  "Coerce SQL parameter values based on their types."
  [enum-val->int params]
  (mapv (fn [v]
          (cond
            (nil? v)     v
            (uuid? v)    (uuid->bytes v)
            (inst? v)    (inst->epoch-ms v)
            (boolean? v) (bool->int v)
            (keyword? v) (if-let [n (get enum-val->int v)]
                           n
                           (throw (ex-info "Unknown enum keyword value"
                                           {:value v
                                            :available (keys enum-val->int)})))
            (map? v)     (nippy/fast-freeze v)
            (vector? v)  (nippy/fast-freeze v)
            (set? v)     (nippy/fast-freeze v)
            :else        v))
        params))

;; ============================================================================
;; SQLite DDL Generation
;; ============================================================================

(defn- generate-column-def
  "Generate a single column definition from a column map."
  [col]
  (let [col-name (col-col-name (:id col))
        col-type (sqlite-type (:type col))
        enum-map (:enum-values col)
        check (when enum-map
                (str " CHECK (" col-name " IN ("
                     (str/join ", " (sort (keys enum-map)))
                     "))"))
        comment (when enum-map
                  (str " -- " (str/join ", " (map (fn [[k v]] (str (name v) " (" k ")"))
                                                  (sort-by key enum-map)))))]
    {:line (str col-name " " col-type
                (when (:primary-key col) " PRIMARY KEY")
                (when (:required col) " NOT NULL")
                check)
     :comment comment}))

(defn- generate-table-constraints
  "Generate FOREIGN KEY, UNIQUE, and other constraints for a table's columns."
  [table-cols]
  (let [fk-constraints
        (into []
              (keep (fn [col]
                      (when-let [ref (:ref col)]
                        (let [col-name (col-col-name (:id col))
                              ref-table (col-table-name ref)
                              ref-col (col-col-name ref)]
                          {:line (str "FOREIGN KEY(" col-name ") REFERENCES "
                                      ref-table "(" ref-col ")")}))))
              table-cols)
        unique-constraints
        (into []
              (keep (fn [col]
                      (when (:unique col)
                        {:line (str "UNIQUE(" (col-col-name (:id col)) ")")})))
              table-cols)
        unique-with-constraints
        (into []
              (keep (fn [col]
                      (when-let [others (:unique-with col)]
                        (let [all-cols (into [(:id col)] others)
                              col-names (str/join ", " (mapv col-col-name all-cols))]
                          {:line (str "UNIQUE(" col-names ")")}))))
              table-cols)]
    (concat fk-constraints unique-constraints unique-with-constraints)))

(defn- generate-indexes
  "Generate CREATE INDEX statements for columns with :index true."
  [table-cols]
  (into []
        (keep (fn [col]
                (when (:index col)
                  (let [tbl (col-table-name (:id col))
                        col-name (col-col-name (:id col))]
                    (str "CREATE INDEX idx_" tbl "_" col-name
                         " ON " tbl "(" col-name ");")))))
        table-cols))

(defn- generate-create-table
  "Generate a CREATE TABLE statement for a group of columns belonging to one table."
  [table-name table-cols]
  (let [col-defs (mapv generate-column-def table-cols)
        constraints (generate-table-constraints table-cols)
        lines (concat col-defs constraints)
        lines (into []
                     (map-indexed (fn [i {:keys [line] comment* :comment}]
                                    (str "  "
                                         line
                                         (when (not= (inc i) (count lines))
                                           ",")
                                         comment*)))
                     lines)]
    (str "CREATE TABLE " table-name " (\n"
         (str/join "\n" lines)
         "\n) STRICT;")))

(defn- topo-sort-tables
  "Topological sort of tables based on foreign key references."
  [grouped-cols]
  (let [tables (set (keys grouped-cols))
        deps (into {}
                   (map (fn [[table-key table-cols]]
                          [table-key
                           (->> table-cols
                                (keep :ref)
                                (map col-table)
                                (filter tables)
                                (remove #{table-key})
                                set)]))
                   grouped-cols)]
    (loop [sorted []
           remaining deps]
      (if (empty? remaining)
        sorted
        (let [ready (into [] (comp (filter #(empty? (val %)))
                                   (map key))
                          remaining)]
          (if (empty? ready)
            (into sorted (keys remaining))
            (recur (into sorted (sort ready))
                   (into {}
                         (comp (remove #(contains? (set ready) (key %)))
                               (map (fn [[k v]]
                                      [k (apply disj v ready)])))
                         remaining))))))))

(defn generate-schema-sql
  "Generate the complete schema SQL from column definitions.
   Returns CREATE TABLE statements, CREATE INDEX statements."
  [columns]
  (let [grouped (group-by (comp col-table :id) columns)
        table-order (topo-sort-tables grouped)
        create-tables (for [table-key table-order
                            :let [table-cols (get grouped table-key)]
                            :when table-cols]
                        (generate-create-table (sql-name (name table-key)) table-cols))
        all-indexes (mapcat #(generate-indexes (get grouped %)) table-order)]
    (str/join "\n\n"
              (concat create-tables all-indexes))))

;; ============================================================================
;; Validation
;; ============================================================================

(defn- default-schema
  "Get the default malli schema for a column type."
  [col]
  (case (:type col)
    :int     :int
    :real    :double
    :text    :string
    :boolean :boolean
    :inst    inst?
    :uuid    :uuid
    :edn     [:or map? set? vector? list?]
    :enum    (into [:enum] (vals (:enum-values col)))
    :string  :string))

(defn- validation-schema
  "Build the combined malli validation schema for a column."
  [col]
  (let [base (default-schema col)
        combined (if-let [user-schema (:schema col)]
                   [:and base user-schema]
                   base)]
    (if (:required col)
      combined
      [:maybe combined])))

(def ^:private schema-for
  "Memoized function to build validation schema for a column keyword given columns."
  (memoize
   (fn [columns col-key]
     (when-let [col (first (filter #(= col-key (:id %)) columns))]
       (validation-schema col)))))

(defn- literal-value?
  "Check if a value in a :set/:values map is a literal (should be validated).
   Returns false for maps and non-lift vectors."
  [v]
  (cond
    (and (vector? v) (= :lift (first v))) true
    (map? v) false
    (vector? v) false
    :else true))

(defn- extract-literal
  "Extract the actual value from a possibly [:lift x] form."
  [v]
  (if (and (vector? v) (= :lift (first v)))
    (second v)
    v))

(defn- validate-values!
  "Validate literal key-value pairs in a map against column schemas.
   Throws on validation failure."
  [columns kv-map]
  (doseq [[k v] kv-map
          :when (literal-value? v)]
    (when-let [schema (schema-for columns k)]
      (let [actual-val (extract-literal v)]
        (when-not (malli/validate schema actual-val)
          (throw (ex-info (str "Validation failed for column " k
                               ": value " (pr-str actual-val)
                               " does not match schema " (pr-str schema))
                          {:column k
                           :value actual-val
                           :schema schema})))))))

(defn- validate-honeysql-input!
  "Validate INSERT/UPDATE HoneySQL input against column schemas."
  [columns input]
  (when (map? input)
    (when-let [set-map (:set input)]
      (validate-values! columns set-map))
    (when-let [values (:values input)]
      (doseq [row values]
        (validate-values! columns row)))))

;; ============================================================================
;; Connection Pool & Queries
;; ============================================================================

(defn start-pool
  "Start a HikariCP connection pool for SQLite at db-path.
   Returns the HikariDataSource."
  [db-path]
  (io/make-parents db-path)
  (HikariDataSource.
   (doto (HikariConfig.)
     (.setJdbcUrl (str "jdbc:sqlite:" db-path))
     (.setMaximumPoolSize 1)
     (.setConnectionInitSql
      (str/join ";" ["PRAGMA journal_mode=WAL"
                     "PRAGMA busy_timeout = 5000"
                     "PRAGMA foreign_keys = ON"
                     "PRAGMA synchronous = NORMAL"])))))

(defn apply-schema!
  "Generate schema SQL from column definitions, write to schema-path,
   and run sqlite3def to apply migrations."
  [db-path schema-path columns extra-sql sqlite3def-path]
  (io/make-parents db-path)
  (io/make-parents schema-path)
  (let [schema-sql (generate-schema-sql columns)
        full-sql (str "-- Auto-generated; do not edit.\n\n"
                      schema-sql
                      (when (seq extra-sql)
                        (str "\n\n" (str/join "\n" extra-sql))))
        _ (spit schema-path full-sql)
        result (process/exec sqlite3def-path db-path "--apply" "-f" schema-path)]
    (when (not-empty result)
      (log/info result))))

(def ^:private memoized-coercions
  "Memoized function that constructs read-coercions and enum-val->int from columns."
  (memoize (fn [columns]
             (let [read-coercions (build-all-read-coercions columns)]
               {:read-coercions read-coercions
                :enum-val->int (build-enum-val->int columns)}))))

(def ^:private write-lock (Object.))

(defn- write-statement? [sql-str]
  (let [trimmed (str/triml sql-str)]
    (boolean (re-find #"(?i)^(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER)\b" trimmed))))

(defn execute
  "Execute a SQL query/statement. Input can be either a HoneySQL map or a raw
   SQL vector. Returns results as qualified kebab-case maps with read coercions
   applied automatically. Write coercions are applied to parameters.

   ctx is the system map, from which :biff/conn and :biff.sqlite/columns are used.
   Write statements are executed under a lock to avoid contention.

   For INSERT/UPDATE HoneySQL maps, literal values in :set and :values are
   validated against column schemas before execution."
  [ctx input]
  (let [{:biff/keys [conn]
         :biff.sqlite/keys [columns]} ctx
        columns (or columns [])
        {:keys [read-coercions enum-val->int]} (memoized-coercions columns)
        _ (validate-honeysql-input! columns input)
        input (if (and (map? input) (:select input))
                (update input :select preprocess-select)
                input)
        sql-vec (cond
                  (map? input) (hsql/format input)
                  (string? input) [input]
                  :else input)
        sql-vec (if enum-val->int
                  (into [(first sql-vec)] (coerce-params enum-val->int (rest sql-vec)))
                  sql-vec)
        column-reader (make-column-reader read-coercions)
        opts {:builder-fn (rs/builder-adapter smart-kebab-maps column-reader)}
        run #(jdbc/execute! conn sql-vec opts)]
    (if (write-statement? (first sql-vec))
      (locking write-lock (run))
      (run))))

;; ============================================================================
;; Component
;; ============================================================================

(defn use-sqlite
  "Biff component that runs schema migrations and starts a HikariCP connection pool.
   Adds :biff/conn to the system context.

   Looks for :biff.sqlite/columns (vector of column maps) and
   :biff.sqlite/extra-sql (vector of SQL strings) in the system map.

   If litestream config is present, automatically handles binary download, DB
   restore from S3 (if no local DB exists), and starts continuous replication.

   Both sqlite3def and litestream binaries are auto-installed if not found globally.

   Column format:
     :id           - (required) column keyword, namespace = table name
     :type         - (required) :int, :real, :text, :boolean, :inst, :uuid, :enum, :edn
     :primary-key  - boolean, adds PRIMARY KEY
     :unique       - boolean, adds UNIQUE constraint
     :unique-with  - vector of column keywords, adds compound UNIQUE constraint
     :required     - boolean, adds NOT NULL
     :ref          - column keyword, adds FOREIGN KEY
     :index        - boolean, adds CREATE INDEX
     :schema       - malli schema for validation
     :enum-values  - map of int -> keyword (required for :enum type)"
  [{:biff.sqlite/keys [db-path columns extra-sql sqlite3def-version]
    :or {db-path "storage/sqlite/main.db"}
    :as ctx}]
  (let [ctx (litestream/use-litestream ctx)
        sqlite3def-path (sqlite3def/resolve-bin!
                         (or sqlite3def-version sqlite3def/default-version))
        _ (apply-schema! db-path "resources/schema.sql"
                         (or columns []) (or extra-sql []) sqlite3def-path)
        datasource (start-pool db-path)]
    (log/info "SQLite connection pool started at" db-path)
    (-> ctx
        (assoc :biff/conn datasource)
        (update :biff/stop conj #(.close datasource)))))
