(ns com.biffweb.sqlite
  "SQLite utilities: schema generation from malli, type coercion, connection pooling.
   Adapted from biff.next/resources/sqlite.clj."
  (:require
   [clojure.java.io :as io]
   [clojure.java.process :as process]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [com.biffweb.sqlite.inference :as inference]
   [com.biffweb.sqlite.litestream :as litestream]
   [com.biffweb.sqlite.sqlite3def :as sqlite3def]
   [honey.sql :as hsql]
   [malli.core :as malli]
   [malli.registry :as malr]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [taoensso.nippy :as nippy])
  (:import
   [com.zaxxer.hikari HikariConfig HikariDataSource]
   [java.nio ByteBuffer]
   [java.sql ResultSet]
   [java.time Instant]
   [java.util UUID]))

;; ============================================================================
;; Schema Info Extraction
;; ============================================================================

(defn- table-id-key
  "Get the ID key for a table (e.g., :user -> :user/id)"
  [table-key]
  (keyword (name table-key) "id"))

(defn- table-ast?
  "Check if an AST represents a table (has an id column)."
  [table-key ast]
  (and (= :map (:type ast))
       (contains? (:keys ast) (table-id-key table-key))))

(defn- deref-ast
  "Dereference a schema and get its AST."
  [schema malli-opts]
  (some-> (try (malli/deref-recursive schema malli-opts) (catch Exception _))
          malli/ast))

(defn- table-asts
  "Get all table ASTs from a schema."
  [table-key malli-opts]
  (when-let [ast (deref-ast table-key malli-opts)]
    (if (table-ast? table-key ast)
      [ast]
      (->> (tree-seq (constantly true) :children ast)
           (filterv #(table-ast? table-key %))))))

(defn- attr-union [m1 m2]
  (let [shared-keys (into [] (filter #(contains? m2 %)) (keys m1))]
    (when-some [conflicting-attr (first (filter #(not= (m1 %) (m2 %)) shared-keys))]
      (throw (ex-info "An attribute has a conflicting definition"
                      {:attr conflicting-attr
                       :definition-1 (m1 conflicting-attr)
                       :definition-2 (m2 conflicting-attr)})))
    (merge m1 m2)))

(defn- table-props
  "Extract map-level properties (e.g. :biff/unique) from the first table AST."
  [table-key malli-opts]
  (some-> (table-asts table-key malli-opts)
          first
          :properties))

(defn schema-info
  "Extract schema info: map of table-key -> attrs map."
  [malli-opts]
  (into {}
        (keep (fn [schema-k]
                (let [attrs (->> (table-asts schema-k malli-opts)
                                 (mapv :keys)
                                 (reduce attr-union {}))]
                  (when (not-empty attrs)
                    [schema-k attrs]))))
        (keys (malr/schemas (:registry malli-opts)))))

;; ============================================================================
;; Type Inference from Malli
;; ============================================================================

(defn- get-schema-type
  "Get the type from an attribute AST, handling nested value structures."
  [ast]
  (or (:type ast)
      (get-in ast [:value :type])))

(defn- infer-sqlite-type
  "Infer SQLite type from malli AST. Throws if type cannot be determined."
  [attr-key ast]
  (let [type-val (get-schema-type ast)]
    (case type-val
      :uuid "BLOB"
      :string "TEXT"
      :int "INT"
      :double "REAL"
      :boolean "INT"
      :set "BLOB"
      :vector "BLOB"
      :map "BLOB"
      :enum "INT"
      inst? "INT"
      (throw (ex-info (str "Cannot infer SQLite type for attribute " attr-key
                           ". Please use a supported malli type: :uuid, :string, :int, :double, "
                           ":boolean, :set, :vector, :map, :enum, or inst?")
                      {:attr attr-key :malli-type type-val})))))

(defn- get-value-ast
  "Get the nested value AST if present, or the AST itself."
  [ast]
  (or (:value ast) ast))

(defn- extract-enum-values
  "Extract enum values from a malli AST, returning map of {0 :val1, 1 :val2, ...}"
  [ast]
  (let [value-ast (get-value-ast ast)]
    (when (= :enum (:type value-ast))
      (into {} (map-indexed (fn [i v] [i v]) (:values value-ast))))))

(defn- infer-coercion-type
  "Infer the coercion type for an attribute from its malli AST."
  [ast]
  (let [type-val (get-schema-type ast)]
    (case type-val
      :uuid :uuid
      :boolean :bool
      :set :nippy
      :vector :nippy
      :map :nippy
      :enum {:enum (extract-enum-values ast)}
      inst? :inst
      nil)))

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

(defn- get-coerce-read-fn [coerce-type]
  (case coerce-type
    :uuid bytes->uuid
    :inst epoch-ms->inst
    :bool int->bool
    :nippy fast-thaw
    (when (map? coerce-type)
      (when-let [enum-map (:enum coerce-type)]
        (make-enum-reader enum-map)))))

(defn- get-coerce-write-fn [coerce-type]
  (case coerce-type
    :uuid uuid->bytes
    :inst inst->epoch-ms
    :bool bool->int
    :nippy fast-freeze
    (when (map? coerce-type)
      (when-let [enum-map (:enum coerce-type)]
        (make-enum-writer enum-map)))))

(defn build-coercions
  "Build coercion maps for a table's attributes.
   Returns {:read {attr coerce-fn} :write {attr coerce-fn}}"
  [attrs]
  (reduce
   (fn [acc [attr ast]]
     (let [coerce-type (infer-coercion-type ast)]
       (if coerce-type
         (let [read-fn (get-coerce-read-fn coerce-type)
               write-fn (get-coerce-write-fn coerce-type)]
           (cond-> acc
             read-fn (assoc-in [:read attr] read-fn)
             write-fn (assoc-in [:write attr] write-fn)))
         acc)))
   {:read {} :write {}}
   attrs))

(defn build-all-read-coercions
  "Build read coercions for all tables from schema-info.
   Returns a map from SQL column name (string) to coerce-fn.
   Includes both table-qualified ('table.column') and unqualified ('column')
   entries. Table-qualified entries are preferred during lookup to avoid
   collisions when multiple tables have columns with the same name but
   different types (e.g., enum columns)."
  [info]
  (into {}
        (mapcat (fn [[table-key attrs]]
                  (let [{:keys [read]} (build-coercions attrs)
                        table-name (str/replace (name table-key) "-" "_")]
                    (mapcat (fn [[attr coerce-fn]]
                              (let [col-name (str/replace (name attr) "-" "_")]
                                [[(str table-name "." col-name) coerce-fn]
                                 [col-name coerce-fn]]))
                            read))))
        info))

(defn build-enum-val->int
  "Build a map from namespaced enum keywords to their integer DB values.
   Enforces that enum values must be namespaced keywords with namespace
   matching the column (e.g. :user.favorite-color/blue for :user/favorite-color)."
  [info]
  (into {}
        (mapcat (fn [[_table-key attrs]]
                  (mapcat (fn [[attr ast]]
                            (when-let [enum-map (extract-enum-values ast)]
                              (let [col-ns (str (namespace attr) "." (name attr))]
                                (map (fn [[idx kw]]
                                       (when-not (and (keyword? kw)
                                                      (= col-ns (namespace kw)))
                                         (throw (ex-info (str "Enum values must be namespaced keywords "
                                                              "with namespace matching the column. "
                                                              "Expected namespace: " col-ns
                                                              ", got: " (pr-str kw))
                                                         {:attr attr :value kw})))
                                       [kw idx])
                                     enum-map))))
                          attrs)))
        info))

;; ============================================================================
;; Namespaced Alias Support
;; ============================================================================

(defn- keyword->sql-label
  "Convert a keyword to a SQL column label string, replacing hyphens with
   underscores. Namespaced keywords produce 'ns/name' format.
   :user/joined-at -> \"user/joined_at\"
   :cnt -> \"cnt\""
  [kw]
  (if (namespace kw)
    (str (str/replace (namespace kw) "-" "_")
         "/"
         (str/replace (name kw) "-" "_"))
    (str/replace (name kw) "-" "_")))

(defn- encode-namespaced-alias
  "Encode a namespaced keyword as a quoted string for use as a SQL alias.
   HoneySQL formats string aliases as quoted identifiers.
   :user/joined-at -> \"user/joined_at\""
  [kw]
  (keyword->sql-label kw))

(defn- preprocess-select
  "Walk a HoneySQL :select clause and encode namespaced keyword aliases
   as strings. In HoneySQL, a 2-element vector [expr alias] where alias
   is a namespaced keyword like :user/age would be formatted as
   'expr AS user.age' (broken). By converting the alias to a string like
   \"user/age\", HoneySQL formats it as 'expr AS \"user/age\"' (correct)."
  [select]
  (cond
    (keyword? select) select
    (vector? select)
    (mapv (fn [item]
            (if (and (vector? item)
                     (= 2 (count item))
                     (keyword? (second item))
                     (namespace (second item)))
              [(first item) (encode-namespaced-alias (second item))]
              item))
          select)
    :else select))

(defn- preprocess-honeysql
  "Pre-process a HoneySQL map:
   1. Encode namespaced keyword aliases in :select as quoted strings
   2. Extract :biff/column-types (not a HoneySQL key)
   Returns [processed-input column-types has-namespaced-aliases?]."
  [input]
  (let [column-types (:biff/column-types input)
        input (dissoc input :biff/column-types)
        select (:select input)
        has-ns-aliases? (and (vector? select)
                             (some (fn [item]
                                     (and (vector? item)
                                          (= 2 (count item))
                                          (keyword? (second item))
                                          (namespace (second item))))
                                   select))
        input (if has-ns-aliases?
                (assoc input :select (preprocess-select select))
                input)]
    [input column-types (boolean has-ns-aliases?)]))

(defn- build-explicit-coercions
  "Build a map from SQL column label (string) to coerce-fn from explicit
   :biff/column-types. Keys are keywords, values are coercion types
   (:uuid, :inst, :bool, :nippy, or {:enum {...}})."
  [column-types]
  (when column-types
    (into {}
          (keep (fn [[kw coerce-type]]
                  (when-let [coerce-fn (get-coerce-read-fn coerce-type)]
                    [(keyword->sql-label kw) coerce-fn])))
          column-types)))

(defn- fix-namespaced-alias-keys
  "Fix result map keys that have double-qualified namespaces from JDBC.
   When SQLite provides a table name for an aliased column whose label
   contains '/', as-kebab-maps creates keys like :user/item/created-at
   (namespace='user', name='item/created-at'). This fixes them to
   :item/created-at by splitting the name on '/'."
  [m]
  (reduce-kv
    (fn [acc k v]
      (let [n (name k)]
        (if-let [slash-idx (str/index-of n "/")]
          (assoc acc (keyword (subs n 0 slash-idx) (subs n (inc slash-idx))) v)
          (assoc acc k v))))
    {}
    m))

(defn- make-column-reader
  "Create a column reader fn for rs/builder-adapter that applies read coercions.
   read-coercions is a map from SQL column name (string) to coerce-fn.
   inferred-columns is an optional vector of inferred column maps from
   inference/infer-columns, used as a fallback when column name is not found
   in read-coercions.
   explicit-coercions is an optional map from SQL column label to coerce-fn
   from :biff/column-types.
   Prefers table-qualified lookups ('table.column') to avoid collisions when
   multiple tables have columns with the same unqualified name."
  [read-coercions inferred-columns explicit-coercions]
  (fn [builder ^ResultSet rs ^Integer i]
    (let [meta (.getMetaData rs)
          col-name (.getColumnLabel meta i)
          table-name (.getTableName meta i)
          value (.getObject rs i)
          coerce-fn (or
                      ;; 1. Explicit column types (highest priority)
                      (when explicit-coercions
                        (get explicit-coercions col-name))
                      ;; 2. Table-qualified lookup from malli schema
                      (when (and table-name (not= table-name ""))
                        (get read-coercions (str table-name "." col-name)))
                      ;; 3. Unqualified lookup from malli schema
                      (get read-coercions col-name)
                      ;; 4. Namespaced alias: col-name contains "/"
                      (when-let [slash-idx (str/index-of col-name "/")]
                        (let [ns-part (subs col-name 0 slash-idx)
                              name-part (subs col-name (inc slash-idx))]
                          (or (get read-coercions (str ns-part "." name-part))
                              (get read-coercions name-part))))
                      ;; 5. Inference fallback
                      (when-let [{:keys [column]} (get inferred-columns (dec i))]
                        (when (and column (not= "*" column))
                          (get read-coercions column))))
          coerced-value (if (and coerce-fn (some? value))
                          (coerce-fn value)
                          value)]
      (rs/read-column-by-index coerced-value (:rsmeta builder) i))))

(defn- coerce-params
  "Coerce SQL parameter values based on their types.
   - UUID → byte array
   - Instant → epoch milliseconds
   - Boolean → 0/1
   - Keyword → enum integer lookup (via enum-val->int map)
   - Map/Vector/Set → nippy serialization
   Throws if a keyword is not found in the enum map."
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

(defn- sql-name
  "Convert an attribute keyword to SQLite column name."
  [k]
  (str/replace (name k) "-" "_"))

(defn- generate-column-def
  "Generate a single column definition."
  [table-key attr ast]
  (let [col-name (sql-name attr)
        col-type (infer-sqlite-type attr ast)
        optional? (get-in ast [:properties :optional])
        id-key (table-id-key table-key)
        is-primary? (= attr id-key)
        enum-map (extract-enum-values ast)
        check-constraint (when enum-map
                           (str " CHECK (" col-name " IN ("
                                (str/join ", " (keys enum-map))
                                "))"))
        enum-comment (when enum-map
                       (str " -- " (str/join ", " (map (fn [[k v]] (str (name v) " (" k ")"))
                                                       (sort-by key enum-map)))))]
    {:line (str col-name " " col-type
                (when is-primary? " PRIMARY KEY")
                (when-not optional? " NOT NULL")
                check-constraint)
     :comment enum-comment}))

(defn- generate-foreign-keys
  "Generate FOREIGN KEY constraints for a table."
  [attrs]
  (into []
        (keep (fn [[attr ast]]
                (when-let [ref-target (get-in ast [:properties :biff/ref])]
                  (let [col-name (sql-name attr)]
                    {:line (str "FOREIGN KEY(" col-name ") REFERENCES "
                                (sql-name ref-target) "(id)")}))))
        attrs))

(defn- generate-unique-constraints
  "Generate UNIQUE constraints from :biff/unique table property.
   :biff/unique is a vector of vectors, where each inner vector is a list of
   column keywords that form a unique constraint."
  [table-props]
  (into []
        (map (fn [cols]
               (let [col-names (str/join ", " (mapv sql-name cols))]
                 {:line (str "UNIQUE(" col-names ")")})))
        (:biff/unique table-props)))

(defn- generate-create-table
  "Generate a CREATE TABLE statement for a table."
  [table-key attrs table-props]
  (let [col-defs (->> attrs
                      (sort-by (comp :order second))
                      (mapv (fn [[attr-key ast]]
                              (generate-column-def table-key attr-key ast))))
        fk-constraints (generate-foreign-keys attrs)
        unique-constraints (generate-unique-constraints table-props)
        lines (concat col-defs fk-constraints unique-constraints)
        lines (into []
                    (map-indexed (fn [i {:keys [line] comment* :comment}]
                                   (str "  "
                                        line
                                        (when (not= (inc i) (count lines))
                                          ",")
                                        comment*)))
                    lines)]
    (str "CREATE TABLE " (sql-name table-key) " (\n"
         (str/join "\n" lines)
         "\n) STRICT;")))

(defn- topo-sort
  "Simple topological sort for table ordering based on foreign key refs.
   Tables without dependencies come first."
  [info]
  (let [tables (set (keys info))
        deps (into {}
                   (map (fn [table-key]
                          [table-key
                           (->> (vals (get info table-key))
                                (keep #(get-in % [:properties :biff/ref]))
                                (map #(keyword (namespace %)))
                                (filter tables)
                                set)]))
                   tables)]
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
  "Generate the complete schema SQL from malli schema."
  [malli-opts]
  (let [info (schema-info malli-opts)
        table-order (topo-sort info)]
    (str/join "\n\n"
              (for [table table-order
                    :let [attrs (get info table)]
                    :when attrs]
                (generate-create-table table attrs (table-props table malli-opts))))))

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
  "Generate schema SQL from malli, concatenate with indexes, write to schema-path,
   and run sqlite3def to apply migrations."
  [db-path schema-path malli-opts indexes-sql sqlite3def-path]
  (io/make-parents db-path)
  (io/make-parents schema-path)
  (let [schema-sql (generate-schema-sql malli-opts)
        full-sql (str "-- Auto-generated; do not edit.\n\n"
                      schema-sql
                      (when (not-empty indexes-sql) (str "\n\n" indexes-sql)))
        _ (spit schema-path full-sql)
        result (process/exec sqlite3def-path db-path "--apply" "-f" schema-path)]
    (when (not-empty result)
      (log/info result))))

(def ^:private memoized-infer-columns
  "Memoized version of inference/infer-columns. Returns nil on parse failure
   instead of throwing."
  (memoize (fn [sql]
             (try
               (inference/infer-columns sql)
               (catch Exception _
                 nil)))))

(def ^:private memoized-coercions
  "Memoized function that constructs read-coercions and enum-val->int from malli-opts."
  (memoize (fn [malli-opts]
             (let [info (schema-info malli-opts)]
               {:read-coercions (build-all-read-coercions info)
                :enum-val->int (build-enum-val->int info)}))))

(def ^:private write-lock (Object.))

(defn- write-statement? [sql-str]
  (let [trimmed (str/triml sql-str)]
    (boolean (re-find #"(?i)^(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER)\b" trimmed))))

(defn execute
  "Execute a SQL query/statement. Input can be either a HoneySQL map or a raw
   SQL vector. Returns results as qualified kebab-case maps with read coercions
   applied automatically. Write coercions are applied to parameters.
   ctx is the system map, from which :biff/conn and :biff/malli-opts are used.
   Write statements are executed under a lock to avoid contention.

   HoneySQL maps support two additional keys:
   - :biff/column-types - map from result keyword to coercion type (:uuid, :inst,
     :bool, :nippy, or {:enum {...}}). Used for computed columns that don't exist
     in the malli schema.
   - Namespaced keyword aliases in :select are automatically encoded as quoted
     SQL identifiers, so {:select [[:joined-at :user/joined-at]]} works correctly."
  [ctx input]
  (let [{:biff/keys [conn malli-opts]} ctx
        {:keys [read-coercions enum-val->int]} (memoized-coercions malli-opts)
        [input column-types has-ns-aliases?]
        (if (map? input)
          (preprocess-honeysql input)
          [input nil false])
        sql-vec (cond
                  (map? input) (hsql/format input)
                  (string? input) [input]
                  :else input)
        sql-vec (if enum-val->int
                  (into [(first sql-vec)] (coerce-params enum-val->int (rest sql-vec)))
                  sql-vec)
        inferred-columns (memoized-infer-columns (first sql-vec))
        explicit-coercions (build-explicit-coercions column-types)
        column-reader (make-column-reader read-coercions inferred-columns explicit-coercions)
        opts {:builder-fn (rs/builder-adapter rs/as-kebab-maps column-reader)}
        run #(jdbc/execute! conn sql-vec opts)
        results (if (write-statement? (first sql-vec))
                  (locking write-lock (run))
                  (run))]
    (if has-ns-aliases?
      (mapv fix-namespaced-alias-keys results)
      results)))

;; ============================================================================
;; Component
;; ============================================================================

(defn use-sqlite
  "Biff component that runs schema migrations and starts a HikariCP connection pool.
   Adds :biff/conn to the system context.

   If litestream config is present, automatically handles binary download, DB
   restore from S3 (if no local DB exists), and starts continuous replication.

   Both sqlite3def and litestream binaries are auto-installed if not found globally.
   Use :biff.sqlite/sqlite3def-version and :biff.sqlite/litestream-version to
   override the default versions.

   Litestream config (all :biff.sqlite/litestream-* keys):
     :biff.sqlite/litestream-bucket           - S3 bucket name (required)
     :biff.sqlite/litestream-access-key-id    - AWS access key (required)
     :biff.sqlite/litestream-secret-access-key - fn that returns AWS secret key (required)
     :biff.sqlite/litestream-path             - Subdirectory within bucket (optional)
     :biff.sqlite/litestream-endpoint         - Custom S3 endpoint URL (optional)
     :biff.sqlite/litestream-region           - AWS region (optional)
     :biff.sqlite/litestream-version          - Litestream version to install (optional)

   sqlite3def config:
     :biff.sqlite/sqlite3def-version - sqlite3def version to install (optional)"
  [{:biff.sqlite/keys [db-path sqlite3def-version]
    :or {db-path "storage/sqlite/main.db"}
    :as ctx}]
  (let [ctx (litestream/use-litestream ctx)
        sqlite3def-path (sqlite3def/resolve-bin!
                         (or sqlite3def-version sqlite3def/default-version))
        indexes-sql (some-> (io/resource "indexes.sql") slurp)
        _ (apply-schema! db-path "resources/schema.sql"
                         (:biff/malli-opts ctx) indexes-sql sqlite3def-path)
        datasource (start-pool db-path)]
    (log/info "SQLite connection pool started at" db-path)
    (-> ctx
        (assoc :biff/conn datasource)
        (update :biff/stop conj #(.close datasource)))))
