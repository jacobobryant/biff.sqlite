(ns com.biffweb.sqlite
  "SQLite integration for Biff: connection pooling, schema migrations, and query execution.

   Public API:
   - `use-sqlite`  — Biff component for schema migrations, connection pooling, and litestream replication.
   - `execute`      — Execute SQL queries/statements with automatic type coercion and validation."
  (:require
   [clojure.java.io :as io]
   [clojure.java.process :as process]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [com.biffweb.sqlite.impl.coerce :as coerce]
   [com.biffweb.sqlite.impl.litestream :as litestream]
   [com.biffweb.sqlite.impl.query :as query]
   [com.biffweb.sqlite.impl.schema :as schema]
   [com.biffweb.sqlite.impl.sqlite3def :as sqlite3def]
   [com.biffweb.sqlite.impl.validate :as validate]
   [honey.sql :as hsql]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs])
  (:import
   [com.zaxxer.hikari HikariConfig HikariDataSource]))

(defn- start-pool
  "Start a HikariCP connection pool for SQLite at db-path."
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

(defn- apply-schema!
  "Generate schema SQL from column definitions, write to schema-path,
   and run sqlite3def to apply migrations."
  [db-path schema-path columns extra-sql sqlite3def-path]
  (io/make-parents db-path)
  (io/make-parents schema-path)
  (let [schema-sql (schema/generate-schema-sql columns)
        full-sql (str "-- Auto-generated; do not edit.\n\n"
                      schema-sql
                      (when (seq extra-sql)
                        (str "\n\n" (str/join "\n" extra-sql))))
        _ (spit schema-path full-sql)
        result (process/exec sqlite3def-path db-path "--apply" "-f" schema-path)]
    (when (not-empty result)
      (log/info result))))

(def ^:private memoized-coercions
  (memoize (fn [columns]
             {:read-coercions (coerce/build-all-read-coercions columns)
              :enum-val->int  (coerce/build-enum-val->int columns)})))

(def ^:private write-lock (Object.))

(defn- write-statement? [sql-str]
  (let [trimmed (str/triml sql-str)]
    (boolean (re-find #"(?i)^(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER)\b" trimmed))))

(defn execute
  "Execute a SQL query/statement. Input can be a HoneySQL map, a raw SQL string,
   or a JDBC-style [sql & params] vector.

   Returns results as qualified kebab-case maps with read coercions applied
   automatically. Write coercions are applied to parameters.

   ctx is the system map, from which `:biff/conn` and `:biff.sqlite/columns` are used.
   Write statements are executed under a lock to avoid contention.

   For INSERT/UPDATE HoneySQL maps, literal values in `:set` and `:values` are
   validated against column schemas before execution."
  [ctx input]
  (let [{:biff/keys [conn]
         :biff.sqlite/keys [columns]} ctx
        columns (or columns [])
        {:keys [read-coercions enum-val->int]} (memoized-coercions columns)
        _ (validate/validate-honeysql-input! columns input)
        input (if (and (map? input) (:select input))
                (update input :select query/preprocess-select)
                input)
        sql-vec (cond
                  (map? input) (hsql/format input)
                  (string? input) [input]
                  :else input)
        sql-vec (if enum-val->int
                  (into [(first sql-vec)] (coerce/coerce-params enum-val->int (rest sql-vec)))
                  sql-vec)
        column-reader (query/make-column-reader read-coercions)
        opts {:builder-fn (rs/builder-adapter query/smart-kebab-maps column-reader)}
        run #(jdbc/execute! conn sql-vec opts)]
    (if (write-statement? (first sql-vec))
      (locking write-lock (run))
      (run))))

(defn use-sqlite
  "Biff component that runs schema migrations and starts a HikariCP connection pool.
   Adds `:biff/conn` to the system context.

   Looks for `:biff.sqlite/columns` (vector of column maps) and
   `:biff.sqlite/extra-sql` (vector of SQL strings) in the system map.

   If litestream config is present, automatically handles binary download, DB
   restore from S3 (if no local DB exists), and starts continuous replication.

   Both sqlite3def and litestream binaries are auto-installed if not found globally.

   Column format:
     :id           - (required) column keyword, namespace = table name
     :type         - (required) :int, :real, :text, :boolean, :inst, :uuid, :enum, :edn, :string
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
