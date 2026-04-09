(ns com.biffweb.sqlite
  "SQLite integration for Biff: connection pooling, schema migrations, and query execution.

   Public API:
   - `use-sqlite`          — Biff component for schema migrations, connection pooling, and litestream replication.
   - `execute`             — Execute SQL queries/statements with automatic type coercion and validation.
   - `authorized-write`    — Execute INSERT/UPDATE/DELETE with authorization checks.
   - `use-litestream`      — Biff component for litestream replication (called by use-sqlite automatically).
   - `generate-schema-sql` — Generate the complete schema SQL string from column definitions.
   - `make-resolvers`      — Create biff.inject resolvers for SQLite tables from column definitions."
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [com.biffweb.sqlite.impl.authorize :as authorize]
   [com.biffweb.sqlite.impl.coerce :as coerce]
   [com.biffweb.sqlite.impl.litestream :as litestream]
   [com.biffweb.sqlite.impl.pool :as pool]
   [com.biffweb.sqlite.impl.query :as query]
   [com.biffweb.sqlite.impl.schema :as schema]
   [com.biffweb.sqlite.impl.sqlite3def :as sqlite3def]
   [com.biffweb.sqlite.impl.util :as util]
   [com.biffweb.sqlite.impl.validate :as validate]
   [honey.sql :as hsql]
   [next.jdbc :as jdbc]))

(def ^:private write-lock (Object.))

(defn generate-schema-sql
  "Generate the complete schema SQL string from column definitions.
   Takes a map with :biff.sqlite/columns (map of column-id → column-props)
   and optional :biff.sqlite/extra-sql (vector of SQL strings).
   Returns the full SQL string."
  [{:biff.sqlite/keys [columns extra-sql]}]
  (schema/generate-schema-sql (util/normalize-columns (or columns {}))
                              (or extra-sql [])))

(defn execute
  "Execute a SQL query/statement. Input can be a HoneySQL map, a raw SQL string,
   or a JDBC-style [sql & params] vector.

   Returns results as qualified kebab-case maps with read coercions applied
   automatically. Write coercions are applied to parameters.

   ctx is the system map, from which `:biff.sqlite/read-pool`,
   `:biff.sqlite/write-conn`, and `:biff.sqlite/columns` are used.
   Write statements are executed on write-conn under a lock.

   For INSERT/UPDATE HoneySQL maps, literal values in `:set` and `:values` are
   validated against column schemas before execution."
  [ctx input]
  (let [{:biff.sqlite/keys [columns read-pool write-conn]} ctx
        columns (or columns {})
        {:keys [builder-fn enum-val->int normalized-columns]} (coerce/memoized-coercions columns)
        _ (validate/validate-honeysql-input! normalized-columns input)
        input (if (and (map? input) (:select input))
                (update input :select query/preprocess-select)
                input)
        sql-vec (cond
                  (map? input) (hsql/format input)
                  (string? input) [input]
                  :else input)
        sql-vec (into [(first sql-vec)] (coerce/coerce-params enum-val->int (rest sql-vec)))
        opts {:builder-fn builder-fn}]
    (if (util/write-statement? (first sql-vec))
      (locking write-lock (jdbc/execute! write-conn sql-vec opts))
      (jdbc/execute! read-pool sql-vec opts))))

(defn authorized-write
  "Execute an INSERT, UPDATE, or DELETE statement with authorization checks.

   Like `execute`, but:
   1. Only accepts HoneySQL maps for INSERT, UPDATE, or DELETE.
   2. Generates a diff describing the changes (vector of maps with :table, :op,
      :before, :after).
   3. Calls the `:biff.sqlite/authorize` function from ctx with `(authorize ctx diff)`.
   4. If authorize returns falsy, aborts the transaction and throws an exception.
   5. INSERT statements with :on-conflict are rejected (throw an exception).

   Uses a separate read transaction on `:biff.sqlite/read-pool` to get a consistent
   snapshot of the database before the write. For UPDATE statements, the read
   transaction is queried for before-values using primary keys from the UPDATE's
   RETURNING clause.

   The diff is a vector of maps:
     {:table  :user        ; table keyword
      :op     :create      ; :create, :update, or :delete
      :before {...}        ; nil for :create
      :after  {...}}       ; nil for :delete

   Returns the diff vector on success."
  [ctx input]
  (when-not (:biff.sqlite/authorize ctx)
    (throw (ex-info "authorized-write requires :biff.sqlite/authorize in ctx."
                    {})))
  (locking write-lock
    (authorize/authorized-write! ctx input)))

(defn use-litestream
  "Biff component for litestream replication. Downloads the litestream binary
   automatically and starts continuous replication to S3.

   This is called by `use-sqlite` already — it's only exposed in case you want
   to use litestream replication without the full `use-sqlite` component."
  [ctx]
  (litestream/use-litestream ctx))

(defn use-sqlite
  "Biff component that runs schema migrations and starts a connection pool.
   Adds `:biff.sqlite/read-pool` (HikariCP pool) and `:biff.sqlite/write-conn`
   (single long-lived connection) to the system context.

   Looks for `:biff.sqlite/columns` (map of column-id → column-props) and
   `:biff.sqlite/extra-sql` (vector of SQL strings) in the system map.

   If litestream config is present, automatically handles binary download, DB
   restore from S3 (if no local DB exists), and starts continuous replication.

   Both sqlite3def and litestream binaries are auto-installed if not found globally.

   Column format (map of qualified keyword → property map):
     {:user/id {:type :uuid :primary-key true}
      :user/email {:type :text :unique true :required true}}

   Column properties:
     :type         - (required) :int, :real, :text, :boolean, :inst, :uuid, :enum, :edn
     :primary-key  - boolean, adds PRIMARY KEY (implies :required)
     :unique       - boolean, adds UNIQUE constraint
     :unique-with  - vector of column keywords, adds compound UNIQUE constraint
     :required     - boolean, adds NOT NULL
     :ref          - column keyword, adds FOREIGN KEY
     :index        - boolean, adds CREATE INDEX
     :schema       - malli schema for validation
     :enum-values  - map of int → keyword (required for :enum type)"
  [{:biff.sqlite/keys [db-path columns extra-sql sqlite3def-version]
    :or {db-path "storage/sqlite/main.db"}
    :as ctx}]
  (let [ctx (litestream/use-litestream ctx)
        cols (util/normalize-columns (or columns {}))
        sqlite3def-path (sqlite3def/resolve-bin!
                         (or sqlite3def-version sqlite3def/default-version))
        _ (schema/apply-schema! db-path "resources/schema.sql"
                                cols (or extra-sql []) sqlite3def-path)
        read-pool (pool/start-read-pool db-path)
        write-conn (pool/start-write-conn db-path)]
    (log/info "SQLite connection pool started at" db-path)
    (-> ctx
        (assoc :biff.sqlite/read-pool read-pool
               :biff.sqlite/write-conn write-conn)
        (update :biff/stop conj (fn []
                                  (.close write-conn)
                                  (.close read-pool))))))

(defn- strip-id-suffix
  "Remove -id suffix from a keyword name: :post/author-id -> :post/author"
  [k]
  (keyword (namespace k) (subs (name k) 0 (- (count (name k)) 3))))

(defn make-resolvers
  "Create biff.inject resolvers for SQLite tables from column definitions.

   Takes a map with :biff.sqlite/columns (map of column-keyword → property-map).
   Returns a vector of resolver maps compatible with com.biffweb.inject/build-index.

   Each resolver:
   - Takes the table's primary key as input
   - Returns all other columns as output (batch resolver)
   - For ref columns ending in -id, adds a join key without the -id suffix
     e.g. :post/author-id #uuid \"...\" → :post/author {:user/id #uuid \"...\"}"
  [{:biff.sqlite/keys [columns]}]
  (let [columns (or columns {})
        by-table (group-by (comp keyword namespace key) columns)]
    (vec
     (for [[table-key table-cols] by-table
           :let [table-cols-map (into {} table-cols)
                 pk-entry (first (filter (fn [[_ props]] (:primary-key props)) table-cols-map))
                 _ (when-not pk-entry
                     (throw (ex-info (str "No primary key found for table " table-key)
                                     {:table table-key})))
                 pk-key (key pk-entry)
                 non-pk-cols (dissoc table-cols-map pk-key)
                 ;; Ref columns whose name ends in -id: {col-key ref-target-key}
                 ref-cols (into {}
                                (keep (fn [[col-key props]]
                                        (when (and (:ref props)
                                                   (str/ends-with? (name col-key) "-id"))
                                          [col-key (:ref props)])))
                                non-pk-cols)
                 ;; Precomputed join mappings: [{:join-key, :col-key, :ref-key}]
                 join-mappings (mapv (fn [[col-key ref-key]]
                                      {:join-key (strip-id-suffix col-key)
                                       :col-key col-key
                                       :ref-key ref-key})
                                    ref-cols)
                 output (vec (concat (keys non-pk-cols) (map :join-key join-mappings)))
                 resolver-id (keyword "com.biffweb.sqlite"
                                      (str (name table-key) "-resolver"))]]
       {:id resolver-id
        :input [pk-key]
        :output output
        :batch true
        :resolve (fn [ctx inputs]
                   (let [ids (mapv pk-key inputs)
                         results (execute ctx {:select :*
                                              :from table-key
                                              :where [:in :id ids]})
                         process-row (fn [row]
                                       (let [row (reduce
                                                  (fn [row {:keys [join-key col-key ref-key]}]
                                                    (let [ref-val (get row col-key)]
                                                      (cond-> row
                                                        (some? ref-val) (assoc join-key {ref-key ref-val}))))
                                                  row
                                                  join-mappings)]
                                         (into {} (filter (fn [[_ v]] (some? v))) row)))
                         results (mapv process-row results)
                         id->result (into {} (map (juxt pk-key identity)) results)]
                     (mapv (fn [input]
                             (get id->result (get input pk-key) {}))
                           inputs)))}))))
