(ns com.biffweb.sqlite
  "SQLite integration for Biff: connection pooling, schema migrations, query execution,
   and a built-in key/value store.

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
   [com.biffweb.sqlite.impl.execute :as exec]
   [com.biffweb.sqlite.impl.litestream :as litestream]
   [com.biffweb.sqlite.impl.pool :as pool]
   [com.biffweb.sqlite.impl.schema :as schema]
   [com.biffweb.sqlite.impl.sqlite3def :as sqlite3def]
   [com.biffweb.sqlite.impl.util :as util]
   [taoensso.nippy :as nippy]))

(def ^:private kv-columns
  {:biff-sqlite-kv/namespace {:type :text
                              :primary-key-with [:biff-sqlite-kv/key]}
   :biff-sqlite-kv/key {:type :text}
   :biff-sqlite-kv/value {:type :blob :required true}})

(defn- validate-kv-args [namespace key]
  (assert (qualified-keyword? namespace) "namespace must be a qualified keyword")
  (assert (string? key) "key must be a string"))

(defn- set-kv-value [ctx namespace key value]
  (validate-kv-args namespace key)
  (if (nil? value)
    (do
      (exec/execute ctx ["DELETE FROM biff_sqlite_kv WHERE namespace = ? AND key_ = ?"
                         (str namespace)
                         key])
      nil)
    (let [value* (nippy/fast-freeze value)]
      (exec/execute ctx [(str "INSERT INTO biff_sqlite_kv (namespace, key_, value_) VALUES (?, ?, ?) "
                              "ON CONFLICT(namespace, key_) DO UPDATE SET value_ = excluded.value_")
                         (str namespace)
                         key
                         value*])
      nil)))

(defn- get-kv-value [ctx namespace key]
  (validate-kv-args namespace key)
  (some-> (first (exec/execute ctx [(str "SELECT value_ AS \"biff_sqlite_kv/value\" "
                                         "FROM biff_sqlite_kv WHERE namespace = ? AND key_ = ?")
                                    (str namespace)
                                    key]))
          :biff-sqlite-kv/value
          nippy/fast-thaw))

(defn generate-schema-sql  "Generate the complete schema SQL string from column definitions.
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
  (exec/execute ctx input))

(defn authorized-write
  "Execute an INSERT, UPDATE, or DELETE statement with authorization checks.

   Like `execute`, but:
   1. Only accepts HoneySQL maps for INSERT, UPDATE, DELETE, or INSERT...ON CONFLICT.
   2. Generates a diff describing the changes (vector of maps with :table, :op,
      :before, :after).
   3. Calls the `:biff.sqlite/authorize` function from ctx with `(authorize ctx diff)`.
      The ctx passed to authorize includes `:biff.sqlite/before-conn` (a read
      transaction opened before the write) and `:biff.sqlite/after-conn` (the write
      transaction connection, which can see uncommitted changes). The authorize
      function can query these via `(execute (set/rename-keys ctx
      {:biff.sqlite/before-conn :biff.sqlite/read-pool}) ...)`.
   4. If authorize returns falsy, aborts the transaction and throws an exception.

   Uses a separate read transaction on `:biff.sqlite/read-pool` to get a consistent
   snapshot of the database before the write. For UPDATE and upsert statements, the
   write is executed first with RETURNING *, then the read transaction is queried for
   the before-values using the returned primary keys.

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
  (locking exec/write-lock
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
     :type         - (required) :int, :real, :text, :boolean, :inst, :uuid, :enum, :edn, :blob
     :primary-key  - boolean, adds PRIMARY KEY (implies :required)
     :unique       - boolean, adds UNIQUE constraint
     :unique-with  - vector of column keywords, adds compound UNIQUE constraint
     :primary-key-with - vector of column keywords, adds compound PRIMARY KEY constraint
     :required     - boolean, adds NOT NULL
     :ref          - column keyword, adds FOREIGN KEY
     :index        - boolean, adds CREATE INDEX
     :schema       - malli schema for validation
     :enum-values  - map of int → keyword (required for :enum type)"
  [{:biff.sqlite/keys [db-path columns extra-sql sqlite3def-version]
     :or {db-path "storage/sqlite/main.db"}
     :as ctx}]
  (let [ctx (litestream/use-litestream ctx)
        columns (merge (or columns {}) kv-columns)
        cols (util/normalize-columns columns)
        sqlite3def-path (sqlite3def/resolve-bin!
                         (or sqlite3def-version sqlite3def/default-version))
        _ (schema/apply-schema! db-path "resources/schema.sql"
                                cols (or extra-sql []) sqlite3def-path)
        read-pool (pool/start-read-pool db-path)
        write-conn (pool/start-write-conn db-path)]
    (log/info "SQLite connection pool started at" db-path)
    (-> ctx
        (assoc :biff.sqlite/columns columns
               :biff.sqlite/read-pool read-pool
               :biff.sqlite/write-conn write-conn
               :biff.kv/get-value get-kv-value
               :biff.kv/set-value set-kv-value)
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
   - Takes the table's primary key column(s) as input
   - Returns all other columns as output (batch resolver)
   - For ref columns ending in -id, adds a join key without the -id suffix
     e.g. :post/author-id #uuid \"...\" → :post/author {:user/id #uuid \"...\"}"
  [{:biff.sqlite/keys [columns]}]
  (let [columns (or columns {})
        by-table (group-by (comp keyword namespace key) columns)]
    (vec
     (for [[table-key table-cols] by-table
           :let [table-cols-map (into {} table-cols)
                  pk-keys (or (seq (keep (fn [[col-key props]]
                                           (when (:primary-key props)
                                             col-key))
                                         table-cols-map))
                              (some (fn [[col-key props]]
                                      (when-let [others (:primary-key-with props)]
                                        (into [col-key] others)))
                                    table-cols-map))
                  _ (when-not pk-keys
                      (throw (ex-info (str "No primary key found for table " table-key)
                                      {:table table-key})))
                  pk-keys (vec pk-keys)
                  non-pk-cols (apply dissoc table-cols-map pk-keys)
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
                  row->pk (fn [row]
                            (if (= 1 (count pk-keys))
                              (get row (first pk-keys))
                              (mapv row pk-keys)))
                  resolver-id (keyword "com.biffweb.sqlite"
                                       (str (name table-key) "-resolver"))]]
        {:id resolver-id
         :input pk-keys
         :output output
         :batch true
         :resolve (fn [ctx inputs]
                    (let [where-clause (when (seq inputs)
                                         (if (= 1 (count pk-keys))
                                           [:in (first pk-keys) (mapv #(get % (first pk-keys)) inputs)]
                                           (into [:or]
                                                 (map (fn [input]
                                                        (into [:and]
                                                              (map (fn [pk-key]
                                                                     [:= pk-key (get input pk-key)])
                                                                   pk-keys))))
                                                 inputs)))
                          results (if where-clause
                                    (execute ctx {:select :*
                                                  :from table-key
                                                  :where where-clause})
                                    [])
                          process-row (fn [row]
                                        (let [row (apply dissoc row pk-keys)
                                              row (reduce
                                                   (fn [row {:keys [join-key col-key ref-key]}]
                                                     (let [ref-val (get row col-key)]
                                                       (cond-> row
                                                         (some? ref-val) (assoc join-key {ref-key ref-val}))))
                                                   row
                                                   join-mappings)]
                                          (into {} (filter (fn [[_ v]] (some? v))) row)))
                          id->result (into {} (map (fn [row]
                                                     [(row->pk row) (process-row row)]))
                                            results)]
                      (mapv (fn [input]
                              (get id->result (row->pk input) {}))
                            inputs)))}))))
