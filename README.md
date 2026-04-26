# biff.sqlite

NOTE: not officially released yet. AI-generated. See https://biffweb.com/p/biff2/

A SQLite integration library for [Biff](https://biffweb.com). Provides connection pooling, declarative schema migrations, automatic type coercion, input validation, a built-in key/value store, [Litestream](https://litestream.io/) replication, and [sqlite3def](https://github.com/sqldef/sqldef) migration support.

## Installation

Add to your `deps.edn`:

```clojure
io.github.jacobobryant/biff.sqlite {:git/tag "..." :git/sha "..."}
```

## Quick Start

```clojure
(require '[com.biffweb.sqlite :as biff.sqlite])

;; Define your columns as a map of column-id → properties
(def columns
  {:user/id        {:type :uuid :primary-key true}
   :user/email     {:type :text :unique true :required true}
   :user/name      {:type :text :required true}
   :user/joined-at {:type :inst :required true}
   :user/active    {:type :boolean}
   :user/role      {:type :enum :required true
                    :enum-values {0 :user.role/member
                                  1 :user.role/admin}}})

;; Use as a Biff component
(defn start-system [ctx]
  (biff.sqlite/use-sqlite
    (assoc ctx
      :biff.sqlite/columns columns
      :biff.sqlite/db-path "storage/sqlite/main.db")))

;; Execute queries (uses :biff.sqlite/read-pool for reads)
(biff.sqlite/execute ctx {:select :* :from :user})
;; => [{:user/id #uuid "...", :user/email "alice@example.com", ...}]

;; Execute writes (uses :biff.sqlite/write-conn, serialized under a lock)
(biff.sqlite/execute ctx {:insert-into :user
                          :values [{:user/id (java.util.UUID/randomUUID)
                                    :user/email "bob@example.com"
                                    :user/name "Bob"
                                    :user/joined-at (java.time.Instant/now)
                                    :user/active true
                                    :user/role :user.role/member}]})

;; Generic kv helpers are added to ctx by use-sqlite.
((:biff.kv/set-value ctx) ctx :demo/settings "theme" {:mode :dark})
((:biff.kv/get-value ctx) ctx :demo/settings "theme")
;; => {:mode :dark}
```

## Public API

### `use-sqlite`

Biff component that runs schema migrations (via sqlite3def) and starts connections. Adds `:biff.sqlite/read-pool` (HikariCP pool for reads) and `:biff.sqlite/write-conn` (single long-lived connection for writes) to the system context.

**System map keys:**

| Key | Description | Default |
|-----|-------------|---------|
| `:biff.sqlite/db-path` | Path to SQLite database file | `"storage/sqlite/main.db"` |
| `:biff.sqlite/columns` | Map of column-id → property map | `{}` |
| `:biff.sqlite/extra-sql` | Vector of extra SQL strings to append to schema | `[]` |
| `:biff.sqlite/sqlite3def-version` | sqlite3def version to auto-install | `"3.10.1"` |
| `:biff.sqlite/litestream-*` | Litestream S3 replication config (see below) | — |

### `execute`

Execute a SQL query or statement. Accepts:

- A **HoneySQL map**: `{:select :* :from :user :where [:= :user/id "u1"]}`
- A **raw SQL string**: `"SELECT * FROM user"`
- A **JDBC vector**: `["SELECT * FROM user WHERE id = ?" "u1"]`

Returns results as qualified kebab-case keyword maps with automatic type coercion. Write statements are executed on the write connection under a lock; reads go through the connection pool.

### `use-litestream`

Biff component for litestream replication. This is called by `use-sqlite` automatically — it's only exposed in case you want to use litestream replication without the full `use-sqlite` component.

### `generate-schema-sql`

Generate the complete schema SQL string from column definitions. Takes a map with `:biff.sqlite/columns` and optional `:biff.sqlite/extra-sql`. Returns the full SQL string including the auto-generated header.

```clojure
(biff.sqlite/generate-schema-sql
  {:biff.sqlite/columns columns
   :biff.sqlite/extra-sql ["CREATE INDEX custom_idx ON user(email);"]})
```

## Column Definitions

Columns are defined as a map from qualified keywords to property maps. The keyword namespace is the table name and the keyword name is the column name.

```clojure
{:user/id    {:type :uuid :primary-key true}
 :user/email {:type :text :unique true :required true}
 :user/name  {:type :text :required true}}
```

**Column properties:**

| Key | Type | Description |
|-----|------|-------------|
| `:type` | keyword | **(required)** One of: `:int`, `:real`, `:text`, `:boolean`, `:inst`, `:uuid`, `:enum`, `:edn`, `:blob` |
| `:primary-key` | boolean | Adds `PRIMARY KEY` constraint (implies `:required`) |
| `:required` | boolean | Adds `NOT NULL` constraint |
| `:unique` | boolean | Adds `UNIQUE` constraint |
| `:unique-with` | vector | Compound `UNIQUE` constraint with other columns |
| `:ref` | keyword | Foreign key reference to another column |
| `:index` | boolean | Creates an index on this column |
| `:schema` | malli schema | Custom validation schema |
| `:enum-values` | map | Map of `int → keyword` for `:enum` type columns |

## Type Coercion

Values are automatically coerced between Clojure and SQLite:

| Column Type | Clojure Type | SQLite Type |
|-------------|-------------|-------------|
| `:uuid` | `java.util.UUID` | `BLOB` (16 bytes) |
| `:inst` | `java.time.Instant` | `INT` (epoch millis) |
| `:boolean` | `true`/`false` | `INT` (0/1) |
| `:enum` | namespaced keyword | `INT` |
| `:edn` | map, vector, list, or set | `BLOB` (nippy) |
| `:blob` | byte array | `BLOB` |
| `:text` | `String` | `TEXT` |
| `:int` | `long` | `INT` |
| `:real` | `double` | `REAL` |

## Built-in KV Store

`use-sqlite` automatically adds a `biff_sqlite_kv` table and puts these
functions on the returned ctx:

| Key | Signature | Description |
|-----|-----------|-------------|
| `:biff.kv/set-value` | `(fn [ctx namespace key value])` | Upserts a nippy-encoded value |
| `:biff.kv/get-value` | `(fn [ctx namespace key])` | Returns a thawed value or `nil` |

`namespace` must be a keyword and `key` must be a string.

## Litestream Replication

To enable continuous S3 replication, add these keys to your system map:

| Key | Description |
|-----|-------------|
| `:biff.sqlite/litestream-bucket` | S3 bucket name |
| `:biff.sqlite/litestream-access-key-id` | AWS access key ID |
| `:biff.sqlite/litestream-secret-access-key` | AWS secret access key (or 0-arg fn) |
| `:biff.sqlite/litestream-endpoint` | S3 endpoint URL (optional) |
| `:biff.sqlite/litestream-region` | AWS region (optional) |
| `:biff.sqlite/litestream-path` | Replica path prefix (optional) |
| `:biff.sqlite/litestream-version` | Litestream version (default: `"0.5.9"`) |

## API Documentation

Full API documentation is available at: https://jacobobryant.github.io/biff.sqlite/

## License

MIT
