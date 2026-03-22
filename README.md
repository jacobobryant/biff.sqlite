# biff.sqlite

A SQLite integration library for [Biff](https://biffweb.com). Provides connection pooling, declarative schema migrations, automatic type coercion, input validation, [Litestream](https://litestream.io/) replication, and [sqlite3def](https://github.com/sqldef/sqldef) migration support.

## Installation

Add to your `deps.edn`:

```clojure
io.github.jacobobryant/biff.sqlite {:git/tag "..." :git/sha "..."}
```

## Quick Start

```clojure
(require '[com.biffweb.sqlite :as sqlite])

;; Define your columns
(def columns
  [{:id :user/id         :type :uuid :primary-key true :required true}
   {:id :user/email      :type :text :unique true :required true}
   {:id :user/name       :type :text :required true}
   {:id :user/joined-at  :type :inst :required true}
   {:id :user/active     :type :boolean}
   {:id :user/role       :type :enum :required true
    :enum-values {0 :user.role/member
                  1 :user.role/admin}}])

;; Use as a Biff component
(defn start-system [ctx]
  (sqlite/use-sqlite
    (assoc ctx
      :biff.sqlite/columns columns
      :biff.sqlite/db-path "storage/sqlite/main.db")))

;; Execute queries
(sqlite/execute ctx {:select :* :from :user})
;; => [{:user/id #uuid "...", :user/email "alice@example.com", ...}]

(sqlite/execute ctx {:insert-into :user
                     :values [{:user/id (java.util.UUID/randomUUID)
                               :user/email "bob@example.com"
                               :user/name "Bob"
                               :user/joined-at (java.time.Instant/now)
                               :user/active true
                               :user/role :user.role/member}]})
```

## Public API

### `use-sqlite`

Biff component that runs schema migrations (via sqlite3def) and starts a HikariCP connection pool. Adds `:biff/conn` to the system context.

**System map keys:**

| Key | Description | Default |
|-----|-------------|---------|
| `:biff.sqlite/db-path` | Path to SQLite database file | `"storage/sqlite/main.db"` |
| `:biff.sqlite/columns` | Vector of column definition maps | `[]` |
| `:biff.sqlite/extra-sql` | Vector of extra SQL strings to append to schema | `[]` |
| `:biff.sqlite/sqlite3def-version` | sqlite3def version to auto-install | `"3.10.1"` |
| `:biff.sqlite/litestream-*` | Litestream S3 replication config (see below) | — |

### `execute`

Execute a SQL query or statement. Accepts:

- A **HoneySQL map**: `{:select :* :from :user :where [:= :user/id "u1"]}`
- A **raw SQL string**: `"SELECT * FROM user"`
- A **JDBC vector**: `["SELECT * FROM user WHERE id = ?" "u1"]`

Returns results as qualified kebab-case keyword maps with automatic type coercion. Write statements are serialized under a lock.

## Column Definitions

Each column is a map with these keys:

| Key | Type | Description |
|-----|------|-------------|
| `:id` | keyword | **(required)** Qualified keyword — namespace is the table name, name is the column name |
| `:type` | keyword | **(required)** One of: `:int`, `:real`, `:text`, `:string`, `:boolean`, `:inst`, `:uuid`, `:enum`, `:edn` |
| `:primary-key` | boolean | Adds `PRIMARY KEY` constraint |
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
| `:edn` | any Clojure data | `BLOB` (nippy) |
| `:text`, `:string` | `String` | `TEXT` |
| `:int` | `long` | `INT` |
| `:real` | `double` | `REAL` |

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
