(ns com.biffweb.sqlite-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.biffweb.sqlite :as biff.sqlite]
   [com.biffweb.sqlite.impl.coerce :as coerce]
   [com.biffweb.sqlite.impl.query :as query]
   [com.biffweb.sqlite.impl.schema :as schema]
   [com.biffweb.sqlite.impl.util :as util]
   [honey.sql :as hsql]
   [next.jdbc :as jdbc]
   [taoensso.nippy :as nippy])
  (:import
   [java.time Instant]
   [java.util UUID]))

;; --- Test column definitions (new map format) ---

(def test-columns
  {:user/id        {:type :text :primary-key true}
   :user/name      {:type :text :required true}
   :user/joined-at {:type :inst :required true}})

(def ^:dynamic *conn* nil)

(defn with-sqlite [f]
  (with-open [conn (jdbc/get-connection "jdbc:sqlite::memory:")]
    (jdbc/execute! conn ["CREATE TABLE user (id TEXT PRIMARY KEY, name TEXT NOT NULL, joined_at INT NOT NULL) STRICT"])
    (jdbc/execute! conn ["INSERT INTO user (id, name, joined_at) VALUES (?, ?, ?)"
                         "u1" "Alice" 1700000000000])
    (binding [*conn* conn]
      (f))))

(use-fixtures :each with-sqlite)

(deftest direct-column-coercion-test
  (let [ctx {:biff.sqlite/read-pool *conn* :biff.sqlite/write-conn *conn*
             :biff.sqlite/columns test-columns}]
    (testing "direct column name matches coercion"
      (is (= [{:user/id "u1" :user/name "Alice" :user/joined-at (Instant/ofEpochMilli 1700000000000)}]
             (biff.sqlite/execute ctx ["SELECT id, name, joined_at FROM user"]))))

    (testing "non-SELECT statement does not fail"
      (is (= [{:next.jdbc/update-count 0}]
             (biff.sqlite/execute ctx ["DELETE FROM user WHERE id = ?" "nonexistent"]))))))

;; --- Schema SQL generation tests ---

(deftest schema-sql-basic-test
  (testing "generates CREATE TABLE with correct types and constraints"
    (let [columns {:widget/id     {:type :uuid    :primary-key true}
                   :widget/label  {:type :text    :required true}
                   :widget/count  {:type :int     :required true}
                   :widget/score  {:type :real    :required true}
                   :widget/active {:type :boolean :required true}}
          sql (schema/generate-schema-sql (util/normalize-columns columns) [])]
      (is (str/includes? sql "CREATE TABLE widget"))
      (is (str/includes? sql "id BLOB PRIMARY KEY NOT NULL"))
      (is (str/includes? sql "label TEXT NOT NULL"))
      (is (str/includes? sql "count INT NOT NULL"))
      (is (str/includes? sql "score REAL NOT NULL"))
      (is (str/includes? sql "active INT NOT NULL"))
      (is (str/includes? sql "STRICT;")))))

(deftest schema-sql-optional-test
  (testing "optional columns omit NOT NULL"
    (let [columns {:item/id   {:type :text :primary-key true}
                   :item/note {:type :text}}
          sql (schema/generate-schema-sql (util/normalize-columns columns) [])]
      (is (str/includes? sql "id TEXT PRIMARY KEY NOT NULL"))
      (is (re-find #"note TEXT\b" sql))
      (is (not (str/includes? sql "note TEXT NOT NULL"))))))

(deftest schema-sql-enum-test
  (testing "enum columns get CHECK constraints"
    (let [columns {:task/id     {:type :text :primary-key true}
                   :task/status {:type :enum :required true
                                 :enum-values {0 :task.status/pending
                                               1 :task.status/done}}}
          sql (schema/generate-schema-sql (util/normalize-columns columns) [])]
      (is (str/includes? sql "status INT NOT NULL CHECK"))
      (is (str/includes? sql "IN (0, 1)")))))

(deftest schema-sql-unique-constraints-test
  (testing "unique constraints from :unique-with"
    (let [columns {:membership/id       {:type :text :primary-key true}
                   :membership/user-id  {:type :text :required true
                                         :unique-with [:membership/group-id]}
                   :membership/group-id {:type :text :required true}}
          sql (schema/generate-schema-sql (util/normalize-columns columns) [])]
      (is (str/includes? sql "UNIQUE(user_id, group_id)")))))

(deftest schema-sql-unique-column-test
  (testing "unique constraint from :unique true"
    (let [columns {:user/id    {:type :uuid :primary-key true}
                   :user/email {:type :text :unique true :required true}}
          sql (schema/generate-schema-sql (util/normalize-columns columns) [])]
      (is (str/includes? sql "UNIQUE(email)")))))

(deftest schema-sql-foreign-key-test
  (testing "foreign key constraints from :ref"
    (let [columns {:account/id     {:type :text :primary-key true}
                   :post/id        {:type :text :primary-key true}
                   :post/author-id {:type :text :required true :ref :account/id}}
          sql (schema/generate-schema-sql (util/normalize-columns columns) [])]
      (is (str/includes? sql "FOREIGN KEY(author_id) REFERENCES account(id)")))))

(deftest schema-sql-index-test
  (testing "index generation from :index true"
    (let [columns {:item/id      {:type :uuid :primary-key true}
                   :item/user-id {:type :uuid :required true :index true}}
          sql (schema/generate-schema-sql (util/normalize-columns columns) [])]
      (is (str/includes? sql "CREATE INDEX idx_item_user_id ON item(user_id)")))))

(deftest schema-sql-edn-type-test
  (testing "edn type generates BLOB column"
    (let [columns {:user/id          {:type :uuid :primary-key true}
                   :user/digest-days {:type :edn}}
          sql (schema/generate-schema-sql (util/normalize-columns columns) [])]
      (is (str/includes? sql "digest_days BLOB")))))

(deftest schema-sql-topo-sort-test
  (testing "tables are ordered by foreign key dependencies"
    (let [columns {:post/id        {:type :uuid :primary-key true}
                   :post/author-id {:type :uuid :required true :ref :user/id}
                   :user/id        {:type :uuid :primary-key true}}
          sql (schema/generate-schema-sql (util/normalize-columns columns) [])]
      (is (< (str/index-of sql "CREATE TABLE user")
             (str/index-of sql "CREATE TABLE post"))))))

(deftest primary-key-implies-required-test
  (testing ":primary-key true implies :required true in schema output"
    (let [columns {:item/id {:type :uuid :primary-key true}}
          sql (schema/generate-schema-sql (util/normalize-columns columns) [])]
      (is (str/includes? sql "id BLOB PRIMARY KEY NOT NULL")))))

(deftest schema-sql-column-sort-order-test
  (testing "columns are sorted: primary key first, then required (alpha), then optional (alpha)"
    (let [columns {:widget/id      {:type :uuid :primary-key true}
                   :widget/zebra   {:type :text :required true}
                   :widget/alpha   {:type :text :required true}
                   :widget/zoptional {:type :text}
                   :widget/beta    {:type :int}}
          sql (schema/generate-schema-sql (util/normalize-columns columns) [])
          id-pos (str/index-of sql "id BLOB PRIMARY KEY")
          alpha-pos (str/index-of sql "alpha TEXT NOT NULL")
          zebra-pos (str/index-of sql "zebra TEXT NOT NULL")
          beta-pos (str/index-of sql "beta INT")
          zoptional-pos (str/index-of sql "zoptional TEXT")]
      (is (some? id-pos) "primary key column should be present")
      (is (some? alpha-pos) "required column alpha should be present")
      (is (some? zebra-pos) "required column zebra should be present")
      (is (some? beta-pos) "optional column beta should be present")
      (is (some? zoptional-pos) "optional column zoptional should be present")
      ;; Primary key comes first
      (is (< id-pos alpha-pos) "primary key before required columns")
      (is (< id-pos zebra-pos) "primary key before required columns")
      ;; Required columns come before optional columns
      (is (< alpha-pos beta-pos) "required before optional")
      (is (< zebra-pos beta-pos) "required before optional")
      ;; Required columns are sorted alphabetically
      (is (< alpha-pos zebra-pos) "required columns sorted alphabetically")
      ;; Optional columns are sorted alphabetically
      (is (< beta-pos zoptional-pos) "optional columns sorted alphabetically"))))

;; --- Type coercion tests ---

(deftest coercion-roundtrip-test
  (testing "UUID, Instant, boolean, enum, and nippy coercions roundtrip correctly"
    (let [columns {:thing/id         {:type :uuid    :primary-key true}
                   :thing/active     {:type :boolean}
                   :thing/created-at {:type :inst}
                   :thing/tags       {:type :edn}
                   :thing/color      {:type :enum
                                      :enum-values {0 :thing.color/red
                                                    1 :thing.color/blue}}}
          cols (util/normalize-columns columns)
          read-coercions (coerce/build-all-read-coercions cols)
          enum-val->int (coerce/build-enum-val->int cols)
          test-uuid (UUID/randomUUID)
          test-inst (Instant/ofEpochMilli 1700000000000)
          test-tags {:a 1 :b [2 3]}]
      ;; UUID roundtrip
      (is (= test-uuid ((get read-coercions :thing/id)
                         (coerce/uuid->bytes test-uuid))))
      ;; Boolean roundtrip
      (is (= true ((get read-coercions :thing/active)
                    (coerce/bool->int true))))
      (is (= false ((get read-coercions :thing/active)
                     (coerce/bool->int false))))
      ;; Instant roundtrip
      (is (= test-inst ((get read-coercions :thing/created-at)
                         (coerce/inst->epoch-ms test-inst))))
      ;; Nippy roundtrip (edn type)
      (let [frozen (nippy/fast-freeze test-tags)]
        (is (= test-tags ((get read-coercions :thing/tags) frozen))))
      ;; Enum roundtrip
      (is (= :thing.color/red ((get read-coercions :thing/color)
                                (get enum-val->int :thing.color/red)))))))

(deftest execute-string-input-test
  (testing "execute accepts a bare SQL string"
    (let [ctx {:biff.sqlite/read-pool *conn* :biff.sqlite/write-conn *conn*
               :biff.sqlite/columns test-columns}]
      (is (= [{:user/id "u1" :user/name "Alice" :user/joined-at (Instant/ofEpochMilli 1700000000000)}]
             (biff.sqlite/execute ctx "SELECT id, name, joined_at FROM user")))))

  (testing "execute accepts a HoneySQL map"
    (let [ctx {:biff.sqlite/read-pool *conn* :biff.sqlite/write-conn *conn*
               :biff.sqlite/columns test-columns}]
      (is (= [{:user/id "u1"}]
             (biff.sqlite/execute ctx {:select :id :from :user}))))))

;; --- Namespaced alias tests ---

(deftest namespaced-alias-honeysql-test
  (let [ctx {:biff.sqlite/read-pool *conn* :biff.sqlite/write-conn *conn*
             :biff.sqlite/columns test-columns}]
    (testing "namespaced alias on column from same table produces correct key"
      (is (= [{:user/joined-at (Instant/ofEpochMilli 1700000000000)}]
             (biff.sqlite/execute ctx {:select [[:joined-at :user/joined-at]]
                                       :from :user}))))

    (testing "namespaced alias on column from different namespace"
      (let [results (biff.sqlite/execute ctx {:select [[:name :item/name]]
                                              :from :user})]
        (is (= [{:item/name "Alice"}] results))
        (is (= :item/name (first (keys (first results)))))))

    (testing "namespaced alias with coercion applied via column definition match"
      (is (= [{:user/joined-at (Instant/ofEpochMilli 1700000000000)}]
             (biff.sqlite/execute ctx {:select [[:joined-at :user/joined-at]]
                                       :from :user}))))

    (testing "mix of regular columns and namespaced aliases"
      (let [results (biff.sqlite/execute ctx {:select [:user/id [:joined-at :user/joined-at]]
                                              :from :user})]
        (is (= "u1" (:user/id (first results))))
        (is (= (Instant/ofEpochMilli 1700000000000) (:user/joined-at (first results))))))

    (testing "namespaced alias on aggregate expression"
      (let [results (biff.sqlite/execute ctx {:select [[[:count :*] :user/total]]
                                              :from :user})]
        (is (= [{:user/total 1}] results))))

    (testing "regular non-namespaced alias gets table-qualified by JDBC"
      (let [results (biff.sqlite/execute ctx {:select [[:name :alias]]
                                              :from :user})]
        (is (= "Alice" (:user/alias (first results))))))))

(deftest namespaced-alias-honeysql-format-test
  (testing "namespaced alias in select is formatted as quoted string"
    (let [input {:select [[:age :user/age-years]] :from :user}
          processed (update input :select query/preprocess-select)
          sql (first (hsql/format processed))]
      (is (str/includes? sql "\"user/age_years\"")))))

(deftest namespaced-alias-raw-sql-test
  (let [ctx {:biff.sqlite/read-pool *conn* :biff.sqlite/write-conn *conn*
             :biff.sqlite/columns test-columns}]
    (testing "raw SQL with quoted namespaced alias works"
      (let [results (biff.sqlite/execute ctx
                      ["SELECT joined_at AS \"user/joined_at\" FROM user"])]
        (is (= [{:user/joined-at (Instant/ofEpochMilli 1700000000000)}] results))))))

;; --- Validation tests ---

(deftest validation-rejects-bad-insert-test
  (testing "validation rejects INSERT with wrong type"
    (let [columns {:user/id        {:type :text :primary-key true}
                   :user/name      {:type :text :required true}
                   :user/joined-at {:type :inst :required true}}
          ctx {:biff.sqlite/read-pool *conn* :biff.sqlite/write-conn *conn*
               :biff.sqlite/columns columns}]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"Validation failed"
           (biff.sqlite/execute ctx {:insert-into :user
                                     :values [{:user/id "u2"
                                               :user/name "Bob"
                                               :user/joined-at "not-an-inst"}]}))))))

(deftest validation-accepts-good-insert-test
  (testing "validation accepts INSERT with correct types"
    (let [columns {:user/id        {:type :text :primary-key true}
                   :user/name      {:type :text :required true}
                   :user/joined-at {:type :inst :required true}}
          ctx {:biff.sqlite/read-pool *conn* :biff.sqlite/write-conn *conn*
               :biff.sqlite/columns columns}]
      ;; Should not throw
      (biff.sqlite/execute ctx {:insert-into :user
                                :values [{:user/id "u2"
                                          :user/name "Bob"
                                          :user/joined-at (Instant/ofEpochMilli 1700000000000)}]}))))

(deftest validation-rejects-bad-update-test
  (testing "validation rejects UPDATE with wrong type via :set"
    (let [columns {:user/id        {:type :text :primary-key true}
                   :user/name      {:type :text :required true}
                   :user/joined-at {:type :inst :required true}}
          ctx {:biff.sqlite/read-pool *conn* :biff.sqlite/write-conn *conn*
               :biff.sqlite/columns columns}]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"Validation failed"
           (biff.sqlite/execute ctx {:update :user
                                     :set {:user/joined-at "not-an-inst"}
                                     :where [:= :user/id "u1"]}))))))

(deftest validation-skips-non-literal-values-test
  (testing "validation skips maps and non-lift vectors"
    (let [columns {:user/id        {:type :text :primary-key true}
                   :user/name      {:type :text :required true}
                   :user/joined-at {:type :inst :required true}}
          ctx {:biff.sqlite/read-pool *conn* :biff.sqlite/write-conn *conn*
               :biff.sqlite/columns columns}]
      ;; vector values (not [:lift ...]) should be skipped - no validation exception
      (biff.sqlite/execute ctx {:update :user
                                :set {:user/name [:|| :user/name " Jr."]}
                                :where [:= :user/id "u1"]}))))

(deftest validation-with-custom-schema-test
  (testing "custom :schema on column is validated"
    (let [columns {:user/id        {:type :text :primary-key true}
                   :user/name      {:type :text :required true
                                    :schema [:re #"^[A-Z].*"]}
                   :user/joined-at {:type :inst :required true}}
          ctx {:biff.sqlite/read-pool *conn* :biff.sqlite/write-conn *conn*
               :biff.sqlite/columns columns}]
      ;; lowercase name should fail validation
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"Validation failed"
           (biff.sqlite/execute ctx {:insert-into :user
                                     :values [{:user/id "u3"
                                               :user/name "lowercase"
                                               :user/joined-at (Instant/ofEpochMilli 1700000000000)}]})))
      ;; uppercase name should pass
      (biff.sqlite/execute ctx {:insert-into :user
                                :values [{:user/id "u3"
                                          :user/name "Uppercase"
                                          :user/joined-at (Instant/ofEpochMilli 1700000000000)}]}))))

(deftest validation-lift-test
  (testing "[:lift value] is treated as a literal and validated"
    (let [columns {:user/id        {:type :text :primary-key true}
                   :user/name      {:type :text :required true}
                   :user/joined-at {:type :inst :required true}}
          ctx {:biff.sqlite/read-pool *conn* :biff.sqlite/write-conn *conn*
               :biff.sqlite/columns columns}]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"Validation failed"
           (biff.sqlite/execute ctx {:update :user
                                     :set {:user/joined-at [:lift "not-an-inst"]}
                                     :where [:= :user/id "u1"]}))))))

(deftest validation-nullable-column-test
  (testing "nullable column accepts nil in validation"
    (let [;; Create a table with a nullable column
          _ (jdbc/execute! *conn* ["CREATE TABLE item (id TEXT PRIMARY KEY, note TEXT) STRICT"])
          _ (jdbc/execute! *conn* ["INSERT INTO item (id, note) VALUES (?, ?)" "i1" "hello"])
          columns {:item/id   {:type :text :primary-key true}
                   :item/note {:type :text}}
          ctx {:biff.sqlite/read-pool *conn* :biff.sqlite/write-conn *conn*
               :biff.sqlite/columns columns}]
      ;; nil should be valid for a non-required column
      (biff.sqlite/execute ctx {:update :item
                                :set {:item/note nil}
                                :where [:= :item/id "i1"]}))))

;; --- generate-schema-sql public API test ---

(deftest generate-schema-sql-test
  (testing "public generate-schema-sql returns full SQL string"
    (let [columns {:user/id    {:type :uuid :primary-key true}
                   :user/email {:type :text :unique true :required true}}
          result (biff.sqlite/generate-schema-sql
                  {:biff.sqlite/columns columns
                   :biff.sqlite/extra-sql ["CREATE INDEX custom_idx ON user(email);"]})]
      (is (str/includes? result "-- Auto-generated; do not edit."))
      (is (str/includes? result "CREATE TABLE user"))
      (is (str/includes? result "CREATE INDEX custom_idx")))))

;; --- make-resolvers tests ---

(def resolver-columns
  {:user/id        {:type :text :primary-key true}
   :user/name      {:type :text :required true}
   :user/joined-at {:type :inst :required true}
   :post/id        {:type :text :primary-key true}
   :post/title     {:type :text :required true}
   :post/author-id {:type :text :required true :ref :user/id}})

(deftest make-resolvers-shape-test
  (testing "returns one resolver per table with correct structure"
    (let [resolvers (biff.sqlite/make-resolvers {:biff.sqlite/columns resolver-columns})
          by-id (into {} (map (juxt :id identity)) resolvers)]
      (is (= 2 (count resolvers)))
      ;; User resolver
      (let [r (by-id :com.biffweb.sqlite/user-resolver)]
        (is (some? r))
        (is (= [:user/id] (:input r)))
        (is (= true (:batch r)))
        (is (every? #{:user/name :user/joined-at} (:output r)))
        (is (not (contains? (set (:output r)) :user/id))))
      ;; Post resolver
      (let [r (by-id :com.biffweb.sqlite/post-resolver)]
        (is (some? r))
        (is (= [:post/id] (:input r)))
        (is (= true (:batch r)))
        (is (contains? (set (:output r)) :post/title))
        (is (contains? (set (:output r)) :post/author-id))
        (is (contains? (set (:output r)) :post/author))))))

(deftest make-resolvers-batch-resolve-test
  (testing "batch resolve returns correct data and join keys"
    (jdbc/execute! *conn* ["CREATE TABLE post (id TEXT PRIMARY KEY, title TEXT NOT NULL, author_id TEXT NOT NULL, FOREIGN KEY(author_id) REFERENCES user(id)) STRICT"])
    (jdbc/execute! *conn* ["INSERT INTO post (id, title, author_id) VALUES (?, ?, ?)" "p1" "Hello" "u1"])
    (jdbc/execute! *conn* ["INSERT INTO post (id, title, author_id) VALUES (?, ?, ?)" "p2" "World" "u1"])
    (let [ctx {:biff.sqlite/read-pool *conn*
               :biff.sqlite/write-conn *conn*
               :biff.sqlite/columns resolver-columns}
          resolvers (biff.sqlite/make-resolvers ctx)
          post-resolver (first (filter #(= :com.biffweb.sqlite/post-resolver (:id %)) resolvers))
          results ((:resolve post-resolver) ctx [{:post/id "p1"} {:post/id "p2"}])]
      (is (= 2 (count results)))
      (is (= "Hello" (:post/title (first results))))
      (is (= "World" (:post/title (second results))))
      ;; Raw ref column preserved
      (is (= "u1" (:post/author-id (first results))))
      ;; Join key added
      (is (= {:user/id "u1"} (:post/author (first results)))))))

(deftest make-resolvers-missing-entity-test
  (testing "batch resolve returns empty map for missing entities"
    (let [ctx {:biff.sqlite/read-pool *conn*
               :biff.sqlite/write-conn *conn*
               :biff.sqlite/columns resolver-columns}
          resolvers (biff.sqlite/make-resolvers ctx)
          user-resolver (first (filter #(= :com.biffweb.sqlite/user-resolver (:id %)) resolvers))
          results ((:resolve user-resolver) ctx [{:user/id "u1"} {:user/id "nonexistent"}])]
      (is (= 2 (count results)))
      (is (= "Alice" (:user/name (first results))))
      (is (= {} (second results))))))

(deftest make-resolvers-no-primary-key-test
  (testing "throws when table has no primary key"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"No primary key"
         (biff.sqlite/make-resolvers
          {:biff.sqlite/columns {:broken/name {:type :text}}})))))

(deftest make-resolvers-no-refs-test
  (testing "tables without ref columns have no extra join keys"
    (let [resolvers (biff.sqlite/make-resolvers
                     {:biff.sqlite/columns {:item/id    {:type :text :primary-key true}
                                            :item/label {:type :text}}})
          r (first resolvers)]
      (is (= [:item/label] (:output r))))))

(deftest make-resolvers-nil-values-test
  (testing "resolver does not return keys with nil values"
    (jdbc/execute! *conn* ["CREATE TABLE entry (id TEXT PRIMARY KEY, title TEXT, author_id TEXT) STRICT"])
    (jdbc/execute! *conn* ["INSERT INTO entry (id, title, author_id) VALUES (?, ?, ?)" "e1" nil nil])
    (jdbc/execute! *conn* ["INSERT INTO entry (id, title, author_id) VALUES (?, ?, ?)" "e2" "Hello" "u1"])
    (let [cols {:entry/id        {:type :text :primary-key true}
                :entry/title     {:type :text}
                :entry/author-id {:type :text :ref :user/id}}
          ctx {:biff.sqlite/read-pool *conn*
               :biff.sqlite/write-conn *conn*
               :biff.sqlite/columns cols}
          resolvers (biff.sqlite/make-resolvers ctx)
          r (first resolvers)
          results ((:resolve r) ctx [{:entry/id "e1"} {:entry/id "e2"}])]
      ;; e1 has all nil values, so result should have no keys (just empty from id->result)
      (is (not (contains? (first results) :entry/title)))
      (is (not (contains? (first results) :entry/author-id)))
      (is (not (contains? (first results) :entry/author)))
      ;; e2 has values, so all keys present
      (is (= "Hello" (:entry/title (second results))))
      (is (= "u1" (:entry/author-id (second results))))
      (is (= {:user/id "u1"} (:entry/author (second results)))))))
