(ns com.biffweb.sqlite-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.biffweb.sqlite :as biff.sqlite]
   [honey.sql :as hsql]
   [next.jdbc :as jdbc])
  (:import
   [java.time Instant]
   [java.util UUID]))

;; --- Test column definitions ---

(def test-columns
  [{:id :user/id    :type :text :primary-key true :required true}
   {:id :user/name  :type :text :required true}
   {:id :user/joined-at :type :inst :required true}])

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
  (let [ctx {:biff/conn *conn* :biff.sqlite/columns test-columns}]
    (testing "direct column name matches coercion"
      (is (= [{:user/id "u1" :user/name "Alice" :user/joined-at (Instant/ofEpochMilli 1700000000000)}]
             (biff.sqlite/execute ctx ["SELECT id, name, joined_at FROM user"]))))

    (testing "non-SELECT statement does not fail"
      (is (= [{:next.jdbc/update-count 0}]
             (biff.sqlite/execute ctx ["DELETE FROM user WHERE id = ?" "nonexistent"]))))))

;; --- Schema SQL generation tests ---

(deftest schema-sql-basic-test
  (testing "generates CREATE TABLE with correct types and constraints"
    (let [columns [{:id :widget/id     :type :uuid    :primary-key true :required true}
                   {:id :widget/label  :type :text    :required true}
                   {:id :widget/count  :type :int     :required true}
                   {:id :widget/score  :type :real    :required true}
                   {:id :widget/active :type :boolean :required true}]
          sql (biff.sqlite/generate-schema-sql columns)]
      (is (str/includes? sql "CREATE TABLE widget"))
      (is (str/includes? sql "id BLOB PRIMARY KEY NOT NULL"))
      (is (str/includes? sql "label TEXT NOT NULL"))
      (is (str/includes? sql "count INT NOT NULL"))
      (is (str/includes? sql "score REAL NOT NULL"))
      (is (str/includes? sql "active INT NOT NULL"))
      (is (str/includes? sql "STRICT;")))))

(deftest schema-sql-optional-test
  (testing "optional columns omit NOT NULL"
    (let [columns [{:id :item/id   :type :text :primary-key true :required true}
                   {:id :item/note :type :text}]
          sql (biff.sqlite/generate-schema-sql columns)]
      (is (str/includes? sql "id TEXT PRIMARY KEY NOT NULL"))
      (is (re-find #"note TEXT\b" sql))
      (is (not (str/includes? sql "note TEXT NOT NULL"))))))

(deftest schema-sql-enum-test
  (testing "enum columns get CHECK constraints"
    (let [columns [{:id :task/id     :type :text :primary-key true :required true}
                   {:id :task/status :type :enum :required true
                    :enum-values {0 :task.status/pending
                                  1 :task.status/done}}]
          sql (biff.sqlite/generate-schema-sql columns)]
      (is (str/includes? sql "status INT NOT NULL CHECK"))
      (is (str/includes? sql "IN (0, 1)")))))

(deftest schema-sql-unique-constraints-test
  (testing "unique constraints from :unique-with"
    (let [columns [{:id :membership/id       :type :text :primary-key true :required true}
                   {:id :membership/user-id  :type :text :required true
                    :unique-with [:membership/group-id]}
                   {:id :membership/group-id :type :text :required true}]
          sql (biff.sqlite/generate-schema-sql columns)]
      (is (str/includes? sql "UNIQUE(user_id, group_id)")))))

(deftest schema-sql-unique-column-test
  (testing "unique constraint from :unique true"
    (let [columns [{:id :user/id    :type :uuid :primary-key true :required true}
                   {:id :user/email :type :text :unique true :required true}]
          sql (biff.sqlite/generate-schema-sql columns)]
      (is (str/includes? sql "UNIQUE(email)")))))

(deftest schema-sql-foreign-key-test
  (testing "foreign key constraints from :ref"
    (let [columns [{:id :account/id       :type :text :primary-key true :required true}
                   {:id :post/id          :type :text :primary-key true :required true}
                   {:id :post/author-id   :type :text :required true :ref :account/id}]
          sql (biff.sqlite/generate-schema-sql columns)]
      (is (str/includes? sql "FOREIGN KEY(author_id) REFERENCES account(id)")))))

(deftest schema-sql-index-test
  (testing "index generation from :index true"
    (let [columns [{:id :item/id      :type :uuid :primary-key true :required true}
                   {:id :item/user-id :type :uuid :required true :index true}]
          sql (biff.sqlite/generate-schema-sql columns)]
      (is (str/includes? sql "CREATE INDEX idx_item_user_id ON item(user_id)")))))

(deftest schema-sql-edn-type-test
  (testing "edn type generates BLOB column"
    (let [columns [{:id :user/id          :type :uuid :primary-key true :required true}
                   {:id :user/digest-days :type :edn}]
          sql (biff.sqlite/generate-schema-sql columns)]
      (is (str/includes? sql "digest_days BLOB")))))

(deftest schema-sql-topo-sort-test
  (testing "tables are ordered by foreign key dependencies"
    (let [columns [{:id :post/id        :type :uuid :primary-key true :required true}
                   {:id :post/author-id :type :uuid :required true :ref :user/id}
                   {:id :user/id        :type :uuid :primary-key true :required true}]
          sql (biff.sqlite/generate-schema-sql columns)]
      (is (< (str/index-of sql "CREATE TABLE user")
             (str/index-of sql "CREATE TABLE post"))))))

;; --- Type coercion tests ---

(deftest coercion-roundtrip-test
  (testing "UUID, Instant, boolean, enum, and nippy coercions roundtrip correctly"
    (let [columns [{:id :thing/id         :type :uuid    :primary-key true}
                   {:id :thing/active     :type :boolean}
                   {:id :thing/created-at :type :inst}
                   {:id :thing/tags       :type :edn}
                   {:id :thing/color      :type :enum
                    :enum-values {0 :thing.color/red
                                  1 :thing.color/blue}}]
          {:keys [read write]} (biff.sqlite/build-coercions columns)
          test-uuid (UUID/randomUUID)
          test-inst (Instant/ofEpochMilli 1700000000000)
          test-tags ["a" "b"]]
      ;; UUID roundtrip
      (is (= test-uuid ((read :thing/id) ((write :thing/id) test-uuid))))
      ;; Boolean roundtrip
      (is (= true ((read :thing/active) ((write :thing/active) true))))
      (is (= false ((read :thing/active) ((write :thing/active) false))))
      ;; Instant roundtrip
      (is (= test-inst ((read :thing/created-at) ((write :thing/created-at) test-inst))))
      ;; Nippy roundtrip (edn type)
      (is (= test-tags ((read :thing/tags) ((write :thing/tags) test-tags))))
      ;; Enum roundtrip
      (is (= :thing.color/red ((read :thing/color) ((write :thing/color) :thing.color/red)))))))

(deftest execute-string-input-test
  (testing "execute accepts a bare SQL string"
    (let [ctx {:biff/conn *conn* :biff.sqlite/columns test-columns}]
      (is (= [{:user/id "u1" :user/name "Alice" :user/joined-at (Instant/ofEpochMilli 1700000000000)}]
             (biff.sqlite/execute ctx "SELECT id, name, joined_at FROM user")))))

  (testing "execute accepts a HoneySQL map"
    (let [ctx {:biff/conn *conn* :biff.sqlite/columns test-columns}]
      (is (= [{:user/id "u1"}]
             (biff.sqlite/execute ctx {:select :id :from :user}))))))

;; --- Namespaced alias tests ---

(deftest namespaced-alias-honeysql-test
  (let [ctx {:biff/conn *conn* :biff.sqlite/columns test-columns}]
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
          processed (update input :select @#'biff.sqlite/preprocess-select)
          sql (first (hsql/format processed))]
      (is (str/includes? sql "\"user/age_years\"")))))

(deftest namespaced-alias-raw-sql-test
  (let [ctx {:biff/conn *conn* :biff.sqlite/columns test-columns}]
    (testing "raw SQL with quoted namespaced alias works"
      (let [results (biff.sqlite/execute ctx
                      ["SELECT joined_at AS \"user/joined_at\" FROM user"])]
        (is (= [{:user/joined-at (Instant/ofEpochMilli 1700000000000)}] results))))))

;; --- Validation tests ---

(deftest validation-rejects-bad-insert-test
  (testing "validation rejects INSERT with wrong type"
    (let [columns [{:id :user/id        :type :text :primary-key true :required true}
                   {:id :user/name      :type :text :required true}
                   {:id :user/joined-at :type :inst :required true}]
          ctx {:biff/conn *conn* :biff.sqlite/columns columns}]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"Validation failed"
           (biff.sqlite/execute ctx {:insert-into :user
                                     :values [{:user/id "u2"
                                               :user/name "Bob"
                                               :user/joined-at "not-an-inst"}]}))))))

(deftest validation-accepts-good-insert-test
  (testing "validation accepts INSERT with correct types"
    (let [columns [{:id :user/id        :type :text :primary-key true :required true}
                   {:id :user/name      :type :text :required true}
                   {:id :user/joined-at :type :inst :required true}]
          ctx {:biff/conn *conn* :biff.sqlite/columns columns}]
      ;; Should not throw
      (biff.sqlite/execute ctx {:insert-into :user
                                :values [{:user/id "u2"
                                          :user/name "Bob"
                                          :user/joined-at (Instant/ofEpochMilli 1700000000000)}]}))))

(deftest validation-rejects-bad-update-test
  (testing "validation rejects UPDATE with wrong type via :set"
    (let [columns [{:id :user/id        :type :text :primary-key true :required true}
                   {:id :user/name      :type :text :required true}
                   {:id :user/joined-at :type :inst :required true}]
          ctx {:biff/conn *conn* :biff.sqlite/columns columns}]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"Validation failed"
           (biff.sqlite/execute ctx {:update :user
                                     :set {:user/joined-at "not-an-inst"}
                                     :where [:= :user/id "u1"]}))))))

(deftest validation-skips-non-literal-values-test
  (testing "validation skips maps and non-lift vectors"
    (let [columns [{:id :user/id        :type :text :primary-key true :required true}
                   {:id :user/name      :type :text :required true}
                   {:id :user/joined-at :type :inst :required true}]
          ctx {:biff/conn *conn* :biff.sqlite/columns columns}]
      ;; vector values (not [:lift ...]) should be skipped - no validation exception
      (biff.sqlite/execute ctx {:update :user
                                :set {:user/name [:|| :user/name " Jr."]}
                                :where [:= :user/id "u1"]}))))

(deftest validation-with-custom-schema-test
  (testing "custom :schema on column is validated"
    (let [columns [{:id :user/id        :type :text :primary-key true :required true}
                   {:id :user/name      :type :text :required true
                    :schema [:re #"^[A-Z].*"]}
                   {:id :user/joined-at :type :inst :required true}]
          ctx {:biff/conn *conn* :biff.sqlite/columns columns}]
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
    (let [columns [{:id :user/id        :type :text :primary-key true :required true}
                   {:id :user/name      :type :text :required true}
                   {:id :user/joined-at :type :inst :required true}]
          ctx {:biff/conn *conn* :biff.sqlite/columns columns}]
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
          columns [{:id :item/id   :type :text :primary-key true :required true}
                   {:id :item/note :type :text}]
          ctx {:biff/conn *conn* :biff.sqlite/columns columns}]
      ;; nil should be valid for a non-required column
      (biff.sqlite/execute ctx {:update :item
                                :set {:item/note nil}
                                :where [:= :item/id "i1"]}))))
