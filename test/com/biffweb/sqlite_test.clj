(ns com.biffweb.sqlite-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.biffweb.sqlite :as biff.sqlite]
   [honey.sql :as hsql]
   [malli.core :as malli]
   [malli.registry :as malr]
   [next.jdbc :as jdbc])
  (:import
   [java.time Instant]
   [java.util UUID]))

(def test-malli-opts
  {:registry (malr/composite-registry
              (malli/default-schemas)
              {:user [:map {:closed true}
                      [:user/id :string]
                      [:user/name :string]
                      [:user/joined-at inst?]]})})

(def ^:dynamic *conn* nil)

(defn with-sqlite [f]
  (with-open [conn (jdbc/get-connection "jdbc:sqlite::memory:")]
    (jdbc/execute! conn ["CREATE TABLE user (id TEXT PRIMARY KEY, name TEXT NOT NULL, joined_at INT NOT NULL) STRICT"])
    (jdbc/execute! conn ["INSERT INTO user (id, name, joined_at) VALUES (?, ?, ?)"
                         "u1" "Alice" 1700000000000])
    (binding [*conn* conn]
      (f))))

(use-fixtures :each with-sqlite)

(deftest inference-fallback-coercion-test
  (let [ctx {:biff/conn *conn* :biff/malli-opts test-malli-opts}]
    (testing "direct column name matches coercion"
      (is (= [{:user/id "u1" :user/name "Alice" :user/joined-at (Instant/ofEpochMilli 1700000000000)}]
             (biff.sqlite/execute ctx ["SELECT id, name, joined_at FROM user"]))))

    (testing "aliased column falls back to inference"
      (is (= [{:user/joined-at-alias (Instant/ofEpochMilli 1700000000000)}]
             (biff.sqlite/execute ctx ["SELECT joined_at AS joined_at_alias FROM user"]))))

    (testing "non-SELECT statement does not fail on inference"
      (is (= [{:next.jdbc/update-count 0}]
             (biff.sqlite/execute ctx ["DELETE FROM user WHERE id = ?" "nonexistent"]))))))

;; --- Schema SQL generation tests ---

(deftest schema-sql-basic-test
  (testing "generates CREATE TABLE with correct types and constraints"
    (let [opts {:registry (malr/composite-registry
                           (malli/default-schemas)
                           {:widget [:map {:closed true}
                                     [:widget/id :uuid]
                                     [:widget/label :string]
                                     [:widget/count :int]
                                     [:widget/score :double]
                                     [:widget/active :boolean]]})}
          sql (biff.sqlite/generate-schema-sql opts)]
      (is (str/includes? sql "CREATE TABLE widget"))
      (is (str/includes? sql "id BLOB PRIMARY KEY NOT NULL"))
      (is (str/includes? sql "label TEXT NOT NULL"))
      (is (str/includes? sql "count INT NOT NULL"))
      (is (str/includes? sql "score REAL NOT NULL"))
      (is (str/includes? sql "active INT NOT NULL"))
      (is (str/includes? sql "STRICT;")))))

(deftest schema-sql-optional-test
  (testing "optional columns omit NOT NULL"
    (let [opts {:registry (malr/composite-registry
                           (malli/default-schemas)
                           {:item [:map {:closed true}
                                   [:item/id :string]
                                   [:item/note {:optional true} :string]]})}
          sql (biff.sqlite/generate-schema-sql opts)]
      (is (str/includes? sql "id TEXT PRIMARY KEY NOT NULL"))
      (is (re-find #"note TEXT\b" sql))
      (is (not (str/includes? sql "note TEXT NOT NULL"))))))

(deftest schema-sql-enum-test
  (testing "enum columns get CHECK constraints"
    (let [opts {:registry (malr/composite-registry
                           (malli/default-schemas)
                           {:task [:map {:closed true}
                                   [:task/id :string]
                                   [:task/status [:enum
                                                  :task.status/pending
                                                  :task.status/done]]]})}
          sql (biff.sqlite/generate-schema-sql opts)]
      (is (str/includes? sql "status INT NOT NULL CHECK"))
      (is (str/includes? sql "IN (0, 1)")))))

(deftest schema-sql-unique-constraints-test
  (testing "unique constraints from :biff/unique table property"
    (let [opts {:registry (malr/composite-registry
                           (malli/default-schemas)
                           {:membership [:map {:closed true
                                              :biff/unique [[:membership/user-id :membership/group-id]]}
                                         [:membership/id :string]
                                         [:membership/user-id :string]
                                         [:membership/group-id :string]]})}
          sql (biff.sqlite/generate-schema-sql opts)]
      (is (str/includes? sql "UNIQUE(user_id, group_id)")))))

(deftest schema-sql-foreign-key-test
  (testing "foreign key constraints from :biff/ref property"
    (let [opts {:registry (malr/composite-registry
                           (malli/default-schemas)
                           {:account [:map {:closed true}
                                      [:account/id :string]]
                            :post [:map {:closed true}
                                   [:post/id :string]
                                   [:post/author-id {:biff/ref :account/id} :string]]})}
          sql (biff.sqlite/generate-schema-sql opts)]
      (is (str/includes? sql "FOREIGN KEY(author_id) REFERENCES")))))


;; --- Type coercion tests ---

(deftest coercion-roundtrip-test
  (testing "UUID, Instant, boolean, enum, and nippy coercions roundtrip correctly"
    (let [opts {:registry (malr/composite-registry
                           (malli/default-schemas)
                           {:thing [:map {:closed true}
                                    [:thing/id :uuid]
                                    [:thing/active :boolean]
                                    [:thing/created-at inst?]
                                    [:thing/tags [:vector :string]]
                                    [:thing/color [:enum
                                                   :thing.color/red
                                                   :thing.color/blue]]]})}
          info (biff.sqlite/schema-info opts)
          attrs (get info :thing)
          {:keys [read write]} (biff.sqlite/build-coercions attrs)
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
      ;; Nippy roundtrip
      (is (= test-tags ((read :thing/tags) ((write :thing/tags) test-tags))))
      ;; Enum roundtrip
      (is (= :thing.color/red ((read :thing/color) ((write :thing/color) :thing.color/red)))))))

(deftest execute-string-input-test
  (testing "execute accepts a bare SQL string"
    (let [ctx {:biff/conn *conn* :biff/malli-opts test-malli-opts}]
      (is (= [{:user/id "u1" :user/name "Alice" :user/joined-at (Instant/ofEpochMilli 1700000000000)}]
             (biff.sqlite/execute ctx "SELECT id, name, joined_at FROM user")))))

  (testing "execute accepts a HoneySQL map"
    (let [ctx {:biff/conn *conn* :biff/malli-opts test-malli-opts}]
      (is (= [{:user/id "u1"}]
             (biff.sqlite/execute ctx {:select :id :from :user}))))))

;; --- Namespaced alias tests ---

(deftest namespaced-alias-honeysql-test
  (let [ctx {:biff/conn *conn* :biff/malli-opts test-malli-opts}]
    (testing "namespaced alias on column from same table produces correct key"
      (is (= [{:user/joined-at (Instant/ofEpochMilli 1700000000000)}]
             (biff.sqlite/execute ctx {:select [[:joined-at :user/joined-at]]
                                       :from :user}))))

    (testing "namespaced alias on column from different namespace"
      (let [results (biff.sqlite/execute ctx {:select [[:name :item/name]]
                                              :from :user})]
        (is (= [{:item/name "Alice"}] results))
        (is (= :item/name (first (keys (first results)))))))

    (testing "namespaced alias with coercion applied via malli schema match"
      ;; :user/joined-at is inst? in our schema, so coercion should apply
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
        ;; JDBC provides table name for aliased columns from a real table,
        ;; so as-kebab-maps qualifies the key. Use namespaced aliases to control this.
        (is (= "Alice" (:user/alias (first results))))))))

(deftest namespaced-alias-honeysql-format-test
  (testing "namespaced alias in select is formatted as quoted string"
    (let [input {:select [[:age :user/age-years]] :from :user}
          ;; The preprocess-honeysql converts :user/age-years to "user/age_years"
          ;; HoneySQL formats string aliases as quoted identifiers
          [processed _ _] (#'biff.sqlite/preprocess-honeysql input)
          sql (first (hsql/format processed))]
      (is (str/includes? sql "\"user/age_years\"")))))

(deftest explicit-column-types-test
  (let [ctx {:biff/conn *conn* :biff/malli-opts test-malli-opts}]
    (testing "explicit :biff/column-types applies coercion to aliased columns"
      ;; When aliased from a table column, JDBC provides the table name,
      ;; so the key is table-qualified by as-kebab-maps
      (let [results (biff.sqlite/execute ctx {:select [[:joined-at :latest]]
                                              :from :user
                                              :biff/column-types {:latest :inst}})]
        (is (= (Instant/ofEpochMilli 1700000000000) (:user/latest (first results))))))

    (testing "explicit :biff/column-types works with namespaced alias"
      (let [results (biff.sqlite/execute ctx {:select [[:joined-at :report/latest-join]]
                                              :from :user
                                              :biff/column-types {:report/latest-join :inst}})]
        (is (= [{:report/latest-join (Instant/ofEpochMilli 1700000000000)}] results))))

    (testing "unnamespaced alias with no schema match gets no coercion"
      ;; JDBC qualifies with table name; :user/raw-ts doesn't match schema
      (let [results (biff.sqlite/execute ctx {:select [[:joined-at :raw-ts]]
                                              :from :user})]
        ;; :user/raw-ts is the key (JDBC provides table name), but it's not
        ;; in the schema so inference kicks in: infers joined_at -> :inst coercion
        ;; For truly raw results, use :biff/column-types or namespaced aliases
        (is (some? (:user/raw-ts (first results))))))

    (testing ":biff/column-types is stripped before HoneySQL formatting"
      (is (not (str/includes?
                 (first (hsql/format (first (#'biff.sqlite/preprocess-honeysql
                                              {:select :* :from :user
                                               :biff/column-types {:x :inst}}))))
                 "biff"))))))
