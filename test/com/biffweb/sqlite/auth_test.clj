(ns com.biffweb.sqlite.auth-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.biffweb.sqlite :as biff.sqlite]
   [com.biffweb.sqlite.impl.auth :as auth-impl]
   [next.jdbc :as jdbc])
  (:import
   [java.time Instant]))

;; --- Test fixtures ---

(def ^:dynamic *ctx* nil)
(def ^:dynamic *db-fns* nil)

(defn with-auth-sqlite [f]
  (let [db-file (java.io.File/createTempFile "biff-sqlite-auth-test" ".db")
        db-path (.getAbsolutePath db-file)]
    (.delete db-file)
    (try
      (with-open [write-conn (jdbc/get-connection (str "jdbc:sqlite:" db-path))
                  read-conn (jdbc/get-connection (str "jdbc:sqlite:" db-path))]
        (jdbc/execute! write-conn ["PRAGMA journal_mode=WAL"])
        (jdbc/execute! read-conn ["PRAGMA journal_mode=WAL"])
        (jdbc/execute! write-conn
                       [(str "CREATE TABLE user ("
                             "id BLOB PRIMARY KEY NOT NULL, "
                             "email TEXT NOT NULL, "
                             "joined_at INT NOT NULL"
                             ") STRICT")])
        (jdbc/execute! write-conn
                       [(str "CREATE UNIQUE INDEX idx_user_email ON user(email)")])
        (jdbc/execute! write-conn
                       [(str "CREATE TABLE biff_auth_signin ("
                             "id BLOB PRIMARY KEY NOT NULL, "
                             "email TEXT NOT NULL UNIQUE, "
                             "code TEXT NOT NULL, "
                             "created_at INT NOT NULL, "
                             "failed_attempts INT NOT NULL, "
                             "params TEXT"
                             ") STRICT")])
        (let [columns (merge {:user/id {:type :uuid :primary-key true}
                              :user/email {:type :text :required true}
                              :user/joined-at {:type :inst :required true}}
                             auth-impl/auth-signin-columns)
              ctx {:biff.sqlite/read-pool read-conn
                   :biff.sqlite/write-conn write-conn
                   :biff.sqlite/columns columns}
              db-fns auth-impl/db-fns]
          (binding [*ctx* ctx
                    *db-fns* db-fns]
            (f))))
      (finally
        (.delete (java.io.File. db-path))
        (.delete (java.io.File. (str db-path "-wal")))
        (.delete (java.io.File. (str db-path "-shm")))))))

(use-fixtures :each with-auth-sqlite)

;; --- auth-module structure tests ---

(deftest auth-module-returns-routes-test
  (let [module (biff.sqlite/auth-module {:biff.auth/app-name "Test App"})]
    (testing "module has :routes"
      (is (some? (:routes module))))
    (testing "module has :biff.sqlite/columns"
      (is (map? (:biff.sqlite/columns module))))
    (testing "columns include biff-auth-signin table"
      (let [cols (:biff.sqlite/columns module)]
        (is (contains? cols :biff-auth-signin/id))
        (is (contains? cols :biff-auth-signin/email))
        (is (contains? cols :biff-auth-signin/code))
        (is (contains? cols :biff-auth-signin/created-at))
        (is (contains? cols :biff-auth-signin/failed-attempts))
        (is (contains? cols :biff-auth-signin/params))))))

;; --- SQLite store implementation tests ---

(deftest sqlite-store-get-user-id-no-user-test
  (testing "get-user-id returns nil when user doesn't exist"
    (let [get-user-id (:biff.auth/get-user-id *db-fns*)]
      (is (nil? (get-user-id *ctx* "nonexistent@example.com"))))))

(deftest sqlite-store-create-and-get-user-test
  (testing "create-user! and get-user-id round-trip"
    (let [create-user! (:biff.auth/create-user! *db-fns*)
          get-user-id (:biff.auth/get-user-id *db-fns*)
          uid (create-user! *ctx* {:email "test@example.com" :params {}})]
      (is (uuid? uid))
      (is (= uid (get-user-id *ctx* "test@example.com"))))))

(deftest sqlite-store-signin-lifecycle-test
  (testing "upsert, get, increment, and delete signin"
    (let [upsert! (:biff.auth/upsert-signin! *db-fns*)
          get-signin (:biff.auth/get-signin *db-fns*)
          increment! (:biff.auth/increment-failed-attempts! *db-fns*)
          delete! (:biff.auth/delete-signin! *db-fns*)
          now (Instant/now)
          email "test@example.com"]
      ;; upsert
      (upsert! *ctx*
       {:biff-auth-signin/email email
        :biff-auth-signin/code "123456"
        :biff-auth-signin/created-at now
        :biff-auth-signin/failed-attempts 0
        :biff-auth-signin/params "{}"})

      ;; get
      (let [record (get-signin *ctx* email)]
        (is (= "123456" (:biff-auth-signin/code record)))
        (is (= 0 (:biff-auth-signin/failed-attempts record)))
        (is (= "{}" (:biff-auth-signin/params record))))

      ;; increment
      (increment! *ctx* email)
      (is (= 1 (:biff-auth-signin/failed-attempts
                 (get-signin *ctx* email))))

      ;; increment again
      (increment! *ctx* email)
      (is (= 2 (:biff-auth-signin/failed-attempts
                 (get-signin *ctx* email))))

      ;; delete
      (delete! *ctx* email)
      (is (nil? (get-signin *ctx* email))))))

(deftest sqlite-store-upsert-overwrites-test
  (testing "upsert overwrites existing signin"
    (let [upsert! (:biff.auth/upsert-signin! *db-fns*)
          get-signin (:biff.auth/get-signin *db-fns*)
          increment! (:biff.auth/increment-failed-attempts! *db-fns*)
          email "test@example.com"
          now (Instant/now)
          later (.plusSeconds now 60)]
      (upsert! *ctx*
       {:biff-auth-signin/email email
        :biff-auth-signin/code "111111"
        :biff-auth-signin/created-at now
        :biff-auth-signin/failed-attempts 0
        :biff-auth-signin/params nil})

      ;; increment to verify reset on upsert
      (increment! *ctx* email)
      (is (= 1 (:biff-auth-signin/failed-attempts
                 (get-signin *ctx* email))))

      ;; upsert again — should reset failed-attempts
      (upsert! *ctx*
       {:biff-auth-signin/email email
        :biff-auth-signin/code "222222"
        :biff-auth-signin/created-at later
        :biff-auth-signin/failed-attempts 0
        :biff-auth-signin/params nil})

      (let [record (get-signin *ctx* email)]
        (is (= "222222" (:biff-auth-signin/code record)))
        (is (= 0 (:biff-auth-signin/failed-attempts record)))))))
