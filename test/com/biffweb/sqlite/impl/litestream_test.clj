(ns com.biffweb.sqlite.impl.litestream-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [com.biffweb.sqlite.impl.litestream :as litestream]))

(deftest default-version-test
  (testing "has a default version set"
    (is (string? litestream/default-version))
    (is (re-matches #"\d+\.\d+\.\d+" litestream/default-version))))

(deftest configured?-test
  (testing "returns false when no S3 config is present"
    (is (not (litestream/configured? {}))))

  (testing "returns false when only bucket is present"
    (is (not (litestream/configured?
              {:biff.sqlite/litestream-bucket "my-bucket"}))))

  (testing "returns false when key is missing"
    (is (not (litestream/configured?
              {:biff.sqlite/litestream-bucket "my-bucket"
               :biff.sqlite/litestream-access-key-id "key"}))))

  (testing "returns true when all required config is present"
    (is (litestream/configured?
         {:biff.sqlite/litestream-bucket "my-bucket"
          :biff.sqlite/litestream-access-key-id "key"
          :biff.sqlite/litestream-secret-access-key "secret"}))))

(deftest use-litestream-skips-when-not-configured
  (testing "returns context unchanged when S3 config is absent"
    (let [ctx {:biff/stop []
               :biff.sqlite/db-path "storage/sqlite/main.db"}
          result (litestream/use-litestream ctx)]
      (is (= ctx result)))))

(deftest write-config-test
  (testing "generates correct YAML config with env var references for secrets"
    (let [dir (str "target/test-litestream-" (System/currentTimeMillis))
          _ (.mkdirs (io/file dir))]
      (with-redefs [litestream/litestream-dir dir
                    litestream/litestream-config-path (fn [] (str dir "/litestream.yml"))]
        (#'litestream/write-config!
         {:biff.sqlite/db-path "storage/sqlite/main.db"
          :biff.sqlite/litestream-bucket "my-bucket"
          :biff.sqlite/litestream-path "myapp"
          :biff.sqlite/litestream-endpoint "https://s3.us-east-1.amazonaws.com"
          :biff.sqlite/litestream-region "us-east-1"
          :biff.sqlite/litestream-access-key-id "AKID"
          :biff.sqlite/litestream-secret-access-key (constantly "SECRET")})
        (let [config (slurp (str dir "/litestream.yml"))]
          (is (str/includes? config "path: storage/sqlite/main.db"))
          (is (str/includes? config "bucket: my-bucket"))
          (is (str/includes? config "path: myapp/main.db"))
          (is (str/includes? config "endpoint: https://s3.us-east-1.amazonaws.com"))
          (is (str/includes? config "region: us-east-1"))
          (is (str/includes? config "access-key-id: $LITESTREAM_ACCESS_KEY_ID"))
          (is (str/includes? config "secret-access-key: $LITESTREAM_SECRET_ACCESS_KEY"))
          (is (not (str/includes? config "access-key-id: AKID")))
          (is (not (str/includes? config "secret-access-key: SECRET\n")))))
      (io/delete-file (str dir "/litestream.yml") true)
      (.delete (io/file dir)))))

(deftest version-check-test
  (testing "check-version returns nil when binary doesn't exist"
    (let [dir (str "target/test-no-litestream-" (System/currentTimeMillis))]
      (is (nil? (#'litestream/check-version (str dir "/litestream")))))))
