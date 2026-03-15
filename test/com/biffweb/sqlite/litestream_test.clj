(ns com.biffweb.sqlite.litestream-test
  (:require [clojure.test :refer [deftest is testing]]
            [com.biffweb.sqlite.litestream :as litestream]))

(deftest configured?-test
  (testing "returns false when no S3 config is present"
    (is (not (litestream/configured? {}))))

  (testing "returns false when only bucket is present"
    (is (not (litestream/configured?
              {:litestream/s3-bucket "my-bucket"}))))

  (testing "returns false when key is missing"
    (is (not (litestream/configured?
              {:litestream/s3-bucket "my-bucket"
               :litestream/s3-access-key-id "key"}))))

  (testing "returns true when all required config is present"
    (is (litestream/configured?
         {:litestream/s3-bucket "my-bucket"
          :litestream/s3-access-key-id "key"
          :litestream/s3-secret-access-key "secret"}))))

(deftest start-skips-when-not-configured
  (testing "returns context unchanged when S3 config is absent"
    (let [ctx {:biff/stop []
               :biff.sqlite/db-path "storage/sqlite/main.db"}
          result (litestream/start! ctx)]
      (is (= ctx result)))))

(deftest write-config-test
  (testing "generates correct YAML config with S3 path"
    (let [dir (str "target/test-litestream-" (System/currentTimeMillis))
          _ (.mkdirs (clojure.java.io/file dir))]
      (with-redefs [litestream/litestream-dir dir
                    litestream/litestream-config-path (fn [] (str dir "/litestream.yml"))]
        (#'litestream/write-config!
         {:biff.sqlite/db-path "storage/sqlite/main.db"
          :litestream/s3-bucket "my-bucket"
          :litestream/s3-path "myapp"
          :litestream/s3-endpoint "https://s3.us-east-1.amazonaws.com"
          :litestream/s3-region "us-east-1"
          :litestream/s3-access-key-id "AKID"
          :litestream/s3-secret-access-key (constantly "SECRET")})
        (let [config (slurp (str dir "/litestream.yml"))]
          (is (clojure.string/includes? config "path: storage/sqlite/main.db"))
          (is (clojure.string/includes? config "bucket: my-bucket"))
          (is (clojure.string/includes? config "path: myapp/main.db"))
          (is (clojure.string/includes? config "endpoint: https://s3.us-east-1.amazonaws.com"))
          (is (clojure.string/includes? config "region: us-east-1"))
          (is (clojure.string/includes? config "access-key-id: AKID"))
          (is (clojure.string/includes? config "secret-access-key: SECRET"))
          (is (clojure.string/includes? config "    replica:\n"))
          (is (not (clojure.string/includes? config "replicas:")))))
      ;; Cleanup
      (clojure.java.io/delete-file (str dir "/litestream.yml") true)
      (.delete (clojure.java.io/file dir)))))

(deftest version-check-test
  (testing "installed-version returns nil when binary doesn't exist"
    (let [dir (str "target/test-no-litestream-" (System/currentTimeMillis))]
      (with-redefs [litestream/litestream-dir dir
                    litestream/litestream-bin-path (constantly (str dir "/litestream"))]
        (is (nil? (#'litestream/installed-version)))))))
