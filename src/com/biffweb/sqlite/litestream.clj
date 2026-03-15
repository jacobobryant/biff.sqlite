(ns com.biffweb.sqlite.litestream
  "Litestream integration for continuous SQLite replication to S3.
   Downloads litestream binary automatically and runs it as a subprocess.
   Adapted from budgetswu.lib.litestream."
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.nio.file Files Paths]
           [java.nio.file.attribute PosixFilePermission]))

(def litestream-version "0.5.9")

(def litestream-dir "storage/litestream")

(defn litestream-bin-path []
  (str litestream-dir "/litestream"))

(defn litestream-config-path []
  (str litestream-dir "/litestream.yml"))

(defn- download-and-extract!
  "Downloads litestream binary from GitHub releases and extracts it."
  []
  (let [url (str "https://github.com/benbjohnson/litestream/releases/download/v"
                 litestream-version
                 "/litestream-" litestream-version "-linux-x86_64.tar.gz")
        tar-path (str litestream-dir "/litestream.tar.gz")
        bin-path (litestream-bin-path)]
    (log/info "Downloading litestream from" url)
    (.mkdirs (io/file litestream-dir))
    (let [process (-> (ProcessBuilder. ["curl" "-sL" "-o" tar-path url])
                      (.inheritIO)
                      (.start))]
      (when-not (zero? (.waitFor process))
        (throw (ex-info "Failed to download litestream" {:url url}))))
    (log/info "Extracting litestream binary...")
    (let [process (-> (ProcessBuilder. ["tar" "xzf" tar-path "-C" litestream-dir])
                      (.inheritIO)
                      (.start))]
      (when-not (zero? (.waitFor process))
        (throw (ex-info "Failed to extract litestream" {}))))
    (io/delete-file tar-path true)
    (let [perms #{PosixFilePermission/OWNER_READ
                  PosixFilePermission/OWNER_WRITE
                  PosixFilePermission/OWNER_EXECUTE}]
      (Files/setPosixFilePermissions (Paths/get bin-path (into-array String [])) perms))
    (log/info "Litestream binary installed at" bin-path)))

(defn- installed-version
  "Returns the version string of the installed litestream binary, or nil if not installed."
  []
  (let [bin (io/file (litestream-bin-path))]
    (when (.exists bin)
      (try
        (let [process (-> (ProcessBuilder. [(litestream-bin-path) "version"])
                          (.redirectErrorStream true)
                          (.start))
              output (slurp (.getInputStream process))]
          (.waitFor process)
          (some->> output str/trim not-empty (re-find #"[\d]+\.[\d]+\.[\d]+")))
        (catch Exception e
          (log/warn "Failed to check litestream version:" (.getMessage e))
          nil)))))

(defn- ensure-binary!
  "Downloads litestream if not present or if the installed version doesn't match."
  []
  (let [current (installed-version)]
    (when (not= current litestream-version)
      (when current
        (log/info "Litestream version mismatch: installed" current "expected" litestream-version))
      (download-and-extract!))))

(defn- write-config!
  "Generates litestream YAML config file."
  [{:biff.sqlite/keys [db-path]
    :litestream/keys [s3-bucket s3-path s3-endpoint s3-region
                      s3-access-key-id s3-secret-access-key]
    :or {db-path "storage/sqlite/main.db"}}]
  (let [replica-path (if (str/blank? s3-path)
                       (str/replace db-path #"^.*/" "")
                       (str (str/replace s3-path #"/$" "") "/"
                            (str/replace db-path #"^.*/" "")))
        config (str "dbs:\n"
                    "  - path: " db-path "\n"
                    "    replica:\n"
                    "      type: s3\n"
                    "      bucket: " s3-bucket "\n"
                    "      path: " replica-path "\n"
                    (when s3-endpoint
                      (str "      endpoint: " s3-endpoint "\n"))
                    (when s3-region
                      (str "      region: " s3-region "\n"))
                    "      access-key-id: " s3-access-key-id "\n"
                    "      secret-access-key: " (s3-secret-access-key) "\n")]
    (.mkdirs (io/file litestream-dir))
    (spit (litestream-config-path) config)
    (log/info "Litestream config written to" (litestream-config-path))))

(defn- restore!
  "Restores SQLite database from S3 replica if local DB doesn't exist.
   Returns true if restore was performed, false otherwise."
  [{:biff.sqlite/keys [db-path]
    :or {db-path "storage/sqlite/main.db"}}]
  (let [db-file (io/file db-path)]
    (if (.exists db-file)
      (do (log/info "Local database exists, skipping restore")
          false)
      (do (log/info "No local database found, attempting restore from S3...")
          (.mkdirs (.getParentFile db-file))
          (let [process (-> (ProcessBuilder.
                             [(litestream-bin-path) "restore"
                              "-config" (litestream-config-path)
                              "-if-replica-exists"
                              db-path])
                            (.inheritIO)
                            (.start))
                exit-code (.waitFor process)]
            (if (zero? exit-code)
              (if (.exists db-file)
                (do (log/info "Database restored from S3")
                    true)
                (do (log/info "No replica found in S3, starting fresh")
                    false))
              (do (log/warn "Litestream restore exited with code" exit-code
                            "- starting fresh")
                  false)))))))

(defn- start-replicate!
  "Starts litestream replicate as a subprocess. Returns the Process."
  []
  (log/info "Starting litestream replicate...")
  (let [process (-> (ProcessBuilder.
                     [(litestream-bin-path) "replicate"
                      "-config" (litestream-config-path)])
                    (.redirectErrorStream true)
                    (.start))]
    (future
      (with-open [reader (io/reader (.getInputStream process))]
        (doseq [line (line-seq reader)]
          (log/info "[litestream]" line))))
    (Thread/sleep 1000)
    (when-not (.isAlive process)
      (throw (ex-info "Litestream replicate failed to start"
                      {:exit-code (.exitValue process)})))
    (log/info "Litestream replicate started (PID:" (.pid process) ")")
    process))

(defn- stop-replicate!
  "Stops the litestream replicate subprocess gracefully."
  [^Process process]
  (when (and process (.isAlive process))
    (log/info "Stopping litestream replicate...")
    (.destroy process)
    (let [exited (.waitFor process 10 java.util.concurrent.TimeUnit/SECONDS)]
      (when-not exited
        (log/warn "Litestream did not stop gracefully, forcing...")
        (.destroyForcibly process)))
    (log/info "Litestream replicate stopped")))

(defn configured?
  "Returns true if the required litestream S3 config is present."
  [{:litestream/keys [s3-bucket s3-access-key-id s3-secret-access-key]}]
  (and (some? s3-bucket)
       (some? s3-access-key-id)
       (some? s3-secret-access-key)))

(defn start!
  "Starts litestream if configured. Called by use-sqlite before opening the pool.
   Returns an updated ctx with a stop function appended to :biff/stop, or the
   original ctx if litestream is not configured.

   Required config:
     :litestream/s3-bucket           - S3 bucket name
     :litestream/s3-access-key-id    - AWS access key
     :litestream/s3-secret-access-key - fn that returns AWS secret key

   Optional config:
     :litestream/s3-path     - Subdirectory within bucket
     :litestream/s3-endpoint - Custom S3 endpoint URL
     :litestream/s3-region   - AWS region (e.g. \"us-east-1\")"
  [ctx]
  (if-not (configured? ctx)
    (do (log/info "Litestream: S3 config not present, skipping")
        ctx)
    (do
      (ensure-binary!)
      (write-config! ctx)
      (restore! ctx)
      (let [process (start-replicate!)]
        (update ctx :biff/stop conj #(stop-replicate! process))))))
