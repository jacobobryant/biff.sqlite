(ns com.biffweb.sqlite.impl.coerce
  "Internal type coercion between Clojure values and SQLite storage types."
  (:require [clojure.string :as str]
            [taoensso.nippy :as nippy])
  (:import [java.nio ByteBuffer]
           [java.time Instant]
           [java.util UUID]))

;; --- Read coercions (DB -> Clojure) ---

(defn bytes->uuid [^bytes ba]
  (when ba
    (let [bb (ByteBuffer/wrap ba)]
      (UUID. (.getLong bb) (.getLong bb)))))

(defn epoch-ms->inst [ms]
  (when ms (Instant/ofEpochMilli ms)))

(defn int->bool [n]
  (when (some? n)
    (case n 0 false 1 true
      (throw (ex-info "Invalid boolean value, expected 0 or 1" {:value n})))))

(defn make-enum-reader [enum-map]
  (fn [db-val]
    (when (some? db-val)
      (or (get enum-map db-val)
          (throw (ex-info "Unknown enum value"
                          {:value db-val :available-values enum-map}))))))

;; --- Write coercions (Clojure -> DB) ---

(defn uuid->bytes [^UUID uuid]
  (let [bb (ByteBuffer/allocate 16)]
    (.putLong bb (.getMostSignificantBits uuid))
    (.putLong bb (.getLeastSignificantBits uuid))
    (.array bb)))

(defn inst->epoch-ms [x]
  (when x (.toEpochMilli ^Instant x)))

(defn bool->int [b]
  (if b 1 0))

(defn make-enum-writer [enum-map]
  (let [reverse-map (into {} (map (fn [[k v]] [v k]) enum-map))]
    (fn [clj-val]
      (when (some? clj-val)
        (or (get reverse-map clj-val)
            (throw (ex-info "Unknown enum value for write"
                            {:value clj-val :available-values reverse-map})))))))

;; --- Coercion dispatch ---

(def ^:private read-fns
  {:uuid  bytes->uuid
   :bool  int->bool
   :inst  epoch-ms->inst
   :nippy nippy/fast-thaw})

(def ^:private write-fns
  {:uuid  uuid->bytes
   :bool  bool->int
   :inst  inst->epoch-ms
   :nippy nippy/fast-freeze})

(defn- coercion-type [col]
  (case (:type col)
    :uuid    :uuid
    :boolean :bool
    :inst    :inst
    :edn     :nippy
    :enum    {:enum (:enum-values col)}
    nil))

(defn- resolve-fn [registry ct]
  (if (keyword? ct)
    (get registry ct)
    (when-let [enum-map (:enum ct)]
      (if (= registry read-fns)
        (make-enum-reader enum-map)
        (make-enum-writer enum-map)))))

(defn build-coercions
  "Build coercion maps from column definitions.
   Returns {:read {col-id coerce-fn} :write {col-id coerce-fn}}"
  [columns]
  (reduce
   (fn [acc col]
     (if-let [ct (coercion-type col)]
       (cond-> acc
         (resolve-fn read-fns ct)  (assoc-in [:read (:id col)] (resolve-fn read-fns ct))
         (resolve-fn write-fns ct) (assoc-in [:write (:id col)] (resolve-fn write-fns ct)))
       acc))
   {:read {} :write {}}
   columns))

(defn build-all-read-coercions
  "Build read coercions indexed by SQL column name strings.
   Includes both 'table.column' and 'column' entries."
  [columns]
  (let [{:keys [read]} (build-coercions columns)]
    (into {}
          (mapcat (fn [[col-id coerce-fn]]
                    (let [tbl (str/replace (str (namespace col-id)) "-" "_")
                          col (str/replace (name col-id) "-" "_")]
                      [[(str tbl "." col) coerce-fn]
                       [col coerce-fn]])))
          read)))

(defn build-enum-val->int
  "Build a map from namespaced enum keywords to their integer DB values."
  [columns]
  (into {}
        (mapcat (fn [col]
                  (when-let [enum-map (:enum-values col)]
                    (let [col-ns (str (namespace (:id col)) "." (name (:id col)))]
                      (map (fn [[idx kw]]
                             (when-not (and (keyword? kw)
                                            (= col-ns (namespace kw)))
                               (throw (ex-info (str "Enum values must be namespaced keywords "
                                                    "with namespace matching the column. "
                                                    "Expected namespace: " col-ns
                                                    ", got: " (pr-str kw))
                                               {:column (:id col) :value kw})))
                             [kw idx])
                           enum-map)))))
        columns))

(defn coerce-params
  "Coerce SQL parameter values based on their types."
  [enum-val->int params]
  (mapv (fn [v]
          (cond
            (nil? v)     v
            (uuid? v)    (uuid->bytes v)
            (inst? v)    (inst->epoch-ms v)
            (boolean? v) (bool->int v)
            (keyword? v) (if-let [n (get enum-val->int v)]
                           n
                           (throw (ex-info "Unknown enum keyword value"
                                           {:value v
                                            :available (keys enum-val->int)})))
            (map? v)     (nippy/fast-freeze v)
            (vector? v)  (nippy/fast-freeze v)
            (set? v)     (nippy/fast-freeze v)
            :else        v))
        params))
