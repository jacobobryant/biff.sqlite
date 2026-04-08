(ns com.biffweb.sqlite.impl.authorize
  "Internal implementation for authorized write transactions.
   Generates diff data structures and manages transaction rollback."
  (:require [com.biffweb.sqlite.impl.coerce :as coerce]
            [com.biffweb.sqlite.impl.query :as query]
            [com.biffweb.sqlite.impl.util :as util]
            [com.biffweb.sqlite.impl.validate :as validate]
            [honey.sql :as hsql]
            [next.jdbc :as jdbc]))

(def ^:private find-primary-key
  "Find the primary key column for the given table keyword from normalized columns. Memoized."
  (memoize
   (fn [normalized-columns table-kw]
     (let [pk-col (first (filter (fn [col]
                                   (and (= table-kw (util/col-table (:id col)))
                                        (:primary-key col)))
                                  normalized-columns))]
       (when pk-col (:id pk-col))))))

(defn- extract-table
  "Extract the table keyword from a HoneySQL statement map."
  [stmt]
  (cond
    (:insert-into stmt) (let [target (:insert-into stmt)]
                          (if (keyword? target)
                            target
                            (if (vector? target)
                              (first target)
                              target)))
    (:update stmt)      (:update stmt)
    (:delete-from stmt) (:delete-from stmt)))

(defn- format-and-coerce
  "Format a HoneySQL map to a SQL vector and apply write coercions."
  [stmt enum-val->int]
  (let [sql-vec (hsql/format stmt)]
    (into [(first sql-vec)] (coerce/coerce-params enum-val->int (rest sql-vec)))))

(defn- execute-sql!
  "Execute a SQL vector on a connection with the given builder-fn."
  [conn sql-vec builder-fn]
  (jdbc/execute! conn sql-vec {:builder-fn builder-fn}))

(defn- process-insert!
  "Process a plain INSERT statement (no :on-conflict): add :returning :*, execute, return diff entries."
  [conn stmt builder-fn enum-val->int]
  (let [table-kw (extract-table stmt)
        returning-stmt (assoc stmt :returning [:*])
        sql-vec (format-and-coerce returning-stmt enum-val->int)
        results (execute-sql! conn sql-vec builder-fn)]
    (mapv (fn [row]
            {:table table-kw
             :op :create
             :before nil
             :after (into {} row)})
          results)))

(defn- process-delete!
  "Process a DELETE statement: add :returning :*, execute, return diff entries."
  [conn stmt builder-fn enum-val->int]
  (let [table-kw (extract-table stmt)
        returning-stmt (assoc stmt :returning [:*])
        sql-vec (format-and-coerce returning-stmt enum-val->int)
        results (execute-sql! conn sql-vec builder-fn)]
    (mapv (fn [row]
            {:table table-kw
             :op :delete
             :before (into {} row)
             :after nil})
          results)))

(defn- process-update!
  "Process an UPDATE or INSERT...ON CONFLICT statement:
   1. Execute the write statement with :returning :* to get after-values
   2. Extract primary keys from the results
   3. Query before-conn for the original records
   4. Pair before/after by primary key to generate diff entries"
  [write-tx before-tx stmt normalized-columns builder-fn enum-val->int]
  (let [table-kw (extract-table stmt)
        pk-key (find-primary-key normalized-columns table-kw)]
    (when-not pk-key
      (throw (ex-info "authorized-write requires a primary key for UPDATE/upsert statements."
                      {:table table-kw})))
    (let [;; Query before-conn for original records using the primary keys
          ;; We need to get the before state BEFORE executing the write
          before-pks (when (:update stmt)
                       (let [select-stmt (cond-> {:select [pk-key]
                                                  :from table-kw
                                                  :where (:where stmt)}
                                           (:order-by stmt) (assoc :order-by (:order-by stmt))
                                           (:limit stmt) (assoc :limit (:limit stmt)))
                             select-sql (format-and-coerce select-stmt enum-val->int)]
                         (mapv #(get % pk-key) (execute-sql! write-tx select-sql builder-fn))))
          ;; For INSERT...ON CONFLICT, get the PKs from the values being inserted
          before-pks (or before-pks
                        (when (:insert-into stmt)
                          (let [pk-name (name pk-key)
                                pk-col-kw (keyword (name table-kw) pk-name)]
                            (->> (:values stmt)
                                 (map #(get % pk-col-kw))
                                 (filter some?)
                                 vec))))
          ;; Query before-conn for original records
          before-rows (when (seq before-pks)
                        (execute-sql! before-tx
                                      (format-and-coerce {:select [:*]
                                                          :from table-kw
                                                          :where [:in pk-key before-pks]}
                                                         enum-val->int)
                                      builder-fn))
          before-by-pk (into {} (map (fn [row] [(get row pk-key) (into {} row)])) before-rows)
          ;; Execute the actual write with :returning :*
          returning-stmt (assoc stmt :returning [:*])
          write-sql (format-and-coerce returning-stmt enum-val->int)
          after-rows (execute-sql! write-tx write-sql builder-fn)
          after-by-pk (into {} (map (fn [row] [(get row pk-key) (into {} row)])) after-rows)
          ;; All PKs involved
          all-pks (distinct (concat (keys before-by-pk) (keys after-by-pk)))]
      (into []
       (mapcat
        (fn [pk]
          (let [before (get before-by-pk pk)
                after (get after-by-pk pk)]
            (cond
              (and before after)
              [{:table table-kw :op :update :before before :after after}]

              (and before (not after))
              [{:table table-kw :op :delete :before before :after nil}]

              (and after (not before))
              [{:table table-kw :op :create :before nil :after after}]))))
       all-pks))))

(defn- classify-statement
  "Classify a HoneySQL statement as :insert, :upsert, :update, or :delete.
   Throws if the statement is not a write statement."
  [stmt]
  (cond
    (not (map? stmt))
    (throw (ex-info "authorized-write only accepts HoneySQL maps."
                    {:input stmt}))

    (and (:insert-into stmt) (:on-conflict stmt)) :upsert
    (:insert-into stmt) :insert
    (:update stmt)      :update
    (:delete-from stmt) :delete

    :else
    (throw (ex-info "authorized-write only accepts INSERT, UPDATE, or DELETE statements."
                    {:statement stmt}))))

(defn authorized-write!
  "Execute a write statement within a transaction, generating a diff and checking
   authorization. Returns the diff if authorized.

   Opens a read transaction (before-conn) and a write transaction (after-conn).
   Both are added to ctx before calling authorize-fn, so it can query the
   database state before and after the write.

   Parameters:
   - ctx: the system context map (must contain :biff.sqlite/write-conn, :biff.sqlite/read-pool,
          :biff.sqlite/columns, and :biff.sqlite/authorize)
   - input: a HoneySQL map (INSERT, UPDATE, DELETE, or INSERT...ON CONFLICT)"
  [ctx input]
  (let [{:biff.sqlite/keys [columns write-conn read-pool authorize]} ctx
        columns (or columns {})
        {:keys [builder-fn enum-val->int normalized-columns]} (coerce/memoized-coercions columns)]
    (validate/validate-honeysql-input! normalized-columns input)
    (let [stmt-type (classify-statement input)]
      (jdbc/with-transaction [read-tx read-pool]
        (jdbc/with-transaction [write-tx write-conn {:isolation :serializable}]
          (let [diff (case stmt-type
                       :insert (process-insert! write-tx input builder-fn enum-val->int)
                       :delete (process-delete! write-tx input builder-fn enum-val->int)
                       (:update :upsert) (process-update! write-tx read-tx input normalized-columns builder-fn enum-val->int))
                auth-ctx (assoc ctx
                                :biff.sqlite/before-conn read-tx
                                :biff.sqlite/after-conn write-tx)]
            (when-not (authorize auth-ctx diff)
              (throw (ex-info "Write rejected by authorization rules."
                              {:diff diff})))
            diff))))))
