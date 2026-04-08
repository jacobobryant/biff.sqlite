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
  "Process an INSERT statement: add :returning :*, execute, return diff entries.
   Throws if the statement includes :on-conflict."
  [conn stmt builder-fn enum-val->int]
  (when (or (:on-conflict stmt) (some #{:on-conflict} (keys stmt)))
    (throw (ex-info "authorized-write does not support INSERT ... ON CONFLICT statements. Split into separate INSERT and UPDATE statements."
                    {:statement stmt})))
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
  "Process an UPDATE statement:
   1. Convert to SELECT :* to get before-values
   2. Execute the UPDATE with :returning :*
   3. Group by primary key to pair before/after
   4. Handle primary key changes (treat as delete + insert)"
  [conn stmt normalized-columns builder-fn enum-val->int]
  (let [table-kw (extract-table stmt)
        pk-key (find-primary-key normalized-columns table-kw)]
    (when-not pk-key
      (throw (ex-info "authorized-write requires a primary key for UPDATE statements."
                      {:table table-kw})))
    (let [;; Build a SELECT to get before-values using the same WHERE clause
          select-stmt (cond-> {:select [:*]
                              :from table-kw
                              :where (:where stmt)}
                       (:order-by stmt) (assoc :order-by (:order-by stmt))
                       (:limit stmt) (assoc :limit (:limit stmt)))
          select-sql (format-and-coerce select-stmt enum-val->int)
          before-rows (execute-sql! conn select-sql builder-fn)
          before-by-pk (into {} (map (fn [row] [(get row pk-key) (into {} row)])) before-rows)
          ;; Execute the actual UPDATE with :returning :*
          returning-stmt (assoc stmt :returning [:*])
          update-sql (format-and-coerce returning-stmt enum-val->int)
          after-rows (execute-sql! conn update-sql builder-fn)
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
  "Classify a HoneySQL statement as :insert, :update, or :delete.
   Throws if the statement is not a write statement."
  [stmt]
  (cond
    (not (map? stmt))
    (throw (ex-info "authorized-write only accepts HoneySQL maps."
                    {:input stmt}))

    (:insert-into stmt) :insert
    (:update stmt)      :update
    (:delete-from stmt) :delete

    :else
    (throw (ex-info "authorized-write only accepts INSERT, UPDATE, or DELETE statements."
                    {:statement stmt}))))

(defn authorized-write!
  "Execute a write statement within a transaction, generating a diff and checking
   authorization. Returns the diff if authorized.

   Parameters:
   - conn: the write connection
   - columns: the raw columns map (keyword -> props)
   - authorize-fn: (fn [ctx diff] ...) — must return truthy to allow the write
   - ctx: the system context map
   - input: a HoneySQL map (INSERT, UPDATE, or DELETE)"
  [conn columns authorize-fn ctx input]
  (let [{:keys [builder-fn enum-val->int normalized-columns]} (coerce/memoized-coercions columns)]
    (validate/validate-honeysql-input! normalized-columns input)
    (let [stmt-type (classify-statement input)]
      (jdbc/with-transaction [tx conn {:isolation :serializable}]
        (let [diff (case stmt-type
                     :insert (process-insert! tx input builder-fn enum-val->int)
                     :delete (process-delete! tx input builder-fn enum-val->int)
                     :update (process-update! tx input normalized-columns builder-fn enum-val->int))]
          (when-not (authorize-fn ctx diff)
            (throw (ex-info "Write rejected by authorization rules."
                            {:diff diff})))
          diff)))))
