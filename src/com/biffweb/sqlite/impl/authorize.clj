(ns com.biffweb.sqlite.impl.authorize
  "Internal implementation for authorized write transactions.
   Generates diff data structures and manages transaction rollback."
  (:require [com.biffweb.sqlite.impl.coerce :as coerce]
            [com.biffweb.sqlite.impl.util :as util]
            [com.biffweb.sqlite.impl.validate :as validate]
            [honey.sql :as hsql]
            [next.jdbc :as jdbc]))

(def ^:private find-primary-keys
  "Find the primary key columns for the given table keyword from normalized columns. Memoized."
  (memoize
   (fn [normalized-columns table-kw]
      (let [table-cols (filter (fn [col]
                                 (= table-kw (util/col-table (:id col))))
                               normalized-columns)
            pk-cols (into []
                          (comp (filter :primary-key)
                                (map :id))
                          table-cols)]
        (or (not-empty pk-cols)
            (some (fn [col]
                    (when-let [others (:primary-key-with col)]
                      (vec (into [(:id col)] others))))
                  table-cols))))))

(defn- pk-token [pk-keys row]
  (if (= 1 (count pk-keys))
    (get row (first pk-keys))
    (mapv row pk-keys)))

(defn- pk-where-clause [pk-keys tokens]
  (when (seq tokens)
    (if (= 1 (count pk-keys))
      [:in (first pk-keys) (vec tokens)]
      (into [:or]
            (map (fn [token]
                   (into [:and]
                         (map (fn [pk-key value]
                                [:= pk-key value])
                              pk-keys
                              token))))
            tokens))))

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
   1. Execute the write statement with :returning :* on write-tx to get after-values
   2. Extract primary keys from the results
   3. Query read-tx for the original records using those primary keys
   4. Pair before/after by primary key to generate diff entries"
  [read-tx write-tx stmt normalized-columns builder-fn enum-val->int]
  (let [table-kw (extract-table stmt)
        pk-keys (find-primary-keys normalized-columns table-kw)]
    (when-not (seq pk-keys)
      (throw (ex-info "authorized-write requires a primary key for UPDATE/upsert statements."
                      {:table table-kw})))
    (let [;; Execute the write with :returning :* on the write transaction
          returning-stmt (assoc stmt :returning [:*])
          write-sql (format-and-coerce returning-stmt enum-val->int)
          after-rows (execute-sql! write-tx write-sql builder-fn)
          after-by-pk (into {} (map (fn [row] [(pk-token pk-keys row) (into {} row)])) after-rows)
          ;; Query the read transaction for before-values using the PKs from the write result
          pks (vec (keys after-by-pk))
          before-rows (when (seq pks)
                        (let [select-stmt {:select [:*]
                                           :from table-kw
                                           :where (pk-where-clause pk-keys pks)}
                              select-sql (format-and-coerce select-stmt enum-val->int)]
                          (execute-sql! read-tx select-sql builder-fn)))
          before-by-pk (into {} (map (fn [row] [(pk-token pk-keys row) (into {} row)])) before-rows)
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
   Throws if the statement is not a write statement, or if it uses REPLACE."
  [stmt]
  (cond
    (not (map? stmt))
    (throw (ex-info "authorized-write only accepts HoneySQL maps."
                    {:input stmt}))

    (:replace-into stmt)
    (throw (ex-info "authorized-write does not support REPLACE INTO statements. Use INSERT ... ON CONFLICT instead."
                    {:statement stmt}))

    (and (:insert-into stmt) (:on-conflict stmt)) :upsert
    (:insert-into stmt) :insert
    (:update stmt)      :update
    (:delete-from stmt) :delete

    :else
    (throw (ex-info "authorized-write only accepts INSERT, UPDATE, or DELETE statements."
                    {:statement stmt}))))

(defn- validate-no-pk-changes!
  "Validate that the statement does not attempt to change primary key columns.
   For UPDATE: asserts that :set is a map with keyword keys, none of which are primary keys.
   For UPSERT: asserts that :do-update-set is a vector of keywords not containing primary keys."
  [stmt stmt-type normalized-columns]
  (let [table-kw (extract-table stmt)
        pk-keys (find-primary-keys normalized-columns table-kw)]
    (when (seq pk-keys)
      (case stmt-type
        :update
        (let [set-val (:set stmt)]
          (when-not (map? set-val)
            (throw (ex-info "authorized-write UPDATE requires :set to be a map."
                            {:set set-val})))
          (when-not (every? keyword? (keys set-val))
            (throw (ex-info "authorized-write UPDATE requires all :set keys to be keywords."
                            {:set-keys (keys set-val)})))
          (when (some #(contains? set-val %) pk-keys)
            (throw (ex-info (str "authorized-write does not allow changing primary key columns. "
                                 "Found primary key(s) " pk-keys " in :set.")
                            {:primary-keys pk-keys :set-keys (keys set-val)}))))

        :upsert
        (let [update-set (:do-update-set stmt)]
          (when-not (vector? update-set)
            (throw (ex-info "authorized-write UPSERT requires :do-update-set to be a vector."
                            {:do-update-set update-set})))
          (when-not (every? keyword? update-set)
            (throw (ex-info "authorized-write UPSERT requires all :do-update-set entries to be keywords."
                            {:do-update-set update-set})))
          (when (some (set pk-keys) update-set)
            (throw (ex-info (str "authorized-write does not allow changing primary key columns. "
                                 "Found primary key(s) " pk-keys " in :do-update-set.")
                            {:primary-keys pk-keys :do-update-set update-set}))))

        nil))))

(defn authorized-write!
  "Execute a write statement within a transaction, generating a diff and checking
   authorization. Returns the diff if authorized.

   Opens a read transaction (before-conn) and a write transaction (after-conn).
   Both are added to ctx before calling authorize-fn, so it can query the
   database state before and after the write.

   Primary key changes are not allowed in UPDATE or UPSERT statements.
   REPLACE INTO statements are rejected.

   Parameters:
   - ctx: the system context map (must contain :biff.sqlite/write-conn, :biff.sqlite/read-pool,
          :biff.sqlite/columns, and :biff.sqlite/authorize)
   - input: a HoneySQL map (INSERT, UPDATE, DELETE, or INSERT...ON CONFLICT)"
  [ctx input]
  (let [{:biff.sqlite/keys [columns write-conn read-pool authorize]} ctx
        columns (or columns {})
        {:keys [builder-fn enum-val->int normalized-columns]} (coerce/memoized-coercions columns)]
    (let [stmt-type (classify-statement input)]
      (validate-no-pk-changes! input stmt-type normalized-columns)
      (validate/validate-honeysql-input! normalized-columns input)
      (jdbc/with-transaction [read-tx read-pool]
        ;; Establish the read snapshot before opening the write transaction
        (jdbc/execute! read-tx ["SELECT 1"])
        (jdbc/with-transaction [write-tx write-conn {:isolation :serializable}]
          (let [diff (case stmt-type
                       :insert (process-insert! write-tx input builder-fn enum-val->int)
                       :delete (process-delete! write-tx input builder-fn enum-val->int)
                       (:update :upsert) (process-update! read-tx write-tx input normalized-columns builder-fn enum-val->int))
                auth-ctx (assoc ctx
                                :biff.sqlite/before-conn read-tx
                                :biff.sqlite/after-conn write-tx)]
            (when-not (authorize auth-ctx diff)
              (throw (ex-info "Write rejected by authorization rules."
                              {:diff diff})))
            diff))))))
