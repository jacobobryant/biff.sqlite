(ns com.biffweb.sqlite.inference
  "SQL type inference for SQLite queries. Parses SQL strings using ANTLR and
   infers which table columns each result set column corresponds to."
  (:require
   [clj-antlr.core :as antlr]
   [clojure.java.io :as io]
   [clojure.string :as str]))

(def ^:private parser
  (delay (antlr/parser (slurp (io/resource "grammars/SQLite.g4"))
                       {:case-sensitive? false})))

(defn- parse-sql [sql]
  (antlr/parse @parser sql))

;; ============================================================================
;; Parse tree traversal helpers
;; ============================================================================

(defn- node? [x]
  (and (seq? x) (keyword? (first x))))

(defn- node-type [node]
  (when (node? node)
    (first node)))

(defn- children [node]
  (when (node? node)
    (rest node)))

(defn- find-children
  "Find immediate children with the given node type."
  [node type]
  (filter #(= type (node-type %)) (children node)))

(defn- find-child
  "Find first immediate child with the given node type."
  [node type]
  (first (find-children node type)))

(defn- extract-name
  "Extract the string name from an any_name, any_name_excluding_string,
   or any_name_excluding_joins node."
  [node]
  (when (node? node)
    (let [child (second node)]
      (cond
        (string? child)
        (let [s (str/trim child)]
          (cond
            ;; Quoted identifier: "foo" or `foo` or [foo]
            (and (str/starts-with? s "\"") (str/ends-with? s "\""))
            (subs s 1 (dec (count s)))

            (and (str/starts-with? s "`") (str/ends-with? s "`"))
            (subs s 1 (dec (count s)))

            (and (str/starts-with? s "[") (str/ends-with? s "]"))
            (subs s 1 (dec (count s)))

            :else (str/lower-case s)))

        ;; fallback, fallback_excluding_conflicts, join_keyword nodes
        ;; contain keyword tokens as strings
        (node? child)
        (let [text (second child)]
          (if (string? text)
            (str/lower-case text)
            (str/lower-case (apply str (filter string? (children child))))))

        :else nil))))

;; ============================================================================
;; Table extraction from FROM/JOIN clause
;; ============================================================================

(declare infer-select-core)

(defn- extract-table-info
  "Extract table name and optional alias from a table_or_subquery node.
   Returns {:name \"table\" :alias \"alias\"} or nil for subqueries without aliases.
   For subqueries with aliases, returns {:name alias :alias alias :subquery-columns [...]}."
  [tos-node]
  (let [table-name-node (find-child tos-node :table_name)
        alias-node (or (find-child tos-node :table_alias)
                       (find-child tos-node :table_alias_excluding_joins))
        subquery-node (find-child tos-node :select_stmt)]
    (cond
      table-name-node
      (let [name (extract-name (find-child table-name-node :any_name))
            alias (when alias-node
                    (extract-name (or (find-child alias-node :any_name)
                                     (find-child alias-node :any_name_excluding_joins))))]
        {:name name :alias alias})

      ;; Subquery: (SELECT ...) [AS alias]
      subquery-node
      (let [alias (when alias-node
                    (extract-name (or (find-child alias-node :any_name)
                                     (find-child alias-node :any_name_excluding_joins))))
            core (find-child subquery-node :select_core)
            sub-cols (when core (infer-select-core core))]
        {:name alias :alias alias :subquery-columns sub-cols})

      :else nil)))

(defn- extract-tables
  "Extract all tables from a join_clause node.
   Returns a list of {:name \"table\" :alias \"alias\"} maps."
  [join-clause]
  (when join-clause
    (->> (find-children join-clause :table_or_subquery)
         (keep extract-table-info))))

(defn- build-table-map
  "Build a map from alias/name -> real table name for all tables in scope."
  [tables]
  (reduce (fn [m {:keys [name alias]}]
            (cond-> (assoc m name name)
              alias (assoc alias name)))
          {}
          tables))

(defn- all-table-names
  "Get the set of all real table names."
  [tables]
  (into #{} (map :name) tables))

(defn- build-subquery-col-map
  "Build a map from {[subquery-alias col-name] -> {:column col :possible-tables tables}}
   for subquery columns, so outer queries can resolve column references through subqueries."
  [tables]
  (into {}
        (comp (filter :subquery-columns)
              (mapcat (fn [{:keys [name subquery-columns]}]
                        (sequence
                         (comp (filter :column)
                               (map (fn [{:keys [column possible-tables]}]
                                      [[name column] {:column column :possible-tables possible-tables}])))
                         subquery-columns))))
        tables))

;; ============================================================================
;; Expression inference
;; ============================================================================

(declare infer-expr)

(defn- first-child-column
  "Return the first non-nil result of infer-expr on the node's children."
  [node table-map]
  (some #(infer-expr % table-map) (children node)))

;; Functions that preserve the type of their first argument.
(def ^:private type-preserving-fns
  #{"abs" "coalesce" "ifnull" "iif" "likelihood" "likely" "max" "min"
    "nullif" "unlikely"})

(defn- infer-case
  "Infer the column from a CASE expression by returning the first THEN/ELSE
   branch that has a type-preserving column reference."
  [expr-node table-map]
  (let [kids (children expr-node)
        ;; Collect the THEN and ELSE branch expressions. In the parse tree,
        ;; THEN branches follow \"THEN\" strings and ELSE branches follow \"ELSE\".
        branch-exprs (loop [remaining kids
                            branches []]
                       (if (empty? remaining)
                         branches
                         (let [[x & more] remaining]
                           (if (and (string? x)
                                    (#{"THEN" "ELSE"} (str/upper-case x))
                                    (seq more))
                             (recur (rest more) (conj branches (first more)))
                             (recur more branches)))))]
    (some #(infer-expr % table-map) branch-exprs)))

(defn- infer-function
  "Infer the column from a function call. Type-preserving functions return
   the first argument's column; others return nil."
  [expr-node table-map]
  (let [fname-node (find-child expr-node :function_name)
        fname (when fname-node
                (extract-name (find-child fname-node :any_name_excluding_raise)))
        arg-exprs (find-children expr-node :expr)]
    (when (and fname (contains? type-preserving-fns fname))
      (some #(infer-expr % table-map) arg-exprs))))

(defn- infer-expr-recursive
  "Infer the column from an expr_recursive node, which may be a CASE expression,
   a function call, CAST, or a pass-through wrapper around expr_or."
  [expr-node table-map]
  (let [kids (children expr-node)]
    (cond
      ;; CASE expression
      (some #(and (string? %) (= "CASE" (str/upper-case %))) kids)
      (infer-case expr-node table-map)

      ;; CAST — always changes type
      (some #(and (string? %) (= "CAST" (str/upper-case %))) kids)
      nil

      ;; Function call — check if type-preserving
      (find-child expr-node :function_name)
      (infer-function expr-node table-map)

      ;; Pass-through wrapper (just wraps expr_or)
      :else
      (first-child-column expr-node table-map))))

(defn- infer-expr
  "Given a parsed expression node and the table-map (alias->real-name),
   find the first column reference whose type is preserved through the
   expression. Returns {:column \"col\" :table \"real_table\"} where :table
   may be nil if not explicitly qualified, or nil if no type-preserving
   column reference is found."
  [expr-node table-map]
  (when (node? expr-node)
    (case (node-type expr-node)
      ;; expr_base is where column references live
      :expr_base
      (let [col-name-node (find-child expr-node :column_name_excluding_string)
            table-name-node (find-child expr-node :table_name)
            col-name-with-table (find-child expr-node :column_name)]
        (cond
          ;; table.column reference
          (and table-name-node col-name-with-table)
          (let [tname (extract-name (find-child table-name-node :any_name))
                cname (extract-name (find-child col-name-with-table :any_name))
                real-table (get table-map tname)]
            {:column cname :table real-table})

          ;; bare column reference
          col-name-node
          (let [cname (extract-name (or (find-child col-name-node :any_name_excluding_string)
                                        (find-child col-name-node :any_name)))]
            {:column cname :table nil})

          ;; literal, bind parameter, subquery, etc.
          :else nil))

      ;; Type-preserving: arithmetic, string concat, unary, collate — return
      ;; the first child column whose type is preserved.
      (:expr_addition :expr_multiplication :expr_string :expr_collate :expr_unary)
      (first-child-column expr-node table-map)

      ;; expr_recursive handles CASE, CAST, function calls, and pass-through
      :expr_recursive
      (infer-expr-recursive expr-node table-map)

      ;; Pass-through wrappers — these just wrap a single sub-expression when no
      ;; actual operator is present. When an operator IS present (indicated by
      ;; string tokens like AND, OR, =, etc.) it changes the type.
      (:expr :expr_or :expr_and :expr_not :expr_comparison
       :expr_bitwise :expr_binary)
      (if (some string? (children expr-node))
        nil
        (first-child-column expr-node table-map))

      ;; Anything else — no type info
      nil)))

(defn- resolve-column
  "Given a column-ref from infer-expr, the set of all table names, and a
   subquery-col-map, resolve the column reference to its possible tables.
   If the column comes from a subquery alias, resolves through to the
   original source tables.
   Returns {:column \"col\" :possible-tables #{\"table1\" ...}}."
  [col-ref all-tables subquery-col-map]
  (if col-ref
    (let [{:keys [column table]} col-ref]
      (if-let [sub-info (get subquery-col-map [table column])]
        ;; Column comes from a subquery — resolve to original source
        sub-info
        ;; Check if an unqualified column matches any subquery column
        (if (and (nil? table) column)
          (let [matches (keep (fn [[[_alias col] info]]
                                (when (= col column) info))
                              subquery-col-map)]
            (if (seq matches)
              ;; Merge possible-tables from all matching subquery columns
              {:column column
               :possible-tables (into #{} (mapcat :possible-tables) matches)}
              {:column column :possible-tables all-tables}))
          {:column column
           :possible-tables (if table #{table} all-tables)})))
    {:column nil :possible-tables #{}}))

;; ============================================================================
;; Result column analysis
;; ============================================================================

(defn- analyze-result-column
  "Analyze a single result_column node to determine what column it maps to.
   Returns a single map with :column and :possible-tables."
  [rc-node table-map all-tables subquery-col-map]
  (let [kids (children rc-node)
        tname-node (find-child rc-node :table_name)]
    (cond
      ;; SELECT *
      (and (= 1 (count kids)) (= "*" (first kids)))
      {:column "*" :possible-tables all-tables}

      ;; SELECT table.*
      (and tname-node (some #(= "*" %) kids))
      (let [tname (extract-name (find-child tname-node :any_name))
            real-table (get table-map tname)]
        {:column "*" :possible-tables (if real-table #{real-table} all-tables)})

      ;; SELECT expr [AS alias]
      :else
      (let [expr-node (find-child rc-node :expr)
            col-ref (infer-expr expr-node table-map)]
        (resolve-column col-ref all-tables subquery-col-map)))))

;; ============================================================================
;; Core analysis
;; ============================================================================

(defn- infer-select-core
  "Analyze a single select_core node and return a vector of inferred column maps."
  [core]
  (let [join-clause (find-child core :join_clause)
        tables (extract-tables join-clause)
        table-map (build-table-map tables)
        all-tables (all-table-names tables)
        subquery-col-map (build-subquery-col-map tables)
        result-columns (find-children core :result_column)]
    (mapv #(analyze-result-column % table-map all-tables subquery-col-map)
          result-columns)))

;; ============================================================================
;; Public API
;; ============================================================================

(defn infer-columns
  "Given a SQL SELECT query string, parse it and infer the column-to-table mapping
   for each result set column. Handles UNION/INTERSECT/EXCEPT by using the first
   SELECT's columns, and resolves subqueries in FROM clauses.

   Returns a vector of maps, one per result column in the SELECT clause. Each map
   contains:
     :column          - the column name (string) if the expression preserves the
                        type of an underlying table column, \"*\" for wildcards,
                        or nil for expressions that change type (CAST, functions,
                        CASE, comparisons, etc.)
     :possible-tables - a set of table names (strings) that this column could
                        belong to

   Examples:
     (infer-columns \"SELECT age FROM user\")
     => [{:column \"age\" :possible-tables #{\"user\"}}]

     (infer-columns \"SELECT age + 1 AS foo FROM user\")
     => [{:column \"age\" :possible-tables #{\"user\"}}]

     (infer-columns \"SELECT count(id) FROM user\")
     => [{:column nil :possible-tables #{}}]"
  [sql]
  (let [tree (parse-sql sql)
        stmt (or (find-child (find-child (find-child tree :sql_stmt_list) :sql_stmt)
                             :select_stmt)
                 (throw (ex-info "Not a SELECT statement" {:sql sql})))
        ;; For UNION/INTERSECT/EXCEPT, use the first select_core
        core (find-child stmt :select_core)]
    (infer-select-core core)))
