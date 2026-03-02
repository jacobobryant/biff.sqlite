(ns com.biffweb.sqlite.inference-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.biffweb.sqlite.inference :as inf]))

(deftest simple-select-test
  (testing "bare column from single table"
    (is (= [{:column "age" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT age FROM user"))))

  (testing "qualified column from single table"
    (is (= [{:column "age" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT user.age FROM user"))))

  (testing "multiple columns from single table"
    (is (= [{:column "name" :possible-tables #{"user"}}
            {:column "age" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT name, age FROM user")))))

(deftest alias-test
  (testing "column alias does not affect inference"
    (is (= [{:column "age" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT age AS user_age FROM user"))))

  (testing "expression with alias still infers underlying column"
    (is (= [{:column "age" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT age + 1 AS foo FROM user"))))

  (testing "table alias resolves to real table name"
    (is (= [{:column "age" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT u.age FROM user AS u"))))

  (testing "table alias without AS keyword"
    (is (= [{:column "age" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT u.age FROM user u")))))

(deftest join-test
  (testing "unqualified column with join gets all possible tables"
    (is (= [{:column "name" :possible-tables #{"user" "pet"}}]
           (inf/infer-columns "SELECT name FROM user JOIN pet ON pet.owner_id = user.id"))))

  (testing "qualified columns with join resolve to specific tables"
    (is (= [{:column "name" :possible-tables #{"user"}}
            {:column "name" :possible-tables #{"pet"}}]
           (inf/infer-columns "SELECT user.name, pet.name FROM user JOIN pet ON pet.owner_id = user.id"))))

  (testing "qualified columns via aliases with join"
    (is (= [{:column "name" :possible-tables #{"user"}}
            {:column "name" :possible-tables #{"pet"}}]
           (inf/infer-columns "SELECT u.name, p.name FROM user u JOIN pet p ON p.owner_id = u.id"))))

  (testing "left join"
    (is (= [{:column "name" :possible-tables #{"user"}}
            {:column "name" :possible-tables #{"pet"}}]
           (inf/infer-columns "SELECT u.name, p.name FROM user u LEFT JOIN pet p ON p.owner_id = u.id"))))

  (testing "comma join (implicit cross join)"
    (is (= [{:column "name" :possible-tables #{"user" "pet"}}]
           (inf/infer-columns "SELECT name FROM user, pet"))))

  (testing "multiple joins"
    (is (= [{:column "name" :possible-tables #{"user"}}
            {:column "name" :possible-tables #{"pet"}}
            {:column "street" :possible-tables #{"address"}}]
           (inf/infer-columns
            "SELECT u.name, p.name, a.street FROM user u JOIN pet p ON p.owner_id = u.id JOIN address a ON a.user_id = u.id")))))

(deftest wildcard-test
  (testing "SELECT * from single table"
    (is (= [{:column "*" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT * FROM user"))))

  (testing "SELECT table.* from joined tables"
    (is (= [{:column "*" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT user.* FROM user JOIN pet ON pet.owner_id = user.id"))))

  (testing "SELECT * from joined tables"
    (is (= [{:column "*" :possible-tables #{"user" "pet"}}]
           (inf/infer-columns "SELECT * FROM user JOIN pet ON pet.owner_id = user.id")))))

(deftest expression-test
  (testing "arithmetic expression preserves type of first column"
    (is (= [{:column "age" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT age + 1 FROM user"))))

  (testing "string concatenation preserves type of first column"
    (is (= [{:column "first_name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT first_name || ' ' || last_name FROM user"))))

  (testing "CAST returns nil column since it changes the type"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT CAST(age AS TEXT) FROM user"))))

  (testing "CASE returns type of first branch with a column"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns
            "SELECT CASE WHEN age > 18 THEN name ELSE email END FROM user"))))

  (testing "literal-only expression returns nil column"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT 1 FROM user"))))

  (testing "string literal returns nil column"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT 'hello' FROM user")))))

(deftest function-test
  (testing "aggregate function returns nil column since it changes type"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT count(id) FROM user"))))

  (testing "count(*) returns nil column"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT count(*) FROM user"))))

  (testing "coalesce preserves type of first arg with a column"
    (is (= [{:column "first_name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT coalesce(first_name, last_name) FROM user")))))

(deftest where-group-order-test
  (testing "WHERE clause does not affect result column inference"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT name FROM user WHERE age > 18"))))

  (testing "GROUP BY does not affect result column inference"
    (is (= [{:column "name" :possible-tables #{"user"}}
            {:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT name, count(id) FROM user GROUP BY name"))))

  (testing "ORDER BY and LIMIT do not affect result column inference"
    (is (= [{:column "name" :possible-tables #{"user"}}
            {:column "age" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT name, age FROM user ORDER BY age DESC LIMIT 10"))))

  (testing "DISTINCT does not affect result column inference"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT DISTINCT name FROM user")))))

(deftest mixed-columns-test
  (testing "mix of qualified and unqualified columns"
    (is (= [{:column "name" :possible-tables #{"user"}}
            {:column "age" :possible-tables #{"user" "pet"}}]
           (inf/infer-columns
            "SELECT user.name, age FROM user JOIN pet ON pet.owner_id = user.id"))))

  (testing "mix of expressions and bare columns"
    (is (= [{:column "name" :possible-tables #{"user"}}
            {:column "age" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT name, age + 1 AS next_age FROM user"))))

  (testing "mix of literals and columns"
    (is (= [{:column "name" :possible-tables #{"user"}}
            {:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT name, 42 AS magic_number FROM user")))))

(deftest complex-expression-test
  (testing "arithmetic preserves type of first column"
    (is (= [{:column "price" :possible-tables #{"product"}}]
           (inf/infer-columns "SELECT price * quantity + 1 AS total FROM product"))))

  (testing "CASE returns type of first branch with a column"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns
            "SELECT CASE WHEN age >= 18 THEN name ELSE 'minor' END FROM user"))))

  (testing "cross-table concat preserves type of first column"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns
            "SELECT u.name || ' owns a ' || p.species FROM user u JOIN pet p ON p.owner_id = u.id")))))

(deftest case-expression-test
  (testing "CASE returns first THEN branch column"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns
            "SELECT CASE WHEN age > 18 THEN name ELSE email END FROM user"))))

  (testing "CASE with literal THEN falls through to ELSE column"
    (is (= [{:column "email" :possible-tables #{"user"}}]
           (inf/infer-columns
            "SELECT CASE WHEN age > 18 THEN 'adult' ELSE email END FROM user"))))

  (testing "CASE with all literal branches returns nil"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns
            "SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM user"))))

  (testing "CASE with multiple WHEN branches returns first column"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns
            "SELECT CASE WHEN age > 65 THEN 'senior' WHEN age > 18 THEN name ELSE email END FROM user"))))

  (testing "simple CASE form"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns
            "SELECT CASE status WHEN 1 THEN name WHEN 2 THEN email ELSE 'unknown' END FROM user")))))

(deftest sqlite-type-preserving-fn-test
  (testing "abs preserves type"
    (is (= [{:column "balance" :possible-tables #{"account"}}]
           (inf/infer-columns "SELECT abs(balance) FROM account"))))

  (testing "coalesce preserves type of first arg"
    (is (= [{:column "nickname" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT coalesce(nickname, name) FROM user"))))

  (testing "ifnull preserves type of first arg"
    (is (= [{:column "nickname" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT ifnull(nickname, name) FROM user"))))

  (testing "iif preserves type of first branch"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT iif(age > 18, name, email) FROM user"))))

  (testing "likelihood preserves type"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT likelihood(name, 0.5) FROM user"))))

  (testing "likely preserves type"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT likely(name) FROM user"))))

  (testing "max preserves type"
    (is (= [{:column "age" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT max(age) FROM user"))))

  (testing "min preserves type"
    (is (= [{:column "age" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT min(age) FROM user"))))

  (testing "nullif preserves type of first arg"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT nullif(name, 'N/A') FROM user"))))

  (testing "unlikely preserves type"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT unlikely(name) FROM user")))))

(deftest sqlite-type-changing-fn-test
  (testing "avg returns nil — changes type to real"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT avg(age) FROM user"))))

  (testing "count returns nil — always integer"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT count(id) FROM user"))))

  (testing "group_concat returns nil — always text"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT group_concat(name) FROM user"))))

  (testing "hex returns nil — always text"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT hex(data) FROM record"))))

  (testing "instr returns nil — always integer"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT instr(name, 'a') FROM user"))))

  (testing "length returns nil — always integer"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT length(name) FROM user"))))

  (testing "lower returns nil — always text"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT lower(name) FROM user"))))

  (testing "ltrim returns nil — always text"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT ltrim(name) FROM user"))))

  (testing "quote returns nil — always text"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT quote(name) FROM user"))))

  (testing "replace returns nil — always text"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT replace(name, 'a', 'b') FROM user"))))

  (testing "round returns nil — may change type"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT round(price) FROM product"))))

  (testing "rtrim returns nil — always text"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT rtrim(name) FROM user"))))

  (testing "substr returns nil — always text"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT substr(name, 1, 3) FROM user"))))

  (testing "sum returns nil — may change type"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT sum(amount) FROM payment"))))

  (testing "total returns nil — always real"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT total(amount) FROM payment"))))

  (testing "trim returns nil — always text"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT trim(name) FROM user"))))

  (testing "typeof returns nil — always text"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT typeof(age) FROM user"))))

  (testing "unicode returns nil — always integer"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT unicode(name) FROM user"))))

  (testing "upper returns nil — always text"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT upper(name) FROM user"))))

  (testing "zeroblob returns nil — always blob"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT zeroblob(16) FROM user"))))

  (testing "random returns nil — no column arg"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT random() FROM user"))))

  (testing "total_changes returns nil — no column arg"
    (is (= [{:column nil :possible-tables #{}}]
           (inf/infer-columns "SELECT total_changes() FROM user")))))

(deftest union-test
  (testing "UNION uses first select_core's columns"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT name FROM user UNION SELECT email FROM user"))))

  (testing "UNION ALL uses first select_core's columns"
    (is (= [{:column "name" :possible-tables #{"user"}}
            {:column "age" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT name, age FROM user UNION ALL SELECT email, id FROM auth_code"))))

  (testing "INTERSECT uses first select_core's columns"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT name FROM user INTERSECT SELECT name FROM pet"))))

  (testing "EXCEPT uses first select_core's columns"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT name FROM user EXCEPT SELECT name FROM pet"))))

  (testing "multiple UNION uses first select_core's columns"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT name FROM user UNION SELECT name FROM pet UNION SELECT name FROM account")))))

(deftest subquery-test
  (testing "subquery with alias resolves to original table"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT name FROM (SELECT name, age FROM user) AS sub"))))

  (testing "qualified column from subquery resolves to original table"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT sub.name FROM (SELECT name, age FROM user) AS sub"))))

  (testing "subquery join with real table"
    (is (= [{:column "name" :possible-tables #{"user"}}
            {:column "species" :possible-tables #{"pet"}}]
           (inf/infer-columns
            "SELECT sub.name, pet.species FROM (SELECT name, id FROM user) AS sub JOIN pet ON pet.owner_id = sub.id"))))

  (testing "subquery without alias returns no table info"
    (is (= [{:column "name" :possible-tables #{"user"}}]
           (inf/infer-columns "SELECT name FROM (SELECT name FROM user)")))))
