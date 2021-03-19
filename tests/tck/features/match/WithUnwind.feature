Feature: With clause and Unwind clause

  Background:
    Given a graph with space named "nba"

  Scenario: with return
    When executing query:
      """
      WITH [1, 2, 3] AS a, "hello" AS b
      RETURN a, b
      """
    Then the result should be, in any order:
      | a       | b       |
      | [1,2,3] | "hello" |
    When executing query:
      """
      WITH [1, 2, 3] AS a WITH a AS a, "hello" AS b
      RETURN a, b
      """
    Then the result should be, in any order, with relax comparison:
      | a       | b       |
      | [1,2,3] | "hello" |
    When executing query:
      """
      WITH [1, 2, 3] AS a
      WITH a, "hello" AS b
      RETURN a, b
      """
    Then the result should be, in any order, with relax comparison:
      | a       | b       |
      | [1,2,3] | "hello" |
    When executing query:
      """
      WITH [1, 2, 3] AS a
      WITH a, "hello"
      RETURN a
      """
    Then a SemanticError should be raised at runtime: Expression in WITH must be aliased (use AS)

  Scenario: with agg return
    When executing query:
      """
      WITH collect([0, 0.0, 100]) AS n
      RETURN n
      """
    Then the result should be, in any order:
      | n               |
      | [[0, 0.0, 100]] |
    When executing query:
      """
      WITH [1, 2, 3] AS a
      RETURN count(a)
      """
    Then the result should be, in any order:
      | count(a) |
      | 1        |

  Scenario: match with return
    When executing query:
      """
      MATCH (v :player{name:"Tim Duncan"})-[]-(v2)
      WITH avg(v2.age) as average_age
      RETURN average_age
      """
    Then the result should be, in any order, with relax comparison:
      | average_age        |
      | 35.888888888888886 |
    When executing query:
      """
      MATCH (v :player{name:"Tim Duncan"})-[]-(v2)-[]-(v3)
      WITH v3.name as names
      RETURN count(names)
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT(names) |
      | 191          |
    When executing query:
      """
      MATCH (v :player{name:"Tim Duncan"})-[]-(v2)
      WITH distinct(v2.name) AS names
      ORDER by names DESC LIMIT 5
      RETURN collect(names)
      """
    Then the result should be, in any order, with relax comparison:
      | COLLECT(names)                                                                   |
      | ["Tony Parker", "Tiago Splitter", "Spurs", "Shaquile O'Neal", "Marco Belinelli"] |

  @skip
  Scenario: with match return
    When executing query:
      """
      WITH "Tony Parker" AS a
      MATCH (v:player{name: a})
      RETURN v.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 23  |

  Scenario: unwind return
    When executing query:
      """
      UNWIND [1, 2, 3] AS a
      RETURN a
      """
    Then the result should be, in any order:
      | a |
      | 1 |
      | 2 |
      | 3 |
    When executing query:
      """
      UNWIND [1, NULL, 3] AS a
      RETURN a
      """
    Then the result should be, in any order:
      | a    |
      | 1    |
      | NULL |
      | 3    |
    When executing query:
      """
      UNWIND [1, [2, 3, NULL, 4], 5] AS a
      RETURN a
      """
    Then the result should be, in any order:
      | a               |
      | 1               |
      | [2, 3, NULL, 4] |
      | 5               |
    When executing query:
      """
      UNWIND [1, [2, 3, NULL, 4], 5] AS a
      UNWIND a AS b
      RETURN b
      """
    Then the result should be, in any order:
      | b    |
      | 1    |
      | 2    |
      | 3    |
      | NULL |
      | 4    |
      | 5    |
    When executing query:
      """
      UNWIND [1, 2, 3] AS a
      UNWIND [4, 5] AS b
      RETURN b, a
      """
    Then the result should be, in any order:
      | b | a |
      | 4 | 1 |
      | 5 | 1 |
      | 4 | 2 |
      | 5 | 2 |
      | 4 | 3 |
      | 5 | 3 |
    When executing query:
      """
      UNWIND [1, 2, 3] AS a
      UNWIND [5, 4] AS b
      UNWIND [8, 9] AS c
      RETURN c, b, a
      """
    Then the result should be, in any order, with relax comparison:
      | c | b | a |
      | 8 | 5 | 1 |
      | 9 | 5 | 1 |
      | 8 | 4 | 1 |
      | 9 | 4 | 1 |
      | 8 | 5 | 2 |
      | 9 | 5 | 2 |
      | 8 | 4 | 2 |
      | 9 | 4 | 2 |
      | 8 | 5 | 3 |
      | 9 | 5 | 3 |
      | 8 | 4 | 3 |
      | 9 | 4 | 3 |
    When executing query:
      """
      UNWIND [[1],[2],[2,1]] AS x
      return min(x) AS min, max(x) AS max
      """
    Then the result should be, in any order:
      | min | max    |
      | [1] | [2, 1] |
    When executing query:
      """
      UNWIND [['a'],["abc"],["ab","kajk"]] AS x
      return min(x) AS min, max(x) AS max
      """
    Then the result should be, in any order:
      | min   | max     |
      | ["a"] | ["abc"] |
    When executing query:
      """
      UNWIND ['a',"abc","ab"] AS x
      return min(x) AS min, max(x) AS max
      """
    Then the result should be, in any order:
      | min | max   |
      | "a" | "abc" |
    When executing query:
      """
      UNWIND [true,false,false] AS x
      return min(x) AS min, max(x) AS max
      """
    Then the result should be, in any order:
      | min   | max  |
      | false | true |

  Scenario: with unwind return
    When executing query:
      """
      WITH [1, 2, 3] AS a, "hello" AS b
      UNWIND a as c
      RETURN c
      """
    Then the result should be, in any order, with relax comparison:
      | c |
      | 1 |
      | 2 |
      | 3 |
    When executing query:
      """
      UNWIND [1, 2, 3] AS a
      WITH "hello" AS b, a AS c
      UNWIND c as d
      RETURN b, c, d
      """
    Then the result should be, in any order, with relax comparison:
      | b       | c | d |
      | "hello" | 1 | 1 |
      | "hello" | 2 | 2 |
      | "hello" | 3 | 3 |

  Scenario: match with return
    When executing query:
      """
      MATCH (v:player)
      WITH v.age AS age, v AS v, v.name AS name
         ORDER BY age DESCENDING, name ASCENDING
         LIMIT 20
         WHERE age > 30
      RETURN v, age
      """
    Then the result should be, in order, with relax comparison:
      | v                                                                                                           | age |
      | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})                                               | 47  |
      | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         | 46  |
      | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})                                                         | 45  |
      | ("Steve Nash" :player{age: 45, name: "Steve Nash"})                                                         | 45  |
      | ("Ray Allen" :player{age: 43, name: "Ray Allen"})                                                           | 43  |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) | 42  |
      | ("Vince Carter" :player{age: 42, name: "Vince Carter"})                                                     | 42  |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   | 41  |
      | ("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})                                                   | 40  |
      | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})                                                       | 40  |
      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   | 39  |
      | ("David West" :player{age: 38, name: "David West"})                                                         | 38  |
      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})                                                         | 38  |
      | ("Yao Ming" :player{age: 38, name: "Yao Ming"})                                                             | 38  |
      | ("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})                                                       | 37  |
      | ("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})                                           | 36  |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         | 36  |
      | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       | 36  |
      | ("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})                                               | 34  |
      | ("LeBron James" :player{age: 34, name: "LeBron James"})                                                     | 34  |

  Scenario: with exists
    When executing query:
      """
      WITH {abc:123} AS m, "hello" AS b
      RETURN exists(m.abc), b
      """
    Then the result should be, in any order, with relax comparison:
      | exists(m.abc) | b       |
      | true          | "hello" |
    When executing query:
      """
      WITH {abc:123} AS m, "hello" AS b
      RETURN exists(m.abc), exists(m.a), exists({label:"hello"}.label) as t, exists({hello:123}.a)
      """
    Then the result should be, in any order, with relax comparison:
      | exists(m.abc) | exists(m.a) | t    | exists({hello:123}.a) |
      | true          | false       | true | false                 |
    When executing query:
      """
      WITH [1,2,3] AS m
      RETURN exists(m.abc)
      """
    Then the result should be, in any order, with relax comparison:
      | exists(m.abc) |
      | BAD_TYPE      |
    When executing query:
      """
      WITH null AS m
      RETURN exists(m.abc), exists((null).abc)
      """
    Then the result should be, in any order, with relax comparison:
      | exists(m.abc) | exists(NULL.abc) |
      | NULL          | NULL             |
