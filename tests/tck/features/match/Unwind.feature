Feature: Unwind clause

  Background:
    Given a graph with space named "nba"

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

  Scenario: unwind match with
    When executing query:
      """
      MATCH p = (x:player{name: "Tim Duncan"})-[:like*..2]->(y)
      UNWIND nodes(p) as n
      WITH p, size(collect(distinct n)) AS testLength
      WHERE testLength == length(p) + 1
      RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                                                                                                                                                                                                         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                                                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                                                         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})-[:like@0 {likeness: 90}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})> |

  Scenario: unwind invalid expression
    When executing query:
      """
      UNWIND collect([1,2,3]) as n return n
      """
    Then a SemanticError should be raised at runtime: Can't use aggregating expressions in unwind clause, `collect([1,2,3])'
    When executing query:
      """
      LOOKUP on player YIELD id(vertex) as id |
      GO 1 TO 3 STEPS FROM $-.id OVER * BIDIRECT YIELD DISTINCT src(edge) as src_id, dst(edge) as dst_id |
      UNWIND collect($-.src_id) + collect($-.dst_id) as vid
      WITH DISTINCT vid
      RETURN collect(vid) as vids
      """
    Then a SyntaxError should be raised at runtime: syntax error near `WITH'
    When executing query:
      """
      MATCH (a:player {name:"Tim Duncan"}) - [e:like] -> (b)
      UNWIND count(b) as num
      RETURN  num
      """
    Then a SemanticError should be raised at runtime: Can't use aggregating expressions in unwind clause, `count(b)'

  Scenario: unwind vtp edge expression
    When executing query:
      """
      match (v:player)
      where v.player.name in ["Tim Duncan"]
      unwind v.player.age as age
      return age;
      """
    Then the result should be, in any order:
      | age |
      | 42  |
    When executing query:
      """
      match (v:player)-[e:like]-()
      where v.player.name in ["Tim Duncan"]
      unwind e.likeness as age
      return age, e
      """
    Then the result should be, in any order:
      | age | e                                                           |
      | 55  | [:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]   |
      | 70  | [:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]       |
      | 80  | [:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]    |
      | 90  | [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]     |
      | 95  | [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]     |
      | 80  | [:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]        |
      | 75  | [:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}] |
      | 80  | [:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]       |
      | 95  | [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]       |
      | 95  | [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]       |
      | 99  | [:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]   |
      | 80  | [:like "Shaquille O'Neal"->"Tim Duncan" @0 {likeness: 80}]  |
