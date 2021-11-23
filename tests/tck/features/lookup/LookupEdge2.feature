Feature: Test lookup on edge index 2
  Examples:
    | vid_type         | id_200 | id_201 | id_202 |
    | int64            | 200    | 201    | 202    |
    | FIXED_STRING(16) | "200"  | "201"  | "202"  |

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9          |
      | replica_factor | 1          |
      | vid_type       | <vid_type> |
    And having executed:
      """
      CREATE EDGE lookup_edge_1(col1 int, col2 int, col3 int);
      CREATE EDGE INDEX e_index_1 ON lookup_edge_1(col1, col2, col3);
      CREATE EDGE INDEX e_index_3 ON lookup_edge_1(col2, col3);
      """
    And wait 6 seconds
    And having executed:
      """
      INSERT EDGE
        lookup_edge_1(col1, col2, col3)
      VALUES
        <id_200> -> <id_201>@0:(201, 201, 201),
        <id_200> -> <id_202>@0:(202, 202, 202)
      """

  Scenario Outline: [edge] Simple test cases
    When executing query:
      """
      LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 201 OR lookup_edge_1.col2 == 201 AND lookup_edge_1.col3 == 202 YIELD edge as e
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON lookup_edge_1 WHERE col1 == 201 YIELD edge as e
      """
    Then a SemanticError should be raised at runtime: Expression (col1==201) not supported yet
    When executing query:
      """
      LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 201 OR lookup_edge_1.col5 == 201 YIELD edge as e
      """
    Then a SemanticError should be raised at runtime: Invalid column: col5
    When executing query:
      """
      LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 300 YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank
      """
    Then the result should be, in any order:
      | src | dst | rank |
    When executing query:
      """
      LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 > 200 AND lookup_edge_1.col1 > 201 YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank
      """
    Then the result should be, in any order:
      | src | dst | rank |
    Then drop the used space
