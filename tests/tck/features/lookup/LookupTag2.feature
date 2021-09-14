Feature: Test lookup on tag index 2
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
      CREATE TAG lookup_tag_1(col1 int, col2 int, col3 int);
      CREATE TAG INDEX t_index_1 ON lookup_tag_1(col1, col2, col3);
      CREATE TAG INDEX t_index_3 ON lookup_tag_1(col2, col3);
      """
    And wait 6 seconds
    And having executed:
      """
      INSERT VERTEX
        lookup_tag_1(col1, col2, col3)
      VALUES
        <id_200>:(200, 200, 200),
        <id_201>:(201, 201, 201),
        <id_202>:(202, 202, 202);
      """

  Scenario Outline: [tag] simple tag test cases
    When executing query:
      """
      LOOKUP ON lookup_tag_1 WHERE col1 == 200;
      """
    Then a SemanticError should be raised at runtime: Expression (col1==200) not supported yet
    When executing query:
      """
      LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 == 200 OR lookup_tag_1.col5 == 20;
      """
    Then a SemanticError should be raised at runtime: Invalid column: col5
    When executing query:
      """
      LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 == 201 OR lookup_tag_1.col2 == 201 AND lookup_tag_1.col3 == 202
      """
    Then a SemanticError should be raised at runtime: Not supported filter
    When executing query:
      """
      LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 == 300
      """
    Then the result should be, in any order:
      | VertexID |
    When executing query:
      """
      lookup on lookup_tag_1 WHERE lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 > 200 AND lookup_tag_1.col1 > 201
      """
    Then the result should be, in any order:
      | VertexID |
    Then drop the used space

  Scenario Outline: [tag] scan without hints
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 != 200
      """
    Then the result should be, in any order:
      | VertexID |
      | <id_201> |
      | <id_202> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 != 200
      YIELD
        lookup_tag_1.col1 AS col1,
        lookup_tag_1.col3
      """
    Then the result should be, in any order:
      | VertexID | col1 | lookup_tag_1.col3 |
      | <id_201> | 201  | 201               |
      | <id_202> | 202  | 202               |
    Then drop the used space

# TODO(yee): Test bool expression
