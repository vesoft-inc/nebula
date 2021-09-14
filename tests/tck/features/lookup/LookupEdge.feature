Feature: Test lookup on edge index
  Examples:
    | where_condition                                                                       |
    | lookup_edge_1.col1 == 201                                                             |
    | lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 == 201                               |
    | lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 >= 201                               |
    | lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 != 200                               |
    | lookup_edge_1.col1 >= 201 AND lookup_edge_1.col2 == 201                               |
    | lookup_edge_1.col1 >= 201 AND lookup_edge_1.col1 <= 201                               |
    | lookup_edge_1.col1 != 202 AND lookup_edge_1.col2 >= 201                               |
    | lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 == 201 AND lookup_edge_1.col3 == 201 |
    | lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 == 201 AND lookup_edge_1.col3 >= 201 |
    | lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 == 201 AND lookup_edge_1.col3 != 200 |
    | lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 >= 201 AND lookup_edge_1.col3 == 201 |
    | lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 >= 201 AND lookup_edge_1.col3 >= 201 |
    | lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 >= 201 AND lookup_edge_1.col3 != 200 |
    | lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 != 200 AND lookup_edge_1.col3 == 201 |
    | lookup_edge_1.col1 >= 201 AND lookup_edge_1.col2 == 201 AND lookup_edge_1.col3 == 201 |
    | lookup_edge_1.col1 >= 201 AND lookup_edge_1.col2 >= 201 AND lookup_edge_1.col3 == 201 |
    | lookup_edge_1.col1 >= 201 AND lookup_edge_1.col2 != 200 AND lookup_edge_1.col3 == 201 |
    | lookup_edge_1.col1 != 200 AND lookup_edge_1.col2 != 200 AND lookup_edge_1.col3 == 201 |
    | lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 >= 201 AND lookup_edge_1.col1 == 201 |
    | lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 >= 201 AND lookup_edge_1.col1 >= 201 |
    | lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 > 200 AND lookup_edge_1.col1 == 201  |
    | lookup_edge_1.col1 == 201 AND lookup_edge_1.col2 > 200 AND lookup_edge_1.col1 > 200   |
    | lookup_edge_1.col1 == 201 OR lookup_edge_1.col2 == 201                                |
    | lookup_edge_1.col1 == 201 OR lookup_edge_1.col2 >= 203                                |
    | lookup_edge_1.col1 == 201 OR lookup_edge_1.col3 == 201                                |

  Scenario Outline: [edge] different condition and yield test for string vid
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | fixed_string(16) |
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
        '200' -> '201'@0:(201, 201, 201),
        '200' -> '202'@0:(202, 202, 202)
      """
    When executing query:
      """
      LOOKUP ON
        lookup_edge_1
      WHERE
        <where_condition>
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | '200'  | '201'  | 0       |
    When executing query:
      """
      LOOKUP ON
        lookup_edge_1
      WHERE
        <where_condition>
      YIELD
        lookup_edge_1.col1 AS col1,
        lookup_edge_1.col2 AS col2,
        lookup_edge_1.col3
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | col1 | col2 | lookup_edge_1.col3 |
      | '200'  | '201'  | 0       | 201  | 201  | 201                |
    Then drop the used space

  Scenario Outline: [edge] different condition and yield test for int vid
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9     |
      | replica_factor | 1     |
      | vid_type       | int64 |
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
        200 -> 201@0:(201, 201, 201),
        200 -> 202@0:(202, 202, 202)
      """
    When executing query:
      """
      LOOKUP ON
        lookup_edge_1
      WHERE
        <where_condition>
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | 200    | 201    | 0       |
    When executing query:
      """
      LOOKUP ON
        lookup_edge_1
      WHERE
        <where_condition>
      YIELD
        lookup_edge_1.col1 AS col1,
        lookup_edge_1.col2 AS col2,
        lookup_edge_1.col3
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | col1 | col2 | lookup_edge_1.col3 |
      | 200    | 201    | 0       | 201  | 201  | 201                |
    Then drop the used space

# TODO(yee): Test bool expression
