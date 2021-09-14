Feature: Test lookup on tag index
  Examples:
    | where_condition                                                                    |
    | lookup_tag_1.col1 == 201                                                           |
    | lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 == 201                              |
    | lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 >= 200                              |
    | lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 != 200                              |
    | lookup_tag_1.col1 >= 201 AND lookup_tag_1.col2 == 201                              |
    | lookup_tag_1.col1 >= 201 AND lookup_tag_1.col1 <= 201                              |
    | lookup_tag_1.col1 >= 201 AND lookup_tag_1.col2 != 202                              |
    | lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 == 201 AND lookup_tag_1.col3 == 201 |
    | lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 >= 201 AND lookup_tag_1.col3 == 201 |
    | lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 >= 201 AND lookup_tag_1.col3 >= 201 |
    | lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 >= 201 AND lookup_tag_1.col3 != 202 |
    | lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 != 202 AND lookup_tag_1.col3 == 201 |
    | lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 != 202 AND lookup_tag_1.col3 >= 201 |
    | lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 != 202 AND lookup_tag_1.col3 != 202 |
    | lookup_tag_1.col1 != 202 AND lookup_tag_1.col2 == 201 AND lookup_tag_1.col3 == 201 |
    | lookup_tag_1.col1 != 202 AND lookup_tag_1.col2 == 201 AND lookup_tag_1.col3 >= 201 |
    | lookup_tag_1.col1 != 202 AND lookup_tag_1.col2 >= 201 AND lookup_tag_1.col3 >= 201 |
    | lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 >= 201 AND lookup_tag_1.col1 == 201 |
    | lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 >= 201 AND lookup_tag_1.col1 >= 201 |
    | lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 > 200 AND lookup_tag_1.col1 == 201  |
    | lookup_tag_1.col1 == 201 AND lookup_tag_1.col2 > 200 AND lookup_tag_1.col1 > 200   |
    | lookup_tag_1.col1 == 201 OR lookup_tag_1.col2 == 201                               |
    | lookup_tag_1.col1 == 201 OR lookup_tag_1.col2 >= 203                               |
    | lookup_tag_1.col1 == 201 OR lookup_tag_1.col3 == 201                               |

  Scenario Outline: [tag] different condition and yield test for string vid
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | fixed_string(16) |
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
        '200':(200, 200, 200),
        '201':(201, 201, 201),
        '202':(202, 202, 202);
      """
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        <where_condition>
      """
    Then the result should be, in any order:
      | VertexID |
      | '201'    |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        <where_condition>
      YIELD
        lookup_tag_1.col1,
        lookup_tag_1.col2,
        lookup_tag_1.col3
      """
    Then the result should be, in any order:
      | VertexID | lookup_tag_1.col1 | lookup_tag_1.col2 | lookup_tag_1.col3 |
      | '201'    | 201               | 201               | 201               |
    Then drop the used space

  Scenario Outline: [tag] different condition and yield test for int vid
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9     |
      | replica_factor | 1     |
      | vid_type       | int64 |
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
        200:(200, 200, 200),
        201:(201, 201, 201),
        202:(202, 202, 202);
      """
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        <where_condition>
      """
    Then the result should be, in any order:
      | VertexID |
      | 201      |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        <where_condition>
      YIELD
        lookup_tag_1.col1,
        lookup_tag_1.col2,
        lookup_tag_1.col3
      """
    Then the result should be, in any order:
      | VertexID | lookup_tag_1.col1 | lookup_tag_1.col2 | lookup_tag_1.col3 |
      | 201      | 201               | 201               | 201               |
    Then drop the used space

# TODO(yee): Test bool expression
