@lookup_update
Feature: lookup and update

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(32) |

  Scenario: LookupTest lookup and update vertex
    Given having executed:
      """
      CREATE TAG lookup_tag_1(col1 int, col2 int, col3 int);
      CREATE TAG lookup_tag_2(col1 bool, col2 int, col3 double, col4 bool);
      CREATE TAG INDEX t_index_1 ON lookup_tag_1(col1, col2, col3);
      CREATE TAG INDEX t_index_2 ON lookup_tag_2(col2, col3, col4);
      CREATE TAG INDEX t_index_3 ON lookup_tag_1(col2, col3);
      CREATE TAG INDEX t_index_4 ON lookup_tag_2(col3, col4);
      CREATE TAG INDEX t_index_5 ON lookup_tag_2(col4);
      """
    And wait 6 seconds
    When executing query and retrying it on failure every 6 seconds for 3 times:
      """
      INSERT VERTEX
        lookup_tag_1(col1, col2, col3)
      VALUES
        "200":(200, 200, 200),
        "201":(201, 201, 201),
        "202":(202, 202, 202)
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col2 == 200 YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id    |
      | "200" |
    When executing query:
      """
      LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col2 == 200 YIELD id(vertex) as id | UPDATE VERTEX ON lookup_tag_1 $-.id SET col2 = 201
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col2 == 201 YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id    |
      | "200" |
      | "201" |

  Scenario: LookupTest lookup and update edge
    Given having executed:
      """
      CREATE EDGE lookup_edge_1(col1 int, col2 int, col3 int);
      CREATE EDGE INDEX e_index_1 ON lookup_edge_1(col1, col2, col3);
      CREATE EDGE INDEX e_index_3 ON lookup_edge_1(col2, col3);
      """
    And wait 6 seconds
    When executing query and retrying it on failure every 6 seconds for 3 times:
      """
      INSERT EDGE
        lookup_edge_1(col1, col2, col3)
      VALUES
        '200' -> '201'@0:(201, 201, 201),
        '200' -> '202'@0:(202, 202, 202)
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col2 > 200 YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank |
      UPDATE EDGE ON lookup_edge_1 $-.src ->$-.dst@$-.rank SET col3 = 203
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON lookup_edge_1 YIELD
        lookup_edge_1.col1 AS col1,
        lookup_edge_1.col2 AS col2,
        lookup_edge_1.col3
      """
    Then the result should be, in any order:
      | col1 | col2 | lookup_edge_1.col3 |
      | 201  | 201  | 203                |
      | 202  | 202  | 203                |
    Then drop the used space
