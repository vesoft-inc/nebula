Feature: Fetch empty prop vertices

  Scenario: fetch on empty prop vertices
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1               |
      | replica_factor | 1               |
      | vid_type       | FIXED_STRING(2) |
    And having executed:
      """
      CREATE TAG empty_tag_0();
      CREATE TAG empty_tag_1();
      CREATE EDGE empty_edge();
      """
    And wait 3 seconds
    When executing query:
      """
      INSERT VERTEX empty_tag_0() values "1":(), "2":();
      INSERT VERTEX empty_tag_1() values "1":(), "2":();
      INSERT EDGE empty_edge() values "1"->"2":();
      """
    Then the execution should be successful
    # fetch emtpy prop tag on vertex
    When executing query:
      """
      FETCH PROP ON empty_tag_0 "1"
      """
    Then the result should include:
      | VertexID |
      | "1"      |
    When executing query:
      """
      FETCH PROP ON * "1"
      """
    Then the result should include:
      | VertexID |
      | "1"      |
    # fetch emtpy prop edgetype on edge
    When executing query:
      """
      FETCH PROP ON empty_edge "1"->"2"
      """
    Then the result should include:
      | empty_edge._src | empty_edge._dst | empty_edge._rank |
      | "1"             | "2"             | 0                |
    # fetch from input
    When executing query:
      """
      GO FROM "1" OVER empty_edge YIELD empty_edge._dst as id
      | FETCH PROP ON empty_tag_0 $-.id
      """
    Then the result should include:
      | VertexID |
      | "2"      |
    When executing query:
      """
      GO FROM "1" OVER empty_edge YIELD empty_edge._src as src, empty_edge._dst as dst
      | FETCH PROP ON empty_edge $-.src->$-.dst
      """
    Then the result should include:
      | empty_edge._src | empty_edge._dst | empty_edge._rank |
      | "1"             | "2"             | 0                |
