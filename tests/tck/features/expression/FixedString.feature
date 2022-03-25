Feature: FixedString expression Eval

  Scenario: FixedString GO Expression
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1        |
      | replica_factor | 1        |
      | vid_type       | int64    |
      | charset        | utf8     |
      | collate        | utf8_bin |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS fixed_string_tag_1(c1 fixed_string(30));
      CREATE EDGE IF NOT EXISTS fixed_string_edge_1(c1 fixed_string(30));
      """
    # insert vertex succeeded
    When try to execute query:
      """
      INSERT VERTEX fixed_string_tag_1(c1) VALUES 1:("row"), 2:("row"), 3:("row")
      """
    Then the execution should be successful
    # insert edge succeeded
    When executing query:
      """
      INSERT EDGE fixed_string_edge_1(c1) VALUES 1->2:("row"), 1->3:("row")
      """
    Then the execution should be successful
    # check fixed string expression with go
    When executing query:
      """
      GO from 1 over fixed_string_edge_1 where $$.fixed_string_tag_1.c1 == "row" yield $$.fixed_string_tag_1.c1 as c1
      """
    Then the result should be, in any order, with relax comparison:
      | c1    |
      | "row" |
      | "row" |
    # check fixed string expression with go
    When executing query:
      """
      GO from 1 over fixed_string_edge_1 where $^.fixed_string_tag_1.c1 == "row" yield $$.fixed_string_tag_1.c1 as c1
      """
    Then the result should be, in any order, with relax comparison:
      | c1    |
      | "row" |
      | "row" |
    # check fixed string expression with go
    When executing query:
      """
      GO from 1 over fixed_string_edge_1 where fixed_string_edge_1.c1 == "row" yield $$.fixed_string_tag_1.c1 as c1
      """
    Then the result should be, in any order, with relax comparison:
      | c1    |
      | "row" |
      | "row" |
