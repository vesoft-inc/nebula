# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: delete vertex without edge

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9     |
      | replica_factor | 1     |
      | vid_type       | int64 |

  Scenario: delete vertex with edge
    Given having executed:
      """
      CREATE TAG t(id int);
      CREATE EDGE e();
      """
    And wait 6 seconds
    When executing query and retrying it on failure every 6 seconds for 9 times:
      """
      INSERT VERTEX t(id) VALUES 1:(1),2:(2),3:(3);
      INSERT EDGE e() VALUES 1->2:(),1->3:();
      """
    Then the execution should be successful
    When executing query:
      """
      GO 1 STEP FROM 1 OVER e YIELD e._dst AS dst, $^.t.id as id;
      """
    Then the result should be, in any order:
      | dst | id |
      | 2   | 1  |
      | 3   | 1  |
    When executing query:
      """
      DELETE VERTEX 1 WITH EDGE;
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON t 1 yield vertex as v;
      """
    Then the result should be, in any order:
      | v |
    When executing query:
      """
      FETCH PROP ON e 1->2 yield edge as e;
      """
    Then the result should be, in any order:
      | e |
    When executing query:
      """
      GO 1 STEP FROM 1 OVER e YIELD e._dst AS dst, $^.t.id as id
      """
    Then the result should be, in any order:
      | dst | id |
    When executing query:
      """
      INSERT VERTEX t(id) VALUES 1:(1)
      """
    Then the execution should be successful
    When executing query:
      """
      GO 1 STEP FROM 1 OVER e YIELD e._dst AS dst, $^.t.id as id;
      """
    Then the result should be, in any order:
      | dst | id |
    Then drop the used space

  Scenario: delete vertex without edge
    Given having executed:
      """
      CREATE TAG t(id int);
      CREATE EDGE e();
      """
    And wait 6 seconds
    When executing query and retrying it on failure every 6 seconds for 3 times:
      """
      INSERT VERTEX t(id) VALUES 1:(1),2:(2),3:(2);
      INSERT EDGE e() VALUES 1->2:(),1->3:();
      """
    Then the execution should be successful
    When executing query:
      """
      GO 1 STEP FROM 1 OVER e YIELD e._dst AS a, $^.t.id AS id;
      """
    Then the result should be, in any order:
      | a | id |
      | 2 | 1  |
      | 3 | 1  |
    When executing query:
      """
      DELETE VERTEX 1;
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON t 1 yield vertex as v;
      """
    Then the result should be, in any order:
      | v |
    When executing query:
      """
      FETCH PROP ON e 1->2 yield edge as e;
      """
    Then the result should be, in any order:
      | e               |
      | [:e 1->2 @0 {}] |
    When executing query:
      """
      GO 1 STEP FROM 1 OVER e YIELD e._dst AS b, $^.t.id as id;
      """
    Then the result should be, in any order:
      | b | id    |
      | 2 | EMPTY |
      | 3 | EMPTY |
    When executing query:
      """
      INSERT VERTEX t(id) VALUES 1:(1)
      """
    Then the execution should be successful
    When executing query:
      """
      GO 1 STEP FROM 1 OVER e YIELD e._dst AS c, $^.t.id as id;
      """
    Then the result should be, in any order:
      | c | id |
      | 2 | 1  |
      | 3 | 1  |
    Then drop the used space
