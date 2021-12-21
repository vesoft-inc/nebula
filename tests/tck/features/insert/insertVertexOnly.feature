# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: insert vertex without tag

  Background: Background name
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9     |
      | replica_factor | 1     |
      | vid_type       | int64 |

  Scenario: insert vertex only
    Given having executed:
      """
      CREATE EDGE e();
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX VALUES 1:(),2:(),3:();
      INSERT EDGE e() VALUES 1->2:(),2->3:();
      """
    Then the execution should be successful
    When executing query:
      """
      GO 2 STEP FROM 1 OVER e yield e._dst AS dst;
      """
    Then the result should be, in any order:
      | dst |
      | 3   |
    When executing query:
      """
      FETCH PROP ON * 1,2 yield vertex AS v;
      """
    Then the result should be, in any order, with relax comparison:
      | v   |
      | (1) |
      | (2) |
    Then drop the used space
