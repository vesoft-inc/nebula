# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Bug fixes with ngdata

  Background:
    Given a graph with space named "ngdata"

  Scenario: Comparing EMPTY values
    When executing query:
      """
      MATCH (v0:Label_0)-[e0]->()-[e1*1..1]->(v1)
      WHERE (id(v0) == 11) AND (v1.Label_6.Label_6_400_Int == v1.Label_6.Label_6_500_Int)
      RETURN count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 0        |
