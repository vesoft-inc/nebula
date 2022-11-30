# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Logical Expression

  Scenario: xor crash bug fix 1
    Given a graph with space named "nba"
    When executing query:
      """
      match (v0:player)-[e:serve]->(v1) where not ((e.start_year == 1997 xor e.end_year != 2016) or (e.start_year > 1000 and e.end_year < 3000)) return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 0        |
    When executing query:
      """
      match (v0:player)-[e:serve]->(v1) where not ((e.start_year == 1997 xor e.end_year != 2016) and (e.start_year > 1000 and e.end_year < 3000)) return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 140      |
