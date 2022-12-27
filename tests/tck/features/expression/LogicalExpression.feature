# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Logical Expression

  Background:
    Given a graph with space named "nba"

  Scenario: xor
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
      | 12       |
    When executing query:
      """
      match (v0:player)-[e:serve]->(v1) with not ((e.start_year == 1997 xor e.end_year != 2016) and (e.start_year > 1000 and e.end_year < 3000)) AS a where a return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 12       |
    When executing query:
      """
      WITH 1 AS a WHERE NOT((NOT TRUE) XOR TRUE) RETURN COUNT(*)
      """
    Then the result should be, in any order:
      | COUNT(*) |
      | 0        |
    When executing query:
      """
      WITH 1 AS a RETURN NOT((NOT TRUE) XOR TRUE) AS b
      """
    Then the result should be, in any order:
      | b     |
      | false |
