# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Regular Expression

  Background:
    Given a graph with space named "nba"

  Scenario: yield regex
    When executing query:
      """
      YIELD "abcd\xA3g1234efgh\x49ijkl" =~ "\\w{4}\xA3g12\\d*e\\w+\x49\\w+" AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD "Tony Parker" =~ "T\\w+\\s\\w+" AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD "010-12345" =~ "\\d{3}\\-\\d{3,8}" AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD "test_space_128" =~ "[a-zA-Z_][0-9a-zA-Z_]{0,19}" AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD "2001-09-01 08:00:00" =~ "\\d+\\-0\\d?\\-\\d+\\s\\d+:00:\\d+" AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD "2019" =~ "\\d+" AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD "jack138tom发abc数据库烫烫烫" =~ "j\\w*\\d+\\w+\u53d1[a-c]+\u6570\u636e\u5e93[\x70EB]+" AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD "a good person" =~ "a\\s\\w+" AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |
    When executing query:
      """
      YIELD "Trail Blazers" =~ "\\w+" AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |
    When executing query:
      """
      YIELD "Tony No.1" =~ "\\w+No\\.\\d+" AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |

  Scenario: use regex in GO
    When executing query:
      """
      GO FROM "Tracy McGrady" OVER like
      WHERE $$.player.name =~ "\\w+\\s?.*"
      YIELD $$.player.name AS name
      """
    Then the result should be, in any order:
      | name          |
      | "Kobe Bryant" |
      | "Grant Hill"  |
      | "Rudy Gay"    |
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve
      WHERE $$.team.name =~ "\\d+\\w+"
      YIELD $$.team.name AS name
      """
    Then the result should be, in any order:
      | name    |
      | "76ers" |

  Scenario: use regex in MATCH
    When executing query:
      """
      MATCH (v:player)
      WHERE v.name =~ "T\\w+\\s?.*"
      RETURN v.name AS name
      """
    Then the result should be, in any order:
      | name             |
      | "Tracy McGrady"  |
      | "Tim Duncan"     |
      | "Tony Parker"    |
      | "Tiago Splitter" |
    When executing query:
      """
      MATCH (v:player)
      WHERE v.name STARTS WITH "Tony"
      RETURN v.name, v.name =~ "T\\w+\\s?.*" AS b
      """
    Then the result should be, in any order:
      | v.name        | b    |
      | "Tony Parker" | true |
