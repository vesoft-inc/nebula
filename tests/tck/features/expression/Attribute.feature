# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Attribute

  Background:
    Given a graph with space named "nba"

  Scenario: Attribute for basic type
    When executing query:
      """
      RETURN date("2021-07-19").month AS month
      """
    Then the result should be, in any order:
      | month |
      | 7     |
    When executing query:
      """
      RETURN date("2021-07-19").MoNth AS month
      """
    Then the result should be, in any order:
      | month |
      | 7     |
    When executing query:
      """
      RETURN time("02:59:40").minute AS minute
      """
    Then the result should be, in any order:
      | minute |
      | 59     |
    When executing query:
      """
      RETURN time("02:59:40").MinUte AS minute
      """
    Then the result should be, in any order:
      | minute |
      | 59     |
    When executing query:
      """
      RETURN datetime("2021-07-19T02:59:40").minute AS minute
      """
    Then the result should be, in any order:
      | minute |
      | 59     |
    When executing query:
      """
      RETURN datetime("2021-07-19T02:59:40").mInutE AS minute
      """
    Then the result should be, in any order:
      | minute |
      | 59     |
    When executing query:
      """
      RETURN {k1 : 1, k2: true}.k1 AS k
      """
    Then the result should be, in any order:
      | k |
      | 1 |
    When executing query:
      """
      RETURN {k1 : 1, k2: true}.K1 AS k
      """
    Then the result should be, in any order:
      | k            |
      | UNKNOWN_PROP |
    When executing query:
      """
      MATCH (v) WHERE id(v) == 'Tim Duncan' RETURN v.name
      """
    Then the result should be, in any order:
      | v.name       |
      | "Tim Duncan" |
    When executing query:
      """
      MATCH (v) WHERE id(v) == 'Tim Duncan' RETURN v.Name
      """
    Then the result should be, in any order:
      | v.Name       |
      | UNKNOWN_PROP |
    When executing query:
      """
      MATCH (v)-[e:like]->() WHERE id(v) == 'Tim Duncan' RETURN e.likeness
      """
    Then the result should be, in any order:
      | e.likeness |
      | 95         |
      | 95         |
    When executing query:
      """
      MATCH (v)-[e:like]->() WHERE id(v) == 'Tim Duncan' RETURN e.Likeness
      """
    Then the result should be, in any order:
      | e.Likeness   |
      | UNKNOWN_PROP |
      | UNKNOWN_PROP |

  Scenario: Not exists attribute
    When executing query:
      """
      RETURN date("2021-07-19").not_exists_attr AS not_exists_attr
      """
    Then the result should be, in any order:
      | not_exists_attr |
      | UNKNOWN_PROP    |
    When executing query:
      """
      RETURN time("02:59:40").not_exists_attr AS not_exists_attr
      """
    Then the result should be, in any order:
      | not_exists_attr |
      | UNKNOWN_PROP    |
    When executing query:
      """
      RETURN datetime("2021-07-19T02:59:40").not_exists_attr AS not_exists_attr
      """
    Then the result should be, in any order:
      | not_exists_attr |
      | UNKNOWN_PROP    |
    When executing query:
      """
      RETURN {k1 : 1, k2: true}.not_exists_attr AS not_exists_attr
      """
    Then the result should be, in any order:
      | not_exists_attr |
      | UNKNOWN_PROP    |
    When executing query:
      """
      MATCH (v) WHERE id(v) == 'Tim Duncan' RETURN v.not_exists_attr
      """
    Then the result should be, in any order:
      | v.not_exists_attr |
      | UNKNOWN_PROP      |
    When executing query:
      """
      MATCH (v)-[e:like]->() WHERE id(v) == 'Tim Duncan' RETURN e.not_exists_attr
      """
    Then the result should be, in any order:
      | e.not_exists_attr |
      | UNKNOWN_PROP      |
      | UNKNOWN_PROP      |

  Scenario: Invalid type
    When executing query:
      """
      RETURN (true).attr
      """
    Then a SemanticError should be raised at runtime: `true.attr', expected type with attribute like Date, Time, DateTime, Map, Vertex or Edge but was BOOL: true
