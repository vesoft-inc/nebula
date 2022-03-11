# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Basic match
  Examples:
      | space_name |
      | nba  |
      | nba_int_vid|

  Background:
    Given a graph with space named "<space_name>"

  Scenario: Undefined aliases
    When executing query:
      """
      MATCH (v:player) WITH (t)-[]-(v) AS p RETURN p
      """
    Then a SemanticError should be raised at runtime: PatternExpression are not allowed to introduce new variables: `t'.
    When executing query:
      """
      MATCH (v:player) UNWIND (t)-[]-(v) AS p RETURN p
      """
    Then a SemanticError should be raised at runtime: PatternExpression are not allowed to introduce new variables: `t'.
    When executing query:
      """
      MATCH (v:player) WHERE (t)-[]-(v) RETURN v
      """
    Then a SemanticError should be raised at runtime: PatternExpression are not allowed to introduce new variables: `t'.
    When executing query:
      """
      MATCH (v:player) RETURN (t)-[]-(v)
      """
    Then a SemanticError should be raised at runtime: PatternExpression are not allowed to introduce new variables: `t'.

  Scenario: Type mismatched aliases
    When executing query:
      """
      MATCH (v:player) WITH (v)-[v]-() AS p RETURN p
      """
    Then a SemanticError should be raised at runtime: Alias `v' should be Edge.
    When executing query:
      """
      MATCH (v:player) UNWIND (v)-[v]-() AS p RETURN p
      """
    Then a SemanticError should be raised at runtime: Alias `v' should be Edge.
    When executing query:
      """
      MATCH (v:player) WHERE (v)-[v]-() RETURN v
      """
    Then a SemanticError should be raised at runtime: Alias `v' should be Edge.
    When executing query:
      """
      MATCH (v:player) RETURN (v)-[v]-()
      """
    Then a SemanticError should be raised at runtime: Alias `v' should be Edge.
