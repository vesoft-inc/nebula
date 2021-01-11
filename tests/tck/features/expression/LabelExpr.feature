# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Label Expression
  Examples:
    | space_name  | vid                | vid2                |
    | nba         | "Tim Duncan"       | "Tony Parker"       |
    | nba_int_vid | hash("Tim Duncan") | hash("Tony Parker") |

  Scenario Outline: raise invalid label semantic errors
    Given a graph with space named "<space_name>"
    When executing query:
      """
      FETCH PROP ON player <vid> YIELD name
      """
    Then a SemanticError should be raised at runtime: Invalid label identifiers: name
    When executing query:
      """
      FETCH PROP ON player <vid> YIELD name + 1
      """
    Then a SemanticError should be raised at runtime: Invalid label identifiers: name
    When executing query:
      """
      FETCH PROP ON like <vid>-><vid2> YIELD likeness
      """
    Then a SemanticError should be raised at runtime: Invalid label identifiers: likeness
    When executing query:
      """
      GO FROM <vid> OVER like YIELD name
      """
    Then a SemanticError should be raised at runtime: Invalid label identifiers: name
    When executing query:
      """
      YIELD name
      """
    Then a SemanticError should be raised at runtime: Invalid label identifiers: name
