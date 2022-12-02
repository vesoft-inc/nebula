# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
# Issue link: https://github.com/vesoft-inc/nebula-ent/issues/1605
Feature: Duplicate alias in MATCH

  Background:
    Given a graph with space named "nba"

  Scenario: Duplicate node alias
    # expand from left to right
    When executing query:
      """
      MATCH (n0)-[]->(n1)-[]->(n1) WHERE (id(n0) == "Tim Duncan") RETURN n1
      """
    Then the result should be, in any order:
      | n1 |
    # expand from right to left
    When executing query:
      """
      MATCH (n1)<-[]-(n1)<-[]-(n0) WHERE (id(n0) == "Tim Duncan") RETURN n1
      """
    Then the result should be, in any order:
      | n1 |
    When executing query:
      """
      MATCH (n0)-[]->(n1)-[]->(n1)-[]->(n1) WHERE (id(n0) == "Tim Duncan") RETURN n1
      """
    Then the result should be, in any order:
      | n1 |
    When executing query:
      """
      MATCH (n0)-[]->(n1)-[]->(n1)-[]->(n1)-[]->(n1) WHERE (id(n0) == "Tim Duncan") RETURN n1
      """
    Then the result should be, in any order:
      | n1 |

  Scenario: Duplicate node alias expand both direction
    # expand from middle to both sides
    When executing query:
      """
      MATCH (n1)-[]->(n0)-[]->(n1)-[]->(n1)-[]->(n1) WHERE (id(n0) == "Tim Duncan") RETURN n1
      """
    Then the result should be, in any order:
      | n1 |
