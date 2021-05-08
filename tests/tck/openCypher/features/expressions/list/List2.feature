# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#
#
# Copyright (c) 2015-2021 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Attribution Notice under the terms of the Apache License 2.0
#
# This work was created by the collective efforts of the openCypher community.
# Without limiting the terms of Section 6, any Derivative Work that is not
# approved by the public consensus process of the openCypher Implementers Group
# should not be described as “Cypher” (and Cypher® is a registered trademark of
# Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
# proposals for change that have been documented or implemented should only be
# described as "implementation extensions to Cypher" or as "proposed changes to
# Cypher that are not yet approved by the openCypher community".
#
#
# encoding: utf-8
Feature: List2 - List Slicing

  Scenario: [1] List slice
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3, 4, 5] AS list
      RETURN list[1..3] AS r
      """
    Then the result should be, in any order:
      | r      |
      | [2, 3] |
    And no side effects

  Scenario: [2] List slice with implicit end
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3] AS list
      RETURN list[1..] AS r
      """
    Then the result should be, in any order:
      | r      |
      | [2, 3] |
    And no side effects

  Scenario: [3] List slice with implicit start
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3] AS list
      RETURN list[..2] AS r
      """
    Then the result should be, in any order:
      | r      |
      | [1, 2] |
    And no side effects

  Scenario: [4] List slice with singleton range
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3] AS list
      RETURN list[0..1] AS r
      """
    Then the result should be, in any order:
      | r   |
      | [1] |
    And no side effects

  Scenario: [5] List slice with empty range
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3] AS list
      RETURN list[0..0] AS r
      """
    Then the result should be, in any order:
      | r  |
      | [] |
    And no side effects

  Scenario: [6] List slice with negative range
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3] AS list
      RETURN list[-3..-1] AS r
      """
    Then the result should be, in any order:
      | r      |
      | [1, 2] |
    And no side effects

  Scenario: [7] List slice with invalid range
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3] AS list
      RETURN list[3..1] AS r
      """
    Then the result should be, in any order:
      | r  |
      | [] |
    And no side effects

  Scenario: [8] List slice with exceeding range
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3] AS list
      RETURN list[-5..5] AS r
      """
    Then the result should be, in any order:
      | r         |
      | [1, 2, 3] |
    And no side effects

  Scenario Outline: [9] List slice with null range
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3] AS list
      RETURN list[<lower>..<upper>] AS r
      """
    Then the result should be, in any order:
      | r    |
      | null |
    And no side effects

    Examples:
      | lower | upper |
      | null  | null  |
      | 1     | null  |
      | null  | 3     |
      |       | null  |
      | null  |       |

  @skip
  Scenario: [10] List slice with parameterised range
    Given any graph
    And parameters are:
      | from | 1 |
      | to   | 3 |
    When executing query:
      """
      WITH [1, 2, 3] AS list
      RETURN list[$from..$to] AS r
      """
    Then the result should be, in any order:
      | r      |
      | [2, 3] |
    And no side effects

  @skip
  Scenario: [11] List slice with parameterised invalid range
    Given any graph
    And parameters are:
      | from | 3 |
      | to   | 1 |
    When executing query:
      """
      WITH [1, 2, 3] AS list
      RETURN list[$from..$to] AS r
      """
    Then the result should be, in any order:
      | r  |
      | [] |
    And no side effects
