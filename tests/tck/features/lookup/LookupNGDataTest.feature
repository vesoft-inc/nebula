# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test lookup on ngdata data

  Background:
    Given a graph with space named "ngdata"

  Scenario: lookup on the tag property of double type
    When executing query:
      """
      LOOKUP ON Label_11 WHERE Label_11.Label_11_5_Double>0 YIELD id(vertex) AS vid
      """
    Then the result should contain:
      | vid |
      | 88  |
      | 11  |
      | 19  |
      | 69  |
      | 104 |
      | 45  |
      | 79  |
      | 83  |
      | 383 |
    When executing query:
      """
      LOOKUP ON Label_11 WHERE Label_11.Label_11_5_Double>10 YIELD id(vertex) AS vid
      """
    Then the result should be, in any order:
      | vid |

  Scenario: lookup on the edge property of double type
    When executing query:
      """
      LOOKUP ON Rel_0 WHERE Rel_0.Rel_0_3_Double>0 YIELD id(edge) AS eid
      """
    Then the result should be, in any order:
      | eid |
    When executing query:
      """
      LOOKUP ON Rel_0 WHERE Rel_0.Rel_0_3_Double>10 YIELD id(edge) AS eid
      """
    Then the result should be, in any order:
      | eid |

  Scenario: lookup on the edge property of geography type
    When executing query:
      """
      LOOKUP ON Rel_3 WHERE Rel_3.Rel_3_1_geography_point>0 YIELD id(edge) AS eid
      """
    Then the result should be, in any order:
      | eid |
    When executing query:
      """
      LOOKUP ON Rel_3 WHERE Rel_3.Rel_3_1_geography_point>10 YIELD id(edge) AS eid
      """
    Then the result should be, in any order:
      | eid |
