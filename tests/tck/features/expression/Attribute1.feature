# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Attribute using test

  Background:
    Given a graph with space named "ngdata"

  Scenario: Attribute with null data
    When executing query:
      """
      MATCH p0 = (v0)-[e0]->()
      WHERE id(v0) in [1,2,3,4,5,6,7,8,9,10]
      UNWIND nodes(p0) AS ua0
      with ua0
      where ua0.Label_5.Label_5_7_Bool
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 61       |
    When executing query:
      """
      MATCH p0 = (v0)-[e0]->()
      WHERE id(v0) in [1,2,3,4,5,6,7,8,9,10]
      UNWIND nodes(p0) AS ua0
      with ua0
      where ua0.Label_5.Label_5_7_Bool == true
      return count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 61       |
