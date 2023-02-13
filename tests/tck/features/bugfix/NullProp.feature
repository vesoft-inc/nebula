# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Null prop from expr

  Background:
    Given a graph with space named "nba"

  Scenario: Null prop from expr
    When executing query:
      """
      match p = (v)-[e:like]->()
      where id(v) in ["Tim Duncan"]
      with relationships(p) as tt, e
      return tt[0].likeness AS l
      """
    Then the result should be, in any order:
      | l  |
      | 95 |
      | 95 |
