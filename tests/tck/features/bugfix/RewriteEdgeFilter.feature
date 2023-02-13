# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test match used in pipe

  Background:
    Given a graph with space named "nba"

  Scenario: Order by after match
    When executing query:
      """
      match (v)-[e:like|teammate{start_year: 2010}]->() where id(v) == 'Tim Duncan' return e
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                             |
      | [:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}] |
