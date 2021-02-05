# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Path2 - Relationships of a path

  Background:
    Given a graph with space named "nba"

  Scenario: [1] Return relationships by fetching them from the path
    When executing query:
      """
      MATCH p = (a:player)-[:teammate*1..1]->(b)
      RETURN relationships(p)
      """
    Then the result should be, in any order, with relax comparison:
      | relationships(p)                                                                       |
      | [[:teammate "Manu Ginobili"->"Tim Duncan" @0 {end_year: 2016, start_year: 2002}]]      |
      | [[:teammate "Manu Ginobili"->"Tony Parker" @0 {end_year: 2016, start_year: 2002}]]     |
      | [[:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}]]        |
      | [[:teammate "Tim Duncan"->"LaMarcus Aldridge" @0 {end_year: 2016, start_year: 2015}]]  |
      | [[:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}]]      |
      | [[:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}]]        |
      | [[:teammate "Tony Parker"->"Kyle Anderson" @0 {end_year: 2016, start_year: 2014}]]     |
      | [[:teammate "Tony Parker"->"LaMarcus Aldridge" @0 {end_year: 2018, start_year: 2015}]] |
      | [[:teammate "Tony Parker"->"Manu Ginobili" @0 {end_year: 2018, start_year: 2002}]]     |
      | [[:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]]        |

  Scenario: [2] Return relationships by fetching them from the path - starting from the end
    When executing query:
      """
      MATCH p = (a)-[:teammate*1..1]->(b:player)
      RETURN relationships(p)
      """
    Then the result should be, in any order, with relax comparison:
      | relationships(p)                                                                       |
      | [[:teammate "Manu Ginobili"->"Tim Duncan" @0 {end_year: 2016, start_year: 2002}]]      |
      | [[:teammate "Manu Ginobili"->"Tony Parker" @0 {end_year: 2016, start_year: 2002}]]     |
      | [[:teammate "Tim Duncan"->"Danny Green" @0 {end_year: 2016, start_year: 2010}]]        |
      | [[:teammate "Tim Duncan"->"LaMarcus Aldridge" @0 {end_year: 2016, start_year: 2015}]]  |
      | [[:teammate "Tim Duncan"->"Manu Ginobili" @0 {end_year: 2016, start_year: 2002}]]      |
      | [[:teammate "Tim Duncan"->"Tony Parker" @0 {end_year: 2016, start_year: 2001}]]        |
      | [[:teammate "Tony Parker"->"Kyle Anderson" @0 {end_year: 2016, start_year: 2014}]]     |
      | [[:teammate "Tony Parker"->"LaMarcus Aldridge" @0 {end_year: 2018, start_year: 2015}]] |
      | [[:teammate "Tony Parker"->"Manu Ginobili" @0 {end_year: 2018, start_year: 2002}]]     |
      | [[:teammate "Tony Parker"->"Tim Duncan" @0 {end_year: 2016, start_year: 2001}]]        |

  @skip
  # unimplement
  Scenario: [3] `relationships()` on null path
    When executing query:
      """
      WITH null AS a
      OPTIONAL MATCH p = (a)-[r]->()
      RETURN relationships(p), relationships(null)
      """
    Then the result should be, in any order:
      | relationships(p) | relationships(null) |
      | null             | null                |
