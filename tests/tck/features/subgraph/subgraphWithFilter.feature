# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: subgraph with fitler

  Background:
    Given a graph with space named "nba"

  Scenario: subgraph with edge filter
    When executing query:
      """
      GET SUBGRAPH FROM 'Tim Duncan' OUT like WHERE like.likeness > 90 YIELD vertices as v, edges as e
      """
    Then the result should be, in any order, with relax comparison:
      | v                                    | e                                                                                                                 |
      | [("Tim Duncan")]                     | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}], [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]]  |
      | [("Manu Ginobili"), ("Tony Parker")] | [[:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}], [:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]] |
    When executing query:
      """
      GET SUBGRAPH FROM 'Tim Duncan' BOTH like WHERE like.likeness > 90 YIELD vertices as v, edges as e
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                         | e                                                                                                                                                                                                                                 |
      | [("Tim Duncan")]                                          | [[:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}],[:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}], [:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}], [:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]] |
      | [("Dejounte Murray"), ("Manu Ginobili"), ("Tony Parker")] | [[:like "Dejounte Murray"->"Manu Ginobili" @0 {likeness: 99}], [:like "Dejounte Murray"->"Tony Parker" @0 {likeness: 99}],[:like "Tony Parker"->"Manu Ginobili" @0 {likeness: 95}]]                                               |
