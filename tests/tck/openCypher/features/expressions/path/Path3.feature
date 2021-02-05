# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Path3 - Length of a path

  Background:
    Given a graph with space named "nba"

  Scenario: [1] `nodes()` on null path
    When executing query:
      """
      MATCH p = (a:team)-[*0..1]->(b)
      RETURN a, length(p) AS l
      """
    Then the result should be, in any order, with relax comparison:
      | a                                              | l |
      | ("Magic" :team{name: "Magic"})                 | 0 |
      | ("Pacers" :team{name: "Pacers"})               | 0 |
      | ("Lakers" :team{name: "Lakers"})               | 0 |
      | ("Nuggets" :team{name: "Nuggets"})             | 0 |
      | ("Suns" :team{name: "Suns"})                   | 0 |
      | ("Mavericks" :team{name: "Mavericks"})         | 0 |
      | ("Kings" :team{name: "Kings"})                 | 0 |
      | ("Hornets" :team{name: "Hornets"})             | 0 |
      | ("76ers" :team{name: "76ers"})                 | 0 |
      | ("Hawks" :team{name: "Hawks"})                 | 0 |
      | ("Warriors" :team{name: "Warriors"})           | 0 |
      | ("Bulls" :team{name: "Bulls"})                 | 0 |
      | ("Grizzlies" :team{name: "Grizzlies"})         | 0 |
      | ("Cavaliers" :team{name: "Cavaliers"})         | 0 |
      | ("Raptors" :team{name: "Raptors"})             | 0 |
      | ("Heat" :team{name: "Heat"})                   | 0 |
      | ("Knicks" :team{name: "Knicks"})               | 0 |
      | ("Bucks" :team{name: "Bucks"})                 | 0 |
      | ("Clippers" :team{name: "Clippers"})           | 0 |
      | ("Jazz" :team{name: "Jazz"})                   | 0 |
      | ("Timberwolves" :team{name: "Timberwolves"})   | 0 |
      | ("Pelicans" :team{name: "Pelicans"})           | 0 |
      | ("Rockets" :team{name: "Rockets"})             | 0 |
      | ("Spurs" :team{name: "Spurs"})                 | 0 |
      | ("Thunders" :team{name: "Thunders"})           | 0 |
      | ("Nets" :team{name: "Nets"})                   | 0 |
      | ("Pistons" :team{name: "Pistons"})             | 0 |
      | ("Celtics" :team{name: "Celtics"})             | 0 |
      | ("Trail Blazers" :team{name: "Trail Blazers"}) | 0 |
      | ("Wizards" :team{name: "Wizards"})             | 0 |

  @skip
  # unimplement
  Scenario: [2] Failing when using `length()` on a node
    When executing query:
      """
      MATCH (n)
      RETURN length(n)
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  @skip
  # unimplement
  Scenario: [3] Failing when using `length()` on a relationship
    When executing query:
      """
      MATCH ()-[r]->()
      RETURN length(r)
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType
