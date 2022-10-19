# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Optional Match

  Background:
    Given a graph with space named "nba"

  Scenario: Optional Match with Predicates
    When executing query:
      """
      optional match (v:player) where v.player.name == "ggg" return v limit 10;
      """
    Then the result should be, in any order:
      | v |
    When executing query:
      """
      optional match (v:player) where v.player.name == "Boris Diaw" return v limit 10;
      """
    Then the result should be, in any order:
      | v                                                   |
      | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"}) |
    When executing query:
      """
      match (v0:player)-[e:like]->(v1:player{name:"LeBron James"})
      optional match (v0)-[e:like]->(v2:player{name:"Tim Duncan"})
      where v0.player.age > 10
      return v0,v1,v2;
      """
    Then the result should be, in any order:
      | v0                                                            | v1                                                      | v2                                                                                                          |
      | ("Kyrie Irving" :player{age: 26, name: "Kyrie Irving"})       | ("LeBron James" :player{age: 34, name: "LeBron James"}) | NULL                                                                                                        |
      | ("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})         | ("LeBron James" :player{age: 34, name: "LeBron James"}) | NULL                                                                                                        |
      | ("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"}) | ("LeBron James" :player{age: 34, name: "LeBron James"}) | NULL                                                                                                        |
      | ("Chris Paul" :player{age: 33, name: "Chris Paul"})           | ("LeBron James" :player{age: 34, name: "LeBron James"}) | NULL                                                                                                        |
      | ("Danny Green" :player{age: 31, name: "Danny Green"})         | ("LeBron James" :player{age: 34, name: "LeBron James"}) | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) |
      | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"}) | ("LeBron James" :player{age: 34, name: "LeBron James"}) | ("Tim Duncan" :player{age: 42, name: "Tim Duncan"} :bachelor{name: "Tim Duncan", speciality: "psychology"}) |
