# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Delete string vid of tag

  Scenario: delete string vid one vertex one tag
    Given an empty graph
    And load "nba" csv data to a new space
    # before delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name  | player.age |
      | "Tim Duncan" | 42         |
    When executing query:
      """
      FETCH PROP ON bachelor "Tim Duncan" YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | bachelor.name | bachelor.speciality |
      | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan" YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id           |
      | "Tim Duncan" |
    # delete one tag
    When executing query:
      """
      DELETE TAG player FROm "Tim Duncan";
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name | player.age |
      | EMPTY       | EMPTY      |
    When executing query:
      """
      FETCH PROP ON bachelor "Tim Duncan" YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | bachelor.name | bachelor.speciality |
      | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan" YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id |
    Then drop the used space

  Scenario: delete string vid one vertex multiple tag
    Given an empty graph
    And load "nba" csv data to a new space
    # before delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name  | player.age |
      | "Tim Duncan" | 42         |
    When executing query:
      """
      FETCH PROP ON bachelor "Tim Duncan" YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | bachelor.name | bachelor.speciality |
      | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan" YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id           |
      | "Tim Duncan" |
    # delete one tag
    When executing query:
      """
      DELETE TAG player, bachelor FROM "Tim Duncan";
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name | player.age |
    When executing query:
      """
      FETCH PROP ON bachelor "Tim Duncan" YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | bachelor.name | bachelor.speciality |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan" YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id |
    Then drop the used space

  Scenario: delete string vid one vertex all tag
    Given an empty graph
    And load "nba" csv data to a new space
    # before delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name  | player.age |
      | "Tim Duncan" | 42         |
    When executing query:
      """
      FETCH PROP ON bachelor "Tim Duncan" YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | bachelor.name | bachelor.speciality |
      | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan" YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id           |
      | "Tim Duncan" |
    # delete all tag
    When executing query:
      """
      DELETE TAG * FROM "Tim Duncan";
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name | player.age |
    When executing query:
      """
      FETCH PROP ON bachelor "Tim Duncan" YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | bachelor.name | bachelor.speciality |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan" YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id |
    Then drop the used space

  Scenario: delete string vid multiple vertex one tag
    Given an empty graph
    And load "nba" csv data to a new space
    # before delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name  | player.age |
      | "Tim Duncan" | 42         |
    When executing query:
      """
      FETCH PROP ON player "Tony Parker" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name   | player.age |
      | "Tony Parker" | 36         |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan" YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id           |
      | "Tim Duncan" |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tony Parker" YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id            |
      | "Tony Parker" |
    # delete one tag
    When executing query:
      """
      DELETE TAG player FROM "Tim Duncan", "Tony Parker";
      """
    Then the execution should be successful
    # after delete tag
    # the output has one row because the vertex has multiple tags
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name | player.age |
      | EMPTY       | EMPTY      |
    When executing query:
      """
      FETCH PROP ON player "Tony Parker" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | player.name | player.age |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan" YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tony Parker" YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id |
    Then drop the used space

  Scenario: delete string vid from pipe
    Given an empty graph
    And load "nba" csv data to a new space
    # before delete tag
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD serve._dst as id
      """
    Then the result should be, in any order:
      | id      |
      | "Spurs" |
    When executing query:
      """
      FETCH PROP ON team "Spurs" YIELD team.name
      """
    Then the result should be, in any order:
      | team.name |
      | "Spurs"   |
    # delete one tag
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD edge as e | DELETE TAG team FROM src($-.e)
      """
    Then a SemanticError should be raised at runtime: `src($-.e)' is not an evaluable expression.
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD serve._dst as id | DELETE TAG team FROM $-.id
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON team "Spurs" YIELD team.name
      """
    Then the result should be, in any order:
      | team.name |
    # delete tag from pipe and normal
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD serve._dst as id | DELETE TAG team FROM $-.id, "Lakers"
      """
    Then a SyntaxError should be raised at runtime.
    Then drop the used space

  Scenario: delete string vid from var
    Given an empty graph
    And load "nba" csv data to a new space
    # before delete tag
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD serve._dst as id
      """
    Then the result should be, in any order:
      | id      |
      | "Spurs" |
    When executing query:
      """
      FETCH PROP ON team "Spurs" YIELD team.name
      """
    Then the result should be, in any order:
      | team.name |
      | "Spurs"   |
    # delete one tag
    When executing query:
      """
      $var = GO FROM "Tim Duncan" OVER serve YIELD serve._dst as id; DELETE TAG team FROM $var.id
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON team "Spurs" YIELD team.name
      """
    Then the result should be, in any order:
      | team.name |
    # delete one tag from var and normal
    When executing query:
      """
      $var = GO FROM "Tim Duncan" OVER serve YIELD serve._dst as id; DELETE TAG team FROM $var.id, "Lakers"
      """
    Then a SyntaxError should be raised at runtime.
    Then drop the used space
