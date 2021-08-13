# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Delete int vid of tag

  Scenario: delete int vid one vertex one tag
    Given an empty graph
    And load "nba_int_vid" csv data to a new space
    # before delete tag
    When executing query:
      """
      FETCH PROP ON player hash("Tim Duncan") YIELD player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | player.age |
      | "Tim Duncan" | "Tim Duncan" | 42         |
    When executing query:
      """
      FETCH PROP ON bachelor hash("Tim Duncan") YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     |
      | "Tim Duncan" |
    # delete one tag
    When executing query:
      """
      DELETE TAG player FROM hash("Tim Duncan");
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON player hash("Tim Duncan") YIELD player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID | player.name | player.age |
    When executing query:
      """
      FETCH PROP ON bachelor hash("Tim Duncan") YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID |

  Scenario: delete int vid one vertex multiple tag
    Given an empty graph
    And load "nba_int_vid" csv data to a new space
    # before delete tag
    When executing query:
      """
      FETCH PROP ON player hash("Tim Duncan") YIELD player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | player.age |
      | "Tim Duncan" | "Tim Duncan" | 42         |
    When executing query:
      """
      FETCH PROP ON bachelor hash("Tim Duncan") YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     |
      | "Tim Duncan" |
    # delete one tag
    When executing query:
      """
      DELETE TAG player, bachelor FROM hash("Tim Duncan");
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON player hash("Tim Duncan") YIELD player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID | player.name | player.age |
    When executing query:
      """
      FETCH PROP ON bachelor hash("Tim Duncan") YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID | bachelor.name | bachelor.speciality |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID |

  Scenario: delete int vid one vertex all tag
    Given an empty graph
    And load "nba_int_vid" csv data to a new space
    # before delete tag
    When executing query:
      """
      FETCH PROP ON player hash("Tim Duncan") YIELD player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | player.age |
      | "Tim Duncan" | "Tim Duncan" | 42         |
    When executing query:
      """
      FETCH PROP ON bachelor hash("Tim Duncan") YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | bachelor.name | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     |
      | "Tim Duncan" |
    # delete one tag
    When executing query:
      """
      DELETE TAG * FROM hash("Tim Duncan");
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON player hash("Tim Duncan") YIELD player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID | player.name | player.age |
    When executing query:
      """
      FETCH PROP ON bachelor hash("Tim Duncan") YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID | bachelor.name | bachelor.speciality |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID |

  Scenario: delete int vid multiple vertex one tag
    Given an empty graph
    And load "nba_int_vid" csv data to a new space
    # before delete tag
    When executing query:
      """
      FETCH PROP ON player hash("Tim Duncan") YIELD player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | player.name  | player.age |
      | "Tim Duncan" | "Tim Duncan" | 42         |
    When executing query:
      """
      FETCH PROP ON player hash("Tony Parker") YIELD player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID      | player.name   | player.age |
      | "Tony Parker" | "Tony Parker" | 36         |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     |
      | "Tim Duncan" |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tony Parker"
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID      |
      | "Tony Parker" |
    # delete one tag
    When executing query:
      """
      DELETE TAG player FROM hash("Tim Duncan"), hash("Tony Parker");
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON player hash("Tim Duncan") YIELD player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID | player.name | player.age |
    When executing query:
      """
      FETCH PROP ON player hash("Tony Parker") YIELD player.name, player.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID | player.name | player.age |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tony Parker"
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID |

  Scenario: delete int vid from pipe
    Given an empty graph
    And load "nba_int_vid" csv data to a new space
    # before delete tag
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER serve YIELD serve._dst as id
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | id      |
      | "Spurs" |
    When executing query:
      """
      FETCH PROP ON team hash("Spurs") YIELD team.name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID | team.name |
      | "Spurs"  | "Spurs"   |
    # delete one tag
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER serve YIELD serve._dst as id | DELETE TAG team FROM $-.id
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON team hash("Spurs") YIELD team.name
      """
    Then the result should be, in any order:
      | VertexID | team.name |

  Scenario: delete int vid from var
    Given an empty graph
    And load "nba_int_vid" csv data to a new space
    # before delete tag
    When executing query:
      """
      GO FROM hash("Tim Duncan") OVER serve YIELD serve._dst as id
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | id      |
      | "Spurs" |
    When executing query:
      """
      FETCH PROP ON team hash("Spurs") YIELD team.name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID | team.name |
      | "Spurs"  | "Spurs"   |
    # delete one tag
    When executing query:
      """
      $var = GO FROM hash("Tim Duncan") OVER serve YIELD serve._dst as id; DELETE TAG team FROM $var.id
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON team hash("Spurs") YIELD team.name
      """
    Then the result should be, in any order:
      | VertexID | team.name |
