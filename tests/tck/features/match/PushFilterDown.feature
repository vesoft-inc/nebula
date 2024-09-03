Feature: Push filter down

  Background: Prepare a new space
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    And having executed:
      """
      CREATE tag player(name string, age int, score int, gender bool);
      CREATE EDGE IF NOT EXISTS follow();
      """
    And having executed:
      """
      INSERT VERTEX player(name, age, score, gender) VALUES "Tim Duncan":("Tim Duncan", 42, 28, true),"Yao Ming":("Yao Ming", 38, 23, true),"Nneka Ogwumike":("Nneka Ogwumike", 35, 13, false);
      INSERT EDGE follow () VALUES "Tim Duncan"->"Yao Ming":();
      """
    And having executed:
      """
      create tag index player_index on player();
      create tag index player_name_index on player(name(8));
      rebuild tag index
      """
    And wait all indexes ready

  Scenario: Single vertex
    When profiling query:
      """
      MATCH (v:player) where v.player.name == "Tim Duncan" and v.player.age > 20 RETURN v
      """
    Then the execution plan should be:
      | id | name           | dependencies | operator info                                                                   |
      | 5  | Project        | 4            |                                                                                 |
      | 4  | Filter         | 3            | {"condition": "((v.player.name==\"Tim Duncan\") AND (v.player.age>20))"}        |
      | 3  | AppendVertices | 2            |                                                                                 |
      | 2  | IndexScan      | 1            | {"indexCtx": {"filter":"((player.name==\"Tim Duncan\") AND (player.age>20))"}}  |
      | 1  | Start          |              |                                                                                 |

    When profiling query:
      """
      MATCH (v:player) where v.player.age > 20 RETURN v
      """
    Then the execution plan should be:
      | id | name           | dependencies | operator info                               |
      | 5  | Project        | 4            |                                             |
      | 4  | Filter         | 3            | {"condition": "(v.player.age>20)"}          |
      | 3  | AppendVertices | 2            |                                             |
      | 2  | IndexScan      | 1            | {"indexCtx": {"filter":"(player.age>20)"}}  |
      | 1  | Start          |              |                                             |

    When profiling query:
      """
      MATCH (v:player) where v.player.age < 3 or v.player.age > 20 RETURN v
      """
    Then the execution plan should be:
      | id | name           | dependencies | operator info                                                   |
      | 5  | Project        | 4            |                                                                 |
      | 4  | Filter         | 3            | {"condition": "((v.player.age<3) OR (v.player.age>20))"}        |
      | 3  | AppendVertices | 2            |                                                                 |
      | 2  | IndexScan      | 1            | {"indexCtx": {"filter":"((player.age<3) OR (player.age>20))"}}  |
      | 1  | Start          |              |                                                                 |

  Scenario: Vertex and edge
    When profiling query:
      """
      MATCH (v:player)-[]-() where v.player.age > 20 RETURN v
      """
    Then the execution plan should be:
      | id | name           | dependencies | operator info                               |
      | 6  | Project        | 5            |                                             |
      | 5  | Filter         | 4            |                                             |
      | 4  | AppendVertices | 3            |                                             |
      | 3  | Traverse       | 2            |                                             |
      | 2  | IndexScan      | 1            | {"indexCtx": {"filter":"(player.age>20)"}}  |
      | 1  | Start          |              |                                             |
    When profiling query:
      """
      MATCH (v:player)-[]-(o:player) where v.player.age > 20 or o.player.name == "Yao Ming"  RETURN v
      """
    Then the execution plan should be:
      | id | name           | dependencies | operator info                               |
      | 6  | Project        | 5            |                                             |
      | 5  | Filter         | 4            |                                             |
      | 4  | AppendVertices | 3            |                                             |
      | 3  | Traverse       | 2            |                                             |
      | 2  | IndexScan      | 1            | {"indexCtx": {"filter":""}}                 |
      | 1  | Start          |              |                                             |
    When profiling query:
      """
      MATCH (v:player)-[]-(o:player) where v.player.age > 20 and o.player.name == "Yao Ming"  RETURN v
      """
    Then the execution plan should be:
      | id | name           | dependencies | operator info                               |
      | 6  | Project        | 5            |                                             |
      | 5  | Filter         | 4            |                                             |
      | 4  | AppendVertices | 3            |                                             |
      | 3  | Traverse       | 2            |                                             |
      | 2  | IndexScan      | 1            | {"indexCtx": {"filter":"(player.age>20)"}}  |
      | 1  | Start          |              |                                             |
