Feature: Lookup edge index full scan

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(15) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    And having executed:
      """
      CREATE EDGE edge_1(col1_str string, col2_int int);
      """
    And wait 3 seconds
    # index on col1_str
    And having executed:
      """
      CREATE EDGE INDEX col1_str_index ON edge_1(col1_str(10))
      """
    # index on col2_int
    And having executed:
      """
      CREATE EDGE INDEX col2_int_index ON edge_1(col2_int)
      """
    And wait 3 seconds
    And having executed:
      """
      INSERT EDGE
        edge_1(col1_str, col2_int)
      VALUES
        '101'->'102':('Red1', 11),
        '102'->'103':('Yellow', 22),
        '103'->'101':('Blue', 33);
      """
    And wait 3 seconds

  Scenario: Edge with relational RegExp filter[1]
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str =~ "\\w+\\d+" YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
      | "101"  | "102"  | 0       | "Red1"          |
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str =~ "\\w+ll\\w+" YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
      | "102"  | "103"  | 0       | "Yellow"        |

  # skip because `make fmt` will delete '\' in the operator info and causes tests fail
  @skip
  Scenario: Edge with relational RegExp filter[2]
    When profiling query:
      """
      LOOKUP ON edge_1 where edge_1.col1_str =~ "\\d+\\w+" YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
      | "101"  | "102"  | 0       | "Red1"          |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                  |
      | 3  | Project           | 2            |                                                |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col1_str=~\"\w+\d+\")"} |
      | 4  | EdgeIndexFullScan | 0            |                                                |
      | 0  | Start             |              |                                                |
    When profiling query:
      """
      LOOKUP ON edge_1 where edge_1.col1_str =~ "\\w+ea\\w+" YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
      | "102"  | "103"  | 0       | "Yellow"        |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                    |
      | 3  | Project           | 2            |                                                  |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col1_str=~\"\w+ea\w+\")"} |
      | 4  | EdgeIndexFullScan | 0            |                                                  |
      | 0  | Start             |              |                                                  |

  Scenario: Edge with relational NE filter
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str != "Yellow" YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
      | "101"  | "102"  | 0       | "Red1"          |
      | "103"  | "101"  | 0       | "Blue"          |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                  |
      | 3  | Project           | 2            |                                                |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col1_str!=\"Yellow\")"} |
      | 4  | EdgeIndexFullScan | 0            |                                                |
      | 0  | Start             |              |                                                |
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col2_int != 11 YIELD edge_1.col2_int
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col2_int |
      | "103"  | "101"  | 0       | 33              |
      | "102"  | "103"  | 0       | 22              |
    And the execution plan should be:
      | id | name              | dependencies | operator info                          |
      | 3  | Project           | 2            |                                        |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col2_int!=11)"} |
      | 4  | EdgeIndexFullScan | 0            |                                        |
      | 0  | Start             |              |                                        |

  Scenario: Edge with relational IN/NOT IN filter
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str IN ["Red", "Yellow"] YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
      | "102"  | "103"  | 0       | "Yellow"        |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                              |
      | 3  | Project           | 2            |                                                            |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col1_str IN [\"Red\",\"Yellow\"])"} |
      | 4  | EdgeIndexFullScan | 0            |                                                            |
      | 0  | Start             |              |                                                            |
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str IN ["non-existed-name"] YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col2_int IN [23 - 1 , 66/2] YIELD edge_1.col2_int
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col2_int |
      | "103"  | "101"  | 0       | 33              |
      | "102"  | "103"  | 0       | 22              |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                 |
      | 3  | Project           | 2            |                                               |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col2_int IN [22,33])"} |
      | 4  | EdgeIndexFullScan | 0            |                                               |
      | 0  | Start             |              |                                               |
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str NOT IN ["Blue"] YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
      | "101"  | "102"  | 0       | "Red1"          |
      | "102"  | "103"  | 0       | "Yellow"        |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                        |
      | 3  | Project           | 2            |                                                      |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col1_str NOT IN [\"Blue\"])"} |
      | 4  | EdgeIndexFullScan | 0            |                                                      |
      | 0  | Start             |              |                                                      |
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col2_int NOT IN [23 - 1 , 66/2] YIELD edge_1.col2_int
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col2_int |
      | "101"  | "102"  | 0       | 11              |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                     |
      | 3  | Project           | 2            |                                                   |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col2_int NOT IN [22,33])"} |
      | 4  | EdgeIndexFullScan | 0            |                                                   |
      | 0  | Start             |              |                                                   |

  Scenario: Edge with relational CONTAINS/NOT CONTAINS filter
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str CONTAINS toLower("L") YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
      | "103"  | "101"  | 0       | "Blue"          |
      | "102"  | "103"  | 0       | "Yellow"        |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                     |
      | 3  | Project           | 2            |                                                   |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col1_str CONTAINS \"l\")"} |
      | 4  | EdgeIndexFullScan | 0            |                                                   |
      | 0  | Start             |              |                                                   |
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str CONTAINS "ABC" YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str NOT CONTAINS toLower("L") YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
      | "101"  | "102"  | 0       | "Red1"          |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                         |
      | 3  | Project           | 2            |                                                       |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col1_str NOT CONTAINS \"l\")"} |
      | 4  | EdgeIndexFullScan | 0            |                                                       |
      | 0  | Start             |              |                                                       |

  Scenario: Edge with relational STARTS/NOT STARTS WITH filter
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str STARTS WITH toUpper("r") YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
      | "101"  | "102"  | 0       | "Red1"          |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                        |
      | 3  | Project           | 2            |                                                      |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col1_str STARTS WITH \"R\")"} |
      | 4  | EdgeIndexFullScan | 0            |                                                      |
      | 0  | Start             |              |                                                      |
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str STARTS WITH "ABC" YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str STARTS WITH 123 YIELD edge_1.col1_str
      """
    Then a SemanticError should be raised at runtime: Column type error : col1_str
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str NOT STARTS WITH toUpper("r") YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
      | "103"  | "101"  | 0       | "Blue"          |
      | "102"  | "103"  | 0       | "Yellow"        |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                            |
      | 3  | Project           | 2            |                                                          |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col1_str NOT STARTS WITH \"R\")"} |
      | 4  | EdgeIndexFullScan | 0            |                                                          |
      | 0  | Start             |              |                                                          |

  Scenario: Edge with relational ENDS/NOT ENDS WITH filter
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str ENDS WITH toLower("E") YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
      | "103"  | "101"  | 0       | "Blue"          |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                      |
      | 3  | Project           | 2            |                                                    |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col1_str ENDS WITH \"e\")"} |
      | 4  | EdgeIndexFullScan | 0            |                                                    |
      | 0  | Start             |              |                                                    |
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str ENDS WITH "ABC" YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str ENDS WITH 123 YIELD edge_1.col1_str
      """
    Then a SemanticError should be raised at runtime: Column type error : col1_str
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str NOT ENDS WITH toLower("E") YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1_str |
      | "101"  | "102"  | 0       | "Red1"          |
      | "102"  | "103"  | 0       | "Yellow"        |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                          |
      | 3  | Project           | 2            |                                                        |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col1_str NOT ENDS WITH \"e\")"} |
      | 4  | EdgeIndexFullScan | 0            |                                                        |
      | 0  | Start             |              |                                                        |
