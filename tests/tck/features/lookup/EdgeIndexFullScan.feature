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
    And wait 6 seconds
    And having executed:
      """
      INSERT EDGE
      edge_1(col1_str, col2_int)
      VALUES
      '101'->'102':('Red1', 11),
      '102'->'103':('Yellow', 22),
      '103'->'101':('Blue', 33);
      """

  Scenario: Edge with relational RegExp filter
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str =~ "\\w+\\d+" YIELD edge_1.col1_str
      """
    Then a SemanticError should be raised at runtime: Expression (edge_1.col1_str=~"\w+\d+") is not supported, please use full-text index as an optimal solution

  Scenario: Edge with relational NE filter
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str != "Yellow" YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col1_str
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col1_str |
      | "101" | "102" | 0    | "Red1"          |
      | "103" | "101" | 0    | "Blue"          |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                  |
      | 3  | Project           | 2            |                                                |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col1_str!=\"Yellow\")"} |
      | 4  | EdgeIndexFullScan | 0            |                                                |
      | 0  | Start             |              |                                                |
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col2_int != 11 YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col2_int
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col2_int |
      | "103" | "101" | 0    | 33              |
      | "102" | "103" | 0    | 22              |
    And the execution plan should be:
      | id | name              | dependencies | operator info                          |
      | 3  | Project           | 2            |                                        |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col2_int!=11)"} |
      | 4  | EdgeIndexFullScan | 0            |                                        |
      | 0  | Start             |              |                                        |

  Scenario: Edge with simple relational IN filter
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str IN ["Red", "Yellow"] YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col1_str
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col1_str |
      | "102" | "103" | 0    | "Yellow"        |
    And the execution plan should be:
      | id | name      | dependencies | operator info |
      | 3  | Project   | 4            |               |
      | 4  | IndexScan | 0            |               |
      | 0  | Start     |              |               |
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str IN ["non-existed-name"] YIELD edge_1.col1_str
      """
    Then the result should be, in any order:
      | edge_1.col1_str |
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col2_int IN [23 - 1 , 66/2] YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col2_int
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col2_int |
      | "103" | "101" | 0    | 33              |
      | "102" | "103" | 0    | 22              |
    And the execution plan should be:
      | id | name      | dependencies | operator info |
      | 3  | Project   | 4            |               |
      | 4  | IndexScan | 0            |               |
      | 0  | Start     |              |               |
    # a IN b OR c
    When profiling query:
      """
      LOOKUP ON edge_1
      WHERE edge_1.col2_int IN [23 - 1 , 66/2] OR edge_1.col2_int==11
      YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col2_int
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col2_int |
      | "101" | "102" | 0    | 11              |
      | "102" | "103" | 0    | 22              |
      | "103" | "101" | 0    | 33              |
    And the execution plan should be:
      | id | name      | dependencies | operator info |
      | 3  | Project   | 4            |               |
      | 4  | IndexScan | 0            |               |
      | 0  | Start     |              |               |
    # a IN b OR c IN d
    When profiling query:
      """
      LOOKUP ON edge_1
      WHERE edge_1.col2_int IN [23 - 1 , 66/2] OR edge_1.col1_str IN [toUpper("r")+"ed1"]
      YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col1_str, edge_1.col2_int
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col1_str | edge_1.col2_int |
      | "101" | "102" | 0    | "Red1"          | 11              |
      | "102" | "103" | 0    | "Yellow"        | 22              |
      | "103" | "101" | 0    | "Blue"          | 33              |
    And the execution plan should be:
      | id | name      | dependencies | operator info |
      | 3  | Project   | 4            |               |
      | 4  | IndexScan | 0            |               |
      | 0  | Start     |              |               |
    # a IN b AND c (EdgeIndexPrefixScan)
    When profiling query:
      """
      LOOKUP ON edge_1
      WHERE edge_1.col2_int IN [11 , 66/2] AND edge_1.col2_int==11
      YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col2_int
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col2_int |
      | "101" | "102" | 0    | 11              |
    And the execution plan should be:
      | id | name              | dependencies | operator info |
      | 3  | Project           | 2            |               |
      | 2  | Filter            | 4            |               |
      | 4  | EdgeIndexFullScan | 0            |               |
      | 0  | Start             |              |               |

  Scenario: Edge with complex relational IN filter
    # (a IN b) AND (c IN d)
    # List has only 1 element, so prefixScan is applied
    When profiling query:
      """
      LOOKUP ON edge_1
      WHERE edge_1.col2_int IN [11 , 33] AND edge_1.col1_str IN ["Red1"]
      YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col1_str, edge_1.col2_int
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col1_str | edge_1.col2_int |
      | "101" | "102" | 0    | "Red1"          | 11              |
    And the execution plan should be:
      | id | name      | dependencies | operator info |
      | 3  | Project   | 4            |               |
      | 4  | IndexScan | 0            |               |
      | 0  | Start     |              |               |
    # (a IN b) AND (c IN d)
    # a, c both have indexes (4 prefixScan will be executed)
    When profiling query:
      """
      LOOKUP ON edge_1
      WHERE edge_1.col2_int IN [11 , 33] AND edge_1.col1_str IN ["Red1", "ABC"]
      YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col1_str, edge_1.col2_int
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col1_str | edge_1.col2_int |
      | "101" | "102" | 0    | "Red1"          | 11              |
    And the execution plan should be:
      | id | name      | dependencies | operator info |
      | 3  | Project   | 4            |               |
      | 4  | IndexScan | 0            |               |
      | 0  | Start     |              |               |
    # (a IN b) AND (c IN d)
    # a, c have a composite index
    When executing query:
      """
      CREATE EDGE INDEX composite_edge_index ON edge_1(col1_str(20), col2_int);
      """
    Then the execution should be successful
    And wait 6 seconds
    When submit a job:
      """
      REBUILD EDGE INDEX composite_edge_index
      """
    Then wait the job to finish
    When profiling query:
      """
      LOOKUP ON edge_1
      WHERE edge_1.col2_int IN [11 , 33] AND edge_1.col1_str IN ["Red1", "ABC"]
      YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col1_str, edge_1.col2_int
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col1_str | edge_1.col2_int |
      | "101" | "102" | 0    | "Red1"          | 11              |
    And the execution plan should be:
      | id | name      | dependencies | operator info |
      | 3  | Project   | 4            |               |
      | 4  | IndexScan | 0            |               |
      | 0  | Start     |              |               |
    # (a IN b) AND (c IN d) while only a has index
    # first drop tag index
    When executing query:
      """
      DROP EDGE INDEX composite_edge_index
      """
    Then the execution should be successful
    When executing query:
      """
      DROP EDGE INDEX col1_str_index
      """
    Then the execution should be successful
    And wait 6 seconds
    # since the edge index has been dropped, here an EdgeIndexFullScan should be performed
    When profiling query:
      """
      LOOKUP ON edge_1
      WHERE edge_1.col1_str IN ["Red1", "ABC"]
      YIELD  src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col1_str, edge_1.col2_int
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col1_str | edge_1.col2_int |
      | "101" | "102" | 0    | "Red1"          | 11              |
    And the execution plan should be:
      | id | name              | dependencies | operator info |
      | 3  | Project           | 2            |               |
      | 2  | Filter            | 4            |               |
      | 4  | EdgeIndexFullScan | 0            |               |
      | 0  | Start             |              |               |
    When profiling query:
      """
      LOOKUP ON edge_1
      WHERE edge_1.col2_int IN [11 , 33] AND edge_1.col1_str IN ["Red1", "ABC"]
      YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col1_str, edge_1.col2_int
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col1_str | edge_1.col2_int |
      | "101" | "102" | 0    | "Red1"          | 11              |
    And the execution plan should be:
      | id | name      | dependencies | operator info |
      | 3  | Project   | 4            |               |
      | 4  | IndexScan | 0            |               |
      | 0  | Start     |              |               |

  Scenario: Edge with relational NOT IN filter
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str NOT IN ["Blue"] YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col1_str
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col1_str |
      | "101" | "102" | 0    | "Red1"          |
      | "102" | "103" | 0    | "Yellow"        |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                        |
      | 3  | Project           | 2            |                                                      |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col1_str NOT IN [\"Blue\"])"} |
      | 4  | EdgeIndexFullScan | 0            |                                                      |
      | 0  | Start             |              |                                                      |
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col2_int NOT IN [23 - 1 , 66/2] YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col2_int
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col2_int |
      | "101" | "102" | 0    | 11              |
    And the execution plan should be:
      | id | name              | dependencies | operator info                                     |
      | 3  | Project           | 2            |                                                   |
      | 2  | Filter            | 4            | {"condition": "(edge_1.col2_int NOT IN [22,33])"} |
      | 4  | EdgeIndexFullScan | 0            |                                                   |
      | 0  | Start             |              |                                                   |

  Scenario: Edge with relational CONTAINS/NOT CONTAINS filter
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str CONTAINS "ABC" YIELD edge_1.col1_str
      """
    Then a SemanticError should be raised at runtime: Expression (edge_1.col1_str CONTAINS "ABC") is not supported, please use full-text index as an optimal solution
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str NOT CONTAINS "ABC" YIELD edge_1.col1_str
      """
    Then a SemanticError should be raised at runtime: Expression (edge_1.col1_str NOT CONTAINS "ABC") is not supported, please use full-text index as an optimal solution

  Scenario: Edge with relational STARTS/NOT STARTS WITH filter
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str STARTS WITH toUpper("r") YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, edge_1.col1_str
      """
    Then the result should be, in any order:
      | src   | dst   | rank | edge_1.col1_str |
      | "101" | "102" | 0    | "Red1"          |
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
      | edge_1.col1_str |
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str STARTS WITH 123 YIELD edge_1.col1_str
      """
    Then a SemanticError should be raised at runtime: Column type error : col1_str
    When profiling query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str NOT STARTS WITH "R" YIELD edge_1.col1_str
      """
    Then a SemanticError should be raised at runtime: Expression (edge_1.col1_str NOT STARTS WITH "R") is not supported, please use full-text index as an optimal solution

  Scenario: Edge with relational ENDS/NOT ENDS WITH filter
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str ENDS WITH "ABC" YIELD edge_1.col1_str
      """
    Then a SemanticError should be raised at runtime: Expression (edge_1.col1_str ENDS WITH "ABC") is not supported, please use full-text index as an optimal solution
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col1_str NOT ENDS WITH toLower("E") YIELD edge_1.col1_str
      """
    Then a SemanticError should be raised at runtime: Expression (edge_1.col1_str NOT ENDS WITH toLower("E")) is not supported, please use full-text index as an optimal solution
