# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Push Limit down IndexScan Rule

  Background:
    Given a graph with space named "nba"

  Scenario: push limit down to IndexScan
    When profiling query:
      """
      LOOKUP ON player |
      Limit 2
      """
    Then the result should be, in any order:
      | VertexID            |
      | "Amar'e Stoudemire" |
      | "Aron Baynes"       |
    And the execution plan should be:
      | id | name             | dependencies | operator info  |
      | 4  | DataCollect      | 5            |                |
      | 5  | Limit            | 6            |                |
      | 6  | Project          | 7            |                |
      | 7  | TagIndexFullScan | 0            | {"limit": "2"} |
      | 0  | Start            |              |                |
    When profiling query:
      """
      LOOKUP ON like |
      Limit 2
      """
    Then the result should be, in any order:
      | SrcVID              | DstVID       | Ranking |
      | "Amar'e Stoudemire" | "Steve Nash" | 0       |
      | "Aron Baynes"       | "Tim Duncan" | 0       |
    And the execution plan should be:
      | id | name              | dependencies | operator info  |
      | 4  | DataCollect       | 5            |                |
      | 5  | Limit             | 6            |                |
      | 6  | Project           | 7            |                |
      | 7  | EdgeIndexFullScan | 0            | {"limit": "2"} |
      | 0  | Start             |              |                |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age == 33 |
      Limit 2
      """
    Then the result should be, in any order:
      | VertexID        |
      | "Chris Paul"    |
      | "Dwight Howard" |
    And the execution plan should be:
      | id | name               | dependencies | operator info  |
      | 4  | DataCollect        | 5            |                |
      | 5  | Limit              | 6            |                |
      | 6  | Project            | 7            |                |
      | 7  | TagIndexPrefixScan | 0            | {"limit": "2"} |
      | 0  | Start              |              |                |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness == 90 |
      Limit 2
      """
    Then the result should be, in any order:
      | SrcVID              | DstVID       | Ranking |
      | "Amar'e Stoudemire" | "Steve Nash" | 0       |
      | "Carmelo Anthony"   | "Chris Paul" | 0       |
    And the execution plan should be:
      | id | name                | dependencies | operator info  |
      | 4  | DataCollect         | 5            |                |
      | 5  | Limit               | 6            |                |
      | 6  | Project             | 7            |                |
      | 7  | EdgeIndexPrefixScan | 0            | {"limit": "2"} |
      | 0  | Start               |              |                |

  Scenario: push limit down to IndexScan with offset
    When profiling query:
      """
      LOOKUP ON player |
      Limit 3, 4
      """
    Then the result should be, in any order:
      | VertexID          |
      | "Blake Griffin"   |
      | "Boris Diaw"      |
      | "Carmelo Anthony" |
      | "Chris Paul"      |
    And the execution plan should be:
      | id | name             | dependencies | operator info  |
      | 0  | DataCollect      | 1            |                |
      | 1  | Limit            | 2            |                |
      | 2  | Project          | 3            |                |
      | 3  | TagIndexFullScan | 4            | {"limit": "7"} |
      | 4  | Start            |              |                |
    When profiling query:
      """
      LOOKUP ON like |
      Limit 3, 4
      """
    Then the result should be, in any order:
      | SrcVID            | DstVID        | Ranking |
      | "Blake Griffin"   | "Chris Paul"  | 0       |
      | "Boris Diaw"      | "Tim Duncan"  | 0       |
      | "Boris Diaw"      | "Tony Parker" | 0       |
      | "Carmelo Anthony" | "Chris Paul"  | 0       |
    And the execution plan should be:
      | id | name              | dependencies | operator info  |
      | 0  | DataCollect       | 1            |                |
      | 1  | Limit             | 2            |                |
      | 2  | Project           | 3            |                |
      | 3  | EdgeIndexFullScan | 4            | {"limit": "7"} |
      | 4  | Start             |              |                |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age == 33 |
      Limit 3, 4
      """
    Then the result should be, in any order:
      | VertexID      |
      | "Rajon Rondo" |
    And the execution plan should be:
      | id | name               | dependencies | operator info  |
      | 0  | DataCollect        | 1            |                |
      | 1  | Limit              | 2            |                |
      | 2  | Project            | 3            |                |
      | 3  | TagIndexPrefixScan | 4            | {"limit": "7"} |
      | 4  | Start              |              |                |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness == 90 |
      Limit 3, 4
      """
    Then the result should be, in any order:
      | SrcVID            | DstVID            | Ranking |
      | "Carmelo Anthony" | "LeBron James"    | 0       |
      | "Chris Paul"      | "Carmelo Anthony" | 0       |
      | "Chris Paul"      | "Dwyane Wade"     | 0       |
      | "Chris Paul"      | "LeBron James"    | 0       |
    And the execution plan should be:
      | id | name                | dependencies | operator info  |
      | 0  | DataCollect         | 1            |                |
      | 1  | Limit               | 2            |                |
      | 2  | Project             | 3            |                |
      | 3  | EdgeIndexPrefixScan | 4            | {"limit": "7"} |
      | 4  | Start               |              |                |
