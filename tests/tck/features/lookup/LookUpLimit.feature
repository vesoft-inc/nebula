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
      LOOKUP ON player Limit 2 | ORDER BY $-.VertexID
      """
    Then the result should be, in any order:
      | VertexID      |
      | /[a-zA-Z ']+/ |
      | /[a-zA-Z ']+/ |
    And the execution plan should be:
      | id | name             | dependencies | operator info  |
      | 4  | DataCollect      | 5            |                |
      | 5  | Sort             | 6            |                |
      | 6  | Project          | 7            |                |
      | 7  | Limit            | 8            | {"count": "2"} |
      | 8  | TagIndexFullScan | 0            | {"limit": "2"} |
      | 0  | Start            |              |                |
    When profiling query:
      """
      LOOKUP ON like Limit 2 | ORDER BY $-.SrcVID
      """
    Then the result should be, in any order:
      | SrcVID        | DstVID        | Ranking |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/   |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/   |
    And the execution plan should be:
      | id | name              | dependencies | operator info  |
      | 4  | DataCollect       | 5            |                |
      | 5  | Sort              | 6            |                |
      | 6  | Project           | 7            |                |
      | 7  | Limit             | 8            | {"count": "2"} |
      | 8  | EdgeIndexFullScan | 0            | {"limit": "2"} |
      | 0  | Start             |              |                |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age == 33 Limit 2 | ORDER BY $-.VertexID
      """
    Then the result should be, in any order:
      | VertexID      |
      | /[a-zA-Z ']+/ |
      | /[a-zA-Z ']+/ |
    And the execution plan should be:
      | id | name               | dependencies | operator info  |
      | 4  | DataCollect        | 5            |                |
      | 5  | Sort               | 6            |                |
      | 6  | Project            | 7            |                |
      | 7  | Limit              | 8            | {"count": "2"} |
      | 8  | TagIndexPrefixScan | 0            | {"limit": "2"} |
      | 0  | Start              |              |                |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness == 90 Limit 2 | ORDER BY $-.SrcVID
      """
    Then the result should be, in any order:
      | SrcVID        | DstVID        | Ranking |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/   |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/   |
    And the execution plan should be:
      | id | name                | dependencies | operator info  |
      | 4  | DataCollect         | 5            |                |
      | 5  | Sort                | 6            |                |
      | 6  | Project             | 7            |                |
      | 7  | Limit               | 8            | {"count": "2"} |
      | 8  | EdgeIndexPrefixScan | 0            | {"limit": "2"} |
      | 0  | Start               |              |                |

  Scenario: push limit down to IndexScan with limit
    When profiling query:
      """
      LOOKUP ON player Limit 3 | LIMIT 3 | ORDER BY $-.VertexID
      """
    Then the result should be, in any order:
      | VertexID      |
      | /[a-zA-Z ']+/ |
      | /[a-zA-Z ']+/ |
      | /[a-zA-Z ']+/ |
    And the execution plan should be:
      | id | name             | dependencies | operator info  |
      | 3  | DataCollect      | 4            |                |
      | 4  | Sort             | 5            |                |
      | 5  | Project          | 6            |                |
      | 6  | Limit            | 7            |                |
      | 7  | Limit            | 8            |                |
      | 8  | TagIndexFullScan | 9            | {"limit": "3"} |
      | 9  | Start            |              |                |
    When profiling query:
      """
      LOOKUP ON like Limit 3 | LIMIT 3 | ORDER BY $-.SrcVID
      """
    Then the result should be, in any order:
      | SrcVID        | DstVID        | Ranking |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/   |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/   |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/   |
    And the execution plan should be:
      | id | name              | dependencies | operator info  |
      | 3  | DataCollect       | 4            |                |
      | 4  | Sort              | 5            |                |
      | 5  | Project           | 6            |                |
      | 6  | Limit             | 7            |                |
      | 7  | Limit             | 8            |                |
      | 8  | EdgeIndexFullScan | 9            | {"limit": "3"} |
      | 9  | Start             |              |                |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age == 33 Limit 3 | LIMIT 3 | ORDER BY $-.VertexID
      """
    Then the result should be, in any order:
      | VertexID      |
      | /[a-zA-Z ']+/ |
      | /[a-zA-Z ']+/ |
      | /[a-zA-Z ']+/ |
    And the execution plan should be:
      | id | name               | dependencies | operator info  |
      | 3  | DataCollect        | 4            |                |
      | 4  | Sort               | 5            |                |
      | 5  | Project            | 6            |                |
      | 6  | Limit              | 7            |                |
      | 7  | Limit              | 8            |                |
      | 8  | TagIndexPrefixScan | 9            | {"limit": "3"} |
      | 9  | Start              |              |                |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness == 90 Limit 3 | LIMIT 3 | ORDER BY $-.SrcVID
      """
    Then the result should be, in any order:
      | SrcVID        | DstVID        | Ranking |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/   |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/   |
      | /[a-zA-Z ']+/ | /[a-zA-Z ']+/ | /\d+/   |
    And the execution plan should be:
      | id | name                | dependencies | operator info  |
      | 3  | DataCollect         | 4            |                |
      | 4  | Sort                | 5            |                |
      | 5  | Project             | 6            |                |
      | 6  | Limit               | 7            |                |
      | 7  | Limit               | 8            |                |
      | 8  | EdgeIndexPrefixScan | 9            | {"limit": "3"} |
      | 9  | Start               |              |                |
