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
      | VertexID            |
      | "Amar'e Stoudemire" |
      | "Aron Baynes"       |
      | "Ben Simmons"       |
      | "Blake Griffin"     |
      | "Boris Diaw"        |
      | "Carmelo Anthony"   |
      | "Chris Paul"        |
      | "Cory Joseph"       |
      | "Damian Lillard"    |
      | "DeAndre Jordan"    |
      | "Dwyane Wade"       |
      | "JaVale McGee"      |
      | "Klay Thompson"     |
      | "Luka Doncic"       |
    And the execution plan should be:
      | id | name             | dependencies | operator info  |
      | 4  | DataCollect      | 5            |                |
      | 5  | Sort             | 6            |                |
      | 6  | Project          | 7            |                |
      | 7  | TagIndexFullScan | 0            | {"limit": "2"} |
      | 0  | Start            |              |                |
    When profiling query:
      """
      LOOKUP ON like Limit 2 | ORDER BY $-.SrcVID
      """
    Then the result should be, in any order:
      | SrcVID              | DstVID              | Ranking |
      | "Ben Simmons"       | "Joel Embiid"       | 0       |
      | "Blake Griffin"     | "Chris Paul"        | 0       |
      | "Damian Lillard"    | "LaMarcus Aldridge" | 0       |
      | "Dirk Nowitzki"     | "Dwyane Wade"       | 0       |
      | "Jason Kidd"        | "Vince Carter"      | 0       |
      | "Klay Thompson"     | "Stephen Curry"     | 0       |
      | "Kyrie Irving"      | "LeBron James"      | 0       |
      | "LaMarcus Aldridge" | "Tim Duncan"        | 0       |
      | "LaMarcus Aldridge" | "Tony Parker"       | 0       |
      | "Marco Belinelli"   | "Tim Duncan"        | 0       |
      | "Marco Belinelli"   | "Tony Parker"       | 0       |
      | "Rajon Rondo"       | "Ray Allen"         | 0       |
      | "Ray Allen"         | "Rajon Rondo"       | 0       |
      | "Rudy Gay"          | "LaMarcus Aldridge" | 0       |
    And the execution plan should be:
      | id | name              | dependencies | operator info  |
      | 4  | DataCollect       | 5            |                |
      | 5  | Sort              | 6            |                |
      | 6  | Project           | 7            |                |
      | 7  | EdgeIndexFullScan | 0            | {"limit": "2"} |
      | 0  | Start             |              |                |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age == 33 Limit 2 | ORDER BY $-.VertexID
      """
    Then the result should be, in any order:
      | VertexID            |
      | "Chris Paul"        |
      | "Dwight Howard"     |
      | "LaMarcus Aldridge" |
      | "Rajon Rondo"       |
    And the execution plan should be:
      | id | name               | dependencies | operator info  |
      | 4  | DataCollect        | 5            |                |
      | 5  | Sort               | 6            |                |
      | 6  | Project            | 7            |                |
      | 7  | TagIndexPrefixScan | 0            | {"limit": "2"} |
      | 0  | Start              |              |                |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness == 90 Limit 2 | ORDER BY $-.SrcVID
      """
    Then the result should be, in any order:
      | SrcVID              | DstVID               | Ranking |
      | "Amar'e Stoudemire" | "Steve Nash"         | 0       |
      | "Carmelo Anthony"   | "Chris Paul"         | 0       |
      | "Chris Paul"        | "Carmelo Anthony"    | 0       |
      | "Chris Paul"        | "Dwyane Wade"        | 0       |
      | "Dwyane Wade"       | "Carmelo Anthony"    | 0       |
      | "Dwyane Wade"       | "Chris Paul"         | 0       |
      | "Jason Kidd"        | "Steve Nash"         | 0       |
      | "Klay Thompson"     | "Stephen Curry"      | 0       |
      | "Luka Doncic"       | "Dirk Nowitzki"      | 0       |
      | "Luka Doncic"       | "Kristaps Porzingis" | 0       |
      | "Paul Gasol"        | "Kobe Bryant"        | 0       |
    And the execution plan should be:
      | id | name                | dependencies | operator info  |
      | 4  | DataCollect         | 5            |                |
      | 5  | Sort                | 6            |                |
      | 6  | Project             | 7            |                |
      | 7  | EdgeIndexPrefixScan | 0            | {"limit": "2"} |
      | 0  | Start               |              |                |

  Scenario: push limit down to IndexScan with limit
    When profiling query:
      """
      LOOKUP ON player Limit 3 | LIMIT 3 | ORDER BY $-.VertexID
      """
    Then the result should be, in any order:
      | VertexID            |
      | "Amar'e Stoudemire" |
      | "Aron Baynes"       |
      | "Ben Simmons"       |
    And the execution plan should be:
      | id | name             | dependencies | operator info  |
      | 3  | DataCollect      | 4            |                |
      | 4  | Sort             | 5            |                |
      | 5  | Limit            | 6            |                |
      | 6  | Project          | 7            |                |
      | 7  | TagIndexFullScan | 8            | {"limit": "3"} |
      | 8  | Start            |              |                |
    When profiling query:
      """
      LOOKUP ON like Limit 3 | LIMIT 3 | ORDER BY $-.SrcVID
      """
    Then the result should be, in any order:
      | SrcVID          | DstVID        | Ranking |
      | "Aron Baynes"   | "Tim Duncan"  | 0       |
      | "Ben Simmons"   | "Joel Embiid" | 0       |
      | "Blake Griffin" | "Chris Paul"  | 0       |
    And the execution plan should be:
      | id | name              | dependencies | operator info  |
      | 3  | DataCollect       | 4            |                |
      | 4  | Sort              | 5            |                |
      | 5  | Limit             | 6            |                |
      | 6  | Project           | 7            |                |
      | 7  | EdgeIndexFullScan | 8            | {"limit": "3"} |
      | 8  | Start             |              |                |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age == 33 Limit 3 | LIMIT 3 | ORDER BY $-.VertexID
      """
    Then the result should be, in any order:
      | VertexID            |
      | "Chris Paul"        |
      | "Dwight Howard"     |
      | "LaMarcus Aldridge" |
    And the execution plan should be:
      | id | name               | dependencies | operator info  |
      | 3  | DataCollect        | 4            |                |
      | 4  | Sort               | 5            |                |
      | 5  | Limit              | 6            |                |
      | 6  | Project            | 7            |                |
      | 7  | TagIndexPrefixScan | 8            | {"limit": "3"} |
      | 8  | Start              |              |                |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness == 90 Limit 3 | LIMIT 3 | ORDER BY $-.SrcVID
      """
    Then the result should be, in any order:
      | SrcVID              | DstVID        | Ranking |
      | "Amar'e Stoudemire" | "Steve Nash"  | 0       |
      | "Carmelo Anthony"   | "Chris Paul"  | 0       |
      | "Carmelo Anthony"   | "Dwyane Wade" | 0       |
    And the execution plan should be:
      | id | name                | dependencies | operator info  |
      | 3  | DataCollect         | 4            |                |
      | 4  | Sort                | 5            |                |
      | 5  | Limit               | 6            |                |
      | 6  | Project             | 7            |                |
      | 7  | EdgeIndexPrefixScan | 8            | {"limit": "3"} |
      | 8  | Start               |              |                |
