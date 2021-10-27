# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Push topN down IndexScan Rule

  Background:
    Given a graph with space named "nba"

  Scenario: push topN down to IndexScan with order by
    When profiling query:
      """
      LOOKUP ON player | ORDER BY $-.VertexID | Limit 5
      """
    Then the result should be, in any order:
      | VertexID            |
      | "Amar'e Stoudemire" |
      | "Aron Baynes"       |
      | "Ben Simmons"       |
      | "Blake Griffin"     |
      | "Boris Diaw"        |
    And the execution plan should be:
      | id | name             | dependencies | operator info             |
      | 5  | DataCollect      | 9            |                           |
      | 9  | TopN             | 10           |                           |
      | 10 | Project          | 11           |                           |
      | 11 | TagIndexFullScan | 0            | {"orderBy": {"pos": "0"}} |
      | 0  | Start            |              |                           |
    When profiling query:
      """
      LOOKUP ON player | ORDER BY $-.VertexID | Limit 5
      """
    Then the result should be, in any order:
      | VertexID            |
      | "Amar'e Stoudemire" |
      | "Aron Baynes"       |
      | "Ben Simmons"       |
      | "Blake Griffin"     |
      | "Boris Diaw"        |
    And the execution plan should be:
      | id | name             | dependencies | operator info  |
      | 5  | DataCollect      | 9            |                |
      | 9  | TopN             | 10           |                |
      | 10 | Project          | 11           |                |
      | 11 | TagIndexFullScan | 0            | {"limit": "5"} |
      | 0  | Start            |              |                |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age == 33 | ORDER BY $-.VertexID | Limit 3
      """
    Then the result should be, in any order:
      | VertexID            |
      | "Chris Paul"        |
      | "Dwight Howard"     |
      | "LaMarcus Aldridge" |
    And the execution plan should be:
      | id | name               | dependencies | operator info             |
      | 5  | DataCollect        | 9            |                           |
      | 9  | TopN               | 10           |                           |
      | 10 | Project            | 11           |                           |
      | 11 | TagIndexPrefixScan | 0            | {"orderBy": {"pos": "0"}} |
      | 0  | Start              |              |                           |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age == 33 | ORDER BY $-.VertexID | Limit 3
      """
    Then the result should be, in any order:
      | VertexID            |
      | "Chris Paul"        |
      | "Dwight Howard"     |
      | "LaMarcus Aldridge" |
    And the execution plan should be:
      | id | name               | dependencies | operator info  |
      | 5  | DataCollect        | 9            |                |
      | 9  | TopN               | 10           |                |
      | 10 | Project            | 11           |                |
      | 11 | TagIndexPrefixScan | 0            | {"limit": "3"} |
      | 0  | Start              |              |                |
    When profiling query:
      """
      LOOKUP ON like | ORDER BY $-.SrcVID | Limit 5
      """
    Then the result should be, in any order:
      | SrcVID              | DstVID        | Ranking |
      | "Amar'e Stoudemire" | "Steve Nash"  | 0       |
      | "Aron Baynes"       | "Tim Duncan"  | 0       |
      | "Ben Simmons"       | "Joel Embiid" | 0       |
      | "Blake Griffin"     | "Chris Paul"  | 0       |
      | "Boris Diaw"        | "Tony Parker" | 0       |
    And the execution plan should be:
      | id | name              | dependencies | operator info             |
      | 5  | DataCollect       | 9            |                           |
      | 9  | TopN              | 10           |                           |
      | 10 | Project           | 11           |                           |
      | 11 | EdgeIndexFullScan | 0            | {"orderBy": {"pos": "0"}} |
      | 0  | Start             |              |                           |
    When profiling query:
      """
      LOOKUP ON like | ORDER BY $-.SrcVID | Limit 5
      """
    Then the result should be, in any order:
      | SrcVID              | DstVID        | Ranking |
      | "Amar'e Stoudemire" | "Steve Nash"  | 0       |
      | "Aron Baynes"       | "Tim Duncan"  | 0       |
      | "Ben Simmons"       | "Joel Embiid" | 0       |
      | "Blake Griffin"     | "Chris Paul"  | 0       |
      | "Boris Diaw"        | "Tony Parker" | 0       |
    And the execution plan should be:
      | id | name              | dependencies | operator info  |
      | 5  | DataCollect       | 9            |                |
      | 9  | TopN              | 10           |                |
      | 10 | Project           | 11           |                |
      | 11 | EdgeIndexFullScan | 0            | {"limit": "5"} |
      | 0  | Start             |              |                |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness == 90 | ORDER BY $-.SrcVID | Limit 5
      """
    Then the result should be, in any order:
      | SrcVID              | DstVID         | Ranking |
      | "Amar'e Stoudemire" | "Steve Nash"   | 0       |
      | "Carmelo Anthony"   | "LeBron James" | 0       |
      | "Carmelo Anthony"   | "Chris Paul"   | 0       |
      | "Carmelo Anthony"   | "Dwyane Wade"  | 0       |
      | "Chris Paul"        | "LeBron James" | 0       |
    And the execution plan should be:
      | id | name                | dependencies | operator info             |
      | 5  | DataCollect         | 9            |                           |
      | 9  | TopN                | 10           |                           |
      | 10 | Project             | 11           |                           |
      | 11 | EdgeIndexPrefixScan | 0            | {"orderBy": {"pos": "0"}} |
      | 0  | Start               |              |                           |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness == 90 | ORDER BY $-.SrcVID | Limit 5
      """
    Then the result should be, in any order:
      | SrcVID              | DstVID         | Ranking |
      | "Amar'e Stoudemire" | "Steve Nash"   | 0       |
      | "Carmelo Anthony"   | "LeBron James" | 0       |
      | "Carmelo Anthony"   | "Chris Paul"   | 0       |
      | "Carmelo Anthony"   | "Dwyane Wade"  | 0       |
      | "Chris Paul"        | "LeBron James" | 0       |
    And the execution plan should be:
      | id | name                | dependencies | operator info  |
      | 5  | DataCollect         | 9            |                |
      | 9  | TopN                | 10           |                |
      | 10 | Project             | 11           |                |
      | 11 | EdgeIndexPrefixScan | 0            | {"limit": "5"} |
      | 0  | Start               |              |                |

  Scenario: push topN down to IndexScan with order by with alias
    When profiling query:
      """
      LOOKUP ON player WHERE player.age == 33 yield player.name as n | ORDER BY $-.n | Limit 3
      """
    Then the result should be, in any order:
      | VertexID            | n                   |
      | "Chris Paul"        | "Chris Paul"        |
      | "Dwight Howard"     | "Dwight Howard"     |
      | "LaMarcus Aldridge" | "LaMarcus Aldridge" |
    And the execution plan should be:
      | id | name               | dependencies | operator info             |
      | 5  | DataCollect        | 9            |                           |
      | 9  | TopN               | 10           |                           |
      | 10 | Project            | 11           |                           |
      | 11 | TagIndexPrefixScan | 0            | {"orderBy": {"pos": "2"}} |
      | 0  | Start              |              |                           |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age == 33 yield player.name as n | ORDER BY $-.n | Limit 3
      """
    Then the result should be, in any order:
      | VertexID            | n                   |
      | "Chris Paul"        | "Chris Paul"        |
      | "Dwight Howard"     | "Dwight Howard"     |
      | "LaMarcus Aldridge" | "LaMarcus Aldridge" |
    And the execution plan should be:
      | id | name               | dependencies | operator info  |
      | 5  | DataCollect        | 9            |                |
      | 9  | TopN               | 10           |                |
      | 10 | Project            | 11           |                |
      | 11 | TagIndexPrefixScan | 0            | {"limit": "3"} |
      | 0  | Start              |              |                |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness < 90 yield like.likeness as likeness | ORDER BY $-.likeness | Limit 5
      """
    Then the result should be, in any order:
      | SrcVID          | DstVID         | Ranking | likeness |
      | "Blake Griffin" | "Chris Paul"   | 0       | -1       |
      | "Rajon Rondo"   | "Ray Allen"    | 0       | -1       |
      | "Ray Allen"     | "Rajon Rondo"  | 0       | 9        |
      | "Dirk Nowitzki" | "Dwyane Wade"  | 0       | 10       |
      | "Kyrie Irving"  | "LeBron James" | 0       | 13       |
    And the execution plan should be:
      | id | name               | dependencies | operator info             |
      | 5  | DataCollect        | 9            |                           |
      | 9  | TopN               | 10           |                           |
      | 10 | Project            | 11           |                           |
      | 11 | EdgeIndexRangeScan | 0            | {"orderBy": {"pos": "4"}} |
      | 0  | Start              |              |                           |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness < 90 yield like.likeness as likeness | ORDER BY $-.likeness | Limit 5
      """
    Then the result should be, in any order:
      | SrcVID          | DstVID         | Ranking | likeness |
      | "Blake Griffin" | "Chris Paul"   | 0       | -1       |
      | "Rajon Rondo"   | "Ray Allen"    | 0       | -1       |
      | "Ray Allen"     | "Rajon Rondo"  | 0       | 9        |
      | "Dirk Nowitzki" | "Dwyane Wade"  | 0       | 10       |
      | "Kyrie Irving"  | "LeBron James" | 0       | 13       |
    And the execution plan should be:
      | id | name               | dependencies | operator info  |
      | 5  | DataCollect        | 9            |                |
      | 9  | TopN               | 10           |                |
      | 10 | Project            | 11           |                |
      | 11 | EdgeIndexRangeScan | 0            | {"limit": "5"} |
      | 0  | Start              |              |                |

  Scenario: push topn down to IndexScan only limit
    When profiling query:
      """
      LOOKUP ON player | Limit 5
      """
    Then the result should be, in any order:
      | VertexID            |
      | "Amar'e Stoudemire" |
      | "Aron Baynes"       |
      | "Ben Simmons"       |
      | "Blake Griffin"     |
      | "Boris Diaw"        |
    And the execution plan should be:
      | id | name             | dependencies | operator info  |
      | 4  | DataCollect      | 7            |                |
      | 7  | Project          | 8            |                |
      | 8  | Limit            | 9            |                |
      | 9  | TagIndexFullScan | 0            | {"limit": "5"} |
      | 0  | Start            |              |                |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age == 33 | Limit 3
      """
    Then the result should be, in any order:
      | VertexID            |
      | "Chris Paul"        |
      | "Dwight Howard"     |
      | "LaMarcus Aldridge" |
    And the execution plan should be:
      | id | name               | dependencies | operator info  |
      | 4  | DataCollect        | 7            |                |
      | 7  | Project            | 8            |                |
      | 8  | Limit              | 9            |                |
      | 9  | TagIndexPrefixScan | 0            | {"limit": "3"} |
      | 0  | Start              |              |                |
    When profiling query:
      """
      LOOKUP ON like | Limit 5
      """
    Then the result should be, in any order:
      | SrcVID          | DstVID        | Ranking |
      | "Aron Baynes"   | "Tim Duncan"  | 0       |
      | "Ben Simmons"   | "Joel Embiid" | 0       |
      | "Blake Griffin" | "Chris Paul"  | 0       |
      | "Boris Diaw"    | "Tim Duncan"  | 0       |
      | "Boris Diaw"    | "Tony Parker" | 0       |
    And the execution plan should be:
      | id | name              | dependencies | operator info  |
      | 5  | DataCollect       | 9            |                |
      | 9  | Project           | 10           |                |
      | 10 | Limit             | 11           |                |
      | 11 | EdgeIndexFullScan | 0            | {"limit": "5"} |
      | 0  | Start             |              |                |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness == 90 | Limit 5
      """
    Then the result should be, in any order:
      | SrcVID              | DstVID            | Ranking |
      | "Amar'e Stoudemire" | "Steve Nash"      | 0       |
      | "Carmelo Anthony"   | "LeBron James"    | 0       |
      | "Carmelo Anthony"   | "Chris Paul"      | 0       |
      | "Carmelo Anthony"   | "Dwyane Wade"     | 0       |
      | "Chris Paul"        | "Carmelo Anthony" | 0       |
    And the execution plan should be:
      | id | name                | dependencies | operator info  |
      | 5  | DataCollect         | 9            |                |
      | 9  | Project             | 10           |                |
      | 10 | Limit               | 11           |                |
      | 11 | EdgeIndexPrefixScan | 0            | {"limit": "5"} |
      | 0  | Start               |              |                |
