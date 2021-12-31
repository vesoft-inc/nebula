# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push TopN down IndexScan Rule

  Background:
    Given a graph with space named "nba"

  Scenario: do not push topN down to IndexScan
    When profiling query:
      """
      LOOKUP ON player YIELD id(vertex) as id | ORDER BY $-.id | Limit 2
      """
    Then the result should be, in any order:
      | id                  |
      | "Amar'e Stoudemire" |
      | "Aron Baynes"       |
    And the execution plan should be:
      | id | name             | dependencies | operator info                     |
      | 4  | DataCollect      | 5            |                                   |
      | 5  | TopN             | 6            | {"count": "2"}                    |
      | 6  | Project          | 7            |                                   |
      | 7  | TagIndexFullScan | 0            | {"limit": "9223372036854775807" } |
      | 0  | Start            |              |                                   |
    And the execution plan should be:
      | id | name             | dependencies | operator info     |
      | 4  | DataCollect      | 5            |                   |
      | 5  | TopN             | 6            |                   |
      | 6  | Project          | 7            |                   |
      | 7  | TagIndexFullScan | 0            | {"orderBy": "[]"} |
      | 0  | Start            |              |                   |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age==30 YIELD id(vertex) as id | ORDER BY $-.id | Limit 2
      """
    Then the result should be, in any order:
      | id               |
      | "Blake Griffin"  |
      | "DeAndre Jordan" |
    And the execution plan should be:
      | id | name               | dependencies | operator info                     |
      | 4  | DataCollect        | 5            |                                   |
      | 5  | TopN               | 6            | {"count": "2"}                    |
      | 6  | Project            | 7            |                                   |
      | 7  | TagIndexPrefixScan | 0            | {"limit": "9223372036854775807" } |
      | 0  | Start              |              |                                   |
    And the execution plan should be:
      | id | name               | dependencies | operator info     |
      | 4  | DataCollect        | 5            |                   |
      | 5  | TopN               | 6            |                   |
      | 6  | Project            | 7            |                   |
      | 7  | TagIndexPrefixScan | 0            | {"orderBy": "[]"} |
      | 0  | Start              |              |                   |
    When profiling query:
      """
      LOOKUP ON player WHERE player.name > "Ti" YIELD id(vertex) as id | ORDER BY $-.id | Limit 2
      """
    Then the result should be, in any order:
      | id               |
      | "Tiago Splitter" |
      | "Tim Duncan"     |
    And the execution plan should be:
      | id | name              | dependencies | operator info                     |
      | 4  | DataCollect       | 5            |                                   |
      | 5  | TopN              | 6            | {"count": "2"}                    |
      | 6  | Project           | 7            |                                   |
      | 7  | TagIndexRangeScan | 0            | {"limit": "9223372036854775807" } |
      | 0  | Start             |              |                                   |
    And the execution plan should be:
      | id | name              | dependencies | operator info     |
      | 4  | DataCollect       | 5            |                   |
      | 5  | TopN              | 6            |                   |
      | 6  | Project           | 7            |                   |
      | 7  | TagIndexRangeScan | 0            | {"orderBy": "[]"} |
      | 0  | Start             |              |                   |
    When profiling query:
      """
      LOOKUP ON player YIELD properties(vertex).name as name | ORDER BY $-.name | Limit 2
      """
    Then the result should be, in any order:
      | name                |
      | "Amar'e Stoudemire" |
      | "Aron Baynes"       |
    And the execution plan should be:
      | id | name             | dependencies | operator info                     |
      | 4  | DataCollect      | 5            |                                   |
      | 5  | TopN             | 6            | {"count": "2"}                    |
      | 6  | Project          | 7            |                                   |
      | 7  | TagIndexFullScan | 0            | {"limit": "9223372036854775807" } |
      | 0  | Start            |              |                                   |
    And the execution plan should be:
      | id | name             | dependencies | operator info     |
      | 4  | DataCollect      | 5            |                   |
      | 5  | TopN             | 6            |                   |
      | 6  | Project          | 7            |                   |
      | 7  | TagIndexFullScan | 0            | {"orderBy": "[]"} |
      | 0  | Start            |              |                   |
    When profiling query:
      """
      LOOKUP ON like YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank | ORDER BY $-.src | Limit 2
      """
    Then the result should be, in any order:
      | src                 | dst          | rank |
      | "Amar'e Stoudemire" | "Steve Nash" | 0    |
      | "Aron Baynes"       | "Tim Duncan" | 0    |
    And the execution plan should be:
      | id | name              | dependencies | operator info                     |
      | 4  | DataCollect       | 5            |                                   |
      | 5  | TopN              | 6            | {"count": "2"}                    |
      | 6  | Project           | 7            |                                   |
      | 7  | EdgeIndexFullScan | 0            | {"limit": "9223372036854775807" } |
      | 0  | Start             |              |                                   |
    And the execution plan should be:
      | id | name              | dependencies | operator info     |
      | 4  | DataCollect       | 5            |                   |
      | 5  | TopN              | 6            |                   |
      | 6  | Project           | 7            |                   |
      | 7  | EdgeIndexFullScan | 0            | {"orderBy": "[]"} |
      | 0  | Start             |              |                   |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness == 90 YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank | ORDER BY $-.src | Limit 2
      """
    Then the result should be, in any order:
      | src                 | dst          | rank |
      | "Amar'e Stoudemire" | "Steve Nash" | 0    |
      | "Carmelo Anthony"   | "Chris Paul" | 0    |
    And the execution plan should be:
      | id | name                | dependencies | operator info                     |
      | 4  | DataCollect         | 5            |                                   |
      | 5  | TopN                | 6            | {"count": "2"}                    |
      | 6  | Project             | 7            |                                   |
      | 7  | EdgeIndexPrefixScan | 0            | {"limit": "9223372036854775807" } |
      | 0  | Start               |              |                                   |
    And the execution plan should be:
      | id | name                | dependencies | operator info     |
      | 4  | DataCollect         | 5            |                   |
      | 5  | TopN                | 6            |                   |
      | 6  | Project             | 7            |                   |
      | 7  | EdgeIndexPrefixScan | 0            | {"orderBy": "[]"} |
      | 0  | Start               |              |                   |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness > 90 YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank | ORDER BY $-.src | Limit 2
      """
    Then the result should be, in any order:
      | src               | dst           | rank |
      | "Dejounte Murray" | "Danny Green" | 0    |
      | "Dejounte Murray" | "Chris Paul"  | 0    |
    And the execution plan should be:
      | id | name               | dependencies | operator info                     |
      | 4  | DataCollect        | 5            |                                   |
      | 5  | TopN               | 6            | {"count": "2"}                    |
      | 6  | Project            | 7            |                                   |
      | 7  | EdgeIndexRangeScan | 0            | {"limit": "9223372036854775807" } |
      | 0  | Start              |              |                                   |
    And the execution plan should be:
      | id | name               | dependencies | operator info     |
      | 4  | DataCollect        | 5            |                   |
      | 5  | TopN               | 6            |                   |
      | 6  | Project            | 7            |                   |
      | 7  | EdgeIndexRangeScan | 0            | {"orderBy": "[]"} |
      | 0  | Start              |              |                   |

  Scenario: push topN down to IndexScan
    When profiling query:
      """
      LOOKUP ON player YIELD player.name as name | ORDER BY $-.name | Limit 2
      """
    Then the result should be, in any order:
      | name                |
      | "Amar'e Stoudemire" |
      | "Aron Baynes"       |
    And the execution plan should be:
      | id | name             | dependencies | operator info   |
      | 4  | DataCollect      | 5            |                 |
      | 5  | TopN             | 6            | {"count": "2"}  |
      | 6  | Project          | 7            |                 |
      | 7  | TagIndexFullScan | 0            | {"limit": "2" } |
      | 0  | Start            |              |                 |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age==30 YIELD player.name as name | ORDER BY $-.name desc | Limit 2
      """
    Then the result should be, in any order:
      | name                |
      | "Russell Westbrook" |
      | "Kevin Durant"      |
    And the execution plan should be:
      | id | name               | dependencies | operator info   |
      | 4  | DataCollect        | 5            |                 |
      | 5  | TopN               | 6            | {"count": "2"}  |
      | 6  | Project            | 7            |                 |
      | 7  | TagIndexPrefixScan | 0            | {"limit": "2" } |
      | 0  | Start              |              |                 |
    When profiling query:
      """
      LOOKUP ON player WHERE player.age==30 YIELD player.name as name | ORDER BY $-.name | Limit 2
      """
    Then the result should be, in any order:
      | name             |
      | "Blake Griffin"  |
      | "DeAndre Jordan" |
    And the execution plan should be:
      | id | name               | dependencies | operator info   |
      | 4  | DataCollect        | 5            |                 |
      | 5  | TopN               | 6            | {"count": "2"}  |
      | 6  | Project            | 7            |                 |
      | 7  | TagIndexPrefixScan | 0            | {"limit": "2" } |
      | 0  | Start              |              |                 |
    When profiling query:
      """
      LOOKUP ON player WHERE player.name > "Ti" YIELD player.name as name | ORDER BY $-.name | Limit 2
      """
    Then the result should be, in any order:
      | name             |
      | "Tiago Splitter" |
      | "Tim Duncan"     |
    And the execution plan should be:
      | id | name              | dependencies | operator info   |
      | 4  | DataCollect       | 5            |                 |
      | 5  | TopN              | 6            | {"count": "2"}  |
      | 6  | Project           | 7            |                 |
      | 7  | TagIndexRangeScan | 0            | {"limit": "2" } |
      | 0  | Start             |              |                 |
    When profiling query:
      """
      LOOKUP ON like YIELD like.likeness as likeness | ORDER BY $-.likeness | Limit 2
      """
    Then the result should be, in any order:
      | likeness |
      | -1       |
      | -1       |
    And the execution plan should be:
      | id | name              | dependencies | operator info   |
      | 4  | DataCollect       | 5            |                 |
      | 5  | TopN              | 6            | {"count": "2"}  |
      | 6  | Project           | 7            |                 |
      | 7  | EdgeIndexFullScan | 0            | {"limit": "2" } |
      | 0  | Start             |              |                 |
    When profiling query:
      """
      LOOKUP ON like YIELD like.likeness as likeness | ORDER BY $-.likeness desc | Limit 2
      """
    Then the result should be, in any order:
      | likeness |
      | 100      |
      | 100      |
    And the execution plan should be:
      | id | name              | dependencies | operator info   |
      | 4  | DataCollect       | 5            |                 |
      | 5  | TopN              | 6            | {"count": "2"}  |
      | 6  | Project           | 7            |                 |
      | 7  | EdgeIndexFullScan | 0            | {"limit": "2" } |
      | 0  | Start             |              |                 |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness == 90 YIELD like.likeness as likeness | ORDER BY $-.likeness | Limit 2
      """
    Then the result should be, in any order:
      | likeness |
      | 90       |
      | 90       |
    And the execution plan should be:
      | id | name                | dependencies | operator info   |
      | 4  | DataCollect         | 5            |                 |
      | 5  | TopN                | 6            | {"count": "2"}  |
      | 6  | Project             | 7            |                 |
      | 7  | EdgeIndexPrefixScan | 0            | {"limit": "2" } |
      | 0  | Start               |              |                 |
    When profiling query:
      """
      LOOKUP ON like WHERE like.likeness > 90 YIELD like.likeness as likeness | ORDER BY $-.likeness | Limit 2
      """
    Then the result should be, in any order:
      | likeness |
      | 95       |
      | 95       |
    And the execution plan should be:
      | id | name               | dependencies | operator info   |
      | 4  | DataCollect        | 5            |                 |
      | 5  | TopN               | 6            | {"count": "2"}  |
      | 6  | Project            | 7            |                 |
      | 7  | EdgeIndexRangeScan | 0            | {"limit": "2" } |
      | 0  | Start              |              |                 |
