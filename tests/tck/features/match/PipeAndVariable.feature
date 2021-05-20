# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Pipe or use variable to store the match results

  Background:
    Given a graph with space named "nba"

  Scenario: pipe match results
    When executing query:
      """
      MATCH (v:player)
      WHERE v.name CONTAINS 'Tim'
      RETURN v.age AS age, id(v) AS vid |
      GO FROM $-.vid
      OVER like REVERSELY
      YIELD
        $-.age AS age,
        ('Tony Parker' == like._dst) AS liked,
        like._src AS src,
        like._dst AS dst
      """
    Then the result should be, in any order:
      | age | liked | src          | dst                 |
      | 42  | false | "Tim Duncan" | "Aron Baynes"       |
      | 42  | false | "Tim Duncan" | "Boris Diaw"        |
      | 42  | false | "Tim Duncan" | "Danny Green"       |
      | 42  | false | "Tim Duncan" | "Dejounte Murray"   |
      | 42  | false | "Tim Duncan" | "LaMarcus Aldridge" |
      | 42  | false | "Tim Duncan" | "Manu Ginobili"     |
      | 42  | false | "Tim Duncan" | "Marco Belinelli"   |
      | 42  | false | "Tim Duncan" | "Shaquile O'Neal"   |
      | 42  | false | "Tim Duncan" | "Tiago Splitter"    |
      | 42  | true  | "Tim Duncan" | "Tony Parker"       |

  Scenario: use variable to store match results
    When executing query:
      """
      $var = MATCH (v:player)
      WHERE v.name CONTAINS 'Tim'
      RETURN v.age AS age, id(v) AS vid;
      GO FROM $var.vid
      OVER like REVERSELY
      YIELD
        $var.age AS age,
        ('Tony Parker' == like._dst) AS liked,
        like._src AS src,
        like._dst AS dst
      """
    Then the result should be, in any order:
      | age | liked | src          | dst                 |
      | 42  | false | "Tim Duncan" | "Aron Baynes"       |
      | 42  | false | "Tim Duncan" | "Boris Diaw"        |
      | 42  | false | "Tim Duncan" | "Danny Green"       |
      | 42  | false | "Tim Duncan" | "Dejounte Murray"   |
      | 42  | false | "Tim Duncan" | "LaMarcus Aldridge" |
      | 42  | false | "Tim Duncan" | "Manu Ginobili"     |
      | 42  | false | "Tim Duncan" | "Marco Belinelli"   |
      | 42  | false | "Tim Duncan" | "Shaquile O'Neal"   |
      | 42  | false | "Tim Duncan" | "Tiago Splitter"    |
      | 42  | true  | "Tim Duncan" | "Tony Parker"       |

  Scenario: yield collect
    When executing query:
      """
      MATCH (v:player)
      WHERE v.name CONTAINS 'Tim'
      RETURN v.age as age, id(v) as vid |
      GO FROM $-.vid OVER like REVERSELY YIELD $-.age AS age, like._dst AS dst |
      YIELD
        any(d IN COLLECT(DISTINCT $-.dst) WHERE d=='Tony Parker') AS d,
        $-.age as age
      """
    Then the result should be, in any order:
      | d    | age |
      | true | 42  |
