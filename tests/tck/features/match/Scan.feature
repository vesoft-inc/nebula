# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Match seek by scan

  Background: Prepare space
    Given a graph with space named "student"

  Scenario: query vertices by scan
    When executing query:
      """
      MATCH (v)
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name       |
      | "Sonya"    |
      | "Ann"      |
      | "Emma"     |
      | "Cynthia"  |
      | "Lisa"     |
      | "WangLe"   |
      | "WuXiao"   |
      | "Harry"    |
      | "Lily"     |
      | "Julie"    |
      | "Kim"      |
      | "ZhangKai" |
      | "Ben"      |
      | "Lilan"    |
      | "Peggy"    |
      | "Ada"      |
      | "Tom"      |
      | "Kevin"    |
      | "Lynn"     |
      | "HeNa"     |
      | "Mary"     |
      | "Jane"     |
      | "Sandy"    |
      | "Carl"     |
      | "Helen"    |
      | "Anne"     |
      | "Bonnie"   |
      | "Peter"    |
      | "XiaMei"   |
      | "Ellen"    |
    When executing query:
      """
      MATCH (v:teacher)
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name       |
      | "Mary"     |
      | "Ann"      |
      | "Julie"    |
      | "Kim"      |
      | "Ellen"    |
      | "ZhangKai" |
      | "Emma"     |
      | "Ben"      |
      | "Helen"    |
      | "Lilan"    |
    # TODO check index validation in match planner entry
    # When executing query:
    # """
    # MATCH (v:teacher)
    # WHERE v.name = "Mary"
    # RETURN v.name AS Name
    # """
    # Then the result should be, in any order:
    # | Name          |
    # | "Mary" |
    # When executing query:
    # """
    # MATCH (v:teacher {name: "Mary"})
    # RETURN v.name AS Name
    # """
    # Then the result should be, in any order:
    # | Name          |
    # | "Mary" |
    When executing query:
      """
      MATCH (v{name: "Mary"})
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name   |
      | "Mary" |
    When executing query:
      """
      MATCH (v:teacher:student)
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name |
    When executing query:
      """
      MATCH (v:person:teacher)
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name       |
      | "Mary"     |
      | "Ann"      |
      | "Julie"    |
      | "Kim"      |
      | "Ellen"    |
      | "ZhangKai" |
      | "Emma"     |
      | "Ben"      |
      | "Helen"    |
      | "Lilan"    |
    When executing query:
      """
      MATCH (v:person{name: "Mary"}:teacher)
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name   |
      | "Mary" |

  Scenario: query edge by scan
    When executing query:
      """
      MATCH ()-[e]->()
      RETURN type(e) AS Type
      """
    Then the result should be, in any order:
      | Type            |
      | "is_friend"     |
      | "is_friend"     |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_colleagues" |
      | "is_teacher"    |
      | "is_teacher"    |
      | "is_teacher"    |
      | "is_teacher"    |
      | "is_teacher"    |
      | "is_teacher"    |
      | "is_teacher"    |
      | "is_teacher"    |
      | "is_teacher"    |
      | "is_colleagues" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_colleagues" |
      | "is_teacher"    |
      | "is_colleagues" |
      | "is_teacher"    |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_friend"     |
      | "is_friend"     |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_friend"     |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_colleagues" |
      | "is_colleagues" |
      | "is_colleagues" |
      | "is_friend"     |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_friend"     |
      | "is_schoolmate" |
      | "is_schoolmate" |
    When executing query:
      """
      MATCH ()-[e:is_teacher]->()
      RETURN type(e) AS Type, e.start_year AS StartYear, e.end_year AS EndYear
      """
    Then the result should be, in any order:
      | Type         | StartYear | EndYear |
      | "is_teacher" | 2018      | 2019    |
      | "is_teacher" | 2018      | 2019    |
      | "is_teacher" | 2018      | 2019    |
      | "is_teacher" | 2018      | 2019    |
      | "is_teacher" | 2017      | 2018    |
      | "is_teacher" | 2015      | 2016    |
      | "is_teacher" | 2015      | 2016    |
      | "is_teacher" | 2015      | 2016    |
      | "is_teacher" | 2014      | 2015    |
      | "is_teacher" | 2017      | 2018    |
      | "is_teacher" | 2018      | 2019    |

# TODO check index validation in match planner entry
# When executing query:
# """
# MATCH ()-[e:is_teacher]->()
# WHERE e.start_year == 2018
# RETURN type(e) AS Type, e.start_year AS StartYear, e.end_year AS EndYear
# """
# Then the result should be, in any order:
# | Type          | StartYear | EndYear |
# | "is_teacher"  | 2018      | 2019    |
# | "is_teacher"  | 2018      | 2019    |
# | "is_teacher"  | 2018      | 2019    |
# | "is_teacher"  | 2018      | 2019    |
# | "is_teacher"  | 2018      | 2019    |
# When executing query:
# """
# MATCH ()-[e:is_teacher {start_year: 2018}]->()
# RETURN type(e) AS Type, e.start_year AS StartYear, e.end_year AS EndYear
# """
# Then the result should be, in any order:
# | Type          | StartYear | EndYear |
# | "is_teacher"  | 2018      | 2019    |
# | "is_teacher"  | 2018      | 2019    |
# | "is_teacher"  | 2018      | 2019    |
# | "is_teacher"  | 2018      | 2019    |
# | "is_teacher"  | 2018      | 2019    |
