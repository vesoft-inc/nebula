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
      RETURN v.student.name AS Name
      LIMIT 3
      """
    Then the result should be, in any order:
      | Name  |
      | /\w+/ |
      | /\w+/ |
      | /\w+/ |
    When executing query:
      """
      MATCH (v:teacher)
      RETURN v.teacher.name AS Name
      LIMIT 3
      """
    Then the result should be, in any order:
      | Name  |
      | /\w+/ |
      | /\w+/ |
      | /\w+/ |
    # TODO check index validation in match planner entry
    # When executing query:
    # """
    # MATCH (v:teacher)
    # WHERE v.name = "Mary"
    # RETURN v.name AS Name
    # LIMIT 3
    # """
    # Then the result should be, in any order:
    # | Name          |
    # | "Mary" |
    # When executing query:
    # """
    # MATCH (v:teacher {name: "Mary"})
    # RETURN v.name AS Name
    # LIMIT 3
    # """
    # Then the result should be, in any order:
    # | Name          |
    # | "Mary" |
    When executing query:
      """
      MATCH (v:teacher:student)
      RETURN v.student.name AS Name
      LIMIT 3
      """
    Then the result should be, in any order:
      | Name |
    When executing query:
      """
      MATCH (v:person:teacher)
      RETURN v.person.name AS Name
      LIMIT 3
      """
    Then the result should be, in any order:
      | Name  |
      | /\w+/ |
      | /\w+/ |
      | /\w+/ |
    When executing query:
      """
      MATCH (v:person{name: "Mary"}:teacher)
      RETURN v.person.name AS Name
      LIMIT 3
      """
    Then the result should be, in any order:
      | Name   |
      | "Mary" |

  Scenario: query vertices by scan with skip limit
    When executing query:
      """
      MATCH (v)
      RETURN v.person.name AS name
      SKIP 10 LIMIT 4
      """
    Then the result should be, in any order:
      | name     |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
    When executing query:
      """
      MATCH (v)
      RETURN v.person.name AS name
      SKIP 10 LIMIT 5
      """
    Then the result should be, in any order:
      | name     |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
    When executing query:
      """
      MATCH (v)
      RETURN v.person.name AS name
      SKIP 10 LIMIT 7
      """
    Then the result should be, in any order:
      | name     |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
    When executing query:
      """
      MATCH (v)
      RETURN v.person.name AS name
      SKIP 10 LIMIT 11
      """
    Then the result should be, in any order:
      | name     |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |

  Scenario: query vertices by scan failed
    When executing query:
      """
      MATCH (v)
      RETURN v.person.name AS Name
      """
    Then the result should be, in any order:
      | Name       |
      | "Anne"     |
      | "Cynthia"  |
      | "Jane"     |
      | "Lisa"     |
      | "Peggy"    |
      | "Kevin"    |
      | "WangLe"   |
      | "WuXiao"   |
      | "Sandy"    |
      | "Harry"    |
      | "Ada"      |
      | "Lynn"     |
      | "Bonnie"   |
      | "Peter"    |
      | "Carl"     |
      | "Sonya"    |
      | "HeNa"     |
      | "Tom"      |
      | "XiaMei"   |
      | "Lily"     |
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
      MATCH (v:person)
      RETURN v.student.name AS Name
      """
    Then the result should be, in any order:
      | Name |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |
      | NULL |

  Scenario: query edge by scan
    When executing query:
      """
      MATCH ()-[e]->()
      RETURN type(e) AS Type
      LIMIT 3
      """
    Then the result should be, in any order:
      | Type     |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
    When executing query:
      """
      MATCH ()-[e:is_teacher]->()
      RETURN type(e) AS Type, e.start_year AS StartYear, e.end_year AS EndYear
      LIMIT 3
      """
    Then the result should be, in any order:
      | Type     | StartYear | EndYear |
      | /[\w_]+/ | /\d{4}/   | /\d{4}/ |
      | /[\w_]+/ | /\d{4}/   | /\d{4}/ |
      | /[\w_]+/ | /\d{4}/   | /\d{4}/ |

  # TODO check index validation in match planner entry
  # When executing query:
  # """
  # MATCH ()-[e:is_teacher]->()
  # WHERE e.start_year == 2018
  # RETURN type(e) AS Type, e.start_year AS StartYear, e.end_year AS EndYear
  # LIMIT 3
  # """
  # Then the result should be, in any order:
  # | Type          | StartYear | EndYear |
  # | /[\w_]+/     | /\d{4}/   | /\d{4}/ |
  # | /[\w_]+/     | /\d{4}/   | /\d{4}/ |
  # | /[\w_]+/     | /\d{4}/   | /\d{4}/ |
  # When executing query:
  # """
  # MATCH ()-[e:is_teacher {start_year: 2018}]->()
  # RETURN type(e) AS Type, e.start_year AS StartYear, e.end_year AS EndYear
  # LIMIT 3
  # """
  # Then the result should be, in any order:
  # | Type          | StartYear | EndYear |
  # | /[\w_]+/     | /\d{4}/   | /\d{4}/ |
  # | /[\w_]+/     | /\d{4}/   | /\d{4}/ |
  # | /[\w_]+/     | /\d{4}/   | /\d{4}/ |
  Scenario: query edge by scan failed
    When executing query:
      """
      MATCH ()-[e]->()
      RETURN type(e) AS Type
      """
    Then the result should be, in any order:
      | Type            |
      | "is_teacher"    |
      | "is_colleagues" |
      | "is_colleagues" |
      | "is_colleagues" |
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
      | "is_friend"     |
      | "is_friend"     |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_colleagues" |
      | "is_friend"     |
      | "is_schoolmate" |
      | "is_friend"     |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_teacher"    |
      | "is_colleagues" |
      | "is_friend"     |
      | "is_friend"     |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_friend"     |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
      | "is_schoolmate" |
    When executing query:
      """
      MATCH (v)-[e]->()
      RETURN v.person.name, type(e) AS Type
      LIMIT 3
      """
    Then the result should be, in any order:
      | v.person.name | Type     |
      | /[\w_]+/      | /[\w_]+/ |
      | /[\w_]+/      | /[\w_]+/ |
      | /[\w_]+/      | /[\w_]+/ |
    When executing query:
      """
      MATCH ()-[e:is_teacher]-()
      RETURN type(e) AS Type, e.start_year AS StartYear, e.end_year AS EndYear
      LIMIT 3
      """
    Then the result should be, in any order:
      | Type     | StartYear | EndYear |
      | /[\w_]+/ | /\d+/     | /\d+/   |
      | /[\w_]+/ | /\d+/     | /\d+/   |
      | /[\w_]+/ | /\d+/     | /\d+/   |
    When executing query:
      """
      MATCH ()-[e]-()
      RETURN type(e) AS Type, e.start_year AS StartYear, e.end_year AS EndYear
      LIMIT 3
      """
    Then the result should be, in any order:
      | Type     | StartYear | EndYear |
      | /[\w_]+/ | /\d+/     | /\d+/   |
      | /[\w_]+/ | /\d+/     | /\d+/   |
      | /[\w_]+/ | /\d+/     | /\d+/   |

  # #5223
  Scenario: query edge by scan with skip limit
    When executing query:
      """
      MATCH ()-[e]->()
      RETURN type(e) AS Type
      SKIP 10 LIMIT 4
      """
    Then the result should be, in any order:
      | Type     |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
    When executing query:
      """
      MATCH ()-[e]->()
      RETURN type(e) AS Type
      SKIP 10 LIMIT 5
      """
    Then the result should be, in any order:
      | Type     |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
    When executing query:
      """
      MATCH ()-[e]->()
      RETURN type(e) AS Type
      SKIP 10 LIMIT 7
      """
    Then the result should be, in any order:
      | Type     |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
    When executing query:
      """
      MATCH ()-[e]->()
      RETURN type(e) AS Type
      SKIP 10 LIMIT 11
      """
    Then the result should be, in any order:
      | Type     |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
      | /[\w_]+/ |
