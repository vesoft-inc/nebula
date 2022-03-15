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

  Scenario: query vertices by scan failed
    When executing query:
      """
      MATCH (v)
      RETURN v.person.name AS Name
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH (v{name: "Mary"})
      RETURN v.student.name AS Name
      LIMIT 3
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.

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
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH (v)-[e]->()
      RETURN v.person.name, type(e) AS Type
      LIMIT 3
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH ()-[e:is_teacher]-()
      RETURN type(e) AS Type, e.start_year AS StartYear, e.end_year AS EndYear
      LIMIT 3
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
    When executing query:
      """
      MATCH ()-[e]-()
      RETURN type(e) AS Type, e.start_year AS StartYear, e.end_year AS EndYear
      LIMIT 3
      """
    Then a ExecutionError should be raised at runtime: Scan vertices or edges need to specify a limit number, or limit number can not push down.
