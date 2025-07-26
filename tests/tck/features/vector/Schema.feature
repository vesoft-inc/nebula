# Copyright (c) 2025 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Vector tag

  Scenario: describe vector tag
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    # empty prop
    When executing query:
      """
      CREATE TAG vectag1(id int, vec vector(3) NOT NULL DEFAULT vector(0.1,0.2,0.3))
      """
    Then the execution should be successful
    # if not exists
    When executing query:
      """
      CREATE TAG IF NOT EXISTS vectag1(id int, vec vector(3))
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      DESCRIBE TAG vectag1
      """
    # desc tag
    When executing query:
      """
      DESCRIBE TAG vectag1
      """
    Then the result should be, in any order:
      | Field | Type        | Null  | Default             | Comment |
      | "id"  | "int64"     | "YES" | EMPTY               | EMPTY   |
      | "vec" | "vector(3)" | "NO"  | vector(0.1,0.2,0.3) | EMPTY   |
    When executing query:
      """
      CREATE TAG vectag2(id int, vec vector(3))
      """
    Then the execution should be successful
    # if not exists
    When executing query:
      """
      CREATE TAG IF NOT EXISTS vectag2(id int, vec vector(3) NOT NULL DEFAULT vector(0.1,0.2,0.3))
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      DESCRIBE TAG vectag2
      """
    # desc tag
    When executing query:
      """
      DESCRIBE TAG vectag2
      """
    Then the result should be, in any order:
      | Field | Type        | Null  | Default | Comment |
      | "id"  | "int64"     | "YES" | EMPTY   | EMPTY   |
      | "vec" | "vector(3)" | "YES" | EMPTY   | EMPTY   |
    # create tag succeed
    When executing query:
      """
      CREATE TAG vectag3(id int, vec vector(3) NOT NULL DEFAULT vector(0.2,0.4,0.6) COMMENT 'vector column')
      """
    Then the execution should be successful
    # if not exists
    When executing query:
      """
      CREATE TAG IF NOT EXISTS vectag3(id int, vec vector(3) )
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      DESCRIBE TAG vectag3
      """
    # desc tag
    When executing query:
      """
      DESCRIBE TAG vectag3
      """
    Then the result should be, in any order:
      | Field | Type        | Null  | Default             | Comment         |
      | "id"  | "int64"     | "YES" | EMPTY               | EMPTY           |
      | "vec" | "vector(3)" | "NO"  | vector(0.2,0.4,0.6) | "vector column" |
    # create tag succeed
    When executing query:
      """
      CREATE EDGE vecedge1(id int, vec vector(3))
      """
    Then the execution should be successful
    # if not exists
    When executing query:
      """
      CREATE EDGE IF NOT EXISTS vecedge1(id int, vec vector(3))
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      DESCRIBE EDGE vecedge1
      """
    # desc edge
    When executing query:
      """
      DESCRIBE EDGE vecedge1
      """
    Then the result should be, in any order:
      | Field | Type        | Null  | Default | Comment |
      | "id"  | "int64"     | "YES" | EMPTY   | EMPTY   |
      | "vec" | "vector(3)" | "YES" | EMPTY   | EMPTY   |
    # create edge succeed
    When executing query:
      """
      CREATE EDGE vecedge2(id int, vec vector(3) NOT NULL DEFAULT vector(0.3,0.6,0.9))
      """
    Then the execution should be successful
    # if not exists
    When executing query:
      """
      CREATE EDGE IF NOT EXISTS vecedge2(id int, vec vector(3))
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      DESCRIBE EDGE vecedge2
      """
    # desc edge
    When executing query:
      """
      DESCRIBE EDGE vecedge2
      """
    Then the result should be, in any order:
      | Field | Type        | Null  | Default             | Comment |
      | "id"  | "int64"     | "YES" | EMPTY               | EMPTY   |
      | "vec" | "vector(3)" | "NO"  | vector(0.3,0.6,0.9) | EMPTY   |
    # create edge succeed
    When executing query:
      """
      CREATE EDGE vecedge3(id int, vec vector(3)NOT NULL DEFAULT vector(0.3,0.6,0.9) COMMENT 'vector column')
      """
    Then the execution should be successful
    # if not exists
    When executing query:
      """
      CREATE EDGE IF NOT EXISTS vecedge3(id int, vec vector(3))
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      DESCRIBE EDGE vecedge3
      """
    # desc edge
    When executing query:
      """
      DESCRIBE EDGE vecedge3
      """
    Then the result should be, in any order:
      | Field | Type        | Null  | Default             | Comment         |
      | "id"  | "int64"     | "YES" | EMPTY               | EMPTY           |
      | "vec" | "vector(3)" | "NO"  | vector(0.3,0.6,0.9) | "vector column" |

# create edge succeed
