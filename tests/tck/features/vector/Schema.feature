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
      CREATE TAG vectag1(id int, vec vector(3))
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
      | Field | Type     | Null  | Default | Comment |
      | "id"  | "int64"  | "YES" | EMPTY   | EMPTY   |
      | "vec" | "vector" | "YES" | EMPTY   | EMPTY   |
    # create tag succeed