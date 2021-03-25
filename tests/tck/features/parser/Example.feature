# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Feature examples

  Scenario: Supported features
    Given an empty graph
    And load "nba" csv data to a new space
    And having executed:
      """
      CREATE TAG IF NOT EXISTS `test_tag`(name string)
      """
    And wait 3 seconds
    When executing query:
      """
      MATCH (v:player{name: "Tim Duncan"})
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name         |
      | "Tim Duncan" |
    When executing query:
      """
      SHOW HOSTS
      """
    Then the result should contain:
      | Host  | Port  | Status   | Leader count | Leader distribution | Partition distribution |
      | /\w+/ | /\d+/ | "ONLINE" | /\d+/        | /.*/                | /.*/                   |
    When executing query:
      """
      SHOW HOSTS
      """
    Then the execution should be successful
    When executing query:
      """
      YIELD $-.id AS id
      """
    Then a SemanticError should be raised at runtime: `$-.id', not exist prop `id'
    When executing query:
      """
      CREATE TAG player(name string);
      """
    Then a ExecutionError should be raised at runtime.
    Then drop the used space

  Scenario: Supported space creation
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    When executing query:
      """
      SHOW SPACES
      """
    Then the execution should be successful
    When executing query:
      """
      YIELD 1 AS id, 2 AS id
      """
    Then the result should be, in any order:
      | id | id |
      | 1  | 2  |
    Then drop the used space
