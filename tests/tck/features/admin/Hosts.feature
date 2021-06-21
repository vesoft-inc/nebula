# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Admin hosts

  Scenario: Show hosts
    When executing query:
      """
      SHOW HOSTS;
      """
    Then the result should contain:
      | Host  | Port  | Status   | Leader count | Leader distribution | Partition distribution |
      | /\w+/ | /\d+/ | "ONLINE" | /\d+/        | /.*/                | /.*/                   |
    When executing query:
      """
      SHOW HOSTS GRAPH;
      """
    Then the result should contain:
      | Host  | Port  | Status   | Role    | Git Info Sha  | Version |
      | /\w+/ | /\d+/ | "ONLINE" | "GRAPH" | /[0-9a-f]{7}/ | EMPTY   |
    When executing query:
      """
      SHOW HOSTS META;
      """
    Then the result should contain:
      | Host  | Port  | Status   | Role   | Git Info Sha  | Version |
      | /\w+/ | /\d+/ | "ONLINE" | "META" | /[0-9a-f]{7}/ | EMPTY   |
    When executing query:
      """
      SHOW HOSTS STORAGE;
      """
    Then the result should contain:
      | Host  | Port  | Status   | Role      | Git Info Sha  | Version |
      | /\w+/ | /\d+/ | "ONLINE" | "STORAGE" | /[0-9a-f]{7}/ | EMPTY   |

  Scenario: Create space
    When executing query:
      """
      CREATE SPACE space_without_vid_type;
      """
    Then a SemanticError should be raised at runtime: space vid_type must be specified explicitly
    When executing query:
      """
      CREATE SPACE space_without_vid_type(partition_num=9, replica_factor=3);
      """
    Then a SemanticError should be raised at runtime: space vid_type must be specified explicitly
    When executing query:
      """
      CREATE SPACE space_without_vid_type(partition_num=9, replica_factor=3) on group_0;
      """
    Then a SemanticError should be raised at runtime: space vid_type must be specified explicitly
    When executing query:
      """
      CREATE SPACE space_without_vid_type on group_0;
      """
    Then a SemanticError should be raised at runtime: space vid_type must be specified explicitly
    When executing query:
      """
      CREATE SPACE space_specify_vid_type(partition_num=9, replica_factor=1, vid_type=FIXED_STRING(8));
      DROP SPACE space_specify_vid_type
      """
    Then the execution should be successful
