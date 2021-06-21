# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Schema Comment

  Scenario Outline: Space Comment
    Given an empty graph
    When executing query:
      """
      CREATE SPACE <space_name> (vid_type=FIXED_STRING(8)) comment = '<space_comment>';
      """
    Then the execution should be successful
    When try to execute query:
      """
      SHOW CREATE SPACE <space_name>;
      """
    Then the result should be, in any order:
      | Space          | Create Space                                                                                                                                                                                        |
      | "<space_name>" | "CREATE SPACE `<space_name>` (partition_num = 100, replica_factor = 1, charset = utf8, collate = utf8_bin, vid_type = FIXED_STRING(8), atomic_edge = false) ON default comment = '<space_comment>'" |
    When executing query:
      """
      DESC SPACE <space_name>;
      """
    Then the result should be, in any order:
      | ID    | Name           | Partition Number | Replica Factor | Charset | Collate    | Vid Type          | Atomic Edge | Group     | Comment           |
      | /\d+/ | "<space_name>" | 100              | 1              | "utf8"  | "utf8_bin" | "FIXED_STRING(8)" | false       | "default" | "<space_comment>" |
    When executing query:
      """
      DROP SPACE <space_name>;
      """
    Then the execution should be successful

    Examples:
      | space_name      | space_comment               |
      | test_comment_en | This is a comment of space. |
      | test_comment_zh | 这是一个图空间注释。        |

  Scenario Outline: Space Comment Not set
    Given an empty graph
    When executing query:
      """
      CREATE SPACE test_comment_not_set (vid_type=FIXED_STRING(8));
      """
    Then the execution should be successful
    When try to execute query:
      """
      SHOW CREATE SPACE test_comment_not_set;
      """
    Then the result should be, in any order:
      | Space                  | Create Space                                                                                                                                                                    |
      | "test_comment_not_set" | "CREATE SPACE `test_comment_not_set` (partition_num = 100, replica_factor = 1, charset = utf8, collate = utf8_bin, vid_type = FIXED_STRING(8), atomic_edge = false) ON default" |
    When executing query:
      """
      DESC SPACE test_comment_not_set;
      """
    Then the result should be, in any order:
      | ID    | Name                   | Partition Number | Replica Factor | Charset | Collate    | Vid Type          | Atomic Edge | Group     | Comment |
      | /\d+/ | "test_comment_not_set" | 100              | 1              | "utf8"  | "utf8_bin" | "FIXED_STRING(8)" | false       | "default" | EMPTY   |
    When executing query:
      """
      DROP SPACE test_comment_not_set;
      """
    Then the execution should be successful

  Scenario Outline: Space Comment Empty String
    Given an empty graph
    When executing query:
      """
      CREATE SPACE test_comment_empty (vid_type=FIXED_STRING(8)) comment = '';
      """
    Then the execution should be successful
    When try to execute query:
      """
      SHOW CREATE SPACE test_comment_empty;
      """
    Then the result should be, in any order:
      | Space                | Create Space                                                                                                                                                                               |
      | "test_comment_empty" | "CREATE SPACE `test_comment_empty` (partition_num = 100, replica_factor = 1, charset = utf8, collate = utf8_bin, vid_type = FIXED_STRING(8), atomic_edge = false) ON default comment = ''" |
    When executing query:
      """
      DESC SPACE test_comment_empty;
      """
    Then the result should be, in any order:
      | ID    | Name                 | Partition Number | Replica Factor | Charset | Collate    | Vid Type          | Atomic Edge | Group     | Comment |
      | /\d+/ | "test_comment_empty" | 100              | 1              | "utf8"  | "utf8_bin" | "FIXED_STRING(8)" | false       | "default" | ""      |
    When executing query:
      """
      DROP SPACE test_comment_empty;
      """
    Then the execution should be successful

  Scenario Outline: schema comment
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    When try to execute query:
      """
      CREATE tag test_comment_tag(
          name string COMMENT 'The name.' NULL,
          age int DEFAULT 233 NULL)
      comment = <tag_of_person_comment>;
      """
    Then the execution should be successful
    # show
    When try to execute query:
      """
      SHOW CREATE tag test_comment_tag;
      """
    Then the result should be, in any order:
      | Tag                | Create Tag                                                                                                                                                                      |
      | "test_comment_tag" | 'CREATE TAG `test_comment_tag` (\n `name` string NULL COMMENT "The name.",\n `age` int64 NULL DEFAULT 233\n) ttl_duration = 0, ttl_col = "", comment = <tag_of_person_comment>' |
    When executing query:
      """
      DESC tag test_comment_tag;
      """
    Then the result should be, in any order:
      | Field  | Type     | Null  | Default | Comment     |
      | "name" | "string" | "YES" | EMPTY   | "The name." |
      | "age"  | "int64"  | "YES" | 233     | EMPTY       |
    # alter
    When executing query:
      """
      ALTER TAG test_comment_tag comment = <tag_of_person_comment_modified>;
      ALTER TAG test_comment_tag ADD (gender string COMMENT 'The gender.');
      ALTER TAG test_comment_tag CHANGE (name string NOT NULL);
      ALTER TAG test_comment_tag DROP (age);
      """
    Then the execution should be successful
    # show again
    When executing query:
      """
      SHOW CREATE tag test_comment_tag;
      """
    Then the result should be, in any order:
      | Tag                | Create Tag                                                                                                                                                                             |
      | "test_comment_tag" | 'CREATE TAG `test_comment_tag` (\n `name` string NOT NULL,\n `gender` string NULL COMMENT "The gender."\n) ttl_duration = 0, ttl_col = "", comment = <tag_of_person_comment_modified>' |
    When executing query:
      """
      DESC tag test_comment_tag;
      """
    Then the result should be, in any order:
      | Field    | Type     | Null  | Default | Comment       |
      | "name"   | "string" | "NO"  | EMPTY   | EMPTY         |
      | "gender" | "string" | "YES" | EMPTY   | "The gender." |
    # tag index
    When executing query:
      """
      CREATE tag index test_comment_tag_index ON test_comment_tag(name(8))
      comment = 'The tag index of person name.';
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG INDEX test_comment_tag_index;
      """
    Then the result should be, in any order:
      | Tag Index Name           | Create Tag Index                                                                                                             |
      | "test_comment_tag_index" | 'CREATE TAG INDEX `test_comment_tag_index` ON `test_comment_tag` (\n `name`(8)\n) comment = "The tag index of person name."' |
    # edge
    When executing query:
      """
      CREATE edge test_comment_edge(
          name string COMMENT 'The name.' NULL,
          age int DEFAULT 233 NULL)
      comment = <edge_of_person_comment>;
      """
    Then the execution should be successful
    # show
    When try to execute query:
      """
      SHOW CREATE edge test_comment_edge;
      """
    Then the result should be, in any order:
      | Edge                | Create Edge                                                                                                                                                                        |
      | "test_comment_edge" | 'CREATE EDGE `test_comment_edge` (\n `name` string NULL COMMENT "The name.",\n `age` int64 NULL DEFAULT 233\n) ttl_duration = 0, ttl_col = "", comment = <edge_of_person_comment>' |
    When executing query:
      """
      DESC edge test_comment_edge;
      """
    Then the result should be, in any order:
      | Field  | Type     | Null  | Default | Comment     |
      | "name" | "string" | "YES" | EMPTY   | "The name." |
      | "age"  | "int64"  | "YES" | 233     | EMPTY       |
    # alter
    When executing query:
      """
      ALTER EDGE test_comment_edge comment = <edge_of_person_comment_modified>;
      ALTER EDGE test_comment_edge ADD (gender string COMMENT 'The gender.');
      ALTER EDGE test_comment_edge CHANGE (name string NOT NULL);
      ALTER EDGE test_comment_edge DROP (age);
      """
    Then the execution should be successful
    # show again
    When executing query:
      """
      SHOW CREATE edge test_comment_edge;
      """
    Then the result should be, in any order:
      | Edge                | Create Edge                                                                                                                                                                               |
      | "test_comment_edge" | 'CREATE EDGE `test_comment_edge` (\n `name` string NOT NULL,\n `gender` string NULL COMMENT "The gender."\n) ttl_duration = 0, ttl_col = "", comment = <edge_of_person_comment_modified>' |
    When executing query:
      """
      DESC edge test_comment_edge;
      """
    Then the result should be, in any order:
      | Field    | Type     | Null  | Default | Comment       |
      | "name"   | "string" | "NO"  | EMPTY   | EMPTY         |
      | "gender" | "string" | "YES" | EMPTY   | "The gender." |
    # edge index
    When executing query:
      """
      CREATE edge index test_comment_edge_index ON test_comment_edge(name(8))
      comment = 'The edge index of person name.';
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE EDGE INDEX test_comment_edge_index;
      """
    Then the result should be, in any order:
      | Edge Index Name           | Create Edge Index                                                                                                                |
      | "test_comment_edge_index" | 'CREATE EDGE INDEX `test_comment_edge_index` ON `test_comment_edge` (\n `name`(8)\n) comment = "The edge index of person name."' |

    Examples:
      | tag_of_person_comment           | tag_of_person_comment_modified           | edge_of_person_comment            | edge_of_person_comment_modified           |
      | "The tag of person infomation." | "The tag of person infomation modified." | "The edge of person information." | "The edge of person infomation modified." |
      | "个人信息标签。"                | "修改过的个人信息标签。"                 | "个人信息边。"                    | "修改过的个人信息边。"                    |
