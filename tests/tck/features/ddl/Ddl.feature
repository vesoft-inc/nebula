@mintest
# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: DDL test

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |

  Scenario: Tag DDL
    When executing query:
      """
      CREATE TAG A();
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG IF NOT EXISTS A(id int, name string);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG  B(
        id int NOT NULL DEFAULT 0+0 COMMENT "primary key",
        name string NOT NULL,
        createDate DATETIME, location geography(polygon),
        isVisited bool COMMENT "kHop search flag",
        nickName TIME DEFAULT time(),
        listString List< string >,
        listInt List< int >,
        listFloat List< float >,
        setString Set< string >,
        setInt Set< int >,
        setFloat Set< float >
        )
        TTL_DURATION = 100, TTL_COL = "id", COMMENT = "TAG B";
      """
    Then the execution should be successful
    When executing query:
      """
      DESC TAG A;
      """
    Then the result should be, in any order:
      | Field | Type | Null | Default | Comment |
    When executing query:
      """
      DESC TAG B;
      """
    Then the result should be, in any order:
      | Field        | Type                 | Null  | Default  | Comment            |
      | "id"         | "int64"              | "NO"  | 0        | "primary key"      |
      | "name"       | "string"             | "NO"  |          |                    |
      | "createDate" | "datetime"           | "YES" |          |                    |
      | "location"   | "geography(polygon)" | "YES" |          |                    |
      | "isVisited"  | "bool"               | "YES" |          | "kHop search flag" |
      | "nickName"   | "time"               | "YES" | "time()" |                    |
      | "listString" | "list_string"        | "YES" |          |                    |
      | "listInt"    | "list_int"           | "YES" |          |                    |
      | "listFloat"  | "list_float"         | "YES" |          |                    |
      | "setString"  | "set_string"         | "YES" |          |                    |
      | "setInt"     | "set_int"            | "YES" |          |                    |
      | "setFloat"   | "set_float"          | "YES" |          |                    |
    When executing query:
      """
      ALTER TAG B DROP (name)
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX idx_A_1 on A();
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX idx_A_2 on A(id);
      """
    Then a ExecutionError should be raised at runtime: Key not existed!
    When executing query:
      """
      CREATE TAG INDEX IF NOT EXISTS idx_A_3 on A();
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX  idx_B_1 on B(isVisited, id, nickName, name(1), createDate);
      """
    Then a ExecutionError should be raised at runtime: Key not existed!
    When executing query:
      """
      ALTER TAG B ADD (name string)
      """
    # IMHO, this is really confusing. https://github.com/vesoft-inc/nebula/issues/2671
    Then a ExecutionError should be raised at runtime: Schema exisited before!
    When executing query:
      """
      ALTER TAG B ADD (namex string)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG B CHANGE (isVisited bool)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG B CHANGE (isVisited int)
      """
    Then a ExecutionError should be raised at runtime: Unsupported!
    When executing query:
      """
      CREATE TAG INDEX  idx_B_2 on B(id);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX  idx_B_4 on B(namex);
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    When executing query:
      """
      DROP TAG INDEX IF EXISTS idx_B_4;
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX  idx_B_5 on B(createDate);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX  idx_B_6 on B(location);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX  idx_B_7 on B(isVisited);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX idx_B_8 on B(nickName);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX idx_B_9 on B(id, nickName, namex(1), createDate);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX idx_B_10 on B(id, nickName, namex(1));
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX idx_E2_11 on E2(id, nickName, namex(1));
      """
    Then a ExecutionError should be raised at runtime: TagNotFound: Tag not existed!
    When executing query:
      """
      CREATE TAG INDEX  idx_B_1 on B(isVisited, id, nickName, namex(1), createDate);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX  idx_listString ON B(listString);
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    When executing query:
      """
      CREATE TAG INDEX  idx_listInt ON B(listInt);
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    When executing query:
      """
      CREATE TAG INDEX  idx_listFloat ON B(listFloat);
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    When executing query:
      """
      CREATE TAG INDEX  idx_setString ON B(setString);
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    When executing query:
      """
      CREATE TAG INDEX  idx_setInt ON B(setInt);
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    When executing query:
      """
      CREATE TAG INDEX  idx_setFloat ON B(setFloat);
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    When executing query:
      """
      ALTER TAG B CHANGE (listString string)
      """
    Then a ExecutionError should be raised at runtime: Unsupported!
    When executing query:
      """
      ALTER TAG B CHANGE (listString Set< string >)
      """
    Then a ExecutionError should be raised at runtime: Unsupported!
    When executing query:
      """
      ALTER TAG B CHANGE (listString List< int >)
      """
    Then a ExecutionError should be raised at runtime: Unsupported!
    When executing query:
      """
      ALTER TAG B CHANGE (setString string)
      """
    Then a ExecutionError should be raised at runtime: Unsupported!
    When executing query:
      """
      ALTER TAG B CHANGE (setString List< string >)
      """
    Then a ExecutionError should be raised at runtime: Unsupported!
    When executing query:
      """
      ALTER TAG B CHANGE (setString Set< int >)
      """
    Then a ExecutionError should be raised at runtime: Unsupported!

  Scenario: Edge DDL
    When executing query:
      """
      CREATE EDGE E1();
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE IF NOT EXISTS E1(id int, name string);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE  E2(
        id int NOT NULL DEFAULT 0+0 COMMENT "primary key",
        name string NOT NULL,
        createDate DATETIME, location geography(polygon),
        isVisited bool COMMENT "kHop search flag",
        nickName TIME DEFAULT time()
        )
        TTL_DURATION = 100, TTL_COL = "id", COMMENT = "EDGE E2";
      """
    Then the execution should be successful
    When executing query:
      """
      DESC EDGE E1;
      """
    Then the result should be, in any order:
      | Field | Type | Null | Default | Comment |
    When executing query:
      """
      DESC EDGE E2;
      """
    Then the result should be, in any order:
      | Field        | Type                 | Null  | Default  | Comment            |
      | "id"         | "int64"              | "NO"  | 0        | "primary key"      |
      | "name"       | "string"             | "NO"  |          |                    |
      | "createDate" | "datetime"           | "YES" |          |                    |
      | "location"   | "geography(polygon)" | "YES" |          |                    |
      | "isVisited"  | "bool"               | "YES" |          | "kHop search flag" |
      | "nickName"   | "time"               | "YES" | "time()" |                    |
    When executing query:
      """
      ALTER EDGE E2 DROP (name)
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX idx_E1_1 on E1();
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX idx_E1_2 on E1(id);
      """
    Then a ExecutionError should be raised at runtime: Key not existed!
    When executing query:
      """
      CREATE EDGE INDEX IF NOT EXISTS idx_E1_3 on E1();
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX  idx_E2_1 on E2(isVisited, id, nickName, name(1), createDate);
      """
    Then a ExecutionError should be raised at runtime: Key not existed!
    When executing query:
      """
      ALTER EDGE E2 ADD (name string)
      """
    Then a ExecutionError should be raised at runtime: Schema exisited before!
    When executing query:
      """
      ALTER EDGE E2 ADD (namex string)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER EDGE E2 CHANGE (isVisited bool)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER EDGE E2 CHANGE (isVisited int)
      """
    Then a ExecutionError should be raised at runtime: Unsupported!
    When executing query:
      """
      CREATE EDGE INDEX  idx_E2_2 on E2(id);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX  idx_E2_4 on E2(namex);
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    When executing query:
      """
      DROP EDGE INDEX IF EXISTS idx_E2_4;
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX  idx_E2_5 on E2(createDate);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX  idx_E2_6 on E2(location);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX  idx_E2_7 on E2(isVisited);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX idx_E2_8 on E2(nickName);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX idx_E2_9 on E2(id, nickName, namex(1), createDate);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX idx_E2_10 on E2(id, nickName, namex(1));
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX idx_E2_11 on E2(id, nickName, namex(1));
      """
    Then a ExecutionError should be raised at runtime: TagNotFound: Tag not existed!
    When executing query:
      """
      CREATE EDGE INDEX  idx_E2_1 on E2(isVisited, id, nickName, namex(1), createDate);
      """
    Then the execution should be successful
