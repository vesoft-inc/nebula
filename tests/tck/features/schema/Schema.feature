# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Insert string vid of vertex and edge

  Scenario: insert vertex and edge test
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    # empty prop
    When executing query:
      """
      CREATE TAG tag1()
      """
    Then the execution should be successful
    # if not exists
    When executing query:
      """
      CREATE TAG IF NOT EXISTS tag1()
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      DESCRIBE TAG tag1
      """
    Then the result should be, in any order:
      | Field | Type | Null | Default | Comment |
    # alter
    When executing query:
      """
      ALTER TAG tag1 ADD (id int, name string)
      """
    Then the execution should be successful
    # desc tag
    When executing query:
      """
      DESCRIBE TAG tag1
      """
    Then the result should be, in any order:
      | Field  | Type     | Null  | Default | Comment |
      | "id"   | "int64"  | "YES" | EMPTY   | EMPTY   |
      | "name" | "string" | "YES" | EMPTY   | EMPTY   |
    # create tag succeed
    When executing query:
      """
      CREATE TAG person(name string, email string DEFAULT "NULL", age int, gender string, row_timestamp timestamp DEFAULT 2020)
      """
    Then the execution should be successful
    # Create Tag with duplicate field, failed
    When executing query:
      """
      CREATE TAG duplicate_tag(name string, name int)
      """
    Then a SemanticError should be raised at runtime: Duplicate column name `name'
    # Create Tag with duplicate field, failed
    When executing query:
      """
      CREATE TAG duplicate_tag(name string, name string)
      """
    Then a SemanticError should be raised at runtime: Duplicate column name `name'
    # Create Tag with DEFAULT value
    When executing query:
      """
      CREATE TAG person_with_default(name string, age int DEFAULT 18)
      """
    Then the execution should be successful
    # test DESCRIBE
    When executing query:
      """
      DESCRIBE TAG person
      """
    Then the result should be, in any order:
      | Field           | Type        | Null  | Default | Comment |
      | "name"          | "string"    | "YES" | EMPTY   | EMPTY   |
      | "email"         | "string"    | "YES" | "NULL"  | EMPTY   |
      | "age"           | "int64"     | "YES" | EMPTY   | EMPTY   |
      | "gender"        | "string"    | "YES" | EMPTY   | EMPTY   |
      | "row_timestamp" | "timestamp" | "YES" | 2020    | EMPTY   |
    # test DESC
    When executing query:
      """
      DESC TAG person
      """
    Then the result should be, in any order:
      | Field           | Type        | Null  | Default | Comment |
      | "name"          | "string"    | "YES" | EMPTY   | EMPTY   |
      | "email"         | "string"    | "YES" | "NULL"  | EMPTY   |
      | "age"           | "int64"     | "YES" | EMPTY   | EMPTY   |
      | "gender"        | "string"    | "YES" | EMPTY   | EMPTY   |
      | "row_timestamp" | "timestamp" | "YES" | 2020    | EMPTY   |
    # describe not existed tag
    When executing query:
      """
      DESCRIBE TAG not_exist
      """
    Then a ExecutionError should be raised at runtime: Tag not existed!
    # unreserved keyword
    When executing query:
      """
      CREATE TAG upper(name string, ACCOUNT string, age int, gender string, row_timestamp timestamp DEFAULT 100)
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      DESCRIBE TAG upper
      """
    Then the result should be, in any order:
      | Field           | Type        | Null  | Default | Comment |
      | "name"          | "string"    | "YES" | EMPTY   | EMPTY   |
      | "account"       | "string"    | "YES" | EMPTY   | EMPTY   |
      | "age"           | "int64"     | "YES" | EMPTY   | EMPTY   |
      | "gender"        | "string"    | "YES" | EMPTY   | EMPTY   |
      | "row_timestamp" | "timestamp" | "YES" | 100     | EMPTY   |
    # create existed tag
    When executing query:
      """
      CREATE TAG person(id int)
      """
    Then a ExecutionError should be raised at runtime: Existed!
    # alter tag
    When executing query:
      """
      ALTER TAG person ADD (col1 int, col2 string), CHANGE (age string), DROP (gender)
      """
    Then the execution should be successful
    # drop not exist prop
    When executing query:
      """
      ALTER TAG person DROP (gender)
      """
    Then a ExecutionError should be raised at runtime: Tag prop not existed!
    # check result
    When executing query:
      """
      DESCRIBE TAG person
      """
    Then the result should be, in any order:
      | Field           | Type        | Null  | Default | Comment |
      | "name"          | "string"    | "YES" | EMPTY   | EMPTY   |
      | "email"         | "string"    | "YES" | "NULL"  | EMPTY   |
      | "age"           | "string"    | "YES" | EMPTY   | EMPTY   |
      | "row_timestamp" | "timestamp" | "YES" | 2020    | EMPTY   |
      | "col1"          | "int64"     | "YES" | EMPTY   | EMPTY   |
      | "col2"          | "string"    | "YES" | EMPTY   | EMPTY   |
    # check result
    # When executing query:
    # """
    # SHOW CREATE TAG person
    # """
    # Then the result should be, in any order:
    # | Tag      | Create Tag |
    # | "person" | 'CREATE TAG `person` (\n `name` string NULL,\n `email` string NULL DEFAULT "NULL",\n `age` string NULL,\n `row_timestamp` timestamp NULL DEFAULT 2020,\n `col1` int64 NULL,\n `col2` string NULL\n) ttl_duration = 0, ttl_col = ""'|
    # show tags
    When executing query:
      """
      SHOW TAGS
      """
    Then the result should be, in any order:
      | Name                  |
      | "person_with_default" |
      | "person"              |
      | "tag1"                |
      | "upper"               |
    # with negative DEFAULT value
    When executing query:
      """
      CREATE TAG default_tag_neg(id int DEFAULT -10, height double DEFAULT -176.0)
      """
    Then the execution should be successful
    # Tag with expression DEFAULT value
    When executing query:
      """
      CREATE TAG default_tag_expr(
        id int64 DEFAULT 3/2*4-5,
        male bool DEFAULT 3 > 2,
        height double DEFAULT abs(-176.0),
        adult bool DEFAULT true AND false
      );
      """
    Then the execution should be successful
    # drop tag succeeded
    When executing query:
      """
      DROP TAG person
      """
    Then the execution should be successful
    # drop not exist tag
    When executing query:
      """
      DROP TAG not_exist_tag
      """
    Then a ExecutionError should be raised at runtime: Tag not existed!
    # drop if exists with not exist tag
    When executing query:
      """
      DROP TAG IF EXISTS not_exist_tag
      """
    Then the execution should be successful
    # drop if exists with existed tag
    When executing query:
      """
      CREATE TAG exist_tag(id int);
      DROP TAG IF EXISTS exist_tag;
      """
    Then the execution should be successful
    # empty edge prop
    When executing query:
      """
      CREATE EDGE edge1()
      """
    Then the execution should be successful
    # IF NOT EXISTS
    When executing query:
      """
      CREATE EDGE IF NOT EXISTS edge1()
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      DESCRIBE EDGE edge1
      """
    Then the result should be, in any order:
      | Field | Type | Null | Default | Comment |
    # alter edge
    When executing query:
      """
      ALTER EDGE edge1 ADD (id int, name string)
      """
    Then the execution should be successful
    # desc edge
    When executing query:
      """
      DESCRIBE EDGE edge1
      """
    Then the result should be, in any order:
      | Field  | Type     | Null  | Default | Comment |
      | "id"   | "int64"  | "YES" | EMPTY   | EMPTY   |
      | "name" | "string" | "YES" | EMPTY   | EMPTY   |
    # create edge succeeded
    When executing query:
      """
      CREATE EDGE buy(id int, time_ string)
      """
    Then the execution should be successful
    # create Edge with duplicate field
    When executing query:
      """
      CREATE EDGE duplicate_buy(time_ int, time_ string)
      """
    Then a SemanticError should be raised at runtime: Duplicate column name `time_'
    # create Edge with duplicate field
    When executing query:
      """
      CREATE EDGE duplicate_buy(time_ int, time_ int)
      """
    Then a SemanticError should be raised at runtime: Duplicate column name `time_'
    # create edge with DEFAULT
    When executing query:
      """
      CREATE EDGE buy_with_default(id int, name string DEFAULT "NULL", time_ timestamp DEFAULT 2020)
      """
    Then the execution should be successful
    # DEFAULT value not match type
    When executing query:
      """
      CREATE EDGE buy_type_mismatch(id int, time_ string DEFAULT 0)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # existent edge
    When executing query:
      """
      CREATE EDGE buy(id int, time_ string)
      """
    Then a ExecutionError should be raised at runtime: Existed!
    # DESCRIBE edge
    When executing query:
      """
      DESCRIBE EDGE buy
      """
    Then the result should be, in any order:
      | Field   | Type     | Null  | Default | Comment |
      | "id"    | "int64"  | "YES" | EMPTY   | EMPTY   |
      | "time_" | "string" | "YES" | EMPTY   | EMPTY   |
    # DESC edge
    When executing query:
      """
      DESC EDGE buy
      """
    Then the result should be, in any order:
      | Field   | Type     | Null  | Default | Comment |
      | "id"    | "int64"  | "YES" | EMPTY   | EMPTY   |
      | "time_" | "string" | "YES" | EMPTY   | EMPTY   |
    # desc nonexistent edge, TODO: FIX ERROR MSG
    When executing query:
      """
      DESCRIBE EDGE not_exist
      """
    Then a ExecutionError should be raised at runtime: Edge not existed!
    # create edge with timestamp
    When executing query:
      """
      CREATE EDGE education(id int, time_ timestamp, school string)
      """
    Then the execution should be successful
    # DESC edge
    When executing query:
      """
      DESC EDGE education
      """
    Then the result should be, in any order:
      | Field    | Type        | Null  | Default | Comment |
      | "id"     | "int64"     | "YES" | EMPTY   | EMPTY   |
      | "time_"  | "timestamp" | "YES" | EMPTY   | EMPTY   |
      | "school" | "string"    | "YES" | EMPTY   | EMPTY   |
    # show edges
    When executing query:
      """
      SHOW EDGES
      """
    Then the result should be, in any order:
      | Name               |
      | "edge1"            |
      | "buy"              |
      | "buy_with_default" |
      | "education"        |
    # alter edge
    When executing query:
      """
      ALTER EDGE education ADD (col1 int, col2 string), CHANGE (school int), DROP (id, time_)
      """
    Then the execution should be successful
    # drop not exist prop, failed
    When executing query:
      """
      ALTER EDGE education DROP (id, time_)
      """
    Then a ExecutionError should be raised at runtime: Edge prop not existed!
    # check result
    When executing query:
      """
      DESC EDGE education
      """
    Then the result should be, in any order:
      | Field    | Type     | Null  | Default | Comment |
      | "school" | "int64"  | "YES" | EMPTY   | EMPTY   |
      | "col1"   | "int64"  | "YES" | EMPTY   | EMPTY   |
      | "col2"   | "string" | "YES" | EMPTY   | EMPTY   |
    # with negative DEFAULT value
    When executing query:
      """
      CREATE EDGE default_edge_neg(id int DEFAULT -10, height double DEFAULT -176.0)
      """
    Then the execution should be successful
    # Tag with expression DEFAULT value
    When executing query:
      """
      CREATE EDGE default_edge_expr(
        id int DEFAULT 3/2*4-5,
        male bool DEFAULT 3 > 2,
        height double DEFAULT abs(-176.0),
        adult bool DEFAULT true AND false
      );
      """
    Then the execution should be successful
    # test drop edge
    When executing query:
      """
      DROP EDGE buy
      """
    Then the execution should be successful
    # drop not exist prop, failed
    When executing query:
      """
      DROP EDGE not_exist_edge
      """
    Then a ExecutionError should be raised at runtime: Edge not existed!
    # drop if exists
    When executing query:
      """
      DROP EDGE IF EXISTS not_exist_edge
      """
    Then the execution should be successful
    # drop if exists
    When executing query:
      """
      CREATE EDGE exist_edge(id int);
      DROP EDGE IF EXISTS exist_edge;
      """
    Then the execution should be successful
    And drop the used space
    # test same tag in different space
    When executing query:
      """
      CREATE SPACE my_space(partition_num=9, replica_factor=1, vid_type=FIXED_STRING(8));
      USE my_space;
      CREATE TAG animal(name string, kind string);
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      DESCRIBE TAG animal
      """
    Then the result should be, in any order:
      | Field  | Type     | Null  | Default | Comment |
      | "name" | "string" | "YES" | EMPTY   | EMPTY   |
      | "kind" | "string" | "YES" | EMPTY   | EMPTY   |
    When executing query:
      """
      CREATE TAG person(name string, interest string)
      """
    Then the execution should be successful
    # show tags
    When executing query:
      """
      SHOW TAGS
      """
    Then the result should be, in any order:
      | Name     |
      | "animal" |
      | "person" |
    # test same tag in different space
    When executing query:
      """
      CREATE SPACE test_multi(vid_type=FIXED_STRING(8));
      USE test_multi;
      CREATE Tag test_tag();
      SHOW TAGS;
      """
    Then the result should be, in any order:
      | Name       |
      | "test_tag" |
    # test same tag in different space
    When try to execute query:
      """
      USE test_multi;
      CREATE TAG test_tag1();
      USE my_space;
      SHOW TAGS;
      """
    Then the result should be, in any order:
      | Name     |
      | "animal" |
      | "person" |
    When executing query:
      """
      DROP SPACE test_multi
      """
    Then the execution should be successful
    # reserved keyword
    When executing query:
      """
      USE my_space;
      CREATE TAG `tag` (`edge` string);
      """
    Then the execution should be successful
    # test drop space
    When executing query:
      """
      SHOW SPACES;
      """
    Then the result should contain:
      | Name       |
      | "my_space" |
    # test drop space
    When executing query:
      """
      DROP SPACE my_space
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      SHOW SPACES;
      """
    Then the result should not contain:
      | Name         |
      | "my_space"   |
      | "test_multi" |
    # test alter tag with default
    When executing query:
      """
      CREATE SPACE tag_space(partition_num=9, vid_type=FIXED_STRING(8));
      USE tag_space;
      CREATE TAG t(name string DEFAULT "N/A", age int DEFAULT -1);
      """
    Then the execution should be successful
    # test alter add
    When executing query:
      """
      ALTER TAG t ADD (description string DEFAULT "none")
      """
    Then the execution should be successful
    And wait 3 seconds
    # insert
    When try to execute query:
      """
      INSERT VERTEX t() VALUES "1":()
      """
    Then the execution should be successful
    # check result by fetch
    When executing query:
      """
      FETCH PROP ON t "1" YIELD t.name, t.age, t.description
      """
    Then the result should be, in any order:
      | t.name | t.age | t.description |
      | "N/A"  | -1    | "none"        |
    # alter change
    When executing query:
      """
      ALTER TAG t CHANGE (description string NOT NULL)
      """
    Then the execution should be successful
    And wait 3 seconds
    # insert
    When executing query:
      """
      INSERT VERTEX t(description) VALUES "1":("some one")
      """
    Then the execution should be successful
    # check result by fetch
    When executing query:
      """
      FETCH PROP ON t "1" YIELD t.name, t.age, t.description
      """
    Then the result should be, in any order:
      | t.name | t.age | t.description |
      | "N/A"  | -1    | "some one"    |
    And wait 3 seconds
    # insert without default prop, failed
    When executing query:
      """
      INSERT VERTEX t() VALUES "1":()
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The not null field doesn't have a default value.
    # test alter edge with default value
    When executing query:
      """
      CREATE EDGE e(name string DEFAULT "N/A", age int DEFAULT -1)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER EDGE e ADD (description string DEFAULT "none")
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query:
      """
      INSERT VERTEX t(description) VALUES "1":("some one")
      """
    Then the execution should be successful
    # check result by fetch
    When executing query:
      """
      FETCH PROP ON t "1" YIELD t.name, t.age, t.description
      """
    Then the result should be, in any order:
      | t.name | t.age | t.description |
      | "N/A"  | -1    | "some one"    |
    # alter drop default
    When executing query:
      """
      ALTER EDGE e CHANGE (description string NOT NULL)
      """
    Then the execution should be successful
    And wait 3 seconds
    # insert without default prop, failed
    When executing query:
      """
      INSERT EDGE e() VALUES "1"->"2":()
      """
    Then a SemanticError should be raised at runtime: The property `description' is not nullable and has no default value.
    # test alter edge with timestamp default
    When executing query:
      """
      DROP SPACE tag_space;
      CREATE SPACE issue2009(vid_type = FIXED_STRING(20));
      USE issue2009;
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE IF NOT EXISTS relation (
        intimacy int DEFAULT 0,
        isReversible bool DEFAULT false,
        name string DEFAULT "N/A",
        startTime timestamp DEFAULT 0
      )
      """
    Then the execution should be successful
    When try to execute query:
      """
      INSERT EDGE relation (intimacy) VALUES "person.Tom" -> "person.Marry"@0:(3)
      """
    Then the execution should be successful
    # create tag with null default value
    When executing query:
      """
      CREATE TAG tag_null_default1(name string NULL DEFAULT "N/A");
      """
    Then the execution should be successful
    When executing query:
      """
      DESC TAG tag_null_default1;
      """
    Then the result should be, in any order:
      | Field  | Type     | Null  | Default | Comment |
      | 'name' | 'string' | 'YES' | 'N/A'   | EMPTY   |
    When executing query:
      """
      CREATE TAG tag_null_default2(name string DEFAULT "N/A" NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      DESC TAG tag_null_default2;
      """
    Then the result should be, in any order:
      | Field  | Type     | Null  | Default | Comment |
      | 'name' | 'string' | 'YES' | 'N/A'   | EMPTY   |
    # test create tag with not null default value
    When executing query:
      """
      CREATE TAG tag_not_null_default1(name string NOT NULL DEFAULT "N/A")
      """
    Then the execution should be successful
    When executing query:
      """
      DESC TAG tag_not_null_default1;
      """
    Then the result should be, in any order:
      | Field  | Type     | Null | Default | Comment |
      | 'name' | 'string' | 'NO' | 'N/A'   | EMPTY   |
    When executing query:
      """
      CREATE TAG tag_not_null_default2(name string DEFAULT "N/A" NOT NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      DESC TAG tag_not_null_default2;
      """
    Then the result should be, in any order:
      | Field  | Type     | Null | Default | Comment |
      | 'name' | 'string' | 'NO' | 'N/A'   | EMPTY   |
    # test with bad null default value
    When executing query:
      """
      CREATE TAG bad_null_default_value(name string DEFAULT "N/A", age int DEFAULT 1%0)
      """
    Then a SemanticError should be raised at runtime: Divide by 0
    # test alter tag with wrong type default value of string when add
    When executing query:
      """
      ALTER TAG tag_not_null_default1 ADD (col1 string DEFAULT 10)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # test alter tag with wrong type default value of timestamp when add
    When executing query:
      """
      ALTER TAG tag_not_null_default1 ADD (col1 timestamp DEFAULT -10)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # test alter tag with wrong type default value of float when add
    When executing query:
      """
      ALTER TAG tag_not_null_default1 ADD (col1 float DEFAULT 10)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # test alter tag with wrong type default value of bool when add
    When executing query:
      """
      ALTER TAG tag_not_null_default1 ADD (col1 bool DEFAULT 10)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # test alter tag with wrong type default value of int8 when add
    When executing query:
      """
      ALTER TAG tag_not_null_default1 ADD (col1 int8 DEFAULT 10000)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # test alter tag with wrong type default value of time when add
    When executing query:
      """
      ALTER TAG tag_not_null_default1 ADD (col1 time DEFAULT 10)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # test alter tag with out of rang string default value of fixed_string when add
    When executing query:
      """
      ALTER TAG tag_not_null_default1 ADD (col1 FIXED_STRING(5) DEFAULT "Hello world!")
      """
    Then the execution should be successful
    When executing query:
      """
      DESC TAG tag_not_null_default1;
      """
    Then the result should be, in any order:
      | Field  | Type              | Null  | Default | Comment |
      | "name" | "string"          | "NO"  | "N/A"   | EMPTY   |
      | "col1" | "fixed_string(5)" | "YES" | "Hello" | EMPTY   |
    # test alter tag with wrong type default value of time when change
    When executing query:
      """
      ALTER TAG tag_not_null_default1 CHANGE (name FIXED_STRING(10) DEFAULT 10)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # test alter edge with wrong type default value of string when add
    When executing query:
      """
      CREATE EDGE edge_not_null_default1(name string NOT NULL DEFAULT "N/A");
      ALTER EDGE edge_not_null_default1 ADD (col1 string DEFAULT 10)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # test alter edge with wrong type default value of timestamp when add
    When executing query:
      """
      ALTER EDGE edge_not_null_default1 ADD (col1 timestamp DEFAULT -10)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # test alter edge with wrong type default value of float when add
    When executing query:
      """
      ALTER EDGE edge_not_null_default1 ADD (col1 float DEFAULT 10)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # test alter edge with wrong type default value of bool when add
    When executing query:
      """
      ALTER EDGE edge_not_null_default1 ADD (col1 bool DEFAULT 10)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # test alter edge with wrong type default value of int8 when add
    When executing query:
      """
      ALTER EDGE edge_not_null_default1 ADD (col1 int8 DEFAULT 10000)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # test alter edge with wrong type default value of time when add
    When executing query:
      """
      ALTER EDGE edge_not_null_default1 ADD (col1 time DEFAULT 10)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # test alter edge with out of rang string default value of fixed_string when add
    When executing query:
      """
      ALTER EDGE edge_not_null_default1 ADD (col1 FIXED_STRING(5) DEFAULT "Hello world!")
      """
    Then the execution should be successful
    When executing query:
      """
      DESC EDGE edge_not_null_default1;
      """
    Then the result should be, in any order:
      | Field  | Type              | Null  | Default | Comment |
      | "name" | "string"          | "NO"  | "N/A"   | EMPTY   |
      | "col1" | "fixed_string(5)" | "YES" | "Hello" | EMPTY   |
    # test alter tag with wrong type default value of time when change
    When executing query:
      """
      ALTER EDGE edge_not_null_default1 CHANGE (name FIXED_STRING(10) DEFAULT 10)
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    # chinese tag without quote mark
    When executing query:
      """
      CREATE TAG 队伍( 名字 string);
      """
    Then a SyntaxError should be raised at runtime:
    # chinese tag and chinese prop
    When executing query:
      """
      CREATE TAG `队伍`(`名字` string);
      """
    Then the execution should be successful
    # show chinese tags
    When executing query:
      """
      SHOW TAGS
      """
    Then the result should contain:
      | Name   |
      | "队伍" |
    # alter chinese tag
    When executing query:
      """
      ALTER TAG `队伍` ADD (`类别` string);
      """
    Then the execution should be successful
    # desc chinese tag
    When executing query:
      """
      DESCRIBE TAG `队伍`
      """
    Then the result should be, in any order:
      | Field  | Type     | Null  | Default | Comment |
      | "名字" | "string" | "YES" | EMPTY   | EMPTY   |
      | "类别" | "string" | "YES" | EMPTY   | EMPTY   |
    # chinese edge and chinese prop
    When executing query:
      """
      CREATE EDGE `服役`();
      """
    Then the execution should be successful
    # show chinese edge
    When executing query:
      """
      SHOW EDGES;
      """
    Then the result should contain:
      | Name   |
      | "服役" |
    # alter chinese edge
    When executing query:
      """
      ALTER EDGE `服役` ADD (`时间` timestamp);
      """
    Then the execution should be successful
    # desc chinese edge
    When executing query:
      """
      DESCRIBE EDGE `服役`
      """
    Then the result should be, in any order:
      | Field  | Type        | Null  | Default | Comment |
      | "时间" | "timestamp" | "YES" | EMPTY   | EMPTY   |
    When executing query:
      """
      CREATE TAG `队伍s2`(`名s字ss1` string);
      """
    Then the execution should be successful
    # desc cn-en mixed tag
    When executing query:
      """
      DESCRIBE TAG `队伍s2`
      """
    Then the result should be, in any order:
      | Field      | Type     | Null  | Default | Comment |
      | "名s字ss1" | "string" | "YES" | EMPTY   | EMPTY   |
    When executing query:
      """
      DROP SPACE issue2009;
      """
    Then the execution should be successful
