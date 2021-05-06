Feature: TTLTest

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(32) |

  Scenario: TTLTest Schematest
    Given having executed:
      """
      CREATE TAG person(name string, email string, age int, gender string, row_timestamp timestamp);
      """
    When executing query:
      """
      DESCRIBE TAG person;
      """
    Then the result should be, in any order:
      | Field           | Type        | Null  | Default | Comment |
      | "name"          | "string"    | "YES" | EMPTY   | EMPTY   |
      | "email"         | "string"    | "YES" | EMPTY   | EMPTY   |
      | "age"           | "int64"     | "YES" | EMPTY   | EMPTY   |
      | "gender"        | "string"    | "YES" | EMPTY   | EMPTY   |
      | "row_timestamp" | "timestamp" | "YES" | EMPTY   | EMPTY   |
    When executing query:
      """
      SHOW CREATE TAG person;
      """
    Then the result should be, in any order:
      | Tag      | Create Tag                                                                                                                                                                          |
      | "person" | 'CREATE TAG `person` (\n `name` string NULL,\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL,\n `row_timestamp` timestamp NULL\n) ttl_duration = 0, ttl_col = ""' |
    When executing query:
      """
      CREATE TAG man(name string, email string,  age int, gender string, row_timestamp timestamp) ttl_duration = 100, ttl_col = "row_timestamp";
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG man;
      """
    Then the result should be, in any order:
      | Tag   | Create Tag                                                                                                                                                                                      |
      | "man" | 'CREATE TAG `man` (\n `name` string NULL,\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL,\n `row_timestamp` timestamp NULL\n) ttl_duration = 100, ttl_col = "row_timestamp"' |
    When executing query:
      """
      CREATE TAG woman(name string, email string, age int, gender string, row_timestamp timestamp) ttl_duration = 100;
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE TAG woman(name string, email string, age int, gender string, row_timestamp timestamp) ttl_col = "name";
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE TAG woman(name string, email string, age int, gender string, row_timestamp timestamp) ttl_duration = 0, ttl_col = "row_timestamp";
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG woman;
      """
    Then the result should be, in any order:
      | Tag     | Create Tag                                                                                                                                                                                      |
      | "woman" | 'CREATE TAG `woman` (\n `name` string NULL,\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL,\n `row_timestamp` timestamp NULL\n) ttl_duration = 0, ttl_col = "row_timestamp"' |
    When executing query:
      """
      CREATE TAG only_ttl_col(name string, email string, age int, gender string, row_timestamp timestamp) ttl_col = "row_timestamp";
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG only_ttl_col;
      """
    Then the result should be, in any order:
      | Tag            | Create Tag                                                                                                                                                                                             |
      | "only_ttl_col" | 'CREATE TAG `only_ttl_col` (\n `name` string NULL,\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL,\n `row_timestamp` timestamp NULL\n) ttl_duration = 0, ttl_col = "row_timestamp"' |
    When executing query:
      """
      ALTER TAG woman ttl_duration = 50, ttl_col = "row_timestamp";
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG woman;
      """
    Then the result should be, in any order:
      | Tag     | Create Tag                                                                                                                                                                                       |
      | "woman" | 'CREATE TAG `woman` (\n `name` string NULL,\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL,\n `row_timestamp` timestamp NULL\n) ttl_duration = 50, ttl_col = "row_timestamp"' |
    When executing query:
      """
      ALTER TAG woman ttl_col = "name";
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      ALTER TAG woman Drop (name) ttl_duration = 200;
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG woman;
      """
    Then the result should be, in any order:
      | Tag     | Create Tag                                                                                                                                                                  |
      | "woman" | 'CREATE TAG `woman` (\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL,\n `row_timestamp` timestamp NULL\n) ttl_duration = 200, ttl_col = "row_timestamp"' |
    When executing query:
      """
      DROP TAG woman;
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG woman(name string, email string, age int, gender string, row_timestamp timestamp) ttl_duration = 0, ttl_col = "row_timestamp";
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG woman Drop (row_timestamp);
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG woman;
      """
    Then the result should be, in any order:
      | Tag     | Create Tag                                                                                                                                       |
      | "woman" | 'CREATE TAG `woman` (\n `name` string NULL,\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL\n) ttl_duration = 0, ttl_col = ""' |
    When executing query:
      """
      ALTER TAG woman ttl_duration = 100, ttl_col = "age";
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG woman;
      """
    Then the result should be, in any order:
      | Tag     | Create Tag                                                                                                                                            |
      | "woman" | 'CREATE TAG `woman` (\n `name` string NULL,\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL\n) ttl_duration = 100, ttl_col = "age"' |
    When executing query:
      """
      ALTER TAG woman CHANGE (age string);
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      ALTER TAG woman ttl_col = "";
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG woman;
      """
    Then the result should be, in any order:
      | Tag     | Create Tag                                                                                                                                       |
      | "woman" | 'CREATE TAG `woman` (\n `name` string NULL,\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL\n) ttl_duration = 0, ttl_col = ""' |
    When executing query:
      """
      ALTER TAG woman CHANGE (age string);
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG woman;
      """
    Then the result should be, in any order:
      | Tag     | Create Tag                                                                                                                                        |
      | "woman" | 'CREATE TAG `woman` (\n `name` string NULL,\n `email` string NULL,\n `age` string NULL,\n `gender` string NULL\n) ttl_duration = 0, ttl_col = ""' |
    When executing query:
      """
      CREATE EDGE work(number string, start_time timestamp);
      """
    Then the execution should be successful
    When executing query:
      """
      DESCRIBE EDGE work;
      """
    Then the result should be, in any order:
      | Field        | Type        | Null  | Default | Comment |
      | "number"     | "string"    | "YES" | EMPTY   | EMPTY   |
      | "start_time" | "timestamp" | "YES" | EMPTY   | EMPTY   |
    When executing query:
      """
      SHOW CREATE EDGE work;
      """
    Then the result should be, in any order:
      | Edge   | Create Edge                                                                                                    |
      | "work" | 'CREATE EDGE `work` (\n `number` string NULL,\n `start_time` timestamp NULL\n) ttl_duration = 0, ttl_col = ""' |
    When executing query:
      """
      CREATE EDGE work1(name string, email string, age int, gender string, row_timestamp timestamp) ttl_duration = 100, ttl_col = "row_timestamp";
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE EDGE work1;
      """
    Then the result should be, in any order:
      | Edge    | Create Edge                                                                                                                                                                                        |
      | "work1" | 'CREATE EDGE `work1` (\n `name` string NULL,\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL,\n `row_timestamp` timestamp NULL\n) ttl_duration = 100, ttl_col = "row_timestamp"' |
    When executing query:
      """
      DROP EDGE work1;
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE work1(name string, email string, age int, gender string, row_timestamp timestamp) ttl_duration = 100, ttl_col = "row_timestamp";
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE work2(number string, start_time timestamp) ttl_duration = 100;
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE EDGE work2(number string, start_time timestamp) ttl_col = "name";
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE EDGE work2(name string, email string,  age int, gender string, start_time timestamp) ttl_duration = 0, ttl_col = "start_time";
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE EDGE work2;
      """
    Then the result should be, in any order:
      | Edge    | Create Edge                                                                                                                                                                                |
      | "work2" | 'CREATE EDGE `work2` (\n `name` string NULL,\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL,\n `start_time` timestamp NULL\n) ttl_duration = 0, ttl_col = "start_time"' |
    When executing query:
      """
      CREATE EDGE edge_only_ttl_col(name string, email string, age int, gender string, row_timestamp timestamp) ttl_col = "row_timestamp";
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE EDGE edge_only_ttl_col;
      """
    Then the result should be, in any order:
      | Edge                | Create Edge                                                                                                                                                                                                  |
      | "edge_only_ttl_col" | 'CREATE EDGE `edge_only_ttl_col` (\n `name` string NULL,\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL,\n `row_timestamp` timestamp NULL\n) ttl_duration = 0, ttl_col = "row_timestamp"' |
    When executing query:
      """
      ALTER EDGE work2 ttl_duration = 50, ttl_col = "start_time";
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE EDGE work2;
      """
    Then the result should be, in any order:
      | Edge    | Create Edge                                                                                                                                                                                 |
      | "work2" | 'CREATE EDGE `work2` (\n `name` string NULL,\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL,\n `start_time` timestamp NULL\n) ttl_duration = 50, ttl_col = "start_time"' |
    When executing query:
      """
      ALTER EDGE work2 ttl_col = "name";
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      ALTER EDGE work2  Drop (name) ttl_duration = 200;
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE EDGE work2;
      """
    Then the result should be, in any order:
      | Edge    | Create Edge                                                                                                                                                            |
      | "work2" | 'CREATE EDGE `work2` (\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL,\n `start_time` timestamp NULL\n) ttl_duration = 200, ttl_col = "start_time"' |
    When executing query:
      """
      ALTER EDGE work2 Drop (start_time);
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE EDGE work2;
      """
    Then the result should be, in any order:
      | Edge    | Create Edge                                                                                                                 |
      | "work2" | 'CREATE EDGE `work2` (\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL\n) ttl_duration = 0, ttl_col = ""' |
    When executing query:
      """
      ALTER Edge work2  ttl_duration = 100, ttl_col = "age";
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE EDGE work2;
      """
    Then the result should be, in any order:
      | Edge    | Create Edge                                                                                                                      |
      | "work2" | 'CREATE EDGE `work2` (\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL\n) ttl_duration = 100, ttl_col = "age"' |
    When executing query:
      """
      ALTER EDGE work2  CHANGE (age string);
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      ALTER EDGE work2 ttl_col = "";
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE EDGE work2;
      """
    Then the result should be, in any order:
      | Edge    | Create Edge                                                                                                                 |
      | "work2" | 'CREATE EDGE `work2` (\n `email` string NULL,\n `age` int64 NULL,\n `gender` string NULL\n) ttl_duration = 0, ttl_col = ""' |
    When executing query:
      """
      ALTER EDGE work2 CHANGE (age string);
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE EDGE work2;
      """
    Then the result should be, in any order:
      | Edge    | Create Edge                                                                                                                  |
      | "work2" | 'CREATE EDGE `work2` (\n `email` string NULL,\n `age` string NULL,\n `gender` string NULL\n) ttl_duration = 0, ttl_col = ""' |
    And drop the used space

  Scenario: TTLTest Datatest
    Given having executed:
      """
      CREATE TAG person(id int) ttl_col="id", ttl_duration=100;
      CREATE TAG career(id int);
      CREATE Edge like(id int) ttl_col="id", ttl_duration=100;
      CREATE Edge friend(id int);
      """
    When try to execute query:
      """
      INSERT VERTEX person(id) VALUES "1":(100), "2":(200);
      INSERT VERTEX career(id) VALUES "2":(200);
      INSERT EDGE like(id) VALUES "100"->"1":(100), "100"->"2":(200);
      INSERT EDGE friend(id) VALUES "100"->"1":(100), "100"->"2":(200);
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON person "1";
      """
    Then the result should be, in any order, with relax comparison:
      | vertices_ |
    When executing query:
      """
      FETCH PROP ON person "1" YIELD person.id as id
      """
    Then the result should be, in any order:
      | VertexID | id |
    When executing query:
      """
      FETCH PROP ON * "1" YIELD person.id, career.id
      """
    Then the result should be, in any order:
      | VertexID | person.id | career.id |
    When executing query:
      """
      FETCH PROP ON person "2" YIELD person.id
      """
    Then the result should be, in any order:
      | VertexID | person.id |
    When executing query:
      """
      FETCH PROP ON person "2" YIELD person.id as id
      """
    Then the result should be, in any order:
      | VertexID | id |
    When executing query:
      """
      FETCH PROP ON career "2" YIELD career.id;
      """
    Then the result should be, in any order:
      | VertexID | career.id |
      | "2"      | 200       |
    When executing query:
      """
      FETCH PROP ON * "2" YIELD person.id, career.id
      """
    Then the result should be, in any order:
      | VertexID | person.id | career.id |
      | "2"      | EMPTY     | 200       |
    When executing query:
      """
      FETCH PROP ON friend "100"->"1","100"->"2" YIELD friend.id;
      """
    Then the result should be, in any order:
      | friend._src | friend._dst | friend._rank | friend.id |
      | "100"       | "1"         | 0            | 100       |
      | "100"       | "2"         | 0            | 200       |
    When executing query:
      """
      FETCH PROP ON friend "100"->"1","100"->"2" YIELD friend.id AS id;
      """
    Then the result should be, in any order:
      | friend._src | friend._dst | friend._rank | id  |
      | "100"       | "1"         | 0            | 100 |
      | "100"       | "2"         | 0            | 200 |
    When executing query:
      """
      FETCH PROP ON like "100"->"1","100"->"2" YIELD like.id;
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.id |
    When executing query:
      """
      FETCH PROP ON like "100"->"1","100"->"2" YIELD like.id AS id;
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | id |
    And drop the used space

  Scenario: TTLTest expire time
    Given having executed:
      """
      CREATE TAG person(id timestamp, age int) ttl_col="id", ttl_duration=5;
      """
    When try to execute query:
      """
      INSERT VERTEX person(id, age) VALUES "1":(now(), 20);
      """
    Then the execution should be successful
    And wait 1 seconds
    When executing query:
      """
      FETCH PROP ON person "1" YIELD person.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | VertexID | age |
      | "1"      | 20  |
    And wait 7 seconds
    When executing query:
      """
      FETCH PROP ON person "1" YIELD person.age as age;
      """
    Then the result should be, in any order:
      | VertexID | age |
    And drop the used space
