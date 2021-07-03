Feature: IndexTest_Vid_String

  Scenario: IndexTest TagIndex
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    And having executed:
      """
      CREATE TAG tag_1(col1 string, col2 int, col3 double, col4 timestamp);
      """
    When executing query:
      """
      CREATE TAG INDEX single_tag_index ON tag_1(col2);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX duplicate_tag_index_1 ON tag_1(col2);
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE TAG INDEX single_person_index ON student(name);
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE TAG INDEX single_tag_index ON tag_1(col5);
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE TAG INDEX single_tag_index ON tag_1();
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE TAG INDEX multi_tag_index ON tag_1(col2, col3);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX duplicate_person_index ON tag_1(col2, col3);
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE TAG INDEX duplicate_person_index ON tag_1(col2, col2);
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE TAG INDEX disorder_tag_index ON tag_1(col3, col2);
      """
    Then the execution should be successful
    And wait 6 seconds
    When try to execute query:
      """
      INSERT VERTEX
        tag_1(col1, col2, col3, col4)
      VALUES
        "Tim":  ("Tim",  18, 11.11, `timestamp`("2000-10-10T10:00:00")),
        "Tony": ("Tony", 18, 11.11, `timestamp`("2000-10-10T10:00:00")),
        "May":  ("May",  18, 11.11, `timestamp`("2000-10-10T10:00:00")),
        "Tom":  ("Tom",  18, 11.11, `timestamp`("2000-10-10T10:00:00"))
      """
    Then the execution should be successful
    When executing query:
      """
      REBUILD TAG INDEX single_tag_index;
      """
    Then the execution should be successful
    When executing query:
      """
      REBUILD TAG INDEX single_tag_index OFFLINE;
      """
    Then a SyntaxError should be raised at runtime:
    When executing query:
      """
      REBUILD TAG INDEX multi_tag_index;
      """
    Then the execution should be successful
    When executing query:
      """
      REBUILD TAG INDEX multi_tag_index OFFLINE;
      """
    Then a SyntaxError should be raised at runtime:
    When executing query:
      """
      SHOW TAG INDEX STATUS;
      """
    Then the execution should be successful
    When executing query:
      """
      DESCRIBE TAG INDEX multi_tag_index;
      """
    Then the result should be, in any order:
      | Field  | Type     |
      | "col2" | "int64"  |
      | "col3" | "double" |
    When executing query:
      """
      DESC TAG INDEX multi_tag_index;
      """
    Then the result should be, in any order:
      | Field  | Type     |
      | "col2" | "int64"  |
      | "col3" | "double" |
    When executing query:
      """
      SHOW CREATE TAG INDEX multi_tag_index
      """
    Then the execution should be successful
    When executing query:
      """
      DROP TAG INDEX multi_tag_index;
      """
    Then the execution should be successful
    When executing query:
      """
      Show TAG INDEXES;
      """
    Then the result should be, in any order:
      | Index Name           | By Tag  | Columns          |
      | "disorder_tag_index" | "tag_1" | ["col3", "col2"] |
      | "single_tag_index"   | "tag_1" | ["col2"]         |
    When executing query:
      """
      Show TAG INDEXES BY tag_1;
      """
    Then the result should be, in any order:
      | Index Name           | Columns          |
      | "disorder_tag_index" | ["col3", "col2"] |
      | "single_tag_index"   | ["col2"]         |
    When executing query:
      """
      DESCRIBE TAG INDEX multi_tag_index;
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      DROP TAG INDEX not_exists_tag_index;
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      DROP TAG INDEX IF EXISTS not_exists_tag_index
      """
    Then the execution should be successful
    Then drop the used space

  Scenario: IndexTest EdgeIndex
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    And having executed:
      """
      CREATE EDGE edge_1(col1 string, col2 int, col3 double, col4 timestamp)
      """
    When executing query:
      """
      CREATE EDGE INDEX single_edge_index ON edge_1(col2);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX duplicate_edge_1_index ON edge_1(col2)
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE EDGE INDEX single_edge_index ON edge_1_ship(name)
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE EDGE INDEX single_edge_index ON edge_1(startTime)
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE EDGE INDEX single_edge_index ON edge_1()
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE EDGE INDEX multi_edge_1_index ON edge_1(col2, col3)
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX duplicate_edge_1_index ON edge_1(col2, col3)
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE EDGE INDEX duplicate_index ON edge_1(col2, col2)
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      CREATE EDGE INDEX disorder_edge_1_index ON edge_1(col3, col2)
      """
    Then the execution should be successful
    And wait 6 seconds
    When executing query:
      """
      INSERT EDGE
        edge_1(col1, col2, col3, col4)
      VALUES
        "Tim"  -> "May":  ("Good", 18, 11.11, `timestamp`("2000-10-10T10:00:00")),
        "Tim"  -> "Tony": ("Good", 18, 11.11, `timestamp`("2000-10-10T10:00:00")),
        "Tony" -> "May":  ("Like", 18, 11.11, `timestamp`("2000-10-10T10:00:00")),
        "May"  -> "Tim":  ("Like", 18, 11.11, `timestamp`("2000-10-10T10:00:00"))
      """
    Then the execution should be successful
    When executing query:
      """
      REBUILD EDGE INDEX single_edge_index
      """
    Then the execution should be successful
    When executing query:
      """
      REBUILD EDGE INDEX single_edge_index OFFLINE;
      """
    Then a SyntaxError should be raised at runtime:
    When executing query:
      """
      REBUILD EDGE INDEX multi_edge_1_index
      """
    Then the execution should be successful
    When executing query:
      """
      REBUILD EDGE INDEX multi_edge_1_index OFFLINE;
      """
    Then a SyntaxError should be raised at runtime:
    When executing query:
      """
      SHOW EDGE INDEX STATUS
      """
    Then the execution should be successful
    When executing query:
      """
      DESCRIBE EDGE INDEX multi_edge_1_index
      """
    Then the result should be, in any order:
      | Field  | Type     |
      | "col2" | "int64"  |
      | "col3" | "double" |
    When executing query:
      """
      DESC EDGE INDEX multi_edge_1_index
      """
    Then the result should be, in any order:
      | Field  | Type     |
      | "col2" | "int64"  |
      | "col3" | "double" |
    When executing query:
      """
      DROP EDGE INDEX multi_edge_1_index;
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW EDGE INDEXES
      """
    Then the result should be, in any order:
      | Index Name              | By Edge  | Columns          |
      | "disorder_edge_1_index" | "edge_1" | ["col3", "col2"] |
      | "single_edge_index"     | "edge_1" | ["col2"]         |
    When executing query:
      """
      SHOW EDGE INDEXES BY edge_1
      """
    Then the result should be, in any order:
      | Index Name              | Columns          |
      | "disorder_edge_1_index" | ["col3", "col2"] |
      | "single_edge_index"     | ["col2"]         |
    When executing query:
      """
      DESCRIBE EDGE INDEX multi_edge_1_index;
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      DROP EDGE INDEX not_exists_edge_index
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      DROP EDGE INDEX IF EXISTS not_exists_edge_index
      """
    Then the execution should be successful
    Then drop the used space

  Scenario: IndexTest TagIndexTTL
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    And having executed:
      """
      CREATE TAG person_ttl(number int, age int, gender int, email string);
      """
    When executing query:
      """
      CREATE TAG INDEX single_person_ttl_index ON person_ttl(age)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG person_ttl ttl_duration = 100, ttl_col = "age"
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      ALTER TAG person_ttl ttl_duration = 100, ttl_col = "gender"
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      DROP TAG INDEX single_person_ttl_index
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG person_ttl ttl_duration = 100, ttl_col = "age"
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG person_ttl ttl_duration = 100, ttl_col = "gender"
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX single_person_ttl_index_second_gender ON person_ttl(gender)
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX single_person_ttl_index_second_age ON person_ttl(age)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG person_ttl  ttl_col = ""
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      ALTER TAG person_ttl ttl_duration = 100, ttl_col = "age"
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      ALTER TAG person_ttl ttl_duration = 100, ttl_col = "gender"
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      DROP TAG INDEX single_person_ttl_index_second_gender
      """
    Then the execution should be successful
    When executing query:
      """
      DROP TAG INDEX single_person_ttl_index_second_age
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG person_ttl  ttl_col = ""
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG person_ttl ttl_duration = 100, ttl_col = "age"
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG person_ttl ttl_duration = 100, ttl_col = "gender"
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG person_ttl_2(number int, age int, gender string)
                             ttl_duration = 200, ttl_col = "age"
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX person_ttl_2_index_number ON person_ttl_2(number)
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX person_ttl_2_index_age ON person_ttl_2(age)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG person_ttl_2 DROP (age)
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      DROP TAG INDEX person_ttl_2_index_number
      """
    Then the execution should be successful
    When executing query:
      """
      DROP TAG INDEX person_ttl_2_index_age
      """
    Then the execution should be successful
    Then drop the used space

  Scenario: IndexTest EdgeIndexTTL
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    And having executed:
      """
      CREATE EDGE edge_1_ttl(degree int, start_time int)
      """
    When executing query:
      """
      CREATE EDGE INDEX single_edge_1_ttl_index ON edge_1_ttl(start_time)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER edge edge_1_ttl ttl_duration = 100, ttl_col = "start_time"
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      ALTER edge edge_1_ttl ttl_duration = 100, ttl_col = "degree"
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      DROP EDGE INDEX single_edge_1_ttl_index
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER edge edge_1_ttl ttl_duration = 100, ttl_col = "start_time"
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER edge edge_1_ttl ttl_duration = 100, ttl_col = "degree"
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX edge_1_ttl_index_second_degree ON edge_1_ttl(degree)
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX edge_1_ttl_index_second_start_time ON edge_1_ttl(start_time)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER EDGE edge_1_ttl ttl_col = ""
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      DROP EDGE INDEX edge_1_ttl_index_second_degree
      """
    Then the execution should be successful
    When executing query:
      """
      DROP EDGE INDEX edge_1_ttl_index_second_start_time
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER EDGE edge_1_ttl  ttl_col = ""
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER edge edge_1_ttl ttl_duration = 100, ttl_col = "start_time"
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER edge edge_1_ttl ttl_duration = 100, ttl_col = "degree"
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE edge_1_ttl_2(degree int, start_time int) ttl_duration = 200, ttl_col = "start_time"
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX edge_1_ttl_index_2_degree ON edge_1_ttl_2(degree)
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX edge_1_ttl_index_2_start_time ON edge_1_ttl_2(start_time)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER EDGE edge_1_ttl_2 DROP (start_time)
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      DROP EDGE INDEX edge_1_ttl_index_2_degree
      """
    Then the execution should be successful
    When executing query:
      """
      DROP EDGE INDEX edge_1_ttl_index_2_start_time
      """
    Then the execution should be successful
    Then drop the used space

  Scenario: IndexTest AlterTag
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    And having executed:
      """
      CREATE TAG tag_1(col1 bool, col2 int, col3 double, col4 timestamp)
      """
    When executing query:
      """
      CREATE TAG INDEX single_person_index ON tag_1(col1)
      """
    Then the execution should be successful
    And wait 3 seconds
    When try to execute query:
      """
      INSERT VERTEX
        tag_1(col1, col2, col3, col4)
      VALUES
        "100":  (true,  18, 1.1, `timestamp`("2000-10-10T10:00:00"))
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG tag_1 ADD (col5 int)
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX single_person_index2 ON tag_1(col5)
      """
    Then the execution should be successful
    And wait 3 seconds
    When try to execute query:
      """
      INSERT VERTEX
        tag_1(col1, col2, col3, col4, col5)
      VALUES
        "100":(true,  18, 1.1, `timestamp`("2000-10-10T10:00:00"), 5)
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON tag_1 WHERE tag_1.col5 == 5 YIELD tag_1.col5, tag_1.col1
      """
    Then the result should be, in any order:
      | VertexID | tag_1.col5 | tag_1.col1 |
      | "100"    | 5          | true       |
    When executing query:
      """
      LOOKUP ON tag_1 WHERE tag_1.col5 == 5 YIELD tag_1.col1, tag_1.col5
      """
    Then the result should be, in any order:
      | VertexID | tag_1.col1 | tag_1.col5 |
      | "100"    | true       | 5          |
    Then drop the used space

  Scenario: IndexTest RebuildTagIndexStatusInfo
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    And having executed:
      """
      CREATE TAG tag_status(col1 int)
      """
    When executing query:
      """
      CREATE TAG INDEX tag_index_status ON tag_status(col1);
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW TAG INDEX STATUS;
      """
    Then the result should be, in any order:
      | Name | Index Status |
    And wait 3 seconds
    When submit a job:
      """
      REBUILD TAG INDEX tag_index_status
      """
    Then wait the job to finish
    When executing query:
      """
      DROP TAG INDEX tag_index_status
      """
    Then the execution should be successful
    When executing query:
      """
      DESCRIBE TAG INDEX tag_index_status
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      SHOW TAG INDEX STATUS;
      """
    Then the result should be, in any order:
      | Name               | Index Status |
      | "tag_index_status" | "FINISHED"   |
    Then drop the used space

  Scenario: IndexTest RebuildEdgeIndexStatusInfo
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    And having executed:
      """
      CREATE EDGE edge_status(col1 int)
      """
    When executing query:
      """
      CREATE EDGE INDEX edge_index_status ON edge_status(col1);
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW EDGE INDEX STATUS;
      """
    Then the result should be, in any order:
      | Name | Index Status |
    And wait 3 seconds
    When submit a job:
      """
      REBUILD EDGE INDEX edge_index_status
      """
    Then wait the job to finish
    When executing query:
      """
      DROP EDGE INDEX edge_index_status
      """
    Then the execution should be successful
    When executing query:
      """
      DESCRIBE EDGE INDEX edge_index_status
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      SHOW EDGE INDEX STATUS;
      """
    Then the result should contain:
      | Name                | Index Status |
      | "edge_index_status" | "FINISHED"   |
    Then drop the used space

  Scenario: IndexTest AlterSchemaTest
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    And having executed:
      """
      CREATE TAG alter_tag(id int);
      """
    When executing query:
      """
      CREATE TAG INDEX alter_index ON alter_tag(id);
      """
    Then the execution should be successful
    And wait 6 seconds
    When try to execute query:
      """
      INSERT VERTEX alter_tag(id) VALUES "100":(1), "200":(2)
      """
    Then the execution should be successful
    When executing query:
      """
      ALTER TAG alter_tag ADD (type int)
      """
    Then the execution should be successful
    And wait 6 seconds
    When executing query:
      """
      LOOKUP ON alter_tag WHERE alter_tag.id == 1 YIELD alter_tag.type
      """
    Then the execution should be successful
    Then drop the used space

  Scenario: IndexTest rebuild all tag indexes by empty input
    Given an empty graph
    And create a space with following options:
      | name     | rebuild_tag_space |
      | vid_type | FIXED_STRING(10)  |
    And having executed:
      """
      CREATE TAG id_tag(id int);
      CREATE TAG name_tag(name string);
      """
    When try to execute query:
      """
      INSERT VERTEX id_tag(id) VALUES "100":(100), "200":(100);
      INSERT VERTEX name_tag(name) VALUES "300":("100"), "400":("100");
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX id_tag_index ON id_tag(id);
      CREATE TAG INDEX name_tag_index ON name_tag(name(10));
      """
    Then the execution should be successful
    And wait 6 seconds
    When submit a job:
      """
      REBUILD TAG INDEX;
      """
    Then wait the job to finish
    When executing query:
      """
      SHOW TAG INDEX STATUS;
      """
    Then the result should contain:
      | Name                                | Index Status |
      | "rebuild_tag_space_all_tag_indexes" | "FINISHED"   |
    When executing query:
      """
      LOOKUP ON id_tag WHERE id_tag.id == 100
      """
    Then the result should be, in any order:
      | VertexID |
      | "100"    |
      | "200"    |
    When executing query:
      """
      LOOKUP ON name_tag WHERE name_tag.name == "100"
      """
    Then the result should be, in any order:
      | VertexID |
      | "300"    |
      | "400"    |
    Then drop the used space

  Scenario: IndexTest rebuild all tag indexes by multi input
    Given an empty graph
    And create a space with following options:
      | vid_type | FIXED_STRING(10) |
    And having executed:
      """
      CREATE TAG id_tag(id int);
      CREATE TAG name_tag(name string);
      CREATE TAG age_tag(age int);
      """
    When try to execute query:
      """
      INSERT VERTEX id_tag(id) VALUES "100":(100), "200":(100);
      INSERT VERTEX name_tag(name) VALUES "300":("100"), "400":("100");
      INSERT VERTEX age_tag(age) VALUES "500":(8), "600":(8);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX id_tag_index ON id_tag(id);
      CREATE TAG INDEX name_tag_index ON name_tag(name(10));
      CREATE TAG INDEX age_tag_index ON age_tag(age);
      """
    Then the execution should be successful
    And wait 6 seconds
    When submit a job:
      """
      REBUILD TAG INDEX id_tag_index, name_tag_index;
      """
    Then wait the job to finish
    When executing query:
      """
      SHOW TAG INDEX STATUS;
      """
    Then the result should contain:
      | Name                          | Index Status |
      | "id_tag_index,name_tag_index" | "FINISHED"   |
    When executing query:
      """
      LOOKUP ON id_tag WHERE id_tag.id == 100
      """
    Then the result should be, in any order:
      | VertexID |
      | "100"    |
      | "200"    |
    When executing query:
      """
      LOOKUP ON name_tag WHERE name_tag.name == "100"
      """
    Then the result should be, in any order:
      | VertexID |
      | "300"    |
      | "400"    |
    When executing query:
      """
      LOOKUP ON age_tag WHERE age_tag.age == 8
      """
    Then the result should be, in any order:
      | VertexID |
    Then drop the used space

  Scenario: IndexTest rebuild all edge indexes by empty input
    Given an empty graph
    And create a space with following options:
      | name     | rebuild_edge_space |
      | vid_type | FIXED_STRING(10)   |
    And having executed:
      """
      CREATE EDGE id_edge(id int);
      CREATE EDGE name_edge(name string);
      """
    When try to execute query:
      """
      INSERT EDGE id_edge(id) VALUES "100"->"200":(100);
      INSERT EDGE name_edge(name) VALUES "300"->"400":("100");
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX id_edge_index ON id_edge(id);
      CREATE EDGE INDEX name_edge_index ON name_edge(name(10));
      """
    Then the execution should be successful
    And wait 6 seconds
    When submit a job:
      """
      REBUILD EDGE INDEX;
      """
    Then wait the job to finish
    When executing query:
      """
      SHOW EDGE INDEX STATUS;
      """
    Then the result should contain:
      | Name                                  | Index Status |
      | "rebuild_edge_space_all_edge_indexes" | "FINISHED"   |
    When executing query:
      """
      LOOKUP ON id_edge WHERE id_edge.id == 100
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "100"  | "200"  | 0       |
    When executing query:
      """
      LOOKUP ON name_edge WHERE name_edge.name == "100"
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "300"  | "400"  | 0       |
    Then drop the used space

  Scenario: IndexTest rebuild all edge indexes by multi input
    Given an empty graph
    And create a space with following options:
      | vid_type | FIXED_STRING(20) |
    And having executed:
      """
      CREATE EDGE id_edge(id int);
      CREATE EDGE name_edge(name string);
      CREATE EDGE age_edge(age int);
      """
    When try to execute query:
      """
      INSERT EDGE id_edge(id) VALUES "100"->"200":(100);
      INSERT EDGE name_edge(name) VALUES "300"->"400":("100");
      INSERT EDGE age_edge(age) VALUES "500"->"600":(8);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX id_edge_index ON id_edge(id);
      CREATE EDGE INDEX name_edge_index ON name_edge(name(10));
      CREATE EDGE INDEX age_edge_index ON age_edge(age);
      """
    Then the execution should be successful
    And wait 6 seconds
    When submit a job:
      """
      REBUILD EDGE INDEX id_edge_index,name_edge_index;
      """
    Then wait the job to finish
    When executing query:
      """
      SHOW EDGE INDEX STATUS;
      """
    Then the result should contain:
      | Name                            | Index Status |
      | "id_edge_index,name_edge_index" | "FINISHED"   |
    When executing query:
      """
      LOOKUP ON id_edge WHERE id_edge.id == 100
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "100"  | "200"  | 0       |
    When executing query:
      """
      LOOKUP ON name_edge WHERE name_edge.name == "100"
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "300"  | "400"  | 0       |
    When executing query:
      """
      LOOKUP ON age_edge WHERE age_edge.age == 8
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
    Then drop the used space

  Scenario: show create tag index
    Given a graph with space named "nba"
    When executing query:
      """
      SHOW CREATE TAG INDEX `player_name_index`
      """
    Then the result should be, in any order:
      | Tag Index Name      | Create Tag Index                                                     |
      | "player_name_index" | "CREATE TAG INDEX `player_name_index` ON `player` (\n `name`(64)\n)" |
    When executing query:
      """
      SHOW CREATE TAG INDEX `player_age_index`
      """
    Then the result should be, in any order:
      | Tag Index Name     | Create Tag Index                                               |
      | "player_age_index" | "CREATE TAG INDEX `player_age_index` ON `player` (\n `age`\n)" |
