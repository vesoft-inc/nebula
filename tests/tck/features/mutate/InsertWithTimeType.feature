Feature: Insert with time-dependent types

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |

  Scenario: insert wrong format timestamp
    Given having executed:
      """
      CREATE TAG IF NOT EXISTS TAG_TIMESTAMP(a timestamp);
      CREATE TAG IF NOT EXISTS TAG_TIME(a time);
      CREATE TAG IF NOT EXISTS TAG_DATE(a date);
      CREATE TAG IF NOT EXISTS TAG_DATETIME(a datetime);
      """
    When try to execute query:
      """
      INSERT VERTEX TAG_TIMESTAMP(a) VALUES "TEST_VERTEX":(NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX TAG_TIMESTAMP(a) VALUES "TEST_VERTEX":("2000.0.0 10:0:0")
      """
    Then a ExecutionError should be raised at runtime:Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When try to execute query:
      """
      INSERT VERTEX TAG_TIME(a) VALUES "TEST_VERTEX":(NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX TAG_TIME(a) VALUES "TEST_VERTEX":("10:0:0")
      """
    Then a ExecutionError should be raised at runtime:Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When try to execute query:
      """
      INSERT VERTEX TAG_DATE(a) VALUES "TEST_VERTEX":(NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX TAG_DATE(a) VALUES "TEST_VERTEX":("2000.0.0")
      """
    Then a ExecutionError should be raised at runtime:Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When try to execute query:
      """
      INSERT VERTEX TAG_DATETIME(a) VALUES "TEST_VERTEX":(NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX TAG_DATETIME(a) VALUES "TEST_VERTEX":("2000.0.0")
      """
    Then a ExecutionError should be raised at runtime:Storage Error: The data type does not meet the requirements. Use the correct type of data.
    And drop the used space

  Scenario: Basic CRUD for timestamp type
    Given having executed:
      """
      CREATE TAG tag_date(f_date DATE, f_time TIME, f_datetime DATETIME);
      CREATE EDGE edge_date(f_date DATE, f_time TIME, f_datetime DATETIME);
      """
    When executing query:
      """
      SHOW CREATE TAG tag_date;
      """
    Then the result should be, in any order:
      | Tag        | Create Tag                                                                                                                           |
      | 'tag_date' | 'CREATE TAG `tag_date` (\n `f_date` date NULL,\n `f_time` time NULL,\n `f_datetime` datetime NULL\n) ttl_duration = 0, ttl_col = ""' |
    When executing query:
      """
      SHOW CREATE EDGE edge_date;
      """
    Then the result should be, in any order:
      | Edge        | Create Edge                                                                                                                            |
      | 'edge_date' | 'CREATE EDGE `edge_date` (\n `f_date` date NULL,\n `f_time` time NULL,\n `f_datetime` datetime NULL\n) ttl_duration = 0, ttl_col = ""' |
    When try to execute query:
      """
      INSERT VERTEX tag_date(f_date, f_time, f_datetime) VALUES "test":(date("2017-03-04"), time("23:01:00"), datetime("2017-03-04T22:30:40"));
      INSERT EDGE edge_date(f_date, f_time, f_datetime) VALUES "test_src"->"test_dst":(date("2017-03-04"), time("23:01:00"), datetime("2017-03-04T22:30:40"));
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX tag_date(f_date, f_time, f_datetime) VALUES "test":("2017-03-04", "23:01:00", 1234)
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      INSERT EDGE edge_date(f_date, f_time, f_datetime) VALUES "test_src"->"test_dst":(true, "23:01:00", "2017-03-04T22:30:40")
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      FETCH PROP ON tag_date "test" YIELD tag_date.f_date, tag_date.f_time, tag_date.f_datetime;
      """
    Then the result should be, in any order:
      | VertexID | tag_date.f_date | tag_date.f_time   | tag_date.f_datetime          |
      | 'test'   | '2017-03-04'    | '23:01:00.000000' | '2017-03-04T22:30:40.000000' |
    When executing query:
      """
      FETCH PROP ON edge_date "test_src"->"test_dst" YIELD edge_date.f_date, edge_date.f_time, edge_date.f_datetime;
      """
    Then the result should be, in any order:
      | edge_date._src | edge_date._dst | edge_date._rank | edge_date.f_date | edge_date.f_time  | edge_date.f_datetime         |
      | 'test_src'     | 'test_dst'     | 0               | '2017-03-04'     | '23:01:00.000000' | '2017-03-04T22:30:40.000000' |
    When executing query:
      """
      UPDATE VERTEX "test"
      SET
        tag_date.f_date = Date("2018-03-04"),
        tag_date.f_time = Time("22:01:00"),
        tag_date.f_datetime = DateTime("2018-03-04T22:30:40")
      YIELD f_date, f_time, f_datetime;
      """
    Then the result should be, in any order:
      | f_date       | f_time            | f_datetime                   |
      | '2018-03-04' | '22:01:00.000000' | '2018-03-04T22:30:40.000000' |
    When executing query:
      """
      UPDATE EDGE "test_src"->"test_dst" OF edge_date
      SET
        edge_date.f_date = Date("2018-03-04"),
        edge_date.f_time = Time("22:01:00"),
        edge_date.f_datetime = DateTime("2018-03-04T22:30:40")
      YIELD f_date, f_time, f_datetime
      """
    Then the result should be, in any order:
      | f_date       | f_time            | f_datetime                   |
      | '2018-03-04' | '22:01:00.000000' | '2018-03-04T22:30:40.000000' |
    When executing query:
      """
      DELETE VERTEX "test";
      DELETE EDGE edge_date "test_src"->"test_dst";
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON tag_date "test";
      """
    Then the result should be, in any order, with relax comparison:
      | vertices_ |
    When executing query:
      """
      FETCH PROP ON edge_date "test_src"->"test_dst";
      """
    Then the result should be, in any order, with relax comparison:
      | edges_ |
    And drop the used space
