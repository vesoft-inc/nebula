Feature: Insert duration

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |

  Scenario: insert duration failed
    Given having executed:
      """
      CREATE TAG IF NOT EXISTS test_failed(a int);
      """
    When try to execute query:
      """
      INSERT VERTEX test_failed(a) VALUES "TEST_VERTEX_FAILED":(duration({years: 3}))
      """
    Then a ExecutionError should be raised at runtime:Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When try to execute query:
      """
      INSERT VERTEX test_failed(a) VALUES "TEST_VERTEX_FAILED":(duration())
      """
    Then a SyntaxError should be raised at runtime: Unknown function  near `duration'

  Scenario: duration don't support index
    Given having executed:
      """
      CREATE TAG IF NOT EXISTS test_tag_index_failed(a duration);
      CREATE EDGE IF NOT EXISTS test_edge_index_failed(a duration);
      """
    When try to execute query:
      """
      CREATE TAG INDEX test_tag_duration_index ON test_tag_index_failed(a);
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    When try to execute query:
      """
      CREATE EDGE INDEX test_edge_duration_index ON test_edge_index_failed(a);
      """
    Then a ExecutionError should be raised at runtime: Invalid param!

  Scenario: Basic CRUD for duration type
    Given having executed:
      """
      CREATE TAG tag_duration(f_duration duration);
      CREATE EDGE edge_duration(f_duration duration);
      """
    When executing query:
      """
      SHOW CREATE TAG tag_duration;
      """
    Then the result should be, in any order:
      | Tag            | Create Tag                                                                                   |
      | 'tag_duration' | 'CREATE TAG `tag_duration` (\n `f_duration` duration NULL\n) ttl_duration = 0, ttl_col = ""' |
    When executing query:
      """
      SHOW CREATE EDGE edge_duration;
      """
    Then the result should be, in any order:
      | Edge            | Create Edge                                                                                    |
      | 'edge_duration' | 'CREATE EDGE `edge_duration` (\n `f_duration` duration NULL\n) ttl_duration = 0, ttl_col = ""' |
    When try to execute query:
      """
      INSERT VERTEX tag_duration(f_duration) VALUES "test":(duration({years: 1, seconds: 0}));
      INSERT EDGE edge_duration(f_duration) VALUES "test_src"->"test_dst":(duration({years: 1, seconds: 0}));
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX tag_duration(f_duration) VALUES "test":(1)
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      INSERT EDGE edge_duration(f_duration) VALUES "test_src"->"test_dst":(true)
      """
    Then a ExecutionError should be raised at runtime:
    # TODO TCK python-client support duration type
    # When executing query:
    # """
    # FETCH PROP ON tag_duration "test" YIELD tag_duration.f_duration
    # """
    # Then the result should be, in any order:
    # | tag_duration.f_duration |
    # | 'P1YT1S'                |
    # When executing query:
    # """
    # FETCH PROP ON edge_duration "test_src"->"test_dst" YIELD edge_duration.f_duration
    # """
    # Then the result should be, in any order:
    # | edge_duration.f_duration |
    # | 'P1YT1S'     |
    # When executing query:
    # """
    # UPDATE VERTEX "test"
    # SET
    # tag_duration.f_duration = duration({years: 3, months: 3, days: 4})
    # YIELD f_duration
    # """
    # Then the result should be, in any order:
    # | f_duration |
    # | 'P3Y3M4D'  |
    # When executing query:
    # """
    # UPDATE EDGE "test_src"->"test_dst" OF edge_duration
    # SET
    # edge_duration.f_duration = duration({years: 3, months: 3, days: 4})
    # YIELD f_duration
    # """
    # Then the result should be, in any order:
    # | f_duration |
    # | 'P3Y3M4D'  |
    When executing query:
      """
      DELETE VERTEX "test";
      DELETE EDGE edge_duration "test_src"->"test_dst";
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON tag_duration "test" YIELD vertex as node;
      """
    Then the result should be, in any order, with relax comparison:
      | node |
    When executing query:
      """
      FETCH PROP ON edge_duration "test_src"->"test_dst" YIELD edge as e;
      """
    Then the result should be, in any order, with relax comparison:
      | e |
    And drop the used space
