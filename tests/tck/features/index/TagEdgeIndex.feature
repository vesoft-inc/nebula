Feature: tag and edge index tests from pytest

  Background:
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
      CREATE EDGE edge_1(col1 string, col2 int, col3 double, col4 timestamp);
      INSERT VERTEX
        tag_1(col1, col2, col3, col4)
      VALUES
        '101':('Tom', 18, 35.4, `timestamp`('2010-09-01T08:00:00')),
        '102':('Jerry', 22, 38.4, `timestamp`('2011-09-01T08:00:00')),
        '103':('Bob', 19, 36.4, `timestamp`('2010-09-01T12:00:00'));
      INSERT EDGE
        edge_1(col1, col2, col3, col4)
      VALUES
        '101'->'102':('Red', 81, 45.3, `timestamp`('2010-09-01T08:00:00')),
        '102'->'103':('Yellow', 22, 423.8, `timestamp`('2011-09-01T08:00:00')),
        '103'->'101':('Blue', 91, 43.1, `timestamp`('2010-09-01T12:00:00'));
      """

  Scenario: test tag index from pytest
    # Single Tag Single Field
    When executing query:
      """
      CREATE TAG INDEX single_tag_index ON tag_1(col2);
      """
    Then the execution should be successful
    # Duplicate Index
    When executing query:
      """
      CREATE TAG INDEX duplicate_tag_index_1 ON tag_1(col2)
      """
    Then an ExecutionError should be raised at runtime.
    # Tag not exist
    When executing query:
      """
      CREATE TAG INDEX single_person_index ON student(name)
      """
    Then an ExecutionError should be raised at runtime.
    # Property not exist
    When executing query:
      """
      CREATE TAG INDEX single_tag_index ON tag_1(col5)
      """
    Then an ExecutionError should be raised at runtime.
    # Property is empty
    When executing query:
      """
      CREATE TAG INDEX single_tag_index ON tag_1()
      """
    Then an ExecutionError should be raised at runtime.
    # Single Tag Multi Field
    When executing query:
      """
      CREATE TAG INDEX multi_tag_index ON tag_1(col2, col3)
      """
    Then the execution should be successful
    # Duplicate Index
    When executing query:
      """
      CREATE TAG INDEX duplicate_person_index ON tag_1(col2, col3)
      """
    Then an ExecutionError should be raised at runtime.
    # Duplicate Field
    When executing query:
      """
      CREATE TAG INDEX duplicate_index ON tag_1(col2, col2)
      """
    Then an ExecutionError should be raised at runtime.
    When executing query:
      """
      CREATE TAG INDEX disorder_tag_index ON tag_1(col3, col2)
      """
    Then the execution should be successful
    And wait 3 seconds
    When submit a job:
      """
      REBUILD TAG INDEX single_tag_index
      """
    Then wait the job to finish
    When submit a job:
      """
      REBUILD TAG INDEX multi_tag_index
      """
    Then wait the job to finish
    When submit a job:
      """
      REBUILD TAG INDEX disorder_tag_index
      """
    Then wait the job to finish
    When executing query:
      """
      REBUILD TAG INDEX non_existent_tag_index
      """
    Then a SemanticError should be raised at runtime.
    When executing query:
      """
      SHOW TAG INDEX STATUS
      """
    Then the result should contain:
      | Name                 | Index Status |
      | 'single_tag_index'   | 'FINISHED'   |
      | 'multi_tag_index'    | 'FINISHED'   |
      | 'disorder_tag_index' | 'FINISHED'   |
    When executing query:
      """
      LOOKUP ON tag_1 WHERE tag_1.col2 == 18 YIELD tag_1.col1
      """
    Then the result should be, in any order:
      | VertexID | tag_1.col1 |
      | '101'    | 'Tom'      |
    When executing query:
      """
      LOOKUP ON tag_1 WHERE tag_1.col3 > 35.7 YIELD tag_1.col1
      """
    Then the result should be, in any order:
      | VertexID | tag_1.col1 |
      | '102'    | 'Jerry'    |
      | '103'    | 'Bob'      |
    When executing query:
      """
      LOOKUP ON tag_1 WHERE tag_1.col2 > 18 AND tag_1.col3 < 37.2 YIELD tag_1.col1
      """
    Then the result should be, in any order:
      | VertexID | tag_1.col1 |
      | '103'    | 'Bob'      |
    When executing query:
      """
      DESC TAG INDEX single_tag_index
      """
    Then the result should be, in any order:
      | Field  | Type    |
      | 'col2' | 'int64' |
    When executing query:
      """
      DESC TAG INDEX multi_tag_index
      """
    Then the result should be, in any order:
      | Field  | Type     |
      | 'col2' | 'int64'  |
      | 'col3' | 'double' |
    When executing query:
      """
      DESC TAG INDEX disorder_tag_index
      """
    Then the result should be, in any order:
      | Field  | Type     |
      | 'col2' | 'int64'  |
      | 'col3' | 'double' |
    When executing query:
      """
      DESC TAG INDEX non_existent_tag_index
      """
    Then an ExecutionError should be raised at runtime.
    When executing query:
      """
      SHOW CREATE TAG INDEX single_tag_index
      """
    Then the result should be, in any order:
      | Tag Index Name     | Create Tag Index                                               |
      | 'single_tag_index' | 'CREATE TAG INDEX `single_tag_index` ON `tag_1` (\n `col2`\n)' |
    When executing query:
      """
      SHOW CREATE TAG INDEX multi_tag_index
      """
    Then the result should be, in any order:
      | Tag Index Name    | Create Tag Index                                                        |
      | 'multi_tag_index' | 'CREATE TAG INDEX `multi_tag_index` ON `tag_1` (\n `col2`,\n `col3`\n)' |
    When executing query:
      """
      DROP TAG INDEX multi_tag_index
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX `multi_tag_index` ON `tag_1` (
       `col2`,
       `col3`
      )
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG INDEX disorder_tag_index
      """
    Then the result should be, in any order:
      | Tag Index Name       | Create Tag Index                                                           |
      | 'disorder_tag_index' | 'CREATE TAG INDEX `disorder_tag_index` ON `tag_1` (\n `col3`,\n `col2`\n)' |
    When executing query:
      """
      DROP TAG INDEX disorder_tag_index
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX `disorder_tag_index` ON `tag_1` (
       `col3`,
       `col2`
      )
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG INDEX non_existent_tag_index
      """
    Then an ExecutionError should be raised at runtime.
    When executing query:
      """
      SHOW CREATE EDGE INDEX disorder_tag_index
      """
    Then an ExecutionError should be raised at runtime.
    When executing query:
      """
      SHOW TAG INDEXES
      """
    Then the result should contain:
      | Index Name           | By Tag  | Columns          |
      | "single_tag_index"   | "tag_1" | ["col2"]         |
      | "multi_tag_index"    | "tag_1" | ["col2", "col3"] |
      | "disorder_tag_index" | "tag_1" | ["col3", "col2"] |
    When executing query:
      """
      SHOW TAG INDEXES BY tag_1
      """
    Then the result should contain:
      | Index Name           | Columns          |
      | "single_tag_index"   | ["col2"]         |
      | "multi_tag_index"    | ["col2", "col3"] |
      | "disorder_tag_index" | ["col3", "col2"] |
    When executing query:
      """
      DROP TAG INDEX single_tag_index
      """
    Then the execution should be successful
    When executing query:
      """
      DESC TAG INDEX single_tag_index
      """
    Then an ExecutionError should be raised at runtime.
    When executing query:
      """
      DROP TAG INDEX multi_tag_index
      """
    Then the execution should be successful
    When executing query:
      """
      DESC TAG INDEX multi_tag_index
      """
    Then an ExecutionError should be raised at runtime.
    When executing query:
      """
      DROP TAG INDEX disorder_tag_index
      """
    Then the execution should be successful
    When executing query:
      """
      DESC TAG INDEX disorder_tag_index
      """
    Then an ExecutionError should be raised at runtime.
    When executing query:
      """
      DROP TAG INDEX non_existent_tag_index
      """
    Then an ExecutionError should be raised at runtime.
    And drop the used space

  Scenario: test edge index from pytest
    # Single Tag Single Field
    When executing query:
      """
      CREATE EDGE INDEX single_edge_index ON edge_1(col2)
      """
    Then the execution should be successful
    # Duplicate Index
    When executing query:
      """
      CREATE EDGE INDEX duplicate_edge_index ON edge_1(col2)
      """
    Then an ExecutionError should be raised at runtime.
    # Edge not exist
    When executing query:
      """
      CREATE EDGE INDEX single_edge_index ON edge_1_ship(name)
      """
    Then an ExecutionError should be raised at runtime.
    # Property not exist
    When executing query:
      """
      CREATE EDGE INDEX single_edge_index ON edge_1(startTime)
      """
    Then an ExecutionError should be raised at runtime.
    # Property is empty
    When executing query:
      """
      CREATE EDGE INDEX single_edge_index ON edge_1()
      """
    Then an ExecutionError should be raised at runtime.
    # Single Edge Multi Field
    When executing query:
      """
      CREATE EDGE INDEX multi_edge_index ON edge_1(col2, col3)
      """
    Then the execution should be successful
    # Duplicate Index
    When executing query:
      """
      CREATE EDGE INDEX duplicate_edge_index ON edge_1(col2, col3)
      """
    Then an ExecutionError should be raised at runtime.
    # Duplicate Field
    When executing query:
      """
      CREATE EDGE INDEX duplicate_index ON edge_1(col2, col2)
      """
    Then an ExecutionError should be raised at runtime.
    When executing query:
      """
      CREATE EDGE INDEX disorder_edge_index ON edge_1(col3, col2)
      """
    Then the execution should be successful
    And wait 3 seconds
    # Rebuild Edge Index
    When submit a job:
      """
      REBUILD EDGE INDEX single_edge_index
      """
    Then wait the job to finish
    When submit a job:
      """
      REBUILD EDGE INDEX multi_edge_index
      """
    Then wait the job to finish
    When submit a job:
      """
      REBUILD EDGE INDEX disorder_edge_index
      """
    Then wait the job to finish
    When executing query:
      """
      REBUILD EDGE INDEX non_existent_edge_index
      """
    Then a SemanticError should be raised at runtime.
    When executing query:
      """
      SHOW EDGE INDEX STATUS
      """
    Then the result should contain:
      | Name                  | Index Status |
      | 'single_edge_index'   | 'FINISHED'   |
      | 'multi_edge_index'    | 'FINISHED'   |
      | 'disorder_edge_index' | 'FINISHED'   |
    # Lookup
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col2 == 22 YIELD edge_1.col2
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col2 |
      | '102'  | '103'  | 0       | 22          |
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col3 > 43.4 YIELD edge_1.col1
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1 |
      | '102'  | '103'  | 0       | 'Yellow'    |
      | '101'  | '102'  | 0       | 'Red'       |
    When executing query:
      """
      LOOKUP ON edge_1 WHERE edge_1.col2 > 45 AND edge_1.col3 < 44.3 YIELD edge_1.col1
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | edge_1.col1 |
      | '103'  | '101'  | 0       | 'Blue'      |
    # Describe Edge Index
    When executing query:
      """
      DESC EDGE INDEX single_edge_index
      """
    Then the result should be, in any order:
      | Field  | Type    |
      | 'col2' | 'int64' |
    When executing query:
      """
      DESC EDGE INDEX multi_edge_index
      """
    Then the result should be, in any order:
      | Field  | Type     |
      | 'col2' | 'int64'  |
      | 'col3' | 'double' |
    When executing query:
      """
      DESC EDGE INDEX disorder_edge_index
      """
    Then the result should be, in any order:
      | Field  | Type     |
      | 'col2' | 'int64'  |
      | 'col3' | 'double' |
    When executing query:
      """
      DESC EDGE INDEX non_existent_edge_index
      """
    Then an ExecutionError should be raised at runtime.
    # Show Create Edge Index
    When executing query:
      """
      SHOW CREATE EDGE INDEX single_edge_index
      """
    Then the result should be, in any order:
      | Edge Index Name     | Create Edge Index                                                 |
      | 'single_edge_index' | 'CREATE EDGE INDEX `single_edge_index` ON `edge_1` (\n `col2`\n)' |
    When executing query:
      """
      SHOW CREATE TAG INDEX single_edge_index
      """
    Then an ExecutionError should be raised at runtime.
    When executing query:
      """
      SHOW CREATE EDGE INDEX multi_edge_index
      """
    Then the result should be, in any order:
      | Edge Index Name    | Create Edge Index                                                          |
      | 'multi_edge_index' | 'CREATE EDGE INDEX `multi_edge_index` ON `edge_1` (\n `col2`,\n `col3`\n)' |
    # Check if show create edge index works well
    When executing query:
      """
      DROP EDGE INDEX multi_edge_index
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX `multi_edge_index` ON `edge_1` (
       `col2`,
       `col3`
      )
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE EDGE INDEX disorder_edge_index
      """
    Then the result should be, in any order:
      | Edge Index Name       | Create Edge Index                                                             |
      | 'disorder_edge_index' | 'CREATE EDGE INDEX `disorder_edge_index` ON `edge_1` (\n `col3`,\n `col2`\n)' |
    # Check if show create edge index works well
    When executing query:
      """
      DROP EDGE INDEX disorder_edge_index
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX `disorder_edge_index` ON `edge_1` (
       `col3`,
       `col2`
      )
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE EDGE INDEX non_existent_edge_index
      """
    Then an ExecutionError should be raised at runtime.
    # Show Edge Indexes
    When executing query:
      """
      SHOW EDGE INDEXES
      """
    Then the result should contain:
      | Index Name            | By Edge  | Columns          |
      | "single_edge_index"   | "edge_1" | ["col2"]         |
      | "multi_edge_index"    | "edge_1" | ["col2", "col3"] |
      | "disorder_edge_index" | "edge_1" | ["col3", "col2"] |
    When executing query:
      """
      SHOW EDGE INDEXES BY edge_1
      """
    Then the result should contain:
      | Index Name            | Columns          |
      | "single_edge_index"   | ["col2"]         |
      | "multi_edge_index"    | ["col2", "col3"] |
      | "disorder_edge_index" | ["col3", "col2"] |
    # Drop Edge Index
    When executing query:
      """
      DROP EDGE INDEX single_edge_index
      """
    Then the execution should be successful
    # Check if the index is truly dropped
    When executing query:
      """
      DESC EDGE INDEX single_edge_index
      """
    Then an ExecutionError should be raised at runtime.
    # Drop Edge Index
    When executing query:
      """
      DROP EDGE INDEX multi_edge_index
      """
    Then the execution should be successful
    # Check if the index is truly dropped
    When executing query:
      """
      DESC EDGE INDEX multi_edge_index
      """
    Then an ExecutionError should be raised at runtime.
    # Drop Edge Index
    When executing query:
      """
      DROP EDGE INDEX disorder_edge_index
      """
    Then the execution should be successful
    # Check if the index is truly dropped
    When executing query:
      """
      DESC EDGE INDEX disorder_edge_index
      """
    Then an ExecutionError should be raised at runtime.
    When executing query:
      """
      DROP EDGE INDEX non_existent_edge_index
      """
    Then an ExecutionError should be raised at runtime.
    And drop the used space
