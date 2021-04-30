# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Insert vertex and edge with if not exists

  Scenario: insert vertex and edge if not exists test
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS person(name string, age int);
      CREATE EDGE IF NOT EXISTS like(likeness int);
      CREATE TAG IF NOT EXISTS student(grade string, number int);
      """
    # insert vertex succeeded
    When try to execute query:
      """
      INSERT VERTEX person(name, age) VALUES "Tom":("Tom", 22)
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX IF NOT EXISTS
        person(name, age)
      VALUES
        "Conan":("Conan", 10),
        "Yao":("Yao", 11),
        "Conan":("Conan", 11);
      """
    Then the execution should be successful
    # check vertex result with fetch
    When executing query:
      """
      FETCH PROP ON person "Conan" YIELD person.age as age
      """
    Then the result should be, in any order, with relax comparison:
      | VertexID | age |
      | "Conan"  | 10  |
    # insert vertex if not exists
    When executing query:
      """
      INSERT VERTEX IF NOT EXISTS person(name, age) VALUES "Conan":("Conan", 20)
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON person "Conan" YIELD person.age as age
      """
    Then the result should be, in any order, with relax comparison:
      | VertexID | age |
      | "Conan"  | 10  |
    # insert same vertex
    When executing query:
      """
      INSERT VERTEX person(name, age) VALUES "Conan":("Conan", 40)
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON person "Conan" YIELD person.age as age
      """
    Then the result should be, in any order, with relax comparison:
      | VertexID | age |
      | "Conan"  | 40  |
    # insert edge
    When try to execute query:
      """
      INSERT EDGE like(likeness) VALUES "Tom"->"Conan":(87)
      """
    Then the execution should be successful
    # check edge result with fetch
    When executing query:
      """
      FETCH PROP ON like "Tom"->"Conan" YIELD like.likeness
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.likeness |
      | 'Tom'     | 'Conan'   | 0          | 87            |
    # insert edge if not exists
    When executing query:
      """
      INSERT EDGE IF NOT EXISTS like(likeness) VALUES "Tom"->"Conan":(10)
      """
    Then the execution should be successful
    # check edge result with fetch
    When executing query:
      """
      FETCH PROP ON like "Tom"->"Conan" YIELD like.likeness
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.likeness |
      | 'Tom'     | 'Conan'   | 0          | 87            |
    # insert same edge
    When executing query:
      """
      INSERT EDGE like(likeness) VALUES "Tom"->"Conan":(100)
      """
    Then the execution should be successful
    # check edge result with fetch
    When executing query:
      """
      FETCH PROP ON like "Tom"->"Conan" YIELD like.likeness
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.likeness |
      | 'Tom'     | 'Conan'   | 0          | 100           |
    # check result with go
    When executing query:
      """
      GO FROM "Tom" over like YIELD like.likeness as like, like._src as src, like._dst as dst
      """
    Then the result should be, in any order:
      | like | src   | dst     |
      | 100  | "Tom" | "Conan" |
    # insert multi vertex multi tags
    When try to execute query:
      """
      INSERT VERTEX IF NOT EXISTS
        person(name, age),
        student(grade, number)
      VALUES
        "Tom":("Tom", 8, "three", 20190901008),
        "Conan":("Conan", 9, "four", 20180901003),
        "Peter":("Peter", 20, "five", 2019020113);
      """
    Then the execution should be successful
    # check same vertex different tag
    When executing query:
      """
      FETCH PROP ON person "Conan" YIELD person.age as age
      """
    Then the result should be, in any order, with relax comparison:
      | VertexID | age |
      | "Conan"  | 40  |
    When executing query:
      """
      FETCH PROP ON student "Conan" YIELD student.number as number
      """
    Then the result should be, in any order, with relax comparison:
      | VertexID | number      |
      | "Conan"  | 20180901003 |
    # insert multi edges if not exists
    When executing query:
      """
      INSERT EDGE IF NOT EXISTS
        like(likeness)
      VALUES
        "Tom"->"Conan":(81),
        "Tom"->"Peter":(83);
      """
    Then the execution should be successful
    # check edge result with fetch
    When executing query:
      """
      FETCH PROP ON like "Tom"->"Conan" YIELD like.likeness
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.likeness |
      | 'Tom'     | 'Conan'   | 0          | 100           |
    When executing query:
      """
      FETCH PROP ON like "Tom"->"Peter" YIELD like.likeness
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.likeness |
      | 'Tom'     | 'Peter'   | 0          | 83            |
    And drop the used space

  Scenario: insert vertex and edge with default propNames
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS person(name string, age int);
      CREATE EDGE IF NOT EXISTS like(likeness int);
      CREATE TAG IF NOT EXISTS student(grade string, number int);
      """
    # insert vertex succeeded
    When try to execute query:
      """
      INSERT VERTEX person(name, age) VALUES "Tom":("Tom", 1)
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX person VALUES "Tom":("Tom", 2)
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX person() VALUES "Tom":("Tom", 2)
      """
    Then a SemanticError should be raised at runtime: Column count doesn't match value count.
    When executing query:
      """
      INSERT VERTEX person VALUES "Tom":("Tom")
      """
    Then a SemanticError should be raised at runtime: Column count doesn't match value count.
    When executing query:
      """
      INSERT VERTEX person VALUES "Tom":(2, "Tom")
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When executing query:
      """
      INSERT VERTEX person VALUES "Tom":("Tom", "2")
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When executing query:
      """
      INSERT VERTEX person VALUES "Tom":("Tom", 2, "3")
      """
    Then a SemanticError should be raised at runtime: Column count doesn't match value count.
    When executing query:
      """
      INSERT VERTEX IF NOT EXISTS
        person
      VALUES
        "Conan":("Conan", 10),
        "Yao":("Yao", 11),
        "Conan":("Conan", 11),
        "Tom":("Tom", 3);
      """
    Then the execution should be successful
    # check vertex result with fetch
    When executing query:
      """
      FETCH PROP ON person "Tom" YIELD person.age as age
      """
    Then the result should be, in any order, with relax comparison:
      | VertexID | age |
      | "Tom"    | 2   |
    # check insert edge with default props
    When try to execute query:
      """
      INSERT EDGE like(likeness) VALUES "Tom"->"Conan":(100)
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT EDGE like VALUES "Tom"->"Conan":(200)
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT EDGE like VALUES "Tom"->"Conan":("200")
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When executing query:
      """
      INSERT EDGE like() VALUES "Tom"->"Conan":(200)
      """
    Then a SemanticError should be raised at runtime: Column count doesn't match value count.
    When executing query:
      """
      INSERT EDGE like VALUES "Tom"->"Conan":(200, 2)
      """
    Then a SemanticError should be raised at runtime: Column count doesn't match value count.
    When executing query:
      """
      INSERT EDGE IF NOT EXISTS like VALUES "Tom"->"Conan":(300)
      """
    Then the execution should be successful
    # check edge result with fetch
    When executing query:
      """
      FETCH PROP ON like "Tom"->"Conan" YIELD like.likeness
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.likeness |
      | 'Tom'     | 'Conan'   | 0          | 200           |
    And drop the used space
