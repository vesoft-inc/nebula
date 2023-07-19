# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Insert vertex and edge with if not exists

  Scenario: insert vertex and edge if not exists test
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
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
      | age |
      | 10  |
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
      | age |
      | 10  |
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
      | age |
      | 40  |
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
      | like.likeness |
      | 87            |
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
      | like.likeness |
      | 87            |
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
      | like.likeness |
      | 100           |
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
      | age |
      | 40  |
    When executing query:
      """
      FETCH PROP ON student "Conan" YIELD student.number as number
      """
    Then the result should be, in any order, with relax comparison:
      | number      |
      | 20180901003 |
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
      | like.likeness |
      | 100           |
    When executing query:
      """
      FETCH PROP ON like "Tom"->"Peter" YIELD like.likeness
      """
    Then the result should be, in any order:
      | like.likeness |
      | 83            |
    And drop the used space

  Scenario: insert vertex and edge with default propNames
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
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
      | age |
      | 2   |
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
      | like.likeness |
      | 200           |
    And drop the used space

  Scenario: vertices index and data consistency check
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS student(name string, age int);
      CREATE TAG INDEX index_s_age on student(age);
      """
    And wait 6 seconds
    When try to execute query:
      """
      INSERT VERTEX
        student(name, age)
      VALUES
        "zhang":("zhang", 19),
        "zhang":("zhang", 29),
        "zhang":("zhang", 39),
        "wang":("wang", 18),
        "li":("li", 16),
        "wang":("wang", 38);
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON student WHERE student.age < 30 YIELD student.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 16  |
    When executing query:
      """
      LOOKUP ON student WHERE student.age > 30 YIELD student.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 39  |
      | 38  |
    When executing query:
      """
      LOOKUP ON student WHERE student.age < 30 YIELD student.name AS name, student.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | name | age |
      | "li" | 16  |
    When executing query:
      """
      LOOKUP ON student WHERE student.age > 30 YIELD student.name as name, student.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | name    | age |
      | "zhang" | 39  |
      | "wang"  | 38  |
    When executing query:
      """
      FETCH PROP ON student "zhang", "wang", "li" YIELD student.name as name, student.age as age
      """
    Then the result should be, in any order, with relax comparison:
      | name    | age |
      | "zhang" | 39  |
      | "wang"  | 38  |
      | "li"    | 16  |
    When try to execute query:
      """
      DELETE TAG student FROM "zhang", "wang", "li";
      """
    Then the execution should be successful
    When try to execute query:
      """
      INSERT VERTEX IF NOT EXISTS
        student(name, age)
      VALUES
        "zhao":("zhao", 19),
        "zhao":("zhao", 29),
        "zhao":("zhao", 39),
        "qian":("qian", 18),
        "sun":("sun", 16),
        "qian":("qian", 38),
        "chen":("chen", 40),
        "chen":("chen", 35);
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON student WHERE student.age < 30 YIELD student.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 19  |
      | 18  |
      | 16  |
    When executing query:
      """
      LOOKUP ON student WHERE student.age > 30 YIELD student.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 40  |
    When executing query:
      """
      LOOKUP ON student WHERE student.age < 30 YIELD student.name AS name, student.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | name   | age |
      | "zhao" | 19  |
      | "qian" | 18  |
      | "sun"  | 16  |
    When executing query:
      """
      LOOKUP ON student WHERE student.age > 30 YIELD student.name as name, student.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | name   | age |
      | "chen" | 40  |
    When executing query:
      """
      FETCH PROP ON student "zhao", "qian", "sun", "chen" YIELD student.name as name, student.age as age
      """
    Then the result should be, in any order, with relax comparison:
      | name   | age |
      | "zhao" | 19  |
      | "qian" | 18  |
      | "sun"  | 16  |
      | "chen" | 40  |
    And drop the used space

  Scenario: edge index and data consistency check
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS student(name string, age int);
      CREATE EDGE IF NOT EXISTS like(likeness int, t1 int);
      CREATE EDGE INDEX index_l_likeness on like(likeness);
      """
    And wait 6 seconds
    When try to execute query:
      """
      INSERT VERTEX
        student(name, age)
      VALUES
        "zhang":("zhang", 19),
        "wang":("wang", 18),
        "li":("li", 16);
      INSERT EDGE
        like(likeness, t1)
      VALUES
        "zhang"->"wang":(19, 19),
        "zhang"->"li":(42, 42),
        "zhang"->"li":(20, 20),
        "zhang"->"wang":(39, 39),
        "wang"->"li":(18, 18),
        "wang"->"zhang":(41, 41);
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON like WHERE like.likeness < 30 YIELD src(edge) as src, dst(edge) as dst, like.likeness as likeness
      """
    Then the result should be, in any order, with relax comparison:
      | src     | dst  | likeness |
      | "zhang" | "li" | 20       |
      | "wang"  | "li" | 18       |
    When executing query:
      """
      LOOKUP ON like WHERE like.likeness > 30 YIELD src(edge) as src, dst(edge) as dst, like.likeness as likeness
      """
    Then the result should be, in any order, with relax comparison:
      | src     | dst     | likeness |
      | "zhang" | "wang"  | 39       |
      | "wang"  | "zhang" | 41       |
    When executing query:
      """
      LOOKUP ON like WHERE like.likeness < 30 YIELD src(edge) as src, dst(edge) as dst, like.likeness as likeness, like.t1 as t1
      """
    Then the result should be, in any order, with relax comparison:
      | src     | dst  | likeness | t1 |
      | "zhang" | "li" | 20       | 20 |
      | "wang"  | "li" | 18       | 18 |
    When executing query:
      """
      LOOKUP ON like WHERE like.likeness > 30 YIELD src(edge) as src, dst(edge) as dst, like.likeness as likeness, like.t1 as t1
      """
    Then the result should be, in any order, with relax comparison:
      | src     | dst     | likeness | t1 |
      | "zhang" | "wang"  | 39       | 39 |
      | "wang"  | "zhang" | 41       | 41 |
    When executing query:
      """
      FETCH PROP ON like "zhang"->"wang", "zhang"->"li", "wang"->"li", "wang"->"zhang" YIELD src(edge) as src, dst(edge) as dst, like.likeness as likeness
      """
    Then the result should be, in any order, with relax comparison:
      | src     | dst     | likeness |
      | "zhang" | "wang"  | 39       |
      | "zhang" | "li"    | 20       |
      | "wang"  | "li"    | 18       |
      | "wang"  | "zhang" | 41       |
    When try to execute query:
      """
      DELETE EDGE like "zhang"->"wang", "zhang"->"li", "wang"->"li", "wang"->"zhang";
      """
    Then the execution should be successful
    When try to execute query:
      """
      INSERT EDGE IF NOT EXISTS
        like(likeness, t1)
      VALUES
        "zhang"->"wang":(19, 19),
        "zhang"->"li":(42, 42),
        "zhang"->"li":(20, 20),
        "zhang"->"wang":(39, 39),
        "wang"->"li":(18, 18),
        "wang"->"zhang":(41, 41);
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON like WHERE like.likeness < 30 YIELD src(edge) as src, dst(edge) as dst, like.likeness as likeness
      """
    Then the result should be, in any order, with relax comparison:
      | src     | dst    | likeness |
      | "zhang" | "wang" | 19       |
      | "wang"  | "li"   | 18       |
    When executing query:
      """
      LOOKUP ON like WHERE like.likeness > 30 YIELD src(edge) as src, dst(edge) as dst, like.likeness as likeness
      """
    Then the result should be, in any order, with relax comparison:
      | src     | dst     | likeness |
      | "zhang" | "li"    | 42       |
      | "wang"  | "zhang" | 41       |
    When executing query:
      """
      LOOKUP ON like WHERE like.likeness < 30 YIELD src(edge) as src, dst(edge) as dst, like.likeness as likeness, like.t1 as t1
      """
    Then the result should be, in any order, with relax comparison:
      | src     | dst    | likeness | t1 |
      | "zhang" | "wang" | 19       | 19 |
      | "wang"  | "li"   | 18       | 18 |
    When executing query:
      """
      LOOKUP ON like WHERE like.likeness > 30 YIELD src(edge) as src, dst(edge) as dst, like.likeness as likeness, like.t1 as t1
      """
    Then the result should be, in any order, with relax comparison:
      | src     | dst     | likeness | t1 |
      | "zhang" | "li"    | 42       | 42 |
      | "wang"  | "zhang" | 41       | 41 |
    When executing query:
      """
      FETCH PROP ON like "zhang"->"wang", "zhang"->"li", "wang"->"li", "wang"->"zhang" YIELD src(edge) as src, dst(edge) as dst, like.likeness as likeness
      """
    Then the result should be, in any order, with relax comparison:
      | src     | dst     | likeness |
      | "zhang" | "wang"  | 19       |
      | "zhang" | "li"    | 42       |
      | "wang"  | "li"    | 18       |
      | "wang"  | "zhang" | 41       |
    And drop the used space
