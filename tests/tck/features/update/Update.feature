# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Update string vid of vertex and edge

  Background: Prepare space
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS course(name string, credits int);
      CREATE TAG IF NOT EXISTS building(name string);
      CREATE TAG IF NOT EXISTS student(name string, age int, gender string);
      CREATE TAG IF NOT EXISTS student_default(
        name string NOT NULL,
        age int NOT NULL,
        gender string DEFAULT "one",
        birthday int DEFAULT 2010
      );
      CREATE EDGE IF NOT EXISTS like(likeness double);
      CREATE EDGE IF NOT EXISTS select(grade int, year int);
      CREATE EDGE IF NOT EXISTS select_default(grade int NOT NULL,year TIMESTAMP DEFAULT 1546308000);
      """
    And having executed:
      """
      INSERT VERTEX
        student(name, age, gender)
      VALUES
        "200":("Monica", 16, "female"),
        "201":("Mike", 18, "male"),
        "202":("Jane", 17, "female");
      INSERT VERTEX
        course(name, credits),
        building(name)
      VALUES
        "101":("Math", 3, "No5"),
        "102":("English", 6, "No11");
      INSERT VERTEX course(name, credits) VALUES "103":("CS", 5);
      INSERT EDGE
        select(grade, year)
      VALUES
        "200"->"101"@0:(5, 2018),
        "200" -> "102"@0:(3, 2018),
        "201" -> "102"@0:(3, 2019),
        "202" -> "102"@0:(3, 2019);
      INSERT EDGE
        like(likeness)
      VALUES
        "200" -> "201"@0:(92.5),
        "201" -> "200"@0:(85.6),
        "201" -> "202"@0:(93.2);
      """

  Scenario: update and upsert test with 1.0 syntax
    When executing query:
      """
      UPDATE VERTEX "101"
      SET course.credits = $^.course.credits + 1;
      """
    Then the execution should be successful
    When executing query:
      """
      UPDATE VERTEX "101"
      SET course.credits = $^.course.credits + 1
      WHEN $^.course.name == "English" AND $^.course.credits > 2
      """
    Then the execution should be successful
    When executing query:
      """
      UPDATE VERTEX "101"
      SET course.credits = $^.course.credits + 1
      WHEN $^.course.name == "Math" AND $^.course.credits > 2
      """
    Then the execution should be successful
    When executing query:
      """
      UPDATE VERTEX "101"
      SET course.credits = $^.course.credits + 1
      YIELD $^.course.name AS Name, $^.course.credits AS Credits
      """
    Then the result should be, in any order:
      | Name   | Credits |
      | 'Math' | 6       |
    When executing query:
      """
      UPDATE VERTEX "101"
      SET course.credits = $^.course.credits + 1
      WHEN $^.course.name == "Math" AND $^.course.credits > 2
      YIELD $^.course.name AS Name, $^.course.credits AS Credits
      """
    Then the result should be, in any order:
      | Name   | Credits |
      | 'Math' | 7       |
    When executing query:
      """
      UPDATE VERTEX "101"
      SET course.credits = $^.course.credits + 1
      WHEN $^.course.name == "notexist" AND $^.course.credits > 2
      YIELD $^.course.name AS Name, $^.course.credits AS Credits
      """
    Then the result should be, in any order:
      | Name   | Credits |
      | 'Math' | 7       |
    When executing query:
      """
      FETCH PROP ON select "200"->"101"@0 YIELD select.grade, select.year
      """
    Then the result should be, in any order:
      | select._src | select._dst | select._rank | select.grade | select.year |
      | "200"       | "101"       | 0            | 5            | 2018        |
    # update edge succeeded
    When executing query:
      """
      UPDATE EDGE "200"->"101"@0 OF select
      SET grade = select.grade + 1, select.year = 2000;
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON select "200"->"101"@0 YIELD select.grade, select.year
      """
    Then the result should be, in any order:
      | select._src | select._dst | select._rank | select.grade | select.year |
      | "200"       | "101"       | 0            | 6            | 2000        |
    When executing query:
      """
      GO FROM "101" OVER select REVERSELY YIELD select.grade, select.year
      """
    Then the result should be, in any order:
      | select.grade | select.year |
      | 6            | 2000        |
    # filter out, 2.0 storage not support update edge can use vertex
    When executing query:
      """
      UPDATE EDGE "200"->"101"@0 OF select
      SET grade = select.grade + 1, year = 2000
      WHEN select.grade > 1024 AND $^.student.age > 15;
      """
    Then a SemanticError should be raised at runtime: Has wrong expr in `((select.grade>1024) AND ($^.student.age>15))'
    # 2.0 test, filter out
    When executing query:
      """
      UPDATE EDGE "200"->"101"@0 OF select
      SET grade = select.grade + 1, year = 2000
      WHEN select.grade > 1024
      """
    Then the execution should be successful
    # set filter, 2.0 storage not support update edge can use vertex
    When executing query:
      """
      UPDATE EDGE "200"->"101"@0 OF select
      SET grade = select.grade + 1, year = 2000
      WHEN select.grade > 4 AND $^.student.age > 15;
      """
    Then a SemanticError should be raised at runtime: Has wrong expr in `((select.grade>4) AND ($^.student.age>15))'
    # set filter
    When executing query:
      """
      UPDATE EDGE "200"->"101"@0 OF select
      SET grade = select.grade + 1, year = 2000 WHEN select.grade > 4
      """
    Then the execution should be successful
    # set yield, 2.0 storage not support update edge can use vertex
    When executing query:
      """
      UPDATE EDGE "200"->"101"@0 OF select
      SET grade = select.grade + 1, year = 2018
      YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year
      """
    Then a SemanticError should be raised at runtime:  Has wrong expr in `$^.student.name'
    # set yield
    When executing query:
      """
      UPDATE EDGE "200"->"101"@0 OF select
      SET grade = select.grade + 1, year = 2018
      YIELD select.grade AS Grade, select.year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year |
      | 8     | 2018 |
    # set filter and yield, 2.0 storage not support update edge can use vertex
    When executing query:
      """
      UPDATE EDGE "200"->"101"@0 OF select
      SET grade = select.grade + 1, year = 2019
      WHEN select.grade > 4 AND $^.student.age > 15
      YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year
      """
    Then a SemanticError should be raised at runtime: Has wrong expr in `((select.grade>4) AND ($^.student.age>15))'
    # filter and yield
    When executing query:
      """
      UPDATE EDGE "200"->"101"@0 OF select
      SET grade = select.grade + 1, year = 2019
      WHEN select.grade > 4
      YIELD select.grade AS Grade, select.year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year |
      | 9     | 2019 |
    # set filter out and yield, 2.0 storage not support update edge can use vertex
    When executing query:
      """
      UPDATE EDGE "200"->"101"@0 OF select
      SET grade = select.grade + 1, year = 2019
      WHEN select.grade > 233333333333 AND $^.student.age > 15
      YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year
      """
    Then a SemanticError should be raised at runtime: Has wrong expr in `((select.grade>233333333333) AND ($^.student.age>15))'
    # set filter out and yield
    When executing query:
      """
      UPDATE EDGE "200"->"101"@0 OF select
      SET grade = select.grade + 1, year = 2019
      WHEN select.grade > 233333333333
      YIELD select.grade AS Grade, select.year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year |
      | 9     | 2019 |
    # update vertex: the item is TagName.PropName = Expression in SET clause
    When executing query:
      """
      UPDATE VERTEX "101"
      SET course.credits = $^.course.credits + 1, course.name = "No9"
      """
    Then the execution should be successful
    # the $$.TagName.PropName expressions are not allowed in any update sentence
    When executing query:
      """
      UPDATE VERTEX "101"
      SET course.credits = $$.course.credits + 1
      WHEN $$.course.name == "Math" AND $^.course.credits > $$.course.credits + 1
      YIELD $^.course.name AS Name, $^.course.credits AS Credits, $$.building.name
      """
    Then a SemanticError should be raised at runtime: Has wrong expr in `($$.course.credits+1)'
    When executing query:
      """
      UPDATE VERTEX "101"
      SET course.credits = 1
      WHEN 123
      YIELD $^.course.name AS Name, $^.course.credits AS Credits
      """
    Then a SemanticError should be raised at runtime: `123', expected Boolean, but was `INT'
    When executing query:
      """
      UPDATE VERTEX "101"
      SET course.credits = 1
      WHEN credits
      YIELD $^.course.name AS Name, $^.course.credits AS Credits
      """
    Then a SemanticError should be raised at runtime: `$^.course.credits', expected Boolean, but was `INT'
    When executing query:
      """
      UPSERT VERTEX "101"
      SET course.credits = 1
      WHEN credits
      YIELD $^.course.name AS Name, $^.course.credits AS Credits
      """
    Then a SemanticError should be raised at runtime: `$^.course.credits', expected Boolean, but was `INT'
    When executing query:
      """
      UPSERT VERTEX "101"
      SET course.credits = 1
      WHEN "xyz"
      YIELD $^.course.name AS Name, $^.course.credits AS Credits
      """
    Then a SemanticError should be raised at runtime: `"xyz"', expected Boolean, but was `STRING'
    When executing query:
      """
      UPDATE VERTEX ON course "101"
      SET credits = 1
      WHEN credits
      YIELD $^.course.name AS Name, $^.course.credits AS Credits
      """
    Then a SemanticError should be raised at runtime: `$^.course.credits', expected Boolean, but was `INT'
    When executing query:
      """
      UPSERT VERTEX ON course "101"
      SET credits = 1
      WHEN "xyz"
      YIELD $^.course.name AS Name, $^.course.credits AS Credits
      """
    Then a SemanticError should be raised at runtime: `"xyz"', expected Boolean, but was `STRING'
    # make sure TagName and PropertyName must exist in all clauses
    When executing query:
      """
      UPDATE VERTEX "101"
      SET nonexistentTag.credits = $^.nonexistentTag.credits + 1
      """
    Then a SemanticError should be raised at runtime: No schema found for `nonexistentTag'
    # # update edge: the item is PropName = Expression in SET clause, 1.0 expect failed, but 2.0 is succeeded
    # When executing query:
    # """
    # UPDATE EDGE "200"->"101"@0 OF select
    # SET select.grade = select.grade + 1, select.year = 2019
    # """
    # Then the execution should be successful
    # make sure EdgeName and PropertyName must exist in all clauses
    When executing query:
      """
      UPDATE EDGE "200"->"101"@0 OF select
      SET nonexistentProperty = select.grade + 1, year = 2019
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Edge prop not found.
    # make sure the edge_type must not exist
    When executing query:
      """
      UPDATE EDGE "200"->"101"@0 OF nonexistentEdgeTypeName
      SET grade = nonexistentEdgeTypeName.grade + 1, year = 2019
      """
    Then a SemanticError should be raised at runtime: No schema found for `nonexistentEdgeTypeName'
    When executing query:
      """
      FETCH PROP ON course "103" YIELD course.name, course.credits
      """
    Then the result should be, in any order:
      | VertexID | course.name | course.credits |
      | "103"    | "CS"        | 5              |
    # not allow to handle multi tagid when update
    When executing query:
      """
      UPDATE VERTEX "103"
      SET course.credits = $^.course.credits + 1, name = $^.building.name
      """
    Then a SemanticError should be raised at runtime: Multi schema name: ,course
    When executing query:
      """
      UPDATE VERTEX "103"
      SET course.credits = $^.course.credits + 1
      WHEN $^.course.name == "CS" AND $^.course.credits > 2
      YIELD $^.course.name AS Name, $^.course.credits AS Credits
      """
    Then the result should be, in any order:
      | Name | Credits |
      | 'CS' | 6       |
    # when tag ON vertex not exists, update failed
    When executing query:
      """
      UPDATE VERTEX "104" SET course.credits = $^.course.credits + 1
      WHEN $^.course.name == "CS" AND $^.course.credits > 2
      YIELD $^.course.name AS Name, $^.course.credits AS Credits
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Vertex or edge not found.
    # Insertable failed, "111" is nonexistent, name and age without default value
    When executing query:
      """
      UPSERT VERTEX "111"
      SET student_default.name = "Tom", student_default.age = $^.student_default.age + 8
      YIELD $^.student_default.name AS Name, $^.student_default.age AS Age
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Invalid field value: may be the filed is not NULL or without default value or wrong schema.
    # Insertable failed, "111" is nonexistent, name without default value
    When executing query:
      """
      UPSERT VERTEX "111"
      SET student_default.gender = "two", student_default.age = 10
      YIELD $^.student_default.name AS Name, $^.student_default.gender AS Gender
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Invalid field value: may be the filed is not NULL or without default value or wrong schema.
    # Insertable: vertex "112" ("Lily") --> ("Lily", "one")
    # "112" is nonexistent, gender with default value
    # update student_default.age with string value
    When executing query:
      """
      UPSERT VERTEX "112"
      SET student_default.name = "Lily", student_default.age = "10"
      YIELD $^.student_default.name AS Name, $^.student_default.gender AS Gender
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Invalid data, may be wrong value type.
    # Insertable: vertex "113" ("Jack") --> ("Jack", "Three")
    # 113 is nonexistent, gender with default value,
    # update student_default.age with string value
    When executing query:
      """
      UPSERT VERTEX "113"
      SET student_default.name = "Ann", student_default.age = "10"
      YIELD $^.student_default.name AS Name, $^.student_default.gender AS Gender
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Invalid data, may be wrong value type.
    # Insertable success, "115" is nonexistent, name and age without default value,
    # the filter is always true.
    When executing query:
      """
      UPSERT VERTEX "115"
      SET student_default.name = "Kate", student_default.age = 12
      WHEN $^.student_default.gender == "two"
      YIELD $^.student_default.name AS Name, $^.student_default.age AS Age, $^.student_default.gender AS gender
      """
    Then the result should be, in any order:
      | Name   | Age | gender |
      | 'Kate' | 12  | 'one'  |
    # Order problem
    # Insertable success, "116" is nonexistent, name and age without default value,
    # the filter is always true.
    When executing query:
      """
      UPSERT VERTEX "116"
      SET student_default.name = "Kate",
      student_default.age = $^.student_default.birthday + 1,
      student_default.birthday = $^.student_default.birthday + 1
      WHEN $^.student_default.gender == "two"
      YIELD $^.student_default.name AS Name,
      $^.student_default.age AS Age,
      $^.student_default.gender AS gender,
      $^.student_default.birthday AS birthday
      """
    Then the result should be, in any order:
      | Name   | Age  | gender | birthday |
      | 'Kate' | 2011 | 'one'  | 2011     |
    # Order problem
    # Insertable success, "117" is nonexistent, name and age without default value,
    # the filter is always true.
    When executing query:
      """
      UPSERT VERTEX "117"
      SET student_default.birthday = $^.student_default.birthday + 1,
      student_default.name = "Kate",
      student_default.age = $^.student_default.birthday + 1
      YIELD $^.student_default.name AS Name,
      $^.student_default.age AS Age,
      $^.student_default.gender AS gender,
      $^.student_default.birthday AS birthday
      """
    Then the result should be, in any order:
      | Name   | Age  | gender | birthday |
      | 'Kate' | 2012 | 'one'  | 2011     |
    When executing query:
      """
      FETCH PROP ON select "200"->"101"@0 YIELD select.grade, select.year
      """
    Then the result should be, in any order:
      | select._src | select._dst | select._rank | select.grade | select.year |
      | "200"       | "101"       | 0            | 9            | 2019        |
    # Insertable, upsert when edge exists, 2.0 storage not support update edge with vertex prop
    When executing query:
      """
      UPSERT EDGE "201" -> "101"@0 OF select
      SET grade = 3, year = 2019
      WHEN $^.student.age > 15 AND $^.student.gender == "male"
      YIELD select.grade AS Grade, select.year AS Year
      """
    Then a SemanticError should be raised at runtime: Has wrong expr in `(($^.student.age>15) AND ($^.student.gender=="male"))'
    When executing query:
      """
      UPSERT EDGE "201" -> "101"@0 OF select
      SET grade = 3, year = 2019 WHEN select.grade > 3
      YIELD select.grade AS Grade, select.year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year |
      | 3     | 2019 |
    # Insertable, upsert when edge not exist
    When executing query:
      """
      UPDATE EDGE ON select "601" -> "101"@0
      SET year = 2019
      WHEN select.grade >10
      YIELD select.grade AS Grade, select.year AS Year
      """
    Then a ExecutionError should be raised at runtime: Storage Error:
    # upsert when edge not exists,success
    # filter condition is always true, insert default value or update value
    When executing query:
      """
      UPSERT EDGE "601" -> "101"@0 OF select
      SET year = 2019, grade = 3 WHEN select.grade >10
      YIELD select.grade AS Grade, select.year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year |
      | 3     | 2019 |
    When executing query:
      """
      FETCH PROP ON select "601"->"101"@0 YIELD select.grade, select.year
      """
    Then the result should be, in any order:
      | select._src | select._dst | select._rank | select.grade | select.year |
      | "601"       | "101"       | 0            | 3            | 2019        |
    # select_default's year with default value, timestamp not support
    When executing query:
      """
      UPSERT EDGE "111" -> "222"@0 OF select_default
      SET grade = 3
      YIELD select_default.grade AS Grade, select_default.year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year       |
      | 3     | 1546308000 |
    # select_default's year is timestamp type, set str type, get default value by timestamp is wrong,
    When executing query:
      """
      UPSERT EDGE "222" ->"333" @0 OF select_default
      SET grade = 3, year = timestamp("2020-01-10T10:00:00")
      YIELD select_default.grade AS Grade, select_default.year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year       |
      | 3     | 1578650400 |
    # select_default's grade without default value
    When executing query:
      """
      UPSERT EDGE "444" -> "555"@0 OF select_default
      SET year = 1546279201
      YIELD select.grade AS Grade, select.year AS Year
      """
    Then a ExecutionError should be raised at runtime: Storage Error:
    # select_default's grade without default value
    When executing query:
      """
      UPSERT EDGE "333" -> "444"@0 OF select_default
      SET grade = 3 + select_default.grade
      YIELD select_default.grade AS Grade, select_default.year AS Year
      """
    Then a ExecutionError should be raised at runtime: Storage Error:
    # update select_default's year with edge prop value, grade is not null value and without default value
    When executing query:
      """
      UPSERT EDGE "222" -> "444"@0 OF select_default
      SET grade = 3, year = select_default.year + 10
      YIELD select_default.grade AS Grade, select_default.year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year       |
      | 3     | 1546308010 |
    # make sure the vertex must not exist
    When executing query:
      """
      UPDATE VERTEX "1010000"
      SET course.credits = $^.course.credits + 1, name = "No9"
      WHEN $^.course.name == "Math" AND $^.course.credits > 2
      YIELD select_default.grade AS Grade, select_default.year AS Year
      """
    Then a SemanticError should be raised at runtime: Multi schema name: ,course
    # make sure the edge(src, dst) must not exist
    When executing query:
      """
      UPDATE EDGE "200" -> "101000000000000"@0 OF select
      SET grade = select.grade + 1, year = 2019
      WHEN select.grade > 4 AND $^.student.age > 15
      YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year
      """
    Then a SemanticError should be raised at runtime: Has wrong expr in `((select.grade>4) AND ($^.student.age>15))
    # make sure the edge(src, ranking, dst) must not exist
    When executing query:
      """
      UPDATE EDGE "200"->"101"@123456789 OF select
      SET grade = select.grade + 1, year = 2019
      WHEN select.grade > 4
      YIELD select.grade AS Grade, select.year AS Year
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Vertex or edge not found.
    # upsert then insert
    When executing query:
      """
      UPSERT VERTEX "100" SET building.name = "No1";
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON building "100" YIELD building.name
      """
    Then the result should be, in any order:
      | VertexID | building.name |
      | "100"    | "No1"         |
    # insert
    When executing query:
      """
      INSERT VERTEX building(name) VALUES "100": ("No2")
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON building "100" YIELD building.name
      """
    Then the result should be, in any order:
      | VertexID | building.name |
      | "100"    | "No2"         |
    When executing query:
      """
      UPSERT VERTEX "101" SET building.name = "No1"
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON building "101" YIELD building.name
      """
    Then the result should be, in any order:
      | VertexID | building.name |
      | "101"    | "No1"         |
    # insert
    When executing query:
      """
      INSERT VERTEX building(name) VALUES "101": ("No2")
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON building "101" YIELD building.name
      """
    Then the result should be, in any order:
      | VertexID | building.name |
      | "101"    | "No2"         |
    # upsert after alter schema
    When executing query:
      """
      INSERT VERTEX building(name) VALUES "100": ("No1");
      ALTER TAG building ADD (new_field string default "123");
      """
    Then the execution should be successful
    And wait 3 seconds
    # upsert after alter schema
    When executing query:
      """
      UPSERT VERTEX "100" SET building.name = "No2"
      """
    Then the execution should be successful
    When try to execute query:
      """
      FETCH PROP ON building "100" YIELD building.name, building.new_field
      """
    Then the result should be, in any order:
      | VertexID | building.name | building.new_field |
      | "100"    | "No2"         | "123"              |
    # upsert after alter schema
    When executing query:
      """
      UPSERT VERTEX "100" SET building.name = "No3", building.new_field = "321"
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON building "100" YIELD building.name, building.new_field
      """
    Then the result should be, in any order:
      | VertexID | building.name | building.new_field |
      | "100"    | "No3"         | "321"              |
    # upsert after alter schema
    When executing query:
      """
      UPSERT VERTEX "101" SET building.name = "No1", building.new_field = "No2"
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON building "101" YIELD building.name, building.new_field
      """
    Then the result should be, in any order:
      | VertexID | building.name | building.new_field |
      | "101"    | "No1"         | "No2"              |
    # Test upsert edge after alter schema
    When executing query:
      """
      INSERT EDGE like(likeness) VALUES "1" -> "100":(1.0);
      ALTER EDGE like ADD (new_field string default "123");
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query:
      """
      UPSERT EDGE "1"->"100" OF like SET likeness = 2.0;
      """
    Then the execution should be successful
    When try to execute query:
      """
      FETCH PROP ON like "1"->"100"@0 YIELD like.likeness, like.new_field
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.likeness | like.new_field |
      | "1"       | "100"     | 0          | 2.0           | "123"          |
    When executing query:
      """
      UPSERT EDGE "1"->"100" OF like SET likeness = 3.0, new_field = "321"
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON like "1"->"100"@0 YIELD like.likeness, like.new_field
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.likeness | like.new_field |
      | "1"       | "100"     | 0          | 3.0           | "321"          |
    When executing query:
      """
      UPSERT EDGE "1"->"101" OF like SET likeness = 1.0, new_field = "111"
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON like "1"->"101"@0 YIELD like.likeness, like.new_field
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.likeness | like.new_field |
      | "1"       | "101"     | 0          | 1.0           | "111"          |
    Then drop the used space

  Scenario: update and upsert test with 2.0 syntax
    When executing query:
      """
      UPDATE VERTEX ON course "101"
      SET credits = credits + 1;
      """
    Then the execution should be successful
    When executing query:
      """
      UPDATE VERTEX ON course "101"
      SET credits = credits + 1
      WHEN name == "English" AND credits > 2
      """
    Then the execution should be successful
    When executing query:
      """
      UPDATE VERTEX ON course "101"
      SET credits = credits + 1
      WHEN name == "Math" AND credits > 2
      """
    Then the execution should be successful
    When executing query:
      """
      UPDATE VERTEX ON course "101"
      SET credits = credits + 1
      YIELD name AS Name, credits AS Credits
      """
    Then the result should be, in any order:
      | Name   | Credits |
      | 'Math' | 6       |
    When executing query:
      """
      UPDATE VERTEX ON course "101"
      SET credits = credits + 1
      WHEN name == "Math" AND credits > 2
      YIELD name AS Name, credits AS Credits
      """
    Then the result should be, in any order:
      | Name   | Credits |
      | 'Math' | 7       |
    When executing query:
      """
      UPDATE VERTEX ON course "101"
      SET credits = credits + 1
      WHEN name == "notexist" AND credits > 2
      YIELD name AS Name, credits AS Credits
      """
    Then the result should be, in any order:
      | Name   | Credits |
      | 'Math' | 7       |
    When executing query:
      """
      FETCH PROP ON select "200"->"101"@0 YIELD select.grade, select.year
      """
    Then the result should be, in any order:
      | select._src | select._dst | select._rank | select.grade | select.year |
      | "200"       | "101"       | 0            | 5            | 2018        |
    # update edge succeeded
    When executing query:
      """
      UPDATE EDGE ON select "200"->"101"@0
      SET grade = grade + 1, year = 2000;
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON select "200"->"101"@0 YIELD select.grade, select.year
      """
    Then the result should be, in any order:
      | select._src | select._dst | select._rank | select.grade | select.year |
      | "200"       | "101"       | 0            | 6            | 2000        |
    When executing query:
      """
      GO FROM "101" OVER select REVERSELY YIELD select.grade, select.year
      """
    Then the result should be, in any order:
      | select.grade | select.year |
      | 6            | 2000        |
    # 2.0 test, filter out
    When executing query:
      """
      UPDATE EDGE ON select "200"->"101"@0
      SET grade = grade + 1, year = 2000
      WHEN grade > 1024
      """
    Then the execution should be successful
    # set filter
    When executing query:
      """
      UPDATE EDGE ON select "200"->"101"@0
      SET grade = grade + 1, year = 2000 WHEN grade > 4
      """
    Then the execution should be successful
    # set yield
    When executing query:
      """
      UPDATE EDGE ON select "200"->"101"@0
      SET grade = grade + 1, year = 2018
      YIELD grade AS Grade, year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year |
      | 8     | 2018 |
    # filter and yield
    When executing query:
      """
      UPDATE EDGE ON select "200"->"101"@0
      SET grade = select.grade + 1, year = 2019
      WHEN select.grade > 4
      YIELD select.grade AS Grade, select.year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year |
      | 9     | 2019 |
    # set filter out and yield
    When executing query:
      """
      UPDATE EDGE ON select "200"->"101"@0
      SET grade = grade + 1, year = 2019
      WHEN grade > 233333333333
      YIELD grade AS Grade, year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year |
      | 9     | 2019 |
    # update vertex: the item is TagName.PropName = Expression in SET clause
    When executing query:
      """
      UPDATE VERTEX ON course "101"
      SET credits = credits + 1, name = "No9"
      """
    Then the execution should be successful
    # make sure TagName and PropertyName must exist in all clauses
    When executing query:
      """
      UPDATE VERTEX ON nonexistentTag "101"
      SET credits = credits + 1
      """
    Then a SemanticError should be raised at runtime: No schema found for `nonexistentTag'
    # make sure EdgeName and PropertyName must exist in all clauses
    When executing query:
      """
      UPDATE EDGE ON select "200"->"101"@0
      SET nonexistentProperty = grade + 1, year = 2019
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Edge prop not found.
    # make sure the edge_type must not exist
    When executing query:
      """
      UPDATE EDGE ON nonexistentEdgeTypeName "200"->"101"@0
      SET grade = grade + 1, year = 2019
      """
    Then a SemanticError should be raised at runtime: No schema found for `nonexistentEdgeTypeName'
    When executing query:
      """
      FETCH PROP ON course "103" YIELD course.name, course.credits
      """
    Then the result should be, in any order:
      | VertexID | course.name | course.credits |
      | "103"    | "CS"        | 5              |
    When executing query:
      """
      UPDATE VERTEX ON course "103"
      SET credits = credits + 1
      WHEN name == "CS" AND credits > 2
      YIELD name AS Name, credits AS Credits
      """
    Then the result should be, in any order:
      | Name | Credits |
      | 'CS' | 6       |
    # when tag ON vertex not exists, update failed
    When executing query:
      """
      UPDATE VERTEX ON course "104" SET credits = credits + 1
      WHEN name == "CS" AND credits > 2
      YIELD name AS Name, credits AS Credits
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Vertex or edge not found.
    # Insertable failed, "111" is nonexistent, name and age without default value
    When executing query:
      """
      UPSERT VERTEX ON student_default "111"
      SET name = "Tom", age = age + 8
      YIELD name AS Name, age AS Age
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Invalid field value: may be the filed is not NULL or without default value or wrong schema.
    # Insertable failed, "111" is nonexistent, name without default value
    When executing query:
      """
      UPSERT VERTEX ON student_default "111"
      SET gender = "two", age = 10
      YIELD name AS Name, gender AS Gender
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Invalid field value: may be the filed is not NULL or without default value or wrong schema.
    # Insertable: vertex "112" ("Lily") --> ("Lily", "one")
    # "112" is nonexistent, gender with default value
    # update student_default.age with string value
    When executing query:
      """
      UPSERT VERTEX ON student_default "112"
      SET name = "Lily", age = "10"
      YIELD name AS Name, gender AS Gender
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Invalid data, may be wrong value type.
    # Insertable: vertex "113" ("Jack") --> ("Jack", "Three")
    # 113 is nonexistent, gender with default value,
    # update student_default.age with string value
    When executing query:
      """
      UPSERT VERTEX ON student_default "113"
      SET name = "Ann", age = "10"
      YIELD name AS Name, gender AS Gender
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Invalid data, may be wrong value type.
    # Insertable success, "115" is nonexistent, name and age without default value,
    # the filter is always true.
    When executing query:
      """
      UPSERT VERTEX ON student_default "115"
      SET name = "Kate", age = 12
      WHEN gender == "two"
      YIELD name AS Name, age AS Age, gender AS gender
      """
    Then the result should be, in any order:
      | Name   | Age | gender |
      | 'Kate' | 12  | 'one'  |
    # Order problem
    # Insertable success, "116" is nonexistent, name and age without default value,
    # the filter is always true.
    When executing query:
      """
      UPSERT VERTEX ON student_default "116"
      SET name = "Kate", age = birthday + 1, birthday = birthday + 1
      WHEN gender == "two"
      YIELD name AS Name, age AS Age, gender AS gender, birthday AS birthday
      """
    Then the result should be, in any order:
      | Name   | Age  | gender | birthday |
      | 'Kate' | 2011 | 'one'  | 2011     |
    # Order problem
    # Insertable success, "117" is nonexistent, name and age without default value,
    # the filter is always true.
    When executing query:
      """
      UPSERT VERTEX ON student_default "117"
      SET birthday = birthday + 1, name = "Kate", age = birthday + 1
      YIELD name AS Name, age AS Age, gender AS gender, birthday AS birthday
      """
    Then the result should be, in any order:
      | Name   | Age  | gender | birthday |
      | 'Kate' | 2012 | 'one'  | 2011     |
    When executing query:
      """
      FETCH PROP ON select "200"->"101"@0 YIELD select.grade, select.year
      """
    Then the result should be, in any order:
      | select._src | select._dst | select._rank | select.grade | select.year |
      | "200"       | "101"       | 0            | 9            | 2019        |
    When executing query:
      """
      UPSERT EDGE ON select "201" -> "101"@0
      SET grade = 3, year = 2019 WHEN select.grade > 3
      YIELD select.grade AS Grade, select.year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year |
      | 3     | 2019 |
    # Insertable, upsert when edge not exist
    When executing query:
      """
      UPDATE EDGE ON select "601" -> "101"@0
      SET year = 2019
      WHEN grade >10
      YIELD grade AS Grade, year AS Year
      """
    Then a ExecutionError should be raised at runtime: Storage Error:
    # upsert when edge not exists,success
    # filter condition is always true, insert default value or update value
    When executing query:
      """
      UPSERT EDGE ON select "601" -> "101"@0
      SET year = 2019, grade = 3 WHEN grade >10
      YIELD grade AS Grade, year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year |
      | 3     | 2019 |
    When executing query:
      """
      FETCH PROP ON select "601"->"101"@0 YIELD select.grade, select.year
      """
    Then the result should be, in any order:
      | select._src | select._dst | select._rank | select.grade | select.year |
      | "601"       | "101"       | 0            | 3            | 2019        |
    # select_default's year with default value, timestamp not support
    When executing query:
      """
      UPSERT EDGE ON select_default "111" -> "222"@0
      SET grade = 3
      YIELD grade AS Grade, year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year       |
      | 3     | 1546308000 |
    # select_default's year is timestamp type, set str type, get default value by timestamp is wrong,
    When executing query:
      """
      UPSERT EDGE ON select_default "222" -> "333"@0
      SET grade = 3, year = timestamp("2020-01-10T10:00:00")
      YIELD grade AS Grade, year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year       |
      | 3     | 1578650400 |
    # select_default's grade without default value
    When executing query:
      """
      UPSERT EDGE ON select_default "444" -> "555"@0
      SET year = 1546279201
      YIELD grade AS Grade, year AS Year
      """
    Then a ExecutionError should be raised at runtime: Storage Error:
    # select_default's grade without default value
    When executing query:
      """
      UPSERT EDGE ON select_default "333" -> "444"@0
      SET grade = 3 + grade
      YIELD grade AS Grade, year AS Year
      """
    Then a ExecutionError should be raised at runtime: Storage Error:
    # update select_default's year with edge prop value, grade is not null value and without default value
    When executing query:
      """
      UPSERT EDGE ON select_default "222" -> "444"@0
      SET grade = 3, year = year + 10
      YIELD grade AS Grade, year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year       |
      | 3     | 1546308010 |
    # make sure the edge(src, dst) must not exist
    When executing query:
      """
      UPDATE EDGE ON select "200" -> "101000000000000"@0
      SET grade = grade + 1, year = 2019
      WHEN grade > 4 AND age > 15
      YIELD grade AS Grade, year AS Year
      """
    Then a SemanticError should be raised at runtime: `select.age', not found the property `age'.
    # make sure the edge(src, ranking, dst) must not exist
    When executing query:
      """
      UPDATE EDGE ON select "200"->"101"@123456789
      SET grade = grade + 1, year = 2019
      WHEN grade > 4 AND year > 15
      YIELD grade AS Grade, year AS Year
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Vertex or edge not found.
    # upsert then insert
    When executing query:
      """
      UPSERT VERTEX ON building "100" SET name = "No1";
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON building "100" YIELD building.name
      """
    Then the result should be, in any order:
      | VertexID | building.name |
      | "100"    | "No1"         |
    # insert
    When executing query:
      """
      INSERT VERTEX building(name) VALUES "100": ("No2")
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON building "100" YIELD building.name
      """
    Then the result should be, in any order:
      | VertexID | building.name |
      | "100"    | "No2"         |
    When executing query:
      """
      UPSERT VERTEX ON building "101" SET name = "No1"
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON building "101" YIELD building.name
      """
    Then the result should be, in any order:
      | VertexID | building.name |
      | "101"    | "No1"         |
    # insert
    When executing query:
      """
      INSERT VERTEX building(name) VALUES "101": ("No2")
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON building "101" YIELD building.name
      """
    Then the result should be, in any order:
      | VertexID | building.name |
      | "101"    | "No2"         |
    # upsert after alter schema
    When executing query:
      """
      INSERT VERTEX building(name) VALUES "100": ("No1");
      ALTER TAG building ADD (new_field string default "123");
      """
    Then the execution should be successful
    And wait 3 seconds
    # upsert after alter schema
    When executing query:
      """
      UPSERT VERTEX  ON building "100" SET name = "No2"
      """
    Then the execution should be successful
    When try to execute query:
      """
      FETCH PROP ON building "100" YIELD building.name, building.new_field
      """
    Then the result should be, in any order:
      | VertexID | building.name | building.new_field |
      | "100"    | "No2"         | "123"              |
    # upsert after alter schema
    When executing query:
      """
      UPSERT VERTEX ON building "100" SET name = "No3", new_field = "321"
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON building "100" YIELD building.name, building.new_field
      """
    Then the result should be, in any order:
      | VertexID | building.name | building.new_field |
      | "100"    | "No3"         | "321"              |
    # upsert after alter schema
    When executing query:
      """
      UPSERT VERTEX  ON building "101" SET name = "No1", new_field = "No2"
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON building "101" YIELD building.name, building.new_field
      """
    Then the result should be, in any order:
      | VertexID | building.name | building.new_field |
      | "101"    | "No1"         | "No2"              |
    # Test upsert edge after alter schema
    When executing query:
      """
      INSERT EDGE like(likeness) VALUES "1" -> "100":(1.0);
      ALTER EDGE like ADD (new_field string default "123");
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query:
      """
      UPSERT EDGE ON like "1"->"100" SET likeness = 2.0;
      """
    Then the execution should be successful
    When try to execute query:
      """
      FETCH PROP ON like "1"->"100"@0 YIELD like.likeness, like.new_field
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.likeness | like.new_field |
      | "1"       | "100"     | 0          | 2.0           | "123"          |
    When executing query:
      """
      UPSERT EDGE ON like "1"->"100" SET likeness = 3.0, new_field = "321"
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON like "1"->"100"@0 YIELD like.likeness, like.new_field
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.likeness | like.new_field |
      | "1"       | "100"     | 0          | 3.0           | "321"          |
    When executing query:
      """
      UPSERT EDGE ON like "1"->"101" SET likeness = 1.0, new_field = "111"
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON like "1"->"101"@0 YIELD like.likeness, like.new_field
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.likeness | like.new_field |
      | "1"       | "101"     | 0          | 1.0           | "111"          |
    Then drop the used space
