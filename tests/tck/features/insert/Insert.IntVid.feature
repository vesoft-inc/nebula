# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Insert int vid of vertex and edge

  Background: Prepare space
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9   |
      | replica_factor | 1   |
      | vid_type       | int |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS person(name string, age int);
      CREATE TAG IF NOT EXISTS personWithDefault(
        name string DEFAULT "",
        age int DEFAULT 18, isMarried bool DEFAULT false,
        BMI double DEFAULT 18.5, department string DEFAULT "engineering",
        birthday timestamp DEFAULT timestamp("2020-01-10T10:00:00")
      );
      CREATE TAG IF NOT EXISTS student(grade string, number int);
      CREATE TAG IF NOT EXISTS studentWithDefault(grade string DEFAULT "one", number int);
      CREATE TAG IF NOT EXISTS employee(name int);
      CREATE TAG IF NOT EXISTS interest(name string);
      CREATE TAG IF NOT EXISTS school(name string, create_time timestamp);
      CREATE EDGE IF NOT EXISTS schoolmate(likeness int, nickname string);
      CREATE EDGE IF NOT EXISTS schoolmateWithDefault(likeness int DEFAULT 80);
      CREATE EDGE IF NOT EXISTS study(start_time timestamp, end_time timestamp);
      """

  Scenario: insert vertex and edge test
    Given wait 3 seconds
    # insert vretex with default property names
    When executing query:
      """
      INSERT VERTEX person VALUES hash("Tom"):("Tom", 18);
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX person(name, age), interest(name) VALUES hash("Tom"):("Tom", 18, "basketball");
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX person, interest(name) VALUES hash("Tom"):("Tom", 18, "basketball");
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX person(name, age), interest VALUES hash("Tom"):("Tom", 18, "basketball");
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX person(age), interest(name) VALUES hash("Tom"):(18, "basketball");
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX person, interest VALUES hash("Tom"):("Tom", 18, "basketball");
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON * hash("Tom") YIELD person.name, person.age, interest.name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID | person.name | person.age | interest.name |
      | "Tom"    | "Tom"       | 18         | "basketball"  |
    # insert vertex wrong type value
    When executing query:
      """
      INSERT VERTEX person(name, age) VALUES hash("Tom"):("Tom", "2");
      """
    Then a ExecutionError should be raised at runtime:
    # insert vertex wrong num of value
    When executing query:
      """
      INSERT VERTEX person(name) VALUES hash("Tom"):("Tom", 2);
      """
    Then a SemanticError should be raised at runtime:
    # insert vertex wrong field
    When executing query:
      """
      INSERT VERTEX person(Name, age) VALUES hash("Tom"):("Tom", 3);
      """
    Then a SemanticError should be raised at runtime:
    # insert vertex wrong type
    When executing query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES hash("Laura")->hash("Amber"):("87", "");
      """
    Then a ExecutionError should be raised at runtime:
    # insert edge wrong number of value
    When executing query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES hash("Laura")->hash("Amber"):("hello", "87", "");
      """
    Then a SemanticError should be raised at runtime:
    # insert edge wrong num of prop
    When executing query:
      """
      INSERT EDGE schoolmate(likeness) VALUES hash("Laura")->hash("Amber"):("hello", "87", "");
      """
    Then a SemanticError should be raised at runtime:
    # insert edge wrong field name
    When executing query:
      """
      INSERT EDGE schoolmate(like, HH) VALUES hash("Laura")->hash("Amber"):(88);
      """
    Then a SemanticError should be raised at runtime:
    # insert edge invalid timestamp
    When executing query:
      """
      INSERT EDGE
        study(start_time, end_time)
      VALUES
        hash("Laura")->hash("sun_school"):(timestamp("2300-01-01T10:00:00"), now()+3600*24*365*3);
      """
    Then a ExecutionError should be raised at runtime:
    And drop the used space

  Scenario: insert vertex unordered order prop vertex succeeded
    # insert vertex succeeded
    When try to execute query:
      """
      INSERT VERTEX person(name, age) VALUES hash("Tom"):("Tom", 22)
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX person(age, name) VALUES hash("Conan"):(10, "Conan")
      """
    Then the execution should be successful
    # check vertex result with fetch
    When executing query:
      """
      FETCH PROP ON person hash("Conan") YIELD person.name, person.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID | person.name | person.age |
      | 'Conan'  | "Conan"     | 10         |
    # # insert vertex with uuid
    # When executing query:
    # """
    # INSERT VERTEX person(name, age) VALUES uuid("Tom"):("Tom", 22)
    # """
    # Then the execution should be successful
    # insert vertex with string timestamp succeeded
    When try to execute query:
      """
      INSERT VERTEX school(name, create_time) VALUES hash("sun_school"):("sun_school", timestamp("2010-01-01T10:00:00"))
      """
    Then the execution should be successful
    # insert edge succeeded
    When try to execute query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES hash("Tom")->hash("Lucy"):(85, "Lily")
      """
    Then the execution should be successful
    # insert edge with unordered prop edge
    When executing query:
      """
      INSERT EDGE schoolmate(nickname, likeness) VALUES hash("Tom")->hash("Bob"):("Superman", 87)
      """
    Then the execution should be successful
    # check edge result with fetch
    When executing query:
      """
      FETCH PROP ON schoolmate hash("Tom")->hash("Bob") YIELD schoolmate.likeness, schoolmate.nickname
      """
    Then the result should be, in any order, and the columns 0,1 should be hashed:
      | schoolmate._src | schoolmate._dst | schoolmate._rank | schoolmate.likeness | schoolmate.nickname |
      | 'Tom'           | 'Bob'           | 0                | 87                  | "Superman"          |
    # insert edge with timestamp succeed
    When try to execute query:
      """
      INSERT EDGE
        study(start_time, end_time)
      VALUES
        hash("Laura")->hash("sun_school"):(timestamp("2019-01-01T10:00:00"), now()+3600*24*365*3)
      """
    Then the execution should be successful
    # check edge result with go
    When executing query:
      """
      GO FROM hash("Laura") OVER study
      YIELD $$.school.name, study._dst, $$.school.create_time, (string)study.start_time
      """
    Then the result should be, in any order, and the columns 1 should be hashed:
      | $$.school.name | study._dst   | $$.school.create_time | (STRING)study.start_time |
      | "sun_school"   | "sun_school" | 1262340000            | "1546336800"             |
    # check edge result with fetch
    When executing query:
      """
      FETCH PROP ON school hash("sun_school") YIELD school.name, school.create_time
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID     | school.name  | school.create_time |
      | "sun_school" | "sun_school" | 1262340000         |
    # insert one vertex multi tags
    When executing query:
      """
      INSERT VERTEX person(name, age), student(grade, number)
      VALUES hash("Lucy"):("Lucy", 8, "three", 20190901001)
      """
    Then the execution should be successful
    # insert one vertex multi tags with unordered order prop
    When executing query:
      """
      INSERT VERTEX person(age, name),student(number, grade)
      VALUES hash("Bob"):(9, "Bob", 20191106001, "four")
      """
    Then the execution should be successful
    # check person tag result with fetch
    When executing query:
      """
      FETCH PROP ON person hash("Bob") YIELD person.name, person.age
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID | person.name | person.age |
      | 'Bob'    | 'Bob'       | 9          |
    # check student tag result with fetch
    When executing query:
      """
      FETCH PROP ON student hash("Bob") YIELD student.grade, student.number
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | VertexID | student.grade | student.number |
      | 'Bob'    | 'four'        | 20191106001    |
    # insert multi vertex multi tags
    When executing query:
      """
      INSERT VERTEX person(name, age),student(grade, number)
      VALUES hash("Laura"):("Laura", 8, "three", 20190901008),hash("Amber"):("Amber", 9, "four", 20180901003)
      """
    Then the execution should be successful
    # insert multi vertex one tag
    When executing query:
      """
      INSERT VERTEX person(name, age) VALUES hash("Kitty"):("Kitty", 8), hash("Peter"):("Peter", 9)
      """
    Then the execution should be successful
    # insert multi edges
    When executing query:
      """
      INSERT EDGE schoolmate(likeness, nickname)
      VALUES hash("Tom")->hash("Kitty"):(81, "Kitty"), hash("Tom")->hash("Peter"):(83, "Kitty")
      """
    Then the execution should be successful
    # check edge result with go
    When executing query:
      """
      GO FROM hash("Tom") OVER schoolmate YIELD $^.person.name, schoolmate.likeness, $$.person.name
      """
    Then the result should be, in any order:
      | $^.person.name | schoolmate.likeness | $$.person.name |
      | 'Tom'          | 85                  | 'Lucy'         |
      | 'Tom'          | 81                  | 'Kitty'        |
      | 'Tom'          | 83                  | 'Peter'        |
      | 'Tom'          | 87                  | 'Bob'          |
    # insert multi tag
    When executing query:
      """
      INSERT EDGE schoolmate(likeness, nickname)
      VALUES hash("Lucy")->hash("Laura"):(90, "Laura"), hash("Lucy")->hash("Amber"):(95, "Amber")
      """
    Then the execution should be successful
    # insert with edge
    When executing query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES hash("Laura")->hash("Aero"):(90, "Aero")
      """
    Then the execution should be successful
    # get multi tag through go
    When executing query:
      """
      GO FROM hash("Lucy") OVER schoolmate
      YIELD schoolmate.likeness, $$.person.name,$$.student.grade, $$.student.number
      """
    Then the result should be, in any order:
      | schoolmate.likeness | $$.person.name | $$.student.grade | $$.student.number |
      | 90                  | 'Laura'        | 'three'          | 20190901008       |
      | 95                  | 'Amber'        | 'four'           | 20180901003       |
    When try to execute query:
      """
      INSERT VERTEX student(grade, number) VALUES hash("Aero"):("four", 20190901003);
      """
    Then the execution should be successful
    # test multi sentences multi tags succeeded
    When executing query:
      """
      INSERT VERTEX person(name, age) VALUES hash("Aero"):("Aero", 8);
      INSERT VERTEX student(grade, number) VALUES hash("Aero"):("four", 20190901003);
      INSERT EDGE schoolmate(likeness, nickname) VALUES hash("Laura")->hash("Aero"):(90, "Aero")
      """
    Then the execution should be successful
    # get result through go
    When executing query:
      """
      GO FROM hash("Laura") OVER schoolmate YIELD $$.student.number, $$.person.name
      """
    Then the result should be, in any order:
      | $$.student.number | $$.person.name |
      | 20190901003       | 'Aero'         |
    # test same prop name diff type
    When try to execute query:
      """
      INSERT VERTEX
        person(name, age),
        employee(name)
      VALUES
        hash("Joy"):("Joy", 18, 123),
        hash("Petter"):("Petter", 19, 456);
      INSERT EDGE
        schoolmate(likeness, nickname)
      VALUES
        hash("Joy")->hash("Petter"):(90, "Petter");
      """
    Then the execution should be successful
    # get result through go
    When executing query:
      """
      GO FROM hash("Joy") OVER schoolmate
      YIELD $^.person.name,schoolmate.likeness, $$.person.name, $$.person.age,$$.employee.name
      """
    Then the result should be, in any order:
      | $^.person.name | schoolmate.likeness | $$.person.name | $$.person.age | $$.employee.name |
      | 'Joy'          | 90                  | 'Petter'       | 19            | 456              |
    # test same prop name same type diff type
    When executing query:
      """
      INSERT VERTEX person(name, age),interest(name) VALUES hash("Bob"):("Bob", 19, "basketball");
      INSERT EDGE schoolmate(likeness, nickname) VALUES hash("Petter")->hash("Bob"):(90, "Bob");
      """
    Then the execution should be successful
    # get result through go
    When executing query:
      """
      GO FROM hash("Petter") OVER schoolmate
      YIELD $^.person.name, $^.employee.name,schoolmate.likeness, $$.person.name,$$.interest.name, $$.person.age
      """
    Then the result should be, in any order:
      | $^.person.name | $^.employee.name | schoolmate.likeness | $$.person.name | $$.interest.name | $$.person.age |
      | 'Petter'       | 456              | 90                  | 'Bob'          | 'basketball'     | 19            |
    # insert vertex using name and age default value
    When try to execute query:
      """
      INSERT VERTEX personWithDefault() VALUES 111:();
      """
    Then the execution should be successful
    # insert vertex lack of the column value
    When executing query:
      """
      INSERT VERTEX personWithDefault(age, isMarried, BMI) VALUES hash("Tom"):(18, false);
      """
    Then a SemanticError should be raised at runtime:
    # insert vertex using age default value
    When executing query:
      """
      INSERT VERTEX personWithDefault(name) VALUES hash("Tom"):("Tom")
      """
    Then the execution should be successful
    # insert vertex with BMI default value
    When executing query:
      """
      INSERT VERTEX personWithDefault(name, age) VALUES hash("Tom"):("Tom", 20)
      """
    Then the execution should be successful
    # insert vertices multi tags with default value
    When try to execute query:
      """
      INSERT VERTEX
        personWithDefault(name, BMI),
        studentWithDefault(number)
      VALUES
        hash("Laura"):("Laura", 21.5, 20190901008),
        hash("Amber"):("Amber", 22.5, 20180901003);
      """
    Then the execution should be successful
    # multi vertices one tag with default value
    When executing query:
      """
      INSERT VERTEX personWithDefault(name) VALUES hash("Kitty"):("Kitty"), hash("Peter"):("Peter")
      """
    Then the execution should be successful
    # insert column doesn't match value count
    When executing query:
      """
      INSERT VERTEX studentWithDefault(grade, number) VALUES hash("Tom"):("one", 111, "");
      """
    Then a SemanticError should be raised at runtime:
    # insert edge with all default value
    When try to execute query:
      """
      INSERT EDGE schoolmateWithDefault() VALUES hash("Tom")->hash("Lucy"):()
      """
    Then the execution should be successful
    # insert edge lack of the column value
    When executing query:
      """
      INSERT EDGE schoolmateWithDefault(likeness) VALUES hash("Tom")->hash("Lucy"):()
      """
    Then a SemanticError should be raised at runtime:
    # insert edge column count doesn't match value count
    When executing query:
      """
      INSERT EDGE schoolmateWithDefault(likeness) VALUES hash("Tom")->hash("Lucy"):(60, "")
      """
    Then a SemanticError should be raised at runtime:
    # insert edge with unknown filed name
    When executing query:
      """
      INSERT EDGE schoolmateWithDefault(likeness, redundant) VALUES hash("Tom")->hash("Lucy"):(90, 0)
      """
    Then a SemanticError should be raised at runtime:
    # insert multi edges with default value
    When executing query:
      """
      INSERT EDGE
        schoolmateWithDefault()
      VALUES
        hash("Tom")->hash("Kitty"):(),
        hash("Tom")->hash("Peter"):(),
        hash("Lucy")->hash("Laura"):(),
        hash("Lucy")->hash("Amber"):()
      """
    Then the execution should be successful
    # get result through go
    When executing query:
      """
      GO FROM hash("Tom") OVER schoolmateWithDefault
      YIELD $^.person.name, schoolmateWithDefault.likeness, $$.person.name
      """
    Then the result should be, in any order:
      | $^.person.name | schoolmateWithDefault.likeness | $$.person.name |
      | 'Tom'          | 80                             | 'Kitty'        |
      | 'Tom'          | 80                             | 'Peter'        |
      | 'Tom'          | 80                             | 'Lucy'         |
    # get result through go
    When executing query:
      """
      GO FROM hash("Lucy") OVER schoolmateWithDefault
      YIELD schoolmateWithDefault.likeness,
      $$.personWithDefault.name,
      $$.personWithDefault.birthday,
      $$.personWithDefault.department,
      $$.studentWithDefault.grade,
      $$.studentWithDefault.number
      """
    Then the result should be, in any order:
      | schoolmateWithDefault.likeness | $$.personWithDefault.name | $$.personWithDefault.birthday | $$.personWithDefault.department | $$.studentWithDefault.grade | $$.studentWithDefault.number |
      | 80                             | 'Laura'                   | 1578650400                    | 'engineering'                   | 'one'                       | 20190901008                  |
      | 80                             | 'Amber'                   | 1578650400                    | 'engineering'                   | 'one'                       | 20180901003                  |
    # insert multi version vertex
    When executing query:
      """
      INSERT VERTEX person(name, age) VALUES hash("Tony"):("Tony", 18), hash("Mack"):("Mack", 19);
      INSERT VERTEX person(name, age) VALUES hash("Mack"):("Mack", 20);
      INSERT VERTEX person(name, age) VALUES hash("Mack"):("Mack", 21)
      """
    Then the execution should be successful
    # insert multi version edge
    When executing query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES hash("Tony")->hash("Mack")@1:(1, "");
      INSERT EDGE schoolmate(likeness, nickname) VALUES hash("Tony")->hash("Mack")@1:(2, "");
      INSERT EDGE schoolmate(likeness, nickname) VALUES hash("Tony")->hash("Mack")@1:(3, "");
      """
    Then the execution should be successful
    # get multi version result through go
    When executing query:
      """
      GO FROM hash("Tony") OVER schoolmate YIELD $$.person.name, $$.person.age, schoolmate.likeness
      """
    Then the result should be, in any order:
      | $$.person.name | $$.person.age | schoolmate.likeness |
      | 'Mack'         | 21            | 3                   |
    # insert multi version edge
    # When executing query:
    # """
    # INSERT VERTEX person(name, age) VALUES uuid("Tony"):("Tony", 18), uuid("Mack"):("Mack", 19);
    # INSERT VERTEX person(name, age) VALUES uuid("Mack"):("Mack", 20);
    # INSERT VERTEX person(name, age) VALUES uuid("Mack"):("Mack", 21)
    # """
    # Then the execution should be successful
    # insert multi version edge with uuid
    # When executing query:
    # """
    # INSERT EDGE schoolmate(likeness, nickname) VALUES uuid("Tony")->uuid("Mack")@1:(1, "");
    # INSERT EDGE schoolmate(likeness, nickname) VALUES uuid("Tony")->uuid("Mack")@1:(2, "");
    # INSERT EDGE schoolmate(likeness, nickname) VALUES uuid("Tony")->uuid("Mack")@1:(3, "");
    # """
    # Then the execution should be successful
    # get multi version result through go
    # When executing query:
    # """
    # GO FROM uuid("Tony") OVER schoolmate YIELD $$.person.name, $$.person.age, schoolmate.likeness
    # """
    # Then the result should be, in any order:
    # |$$.person.name| $$.person.age| schoolmate.likeness|
    # |'Mack'        | 21           | 3                  |
    Then drop the used space
