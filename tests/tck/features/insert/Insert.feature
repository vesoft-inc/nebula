# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Insert string vid of vertex and edge

  Scenario: insert vertex and edge test
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
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
    # insert vertex succeeded
    When try to execute query:
      """
      INSERT VERTEX person(name, age) VALUES "Tom":("Tom", 22)
      """
    Then the execution should be successful
    # insert vertex with default property names
    When executing query:
      """
      INSERT VERTEX person VALUES "Tom":("Tom", 18);
      """
    Then the execution should be successful
    When try to execute query:
      """
      INSERT VERTEX person(name, age), interest(name) VALUES "Tom":("Tom", 18, "basketball");
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX person, interest(name) VALUES "Tom":("Tom", 18, "basketball");
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX person(name, age), interest VALUES "Tom":("Tom", 18, "basketball");
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX person(age), interest(name) VALUES "Tom":(18, "basketball");
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX person, interest VALUES "Tom":("Tom", 18, "basketball");
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON * "Tom" YIELD vertex as node
      """
    Then the result should be, in any order, with relax comparison:
      | node                                                           |
      | ("Tom":person{name:"Tom", age:18}:interest{name:"basketball"}) |
    # insert vertex wrong type value
    When executing query:
      """
      INSERT VERTEX person(name, age) VALUES "Tom":("Tom", "2");
      """
    Then a ExecutionError should be raised at runtime:
    # insert vertex wrong num of value
    When executing query:
      """
      INSERT VERTEX person(name) VALUES "Tom":("Tom", 2);
      """
    Then a SemanticError should be raised at runtime:
    # insert vertex wrong field
    When executing query:
      """
      INSERT VERTEX person(Name, age) VALUES "Tom":("Tom", 3);
      """
    Then a SemanticError should be raised at runtime:
    # insert edge succeeded
    When try to execute query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES "Tom"->"Lucy":(85, "Lily")
      """
    Then the execution should be successful
    # insert edge wrong type
    When executing query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES "Laura"->"Amber":("87", "");
      """
    Then a ExecutionError should be raised at runtime:
    # insert edge wrong number of value
    When executing query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES "Laura"->"Amber":("hello", "87", "");
      """
    Then a SemanticError should be raised at runtime:
    # insert edge wrong num of prop
    When executing query:
      """
      INSERT EDGE schoolmate(likeness) VALUES "Laura"->"Amber":("hello", "87", "");
      """
    Then a SemanticError should be raised at runtime:
    # insert edge wrong field name
    When executing query:
      """
      INSERT EDGE schoolmate(like, HH) VALUES "Laura"->"Amber":(88);
      """
    Then a SemanticError should be raised at runtime:
    # insert edge with timestamp succeed
    When try to execute query:
      """
      INSERT EDGE study(start_time, end_time) VALUES "Laura"->"sun_school":(timestamp("2019-01-01T10:00:00"), now()+3600*24*365*3)
      """
    Then the execution should be successful
    # insert edge invalid timestamp
    When executing query:
      """
      INSERT EDGE
        study(start_time, end_time)
      VALUES
        "Laura"->"sun_school":(timestamp("2300-01-01T10:00:00"), now()+3600*24*365*3);
      """
    Then a ExecutionError should be raised at runtime:
    # insert vertex unordered order prop vertex succeeded
    When executing query:
      """
      INSERT VERTEX person(age, name) VALUES "Conan":(10, "Conan")
      """
    Then the execution should be successful
    # check vertex result with fetch
    When executing query:
      """
      FETCH PROP ON person "Conan" YIELD vertex as node
      """
    Then the result should be, in any order, with relax comparison:
      | node      |
      | ('Conan') |
    # insert vertex with string timestamp succeeded
    When try to execute query:
      """
      INSERT VERTEX school(name, create_time) VALUES "sun_school":("sun_school", timestamp("2010-01-01T10:00:00"))
      """
    Then the execution should be successful
    # insert edge with unordered prop edge
    When executing query:
      """
      INSERT EDGE schoolmate(nickname, likeness) VALUES "Tom"->"Bob":("Superman", 87)
      """
    Then the execution should be successful
    # check edge result with fetch
    When executing query:
      """
      FETCH PROP ON schoolmate "Tom"->"Bob" YIELD schoolmate.likeness, schoolmate.nickname
      """
    Then the result should be, in any order:
      | schoolmate.likeness | schoolmate.nickname |
      | 87                  | 'Superman'          |
    # check edge result with go
    When executing query:
      """
      GO FROM "Laura" OVER study
      YIELD $$.school.name, study._dst, $$.school.create_time, (string)study.start_time
      """
    Then the result should be, in any order:
      | $$.school.name | study._dst   | $$.school.create_time | (STRING)study.start_time |
      | "sun_school"   | "sun_school" | 1262340000            | "1546336800"             |
    # check edge result with fetch
    When executing query:
      """
      FETCH PROP ON school "sun_school" YIELD school.name, school.create_time
      """
    Then the result should be, in any order:
      | school.name  | school.create_time |
      | "sun_school" | 1262340000         |
    # insert one vertex multi tags
    When executing query:
      """
      INSERT VERTEX person(name, age), student(grade, number) VALUES "Lucy":("Lucy", 8, "three", 20190901001)
      """
    Then the execution should be successful
    # insert one vertex multi tags with unordered order prop
    When executing query:
      """
      INSERT VERTEX person(age, name),student(number, grade) VALUES "Bob":(9, "Bob", 20191106001, "four")
      """
    Then the execution should be successful
    # check person tag result with fetch
    When executing query:
      """
      FETCH PROP ON person "Bob" YIELD person.name, person.age
      """
    Then the result should be, in any order:
      | person.name | person.age |
      | 'Bob'       | 9          |
    # check student tag result with fetch
    When executing query:
      """
      FETCH PROP ON student "Bob" YIELD student.grade, student.number
      """
    Then the result should be, in any order:
      | student.grade | student.number |
      | 'four'        | 20191106001    |
    # insert multi vertex multi tags
    When executing query:
      """
      INSERT VERTEX
        person(name, age),
        student(grade, number)
      VALUES
        "Laura":("Laura", 8, "three", 20190901008),
        "Amber":("Amber", 9, "four", 20180901003);
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON person "Laura" YIELD person.name, person.age
      """
    Then the result should be, in any order:
      | person.name | person.age |
      | 'Laura'     | 8          |
    When executing query:
      """
      FETCH PROP ON student "Laura" YIELD student.grade, student.number
      """
    Then the result should be, in any order:
      | student.grade | student.number |
      | 'three'       | 20190901008    |
    When executing query:
      """
      FETCH PROP ON person "Amber" YIELD person.name, person.age
      """
    Then the result should be, in any order:
      | person.name | person.age |
      | 'Amber'     | 9          |
    When executing query:
      """
      FETCH PROP ON student "Amber" YIELD student.grade, student.number
      """
    Then the result should be, in any order:
      | student.grade | student.number |
      | 'four'        | 20180901003    |
    # insert multi vertex one tag
    When executing query:
      """
      INSERT VERTEX person(name, age) VALUES "Kitty":("Kitty", 8), "Peter":("Peter", 9)
      """
    Then the execution should be successful
    # insert multi edges
    When executing query:
      """
      INSERT EDGE
        schoolmate(likeness, nickname)
      VALUES
        "Tom"->"Kitty":(81, "Kitty"),
        "Tom"->"Peter":(83, "Kitty");
      """
    Then the execution should be successful
    # check edge result with go
    When executing query:
      """
      GO FROM "Tom" OVER schoolmate
      YIELD $^.person.name, schoolmate.likeness, $$.person.name
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
      INSERT EDGE schoolmate(likeness, nickname) VALUES "Lucy"->"Laura":(90, "Laura"), "Lucy"->"Amber":(95, "Amber")
      """
    Then the execution should be successful
    # insert with edge
    When executing query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES "Laura"->"Aero":(90, "Aero")
      """
    Then the execution should be successful
    # get multi tag through go
    When executing query:
      """
      GO FROM "Lucy" OVER schoolmate
      YIELD schoolmate.likeness, $$.person.name,$$.student.grade, $$.student.number
      """
    Then the result should be, in any order:
      | schoolmate.likeness | $$.person.name | $$.student.grade | $$.student.number |
      | 90                  | 'Laura'        | 'three'          | 20190901008       |
      | 95                  | 'Amber'        | 'four'           | 20180901003       |
    # test multi sentences multi tags succeeded
    When executing query:
      """
      INSERT VERTEX person(name, age) VALUES "Aero":("Aero", 8);
      INSERT VERTEX student(grade, number) VALUES "Aero":("four", 20190901003);
      INSERT EDGE schoolmate(likeness, nickname) VALUES "Laura"->"Aero":(90, "Aero");
      """
    Then the execution should be successful
    # get result through go
    When executing query:
      """
      GO FROM "Laura" OVER schoolmate YIELD $$.student.number, $$.person.name
      """
    Then the result should be, in any order:
      | $$.student.number | $$.person.name |
      | 20190901003       | 'Aero'         |
    # test same prop name diff type
    When try to execute query:
      """
      INSERT VERTEX person(name, age), employee(name) VALUES "Joy":("Joy", 18, 123), "Petter":("Petter", 19, 456);
      INSERT EDGE schoolmate(likeness, nickname) VALUES "Joy"->"Petter":(90, "Petter");
      """
    Then the execution should be successful
    # get result through go
    When executing query:
      """
      GO FROM "Joy" OVER schoolmate
      YIELD $^.person.name,schoolmate.likeness, $$.person.name, $$.person.age,$$.employee.name
      """
    Then the result should be, in any order:
      | $^.person.name | schoolmate.likeness | $$.person.name | $$.person.age | $$.employee.name |
      | 'Joy'          | 90                  | 'Petter'       | 19            | 456              |
    # test same prop name same type diff type
    When executing query:
      """
      INSERT VERTEX person(name, age),interest(name) VALUES "Bob":("Bob", 19, "basketball");
      INSERT EDGE schoolmate(likeness, nickname) VALUES "Petter"->"Bob":(90, "Bob");
      """
    Then the execution should be successful
    # get result through go
    When executing query:
      """
      GO FROM "Petter" OVER schoolmate
      YIELD $^.person.name,
      $^.employee.name,schoolmate.likeness,
      $$.person.name,
      $$.interest.name,
      $$.person.age
      """
    Then the result should be, in any order:
      | $^.person.name | $^.employee.name | schoolmate.likeness | $$.person.name | $$.interest.name | $$.person.age |
      | 'Petter'       | 456              | 90                  | 'Bob'          | 'basketball'     | 19            |
    # insert vertex using name and age default value
    When try to execute query:
      """
      INSERT VERTEX personWithDefault() VALUES "111":();
      """
    Then the execution should be successful
    # insert vertex lack of the column value
    When executing query:
      """
      INSERT VERTEX personWithDefault(age, isMarried, BMI) VALUES "Tom":(18, false);
      """
    Then a SemanticError should be raised at runtime:
    # insert vertex using age default value
    When executing query:
      """
      INSERT VERTEX personWithDefault(name) VALUES "Tom":("Tom")
      """
    Then the execution should be successful
    # insert vertex with BMI default value
    When executing query:
      """
      INSERT VERTEX personWithDefault(name, age) VALUES "Tom":("Tom", 20)
      """
    Then the execution should be successful
    # insert vertices multi tags with default value
    When try to execute query:
      """
      INSERT VERTEX
        personWithDefault(name, BMI),
        studentWithDefault(number)
      VALUES
        "Laura":("Laura", 21.5, 20190901008),
        "Amber":("Amber", 22.5, 20180901003)
      """
    Then the execution should be successful
    # insert column doesn't match value count
    When executing query:
      """
      INSERT VERTEX studentWithDefault(grade, number) VALUES "Tom":("one", 111, "");
      """
    Then a SemanticError should be raised at runtime:
    # multi vertices one tag with default value
    When executing query:
      """
      INSERT VERTEX personWithDefault(name) VALUES "Kitty":("Kitty"), "Peter":("Peter")
      """
    Then the execution should be successful
    # insert edge with all default value
    When try to execute query:
      """
      INSERT EDGE schoolmateWithDefault() VALUES "Tom"->"Lucy":()
      """
    Then the execution should be successful
    # insert edge lack of the column value
    When executing query:
      """
      INSERT EDGE schoolmateWithDefault(likeness) VALUES "Tom"->"Lucy":()
      """
    Then a SemanticError should be raised at runtime:
    # insert edge column count doesn't match value count
    When executing query:
      """
      INSERT EDGE schoolmateWithDefault(likeness) VALUES "Tom"->"Lucy":(60, "")
      """
    Then a SemanticError should be raised at runtime:
    # insert edge with unknown filed name
    When executing query:
      """
      INSERT EDGE schoolmateWithDefault(likeness, redundant) VALUES "Tom"->"Lucy":(90, 0)
      """
    Then a SemanticError should be raised at runtime:
    # insert multi edges with default value
    When executing query:
      """
      INSERT EDGE schoolmateWithDefault() VALUES "Tom"->"Kitty":(), "Tom"->"Peter":(), "Lucy"->"Laura":(), "Lucy"->"Amber":()
      """
    Then the execution should be successful
    # get result through go
    When executing query:
      """
      GO FROM "Tom" OVER schoolmateWithDefault
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
      GO FROM "Lucy" OVER schoolmateWithDefault
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
      INSERT VERTEX person(name, age) VALUES "Tony":("Tony", 18), "Mack":("Mack", 19);
      INSERT VERTEX person(name, age) VALUES "Mack":("Mack", 20);
      INSERT VERTEX person(name, age) VALUES "Mack":("Mack", 21)
      """
    Then the execution should be successful
    # insert multi version edge
    When executing query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES "Tony"->"Mack"@1:(1, "");
      INSERT EDGE schoolmate(likeness, nickname) VALUES "Tony"->"Mack"@1:(2, "");
      INSERT EDGE schoolmate(likeness, nickname) VALUES "Tony"->"Mack"@1:(3, "");
      """
    Then the execution should be successful
    # get multi version result through go
    When executing query:
      """
      GO FROM "Tony" OVER schoolmate YIELD $$.person.name, $$.person.age, schoolmate.likeness
      """
    Then the result should be, in any order:
      | $$.person.name | $$.person.age | schoolmate.likeness |
      | 'Mack'         | 21            | 3                   |
    Then drop the used space

  Scenario: insert vertex and edge test by the 2.0 new type
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(10) |
    And having executed:
      """
      CREATE TAG student(name string NOT NULL, age int);
      CREATE TAG course(name fixed_string(5) NOT NULL, introduce string DEFAULT NULL);
      CREATE TAG INDEX student_i ON student(name(30), age);
      CREATE TAG INDEX course_i ON course(name, introduce(30));
      """
    # test insert with fixed_string
    When try to execute query:
      """
      INSERT VERTEX course(name) VALUES "Math":("Math")
      """
    Then the execution should be successful
    # test insert with empty str vid
    When try to execute query:
      """
      INSERT VERTEX student(name, age) VALUES "":("Tom", 12)
      """
    Then the execution should be successful
    # test insert out of range id size
    When executing query:
      """
      INSERT VERTEX student(name, age) VALUES "12345678901":("Tom", 2)
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    # test insert not null prop
    When executing query:
      """
      INSERT VERTEX student(name, age) VALUES "Tom":(NULL, 12)
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The not null field cannot be null.
    # out of fixed_string's size
    When executing query:
      """
      INSERT VERTEX course(name) VALUES "English":("English")
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      FETCH PROP ON course "English" YIELD vertex as node
      """
    Then the result should be, in any order, with relax comparison:
      | node        |
      | ('English') |
    # insert Chinese character
    # if the Chinese character is out of the fixed_string's size, it will be truncated,
    # and no invalid char should be inserted
    When executing query:
      """
      INSERT VERTEX course(name) VALUES "Chinese":("中文字符")
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      FETCH PROP ON course "Chinese" YIELD vertex as node
      """
    Then the result should be, in any order, with relax comparison:
      | node        |
      | ('Chinese') |
    # check result
    When executing query:
      """
      FETCH PROP ON student "" YIELD student.name, student.age
      """
    Then the result should be, in any order:
      | student.name | student.age |
      | 'Tom'        | 12          |
    # check result
    When executing query:
      """
      LOOKUP on course YIELD course.name, course.introduce
      """
    Then the result should be, in any order:
      | course.name | course.introduce |
      | 'Engli'     | NULL             |
      | 'Math'      | NULL             |
      | '中'        | NULL             |
    When executing query:
      """
      LOOKUP ON student YIELD student.name, student.age
      """
    Then the result should be, in any order:
      | student.name | student.age |
      | 'Tom'        | 12          |
    Then drop the used space

  Scenario: string id ignore existed index
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(10) |
    And having executed:
      """
      CREATE TAG person(id int);
      CREATE TAG INDEX id_index ON person(id);
      CREATE EDGE like(grade int);
      CREATE EDGE INDEX grade_index ON like(grade);
      """
    And wait 6 seconds
    # test insert vertex
    When try to execute query:
      """
      INSERT VERTEX person(id) VALUES "100":(1), "200":(1)
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON person WHERE person.id == 1 YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id    |
      | "100" |
      | "200" |
    When try to execute query:
      """
      INSERT VERTEX IGNORE_EXISTED_INDEX person(id) VALUES "200":(2)
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON person WHERE person.id == 1 YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id    |
      | "100" |
      | "200" |
    When executing query:
      """
      LOOKUP ON person WHERE person.id == 2 YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id    |
      | "200" |
    # test insert edge
    When try to execute query:
      """
      INSERT EDGE like(grade) VALUES "100" -> "200":(666), "300" -> "400":(666);
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON like WHERE like.grade == 666 YIELD src(edge) as src, dst(edge) as dst
      """
    Then the result should be, in any order:
      | src   | dst   |
      | "100" | "200" |
      | "300" | "400" |
    When try to execute query:
      """
      INSERT EDGE IGNORE_EXISTED_INDEX like(grade) VALUES "300" -> "400":(888)
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON like WHERE like.grade == 666 YIELD src(edge) as src, dst(edge) as dst
      """
    Then the result should be, in any order:
      | src   | dst   |
      | "100" | "200" |
      | "300" | "400" |
    When executing query:
      """
      LOOKUP ON like WHERE like.grade == 888 YIELD src(edge) as src, dst(edge) as dst
      """
    Then the result should be, in any order:
      | src   | dst   |
      | "300" | "400" |
    When try to execute query:
      """
      INSERT EDGE like(grade) VALUES "3000000000000000000000000000000000000000000000000000000000" -> "400":(888)
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    When try to execute query:
      """
      INSERT EDGE like(grade) VALUES "300" -> "4000000000000000000000000000000000000000000000000000000000":(888)
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    When try to execute query:
      """
      INSERT EDGE like(grade) VALUES "300000000000000000000000000000000000000000000000000" -> "4000000000000000000000000000000000000000000000000000000000":(888)
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit integer or a string fitting space vertex id length limit.
    Then drop the used space

  Scenario: insert vertex with non existent tag
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(10) |
    When try to execute query:
      """
      INSERT VERTEX invalid_vertex VALUES "non_existed_tag":()
      """
    Then a SemanticError should be raised at runtime: No schema found

  Scenario: insert player(name string, age int, hobby List< string >, ids List< int >, score List< float >)
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    When executing query:
      """
      CREATE TAG player(name string, age int, hobby List< string >, ids List< int >, score List< float >);
      CREATE TAG playerWithDefault(
          name string DEFAULT "",
          age int DEFAULT 18, isMarried bool DEFAULT false,
          BMI double DEFAULT 18.5, department string DEFAULT "engineering",
          birthday timestamp DEFAULT timestamp("2020-01-10T10:00:00"),
          number int DEFAULT 0
      );
      CREATE TAG school(name string, create_time timestamp);
      CREATE EDGE schoolmate(likeness int, nickname string);
      CREATE EDGE schoolmateWithDefault(likeness int DEFAULT 80);
      CREATE EDGE study(start_time timestamp, end_time timestamp);
      """
    Then the execution should be successful
    And wait 3 seconds
    When try to execute query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 42, [], [], []);
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON * "player100" YIELD vertex as node;
      """
    Then the result should be, in any order, with relax comparison:
      | node                                                                        |
      | ("player100":player{name:"Tim Duncan", age:42, hobby:[], ids:[], score:[]}) |
    # Handle the edge cases by inserting incorrect types and handling errors
    When try to execute query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 42, ["Basketball", "Swimming", "Reading"], [1, 2, 3], [10.5, 20.5, 30.5]);
      """
    Then the execution should be successful
    # Insert and update the player vertex to modify only the lists
    When try to execute query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 42, ["Basketball", "Gaming"], [4, 5, 6], [40.5, 50.5, 60.5]);
      """
    Then the execution should be successful
    # Verify that the latest data matches the expected state
    When executing query:
      """
      FETCH PROP ON * "player100" YIELD vertex as node;
      """
    Then the result should be, in any order, with relax comparison:
      | node                                                                                                                     |
      | ("player100":player{name:"Tim Duncan", age:42, hobby:["Basketball", "Gaming"], ids:[4, 5, 6], score:[40.5, 50.5, 60.5]}) |
    # Handle the edge cases by inserting incorrect types and handling errors
    When executing query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 2, [10, 11, 12], ["Swimming", "Painting"], {100.5, 110.5, 120.5});
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    # Insert edges and fetch results
    When try to execute query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES "player100"->"player200":(85, "Lily");
      INSERT EDGE schoolmate(likeness, nickname) VALUES "player200"->"player300":("87", "");
      INSERT EDGE schoolmate(likeness) VALUES "player200"->"player300":("hello", "87");
      INSERT EDGE schoolmate(likeness, HH) VALUES "player200"->"player300":(88, "");
      INSERT EDGE study(start_time, end_time) VALUES "player200"->"school1":(timestamp("2019-01-01T10:00:00"), now()+3600*24*365*3);
      """
    Then the execution should be successful
    # Insert vertices with default properties
    When try to execute query:
      """
      INSERT VERTEX playerWithDefault() VALUES "player400":();
      INSERT VERTEX playerWithDefault(age, isMarried, BMI) VALUES "player100":(18, false, 19.5);
      INSERT VERTEX playerWithDefault(name) VALUES "player100":("Tim Duncan");
      INSERT VERTEX playerWithDefault(name, age) VALUES "player100":("Tim Duncan", 20);
      INSERT VERTEX playerWithDefault(name, BMI, number) VALUES "player200":("Laura", 21.5, 20190901008), "player300":("Amber", 22.5, 20180901003);
      """
    Then the execution should be successful
    # Fetch properties and validate results
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD player.name, player.age, player.hobby, player.ids, player.score;
      """
    Then the result should be, in any order, with relax comparison:
      | player.name  | player.age | player.hobby             | player.ids | player.score       |
      | "Tim Duncan" | 42         | ["Basketball", "Gaming"] | [4, 5, 6]  | [40.5, 50.5, 60.5] |
    When executing query:
      """
      FETCH PROP ON playerWithDefault "player200" YIELD playerWithDefault.name, playerWithDefault.birthday, playerWithDefault.department;
      """
    Then the execution should be successful
    # Insert and query multi-tagged vertices and edges
    When executing query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player200":("Laura", 8, ["Basketball", "Reading"], [7, 8, 9], [70.5, 80.5, 90.5]);
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player300":("Amber", 9, ["Swimming", "Painting"], [10, 11, 12], [100.5, 110.5, 120.5]);
      INSERT EDGE schoolmateWithDefault() VALUES "player100"->"player200":(), "player100"->"player300":();
      GO FROM "player100" OVER schoolmateWithDefault YIELD $^.player.name, schoolmateWithDefault.likeness, $$.player.name;
      """
    Then the execution should be successful

  Scenario: insert player(name string, age int, hobby Set< string >, ids Set< int >, score Set< float >)
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    When executing query:
      """
      CREATE TAG player(name string, age int, hobby Set< string >, ids Set< int >, score Set< float >);
      CREATE TAG playerWithDefault(
          name string DEFAULT "",
          age int DEFAULT 18, isMarried bool DEFAULT false,
          BMI double DEFAULT 18.5, department string DEFAULT "engineering",
          birthday timestamp DEFAULT timestamp("2020-01-10T10:00:00"),
          number int DEFAULT 0
      );
      CREATE TAG school(name string, create_time timestamp);
      CREATE EDGE schoolmate(likeness int, nickname string);
      CREATE EDGE schoolmateWithDefault(likeness int DEFAULT 80);
      CREATE EDGE study(start_time timestamp, end_time timestamp);
      """
    Then the execution should be successful
    And wait 3 seconds
    When try to execute query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 42, set{}, set{}, set{});
      """
    Then the execution should be successful
    # Handle the edge cases by inserting incorrect types and handling errors
    When try to execute query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 42, {"Basketball", "Swimming", "Basketball"}, {1, 2, 2, 3}, {10.5, 20.5, 10.5, 30.5});
      """
    Then the execution should be successful
    # Insert and update the player vertex to modify only the sets
    When try to execute query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 42, {"Basketball", "Gaming"}, {4, 5, 6, 6}, {40.5, 50.5, 60.5});
      """
    Then the execution should be successful
    # Verify that the latest data matches the expected state
    When executing query:
      """
      FETCH PROP ON * "player100" YIELD vertex as node;
      """
    Then the result should be, in any order, with relax comparison:
      | node                                                                                                                     |
      | ("player100":player{name:"Tim Duncan", age:42, hobby:{"Basketball", "Gaming"}, ids:{4, 5, 6}, score:{40.5, 50.5, 60.5}}) |
    # Handle the edge cases by inserting incorrect types and handling errors
    When executing query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 2, ["Basketball", "Gaming"], {40.5, 50.5, 60.5}, {4, 5, 6, 6});
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    # Insert edges and fetch results
    When try to execute query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES "player100"->"player200":(85, "Lily");
      INSERT EDGE schoolmate(likeness, nickname) VALUES "player200"->"player300":("87", "");
      INSERT EDGE schoolmate(likeness) VALUES "player200"->"player300":("hello", "87");
      INSERT EDGE schoolmate(likeness) VALUES "player200"->"player300":(88, "");
      INSERT EDGE study(start_time, end_time) VALUES "player200"->"school1":(timestamp("2019-01-01T10:00:00"), now()+3600*24*365*3);
      """
    Then the execution should be successful
    # Insert vertices with default properties
    When try to execute query:
      """
      INSERT VERTEX playerWithDefault() VALUES "player400":();
      INSERT VERTEX playerWithDefault(age, isMarried, BMI) VALUES "player100":(18, false, 19.5);
      INSERT VERTEX playerWithDefault(name) VALUES "player100":("Tim Duncan");
      INSERT VERTEX playerWithDefault(name, age) VALUES "player100":("Tim Duncan", 20);
      INSERT VERTEX playerWithDefault(name, BMI, number) VALUES "player200":("Laura", 21.5, 20190901008), "player300":("Amber", 22.5, 20180901003);
      """
    Then the execution should be successful
    # Fetch properties and validate results
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD player.name, player.age, player.hobby, player.ids, player.score;
      """
    Then the result should be, in any order, with relax comparison:
      | player.name  | player.age | player.hobby             | player.ids | player.score       |
      | "Tim Duncan" | 42         | {"Basketball", "Gaming"} | {4, 5, 6}  | {40.5, 50.5, 60.5} |
    When executing query:
      """
      FETCH PROP ON playerWithDefault "player200" YIELD playerWithDefault.name, playerWithDefault.birthday, playerWithDefault.department;
      """
    Then the execution should be successful
    # Insert and query multi-tagged vertices and edges
    When executing query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player200":("Laura", 8, {"Basketball", "Reading"}, {7, 8, 9}, {70.5, 80.5, 90.5});
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player300":("Amber", 9, {"Swimming", "Painting"}, {10, 11, 12}, {100.5, 110.5, 120.5});
      INSERT EDGE schoolmateWithDefault() VALUES "player100"->"player200":(), "player100"->"player300":();
      GO FROM "player100" OVER schoolmateWithDefault YIELD $^.player.name, schoolmateWithDefault.likeness, $$.player.name;
      """
    Then the execution should be successful
