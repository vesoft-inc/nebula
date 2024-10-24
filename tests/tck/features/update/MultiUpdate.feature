# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
@multi_update
Feature: Multi Update string vid of vertex and edge

  Background: Prepare space
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                 |
      | replica_factor | 1                 |
      | vid_type       | FIXED_STRING(200) |
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
        "200"->"102"@0:(3, 2018),
        "201"->"102"@0:(3, 2019),
        "202"->"102"@0:(3, 2019);
      INSERT EDGE
        like(likeness)
      VALUES
        "200"->"201"@0:(92.5),
        "201"->"200"@0:(85.6),
        "201"->"202"@0:(93.2);
      """

  Scenario: multi update test
    When executing query:
      """
      UPDATE VERTEX ON course "101", "102"
      SET credits = credits + 1;
      """
    Then the execution should be successful
    When executing query:
      """
      UPDATE VERTEX ON course "101", "102"
      SET credits = credits + 1
      WHEN name == "Math" AND credits > 2
      """
    Then the execution should be successful
    When executing query:
      """
      UPDATE VERTEX ON course "101", "102"
      SET credits = credits + 1
      YIELD name AS Name, credits AS Credits
      """
    Then the result should be, in any order:
      | Name      | Credits |
      | 'Math'    | 6       |
      | 'English' | 8       |
    When executing query:
      """
      UPDATE VERTEX ON course "101", "102"
      SET credits = credits + 1
      WHEN name == "Math" AND credits > 2
      YIELD name AS Name, credits AS Credits
      """
    Then the result should be, in any order:
      | Name      | Credits |
      | 'Math'    | 7       |
      | 'English' | 8       |
    When executing query:
      """
      UPDATE VERTEX ON course "101", "102"
      SET credits = credits + 1
      WHEN name == "nonexistent" AND credits > 2
      YIELD name AS Name, credits AS Credits
      """
    Then the result should be, in any order:
      | Name      | Credits |
      | 'Math'    | 7       |
      | 'English' | 8       |
    When executing query:
      """
      FETCH PROP ON select "200"->"101"@0 YIELD select.grade, select.year
      """
    Then the result should be, in any order:
      | select.grade | select.year |
      | 5            | 2018        |
    When executing query:
      """
      FETCH PROP ON select "200" -> "102"@0 YIELD select.grade, select.year
      """
    Then the result should be, in any order:
      | select.grade | select.year |
      | 3            | 2018        |
    # update edge succeeded
    When executing query:
      """
      UPDATE EDGE ON select "200"->"101"@0, "200"->"102"@0
      SET grade = grade + 1, year = 2000;
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON select "200"->"101"@0 YIELD select.grade, select.year
      """
    Then the result should be, in any order:
      | select.grade | select.year |
      | 6            | 2000        |
    When executing query:
      """
      FETCH PROP ON select "200"->"102"@0 YIELD select.grade, select.year
      """
    Then the result should be, in any order:
      | select.grade | select.year |
      | 4            | 2000        |
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
      UPDATE EDGE ON select "200"->"101"@0, "200"->"102"@0
      SET grade = grade + 1, year = 2000
      WHEN grade > 1024
      """
    Then the execution should be successful
    # set filter
    When executing query:
      """
      UPDATE EDGE ON select "200"->"101"@0, "200"->"102"@0
      SET grade = grade + 1, year = 2000 WHEN grade > 4
      """
    Then the execution should be successful
    # set yield
    When executing query:
      """
      UPDATE EDGE ON select "200"->"101"@0, "200"->"102"@0
      SET grade = grade + 1, year = 2018
      YIELD grade AS Grade, year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year |
      | 8     | 2018 |
      | 5     | 2018 |
    # filter and yield
    When executing query:
      """
      UPDATE EDGE ON select "200"->"101"@0, "200"->"102"@0
      SET grade = select.grade + 1, year = 2019
      WHEN select.grade > 6
      YIELD select.grade AS Grade, select.year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year |
      | 9     | 2019 |
      | 5     | 2018 |
    # set filter out and yield
    When executing query:
      """
      UPDATE EDGE ON select "200"->"101"@0, "200"->"102"@0
      SET grade = grade + 1, year = 2019
      WHEN grade > 233333333333
      YIELD grade AS Grade, year AS Year
      """
    Then the result should be, in any order:
      | Grade | Year |
      | 9     | 2019 |
      | 5     | 2018 |
    # update vertex: the item is TagName.PropName = Expression in SET clause
    When executing query:
      """
      UPDATE VERTEX ON course "101", "102"
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
      UPDATE EDGE ON select "200"->"101"@0, "200"->"102"@0
      SET nonexistentProperty = grade + 1, year = 2019
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Edge prop not found.
    # make sure the edge_type must not exist
    When executing query:
      """
      UPDATE EDGE ON nonexistentEdgeTypeName "200"->"101"@0, "200"->"102"@0
      SET grade = grade + 1, year = 2019
      """
    Then a SemanticError should be raised at runtime: No schema found for `nonexistentEdgeTypeName'
    When executing query:
      """
      FETCH PROP ON course "103" YIELD course.name, course.credits
      """
    Then the result should be, in any order:
      | course.name | course.credits |
      | "CS"        | 5              |
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
      UPDATE VERTEX ON course "103", "104" SET credits = credits + 1
      WHEN name == "CS" AND credits > 2
      YIELD name AS Name, credits AS Credits
      """
    Then a ExecutionError should be raised at runtime: Storage Error: Vertex or edge not found.
