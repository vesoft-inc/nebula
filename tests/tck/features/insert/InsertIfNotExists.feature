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

  Scenario: insert player(name string, age int, hobby List< string >, ids List< int >, score List< float >)
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS player(name string, age int, hobby List< string >, ids List< int >, score List< float >);
      CREATE EDGE IF NOT EXISTS like(likeness int);
      CREATE TAG INDEX IF NOT EXISTS index_player_age ON player(age);
      """
    And wait 6 seconds  # Wait for the index to be created
    # Insert vertex data for player100
    When try to execute query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 42, ["Basketball", "Swimming"], [1, 2, 3], [10.0, 20.0]);
      """
    Then the execution should be successful
    # Insert multiple player vertices, using IF NOT EXISTS to avoid duplicate entries
    When executing query:
      """
      INSERT VERTEX IF NOT EXISTS
        player(name, age, hobby, ids, score)
      VALUES
        "player101":("Michael Jordan", 35, ["Basketball", "Baseball"], [4, 5, 6], [30.0, 40.0]),
        "player102":("LeBron James", 36, ["Basketball", "Football"], [7, 8, 9], [50.0, 60.0]),
        "player103":("Kobe Bryant", 34, ["Basketball", "Soccer"], [10, 11, 12], [70.0, 80.0]),
        "player100":("Tim Duncan", 40, ["Basketball", "Golf"], [13, 14, 15], [90.0, 100.0]);
      """
    Then the execution should be successful
    # Fetch the age property of player100
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD player.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 42  |
    # Insert an existing player vertex using IF NOT EXISTS
    When executing query:
      """
      INSERT VERTEX IF NOT EXISTS player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 45, ["Basketball", "Tennis"], [16, 17, 18], [110.0, 120.0]);
      """
    Then the execution should be successful
    # Fetch the age property of player100 again to verify data
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD player.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 42  |
    # Directly insert the existing player100 vertex and update its data
    When executing query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 50, ["Basketball", "Table Tennis"], [19, 20, 21], [130.0, 140.0]);
      """
    Then the execution should be successful
    # Fetch the age property of player100 again to verify the updated data
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD player.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 50  |
    # Insert an edge 'like', indicating that player100 likes player101
    When try to execute query:
      """
      INSERT EDGE like(likeness) VALUES "player100"->"player101":(95);
      """
    Then the execution should be successful
    # Fetch the edge property
    When executing query:
      """
      FETCH PROP ON like "player100"->"player101" YIELD like.likeness;
      """
    Then the result should be, in any order:
      | like.likeness |
      | 95            |
    # Insert an edge using IF NOT EXISTS
    When executing query:
      """
      INSERT EDGE IF NOT EXISTS like(likeness) VALUES "player100"->"player101":(50);
      """
    Then the execution should be successful
    # Fetch the edge property again to verify data
    When executing query:
      """
      FETCH PROP ON like "player100"->"player101" YIELD like.likeness;
      """
    Then the result should be, in any order:
      | like.likeness |
      | 95            |
    # Directly insert an existing edge and update its property
    When executing query:
      """
      INSERT EDGE like(likeness) VALUES "player100"->"player101":(100);
      """
    Then the execution should be successful
    # Fetch the edge property again to verify the updated data
    When executing query:
      """
      FETCH PROP ON like "player100"->"player101" YIELD like.likeness;
      """
    Then the result should be, in any order:
      | like.likeness |
      | 100           |
    # Query destination vertices starting from player100 using the like edge
    When executing query:
      """
      GO FROM "player100" over like YIELD like.likeness as like, like._src as src, like._dst as dst;
      """
    Then the result should be, in any order:
      | like | src        | dst        |
      | 100  | "player100" | "player101" |
    # Insert multiple player vertices and use multiple tags
    When try to execute query:
      """
      INSERT VERTEX IF NOT EXISTS
        player(name, age, hobby, ids, score)
      VALUES
        "player104":("Neal", 49, ["Basketball", "Acting"], [22, 23, 24], [150.0, 160.0]),
        "player105":("Magic Johnson", 60, ["Basketball", "Business"], [25, 26, 27], [170.0, 180.0]);
      """
    Then the execution should be successful
    # Fetch the age property of player104
    When executing query:
      """
      FETCH PROP ON player "player104" YIELD player.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 49  |
    # Fetch the hobby property of player105
    When executing query:
      """
      FETCH PROP ON player "player105" YIELD player.hobby as hobby;
      """
    Then the result should be, in any order, with relax comparison:
      | hobby                   |
      | ["Basketball", "Business"] |
    # Insert multiple edges, indicating associations between player100 and other players
    When try to execute query:
      """
      INSERT EDGE IF NOT EXISTS
        like(likeness)
      VALUES
        "player100"->"player102":(85),
        "player100"->"player103":(88);
      """
    Then the execution should be successful
    # Fetch the edge property
    When executing query:
      """
      FETCH PROP ON like "player100"->"player102" YIELD like.likeness;
      """
    Then the result should be, in any order:
      | like.likeness |
      | 85            |
    When executing query:
      """
      FETCH PROP ON like "player100"->"player103" YIELD like.likeness;
      """
    Then the result should be, in any order:
      | like.likeness |
      | 88            |
    # Delete player vertices
    When try to execute query:
      """
      DELETE TAG player FROM "player100", "player101", "player102", "player103";
      """
    Then the execution should be successful
    # Reinsert previously deleted player vertices
    When try to execute query:
      """
      INSERT VERTEX IF NOT EXISTS
        player(name, age, hobby, ids, score)
      VALUES
        "player106":("Stephen Curry", 32, ["Basketball", "Golf"], [28, 29, 30], [190.0, 200.0]),
        "player107":("Kevin Durant", 31, ["Basketball", "Video Games"], [31, 32, 33], [210.0, 220.0]);
      """
    Then the execution should be successful
    # Lookup player vertices with age less than 35
    When executing query:
      """
      LOOKUP ON player WHERE player.age < 35 YIELD player.name as name, player.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | name           | age |
      | "Stephen Curry" | 32  |
      | "Kevin Durant"  | 31  |
    # Lookup player vertices with age greater than 35
    When executing query:
      """
      LOOKUP ON player WHERE player.age > 35 YIELD player.name as name, player.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | name            | age |
      | "Neal"            | 49  |
      | "Magic Johnson"   | 60  |
    # Fetch properties for multiple player vertices
    When executing query:
      """
      FETCH PROP ON player "player106", "player107" YIELD player.name as name, player.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | name           | age |
      | "Stephen Curry" | 32  |
      | "Kevin Durant"  | 31  |
    # Delete player106 and player107 vertices
    When try to execute query:
      """
      DELETE TAG player FROM "player106", "player107";
      """
    Then the execution should be successful
  Scenario: insert player(name string, age int, hobby Set< string >, ids Set< int >, score Set< float >)
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS player(name string, age int, hobby Set< string >, ids Set< int >, score Set< float >);
      CREATE EDGE IF NOT EXISTS like(likeness int);
      CREATE TAG INDEX IF NOT EXISTS index_player_age ON player(age);
      """
    And wait 6 seconds  # Wait for the index to be created
    # Insert vertex data for player100 with duplicate items
    When try to execute query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 42, {"Basketball", "Swimming", "Basketball"}, {1, 2, 2, 3}, {10.0, 10.0, 20.0});
      """
    Then the execution should be successful
    # Insert multiple player vertices, using IF NOT EXISTS to avoid duplicate entries
    When executing query:
      """
      INSERT VERTEX IF NOT EXISTS
        player(name, age, hobby, ids, score)
      VALUES
        "player101":("Michael Jordan", 35, {"Basketball", "Baseball"}, {4, 5, 5, 6}, {30.0, 40.0}),
        "player102":("LeBron James", 36, {"Basketball", "Football"}, {7, 8, 8, 9}, {50.0, 60.0}),
        "player103":("Kobe Bryant", 34, {"Basketball", "Soccer"}, {10, 11, 11, 12}, {70.0, 80.0}),
        "player100":("Tim Duncan", 40, {"Basketball", "Golf"}, {13, 14, 14, 15}, {90.0, 100.0});
      """
    Then the execution should be successful
    # Fetch the age property of player100
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD player.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 42  |
    # Insert an existing player vertex using IF NOT EXISTS
    When executing query:
      """
      INSERT VERTEX IF NOT EXISTS player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 45, {"Basketball", "Tennis"}, {16, 17, 17, 18}, {110.0, 120.0});
      """
    Then the execution should be successful
    # Fetch the age property of player100 again to verify data
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD player.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 42  |
    # Directly insert the existing player100 vertex and update its data
    When executing query:
      """
      INSERT VERTEX player(name, age, hobby, ids, score) VALUES "player100":("Tim Duncan", 50, {"Basketball", "Table Tennis"}, {19, 20, 20, 21}, {130.0, 140.0});
      """
    Then the execution should be successful
    # Fetch the age property of player100 again to verify the updated data
    When executing query:
      """
      FETCH PROP ON player "player100" YIELD player.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 50  |
    # Insert an edge 'like', indicating that player100 likes player101
    When try to execute query:
      """
      INSERT EDGE like(likeness) VALUES "player100"->"player101":(95);
      """
    Then the execution should be successful
    # Fetch the edge property
    When executing query:
      """
      FETCH PROP ON like "player100"->"player101" YIELD like.likeness;
      """
    Then the result should be, in any order:
      | like.likeness |
      | 95            |
    # Insert an edge using IF NOT EXISTS
    When executing query:
      """
      INSERT EDGE IF NOT EXISTS like(likeness) VALUES "player100"->"player101":(50);
      """
    Then the execution should be successful
    # Fetch the edge property again to verify data
    When executing query:
      """
      FETCH PROP ON like "player100"->"player101" YIELD like.likeness;
      """
    Then the result should be, in any order:
      | like.likeness |
      | 95            |
    # Directly insert an existing edge and update its property
    When executing query:
      """
      INSERT EDGE like(likeness) VALUES "player100"->"player101":(100);
      """
    Then the execution should be successful
    # Fetch the edge property again to verify the updated data
    When executing query:
      """
      FETCH PROP ON like "player100"->"player101" YIELD like.likeness;
      """
    Then the result should be, in any order:
      | like.likeness |
      | 100           |
    # Query destination vertices starting from player100 using the like edge
    When executing query:
      """
      GO FROM "player100" over like YIELD like.likeness as like, like._src as src, like._dst as dst;
      """
    Then the result should be, in any order:
      | like | src        | dst        |
      | 100  | "player100" | "player101" |
    # Insert multiple player vertices and use multiple tags
    When try to execute query:
      """
      INSERT VERTEX IF NOT EXISTS
        player(name, age, hobby, ids, score)
      VALUES
        "player104":("Neal", 49, {"Basketball", "Acting"}, {22, 23, 23, 24}, {150.0, 160.0}),
        "player105":("Magic Johnson", 60, {"Basketball", "Business"}, {25, 26, 26, 27}, {170.0, 180.0});
      """
    Then the execution should be successful
    # Fetch the age property of player104
    When executing query:
      """
      FETCH PROP ON player "player104" YIELD player.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 49  |
    # Fetch the hobby property of player105
    When executing query:
      """
      FETCH PROP ON player "player105" YIELD player.hobby as hobby;
      """
    Then the result should be, in any order, with relax comparison:
      | hobby                   |
      | {"Basketball", "Business"} |
    # Insert multiple edges, indicating associations between player100 and other players
    When try to execute query:
      """
      INSERT EDGE IF NOT EXISTS
        like(likeness)
      VALUES
        "player100"->"player102":(85),
        "player100"->"player103":(88);
      """
    Then the execution should be successful
    # Fetch the edge property
    When executing query:
      """
      FETCH PROP ON like "player100"->"player102" YIELD like.likeness;
      """
    Then the result should be, in any order:
      | like.likeness |
      | 85            |
    When executing query:
      """
      FETCH PROP ON like "player100"->"player103" YIELD like.likeness;
      """
    Then the result should be, in any order:
      | like.likeness |
      | 88            |
    # Delete player vertices
    When try to execute query:
      """
      DELETE TAG player FROM "player100", "player101", "player102", "player103";
      """
    Then the execution should be successful
    # Reinsert previously deleted player vertices
    When try to execute query:
      """
      INSERT VERTEX IF NOT EXISTS
        player(name, age, hobby, ids, score)
      VALUES
        "player106":("Stephen Curry", 32, {"Basketball", "Golf"}, {28, 29, 29, 30}, {190.0, 200.0}),
        "player107":("Kevin Durant", 31, {"Basketball", "Video Games"}, {31, 32, 32, 33}, {210.0, 220.0});
      """
    Then the execution should be successful
    # Lookup player vertices with age less than 35
    When executing query:
      """
      LOOKUP ON player WHERE player.age < 35 YIELD player.name as name, player.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | name           | age |
      | "Stephen Curry" | 32  |
      | "Kevin Durant"  | 31  |
    # Lookup player vertices with age greater than 35
    When executing query:
      """
      LOOKUP ON player WHERE player.age > 35 YIELD player.name as name, player.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | name            | age |
      | "Neal"            | 49  |
      | "Magic Johnson"   | 60  |
    # Fetch properties for multiple player vertices
    When executing query:
      """
      FETCH PROP ON player "player106", "player107" YIELD player.name as name, player.age as age;
      """
    Then the result should be, in any order, with relax comparison:
      | name           | age |
      | "Stephen Curry" | 32  |
      | "Kevin Durant"  | 31  |
    # Delete player106 and player107 vertices
    When try to execute query:
      """
      DELETE TAG player FROM "player106", "player107";
      """
    Then the execution should be successful