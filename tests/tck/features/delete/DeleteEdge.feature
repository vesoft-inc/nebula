# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Delete string vid of edge

  Scenario: delete edges
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS person(name string, age int);
      CREATE EDGE IF NOT EXISTS friend(intimacy int);
      CREATE EDGE IF NOT EXISTS schoolmate(likeness int);
      CREATE EDGE IF NOT EXISTS transfer(money int);
      """
    And having executed:
      """
      INSERT VERTEX
        person(name, age)
      VALUES
        "Zhangsan":("Zhangsan", 22),
        "Lisi":("Lisi", 23),
        "Jack":("Jack", 18),
        "Rose":("Rose", 19);
      INSERT EDGE
        friend(intimacy)
      VALUES
        "Zhangsan"->"Lisi"@15:(90),
        "Zhangsan"->"Jack"@12:(50),
        "Jack"->"Rose"@13:(100);
      INSERT EDGE
        schoolmate(likeness)
      VALUES
        "Zhangsan"->"Jack":(60),
        "Lisi"->"Rose":(70);
      INSERT EDGE
        transfer(money)
      VALUES
        "Zhangsan"->"Lisi"@1561013236:(33),
        "Zhangsan"->"Lisi"@1561013237:(77);
      """
    # before get result by go
    When executing query:
      """
      GO FROM "Zhangsan", "Jack" OVER friend
      YIELD $^.person.name, friend.intimacy, friend._dst
      """
    Then the result should be, in any order:
      | $^.person.name | friend.intimacy | friend._dst |
      | "Zhangsan"     | 90              | "Lisi"      |
      | "Zhangsan"     | 50              | "Jack"      |
      | "Jack"         | 100             | "Rose"      |
    # before delete edge to check value by go
    When executing query:
      """
      GO FROM "Zhangsan", "Lisi" OVER schoolmate
      YIELD $^.person.name, schoolmate.likeness, schoolmate._dst
      """
    Then the result should be, in any order:
      | $^.person.name | schoolmate.likeness | schoolmate._dst |
      | "Zhangsan"     | 60                  | "Jack"          |
      | "Lisi"         | 70                  | "Rose"          |
    # before delete edge to check value by go
    When executing query:
      """
      GO FROM "Zhangsan" OVER transfer
      YIELD $^.person.name,transfer._rank, transfer.money, transfer._dst
      """
    Then the result should be, in any order:
      | $^.person.name | transfer._rank | transfer.money | transfer._dst |
      | "Zhangsan"     | 1561013236     | 33             | "Lisi"        |
      | "Zhangsan"     | 1561013237     | 77             | "Lisi"        |
    # delete edge friend
    When executing query:
      """
      DELETE EDGE friend "Zhangsan"->"Lisi"@15, "Jack"->"Rose"@13;
      """
    Then the execution should be successful
    # delete edge schoolmate
    When executing query:
      """
      DELETE EDGE schoolmate "Lisi"->"Rose"
      """
    Then the execution should be successful
    # delete edge transfer
    When executing query:
      """
      DELETE EDGE transfer "Zhangsan"->"Lisi"@1561013237
      """
    Then the execution should be successful
    # after delete edge to check value by go
    When executing query:
      """
      GO FROM "Zhangsan", "Jack" OVER friend
      YIELD $^.person.name, friend.intimacy, friend._dst
      """
    Then the result should be, in any order:
      | $^.person.name | friend.intimacy | friend._dst |
      | "Zhangsan"     | 50              | "Jack"      |
    # after delete edge to check value by go
    When executing query:
      """
      GO FROM "Zhangsan", "Lisi" OVER schoolmate
      YIELD $^.person.name, schoolmate.likeness, schoolmate._dst
      """
    Then the result should be, in any order:
      | $^.person.name | schoolmate.likeness | schoolmate._dst |
      | "Zhangsan"     | 60                  | "Jack"          |
    # after delete edge to check value by go
    When executing query:
      """
      GO FROM "Zhangsan" OVER transfer
      YIELD $^.person.name,transfer._rank, transfer.money, transfer._dst
      """
    Then the result should be, in any order:
      | $^.person.name | transfer._rank | transfer.money | transfer._dst |
      | "Zhangsan"     | 1561013236     | 33             | "Lisi"        |
    # delete non-existing edges and a same edge
    When executing query:
      """
      DELETE EDGE friend "Zhangsan"->"Rose", "1008"->"1009"@17,"Zhangsan"->"Lisi"@15
      """
    Then the execution should be successful
    # check value by go
    When executing query:
      """
      GO FROM "Zhangsan", "Jack" OVER friend
      YIELD $^.person.name, friend.intimacy, friend._dst
      """
    Then the result should be, in any order:
      | $^.person.name | friend.intimacy | friend._dst |
      | "Zhangsan"     | 50              | "Jack"      |
    Then drop the used space

  Scenario: delete edges use pipe
    Given load "nba" csv data to a new space
    # test delete with pipe wrong vid type
    When executing query:
      """
      GO FROM "Boris Diaw" OVER like YIELD like._type as id | DELETE EDGE like $-.id->$-.id
      """
    Then a ExecutionError should be raised at runtime: Wrong srcId type `INT`, value
    # delete with pipe, get result by go
    When executing query:
      """
      GO FROM "Boris Diaw" OVER like
      """
    Then the result should be, in any order:
      | like._dst     |
      | "Tony Parker" |
      | "Tim Duncan"  |
    When executing query:
      """
      GO FROM "Boris Diaw" OVER like
      YIELD like._src as src, like._dst as dst, like._rank as rank
      | DELETE EDGE like $-.src->$-.dst @ $-.rank
      """
    Then the execution should be successful
    When executing query:
      """
      GO FROM "Boris Diaw" OVER like
      """
    Then the result should be, in any order:
      | like._dst |
    # delete with var, get result by go
    When executing query:
      """
      GO FROM "Russell Westbrook" OVER like
      """
    Then the result should be, in any order:
      | like._dst      |
      | "Paul George"  |
      | "James Harden" |
    When executing query:
      """
      $var = GO FROM "Russell Westbrook" OVER like YIELD
      like._src as src, like._dst as dst, like._rank as rank;
      DELETE EDGE like $var.src -> $var.dst @ $var.rank
      """
    Then the execution should be successful
    When executing query:
      """
      GO FROM "Russell Westbrook" OVER like
      """
    Then the result should be, in any order:
      | like._dst |
    And drop the used space
