# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Delete int vid of edge

  Background: Prepare space
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9   |
      | replica_factor | 1   |
      | vid_type       | int |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS person(name string, age int);
      CREATE EDGE IF NOT EXISTS friend(intimacy int);
      CREATE EDGE IF NOT EXISTS schoolmate(likeness int);
      CREATE EDGE IF NOT EXISTS transfer(money int);
      """
    And wait 3 seconds

  Scenario: delete edges
    When executing query:
      """
      INSERT VERTEX person(name, age) VALUES
      hash("Zhangsan"):("Zhangsan", 22),
      hash("Lisi"):("Lisi", 23),
      hash("Jack"):("Jack", 18),
      hash("Rose"):("Rose", 19);
      INSERT EDGE friend(intimacy) VALUES
      hash("Zhangsan")->hash("Lisi")@15:(90),
      hash("Zhangsan")->hash("Jack")@12:(50),
      hash("Jack")->hash("Rose")@13:(100);
      INSERT EDGE schoolmate(likeness) VALUES
      hash("Zhangsan")->hash("Jack"):(60),
      hash("Lisi")->hash("Rose"):(70);
      INSERT EDGE transfer(money) VALUES
      hash("Zhangsan")->hash("Lisi")@1561013236:(33),
      hash("Zhangsan")->hash("Lisi")@1561013237:(77);
      """
    Then the execution should be successful
    # before get result by go
    When executing query:
      """
      GO FROM hash("Zhangsan"), hash("Jack") OVER friend
      YIELD $^.person.name, friend.intimacy, friend._dst
      """
    Then the result should be, in any order:
      | $^.person.name | friend.intimacy | friend._dst  |
      | "Zhangsan"     | 90              | hash("Lisi") |
      | "Zhangsan"     | 50              | hash("Jack") |
      | "Jack"         | 100             | hash("Rose") |
    # before delete edge to check value by go
    When executing query:
      """
      GO FROM hash("Zhangsan"), hash("Lisi") OVER schoolmate
      YIELD $^.person.name, schoolmate.likeness, schoolmate._dst
      """
    Then the result should be, in any order:
      | $^.person.name | schoolmate.likeness | schoolmate._dst |
      | "Zhangsan"     | 60                  | hash("Jack")    |
      | "Lisi"         | 70                  | hash("Rose")    |
    # before delete edge to check value by go
    When executing query:
      """
      GO FROM hash("Zhangsan") OVER transfer
      YIELD $^.person.name,transfer._rank, transfer.money, transfer._dst
      """
    Then the result should be, in any order:
      | $^.person.name | transfer._rank | transfer.money | transfer._dst |
      | "Zhangsan"     | 1561013236     | 33             | hash("Lisi")  |
      | "Zhangsan"     | 1561013237     | 77             | hash("Lisi")  |
    # delete edge friend
    When executing query:
      """
      DELETE EDGE friend hash("Zhangsan")->hash("Lisi")@15, hash("Jack")->hash("Rose")@13;
      """
    Then the execution should be successful
    # delete edge schoolmate
    When executing query:
      """
      DELETE EDGE schoolmate hash("Lisi")->hash("Rose")
      """
    Then the execution should be successful
    # delete edge transfer
    When executing query:
      """
      DELETE EDGE transfer hash("Zhangsan")->hash("Lisi")@1561013237
      """
    Then the execution should be successful
    # after delete edge to check value by go
    When executing query:
      """
      GO FROM hash("Zhangsan"), hash("Jack") OVER friend
      YIELD $^.person.name, friend.intimacy, friend._dst
      """
    Then the result should be, in any order:
      | $^.person.name | friend.intimacy | friend._dst  |
      | "Zhangsan"     | 50              | hash("Jack") |
    # after delete edge to check value by go
    When executing query:
      """
      GO FROM hash("Zhangsan"), hash("Lisi") OVER schoolmate
      YIELD $^.person.name, schoolmate.likeness, schoolmate._dst
      """
    Then the result should be, in any order:
      | $^.person.name | schoolmate.likeness | schoolmate._dst |
      | "Zhangsan"     | 60                  | hash("Jack")    |
    # after delete edge to check value by go
    When executing query:
      """
      GO FROM hash("Zhangsan") OVER transfer
      YIELD $^.person.name,transfer._rank, transfer.money, transfer._dst
      """
    Then the result should be, in any order:
      | $^.person.name | transfer._rank | transfer.money | transfer._dst |
      | "Zhangsan"     | 1561013236     | 33             | hash("Lisi")  |
    # delete non-existing edges and a same edge
    When executing query:
      """
      DELETE EDGE friend hash("Zhangsan")->hash("Rose"), hash("1008")->hash("1009")@17,hash("Zhangsan")->hash("Lisi")@15
      """
    Then the execution should be successful
    # check value by go
    When executing query:
      """
      GO FROM hash("Zhangsan"),hash("Jack") OVER friend
      YIELD $^.person.name, friend.intimacy, friend._dst
      """
    Then the result should be, in any order:
      | $^.person.name | friend.intimacy | friend._dst  |
      | "Zhangsan"     | 50              | hash("Jack") |
    Then drop the used space
