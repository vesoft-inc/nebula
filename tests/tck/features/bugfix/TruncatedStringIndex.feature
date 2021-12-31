# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Truncated string index

  Scenario: Truncated string index
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    When executing query:
      """
      create tag person(name string);
      create tag index p1 on person(name(3));
      """
    Then the execution should be successful
    And wait 6 seconds
    When executing query:
      """
      insert vertex person(name) values "1":("abc1"),"2":("abc2");
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON person WHERE person.name=="abc" YIELD id(vertex) as id
      """
    Then the result should be, in any order:
      | id |
    When executing query:
      """
      LOOKUP ON person WHERE person.name=="abc" YIELD person.name
      """
    Then the result should be, in any order:
      | person.name |
    When executing query:
      """
      match (v:person) where v.person.name == "abc" return v;
      """
    Then the result should be, in any order, with relax comparison:
      | v |
    When executing query:
      """
      match (v:person) where v.person.name >= "abc" return v;
      """
    Then the result should be, in any order, with relax comparison:
      | v                           |
      | ("1" :person{name: "abc1"}) |
      | ("2" :person{name: "abc2"}) |
    When executing query:
      """
      match (v:person{name:"abc1"}) return v;
      """
    Then the result should be, in any order, with relax comparison:
      | v                           |
      | ("1" :person{name: "abc1"}) |
    When executing query:
      """
      match (v:person) where v.person.name>"abc" return v;
      """
    Then the result should be, in any order, with relax comparison:
      | v                           |
      | ("1" :person{name: "abc1"}) |
      | ("2" :person{name: "abc2"}) |
    When executing query:
      """
      match (v:person) where v.person.name<="abc2" return v;
      """
    Then the result should be, in any order, with relax comparison:
      | v                           |
      | ("1" :person{name: "abc1"}) |
      | ("2" :person{name: "abc2"}) |
    Then drop the used space
