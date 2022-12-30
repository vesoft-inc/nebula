# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: match node label filter

  Background:
    Given a graph with space named "nba"

  Scenario: node label filter
    When executing query:
      """
      MATCH (v0)<-[e0:like]-(v1:bachelor)-[e1*2]->()
      WHERE id(v0) in ['Tony Parker', 'Spurs', 'Yao Ming']
      return type(e0), labels(v1)
      """
    Then the result should be, in any order:
      | type(e0) | labels(v1)             |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
      | "like"   | ["bachelor", "player"] |
    When executing query:
      """
      MATCH (v0)<-[e0:like]-(v1:bachelor)-[e1*0..2]->()
      WHERE id(v0) in ['Tony Parker', 'Spurs', 'Yao Ming']
      return labels(v1), count(*)
      """
    Then the result should be, in any order:
      | labels(v1)             | count(*) |
      | ["bachelor", "player"] | 34       |
    When executing query:
      """
      MATCH (v:bachelor)<-[e*2..2]-()<-[e1]-()
      WHERE id(v) in ['Tony Parker', 'Spurs', 'Tim Duncan']
      return labels(v), count(*)
      """
    Then the result should be, in any order:
      | labels(v)              | count(*) |
      | ["bachelor", "player"] | 184      |
