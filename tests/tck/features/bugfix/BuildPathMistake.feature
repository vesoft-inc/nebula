# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test crash when null path expression

  Background:
    Given a graph with space named "nba"

  Scenario: Null path expression in multiple patterns
    When executing query:
      """
      MATCH p = ()-[:like*2]->(v:player) WHERE id(v) == 'Grant Hill' RETURN p
      """
    Then the result should be, in any order, with relax comparison:
      | p                                                                                                                                                                                                                           |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:like@0 {likeness: 90}]->("Grant Hill" :player{age: 46, name: "Grant Hill"})>         |
      | <("Grant Hill" :player{age: 46, name: "Grant Hill"})<-[:like@0 {likeness: 90}]-("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})<-[:like@0 {likeness: 90}]-("Grant Hill" :player{age: 46, name: "Grant Hill"})>     |
      | <("Vince Carter" :player{age: 42, name: "Vince Carter"})-[:like@0 {likeness: 90}]->("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})-[:like@0 {likeness: 90}]->("Grant Hill" :player{age: 46, name: "Grant Hill"})> |
