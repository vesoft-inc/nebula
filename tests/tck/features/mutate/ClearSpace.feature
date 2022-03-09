# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

Feature: Clear space test
  This file is mainly used to test the function of clear space.

  Background:
    Given a session to nebula graph
  
  Scenario: [Syntax Test] Execute statement within space
    Given Given a space
    When Execute the clear statement in the space
    Then execution succeed
  
  Scenario: [Syntax Test] Execute statement without space
    Given Given a space
    When Execute the clear statement out the space
    Then execution succeed
  
  Scenario: [Syntax Test] Statement with if exists
    Given Given a space
    When Execute the clear with if exists
    Then execution succeed
  
  Scenario: [Syntax Test] Statement with if exists
    Given Not given a space
    When Execute the clear with if exists
    Then execution succeed
  
  Scenario: [Syntax Test] Statement without if exists
    Given Given a space
    When Execute the clear without if exists
    Then execution succeed
  
  Scenario: [Syntax Test] Statement without if exists
    Given Not given a space
    When Execute the clear without if exists
    Then execution failed