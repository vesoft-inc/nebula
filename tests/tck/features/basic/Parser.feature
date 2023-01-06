# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Parser

  Scenario: Test special white space character
    When executing query:
      """
      SHOW  SPACES
      """
    Then the execution should be successful
    When executing query:
      """
      RETURN  1
      """
    Then the execution should be successful
