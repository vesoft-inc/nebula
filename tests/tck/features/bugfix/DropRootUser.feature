# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test drop root user

  # #4879
  Scenario: Drop root user
    When executing query:
      """
      DROP USER root
      """
    Then a SemanticError should be raised at runtime: Can't drop root user.
