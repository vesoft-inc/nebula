# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Dangle limit

  # issue #2548
  Scenario: Dangle limit
    When executing query:
      """
      YIELD 1; LIMIT 1;
      """
    Then a SemanticError should be raised at runtime: Don't allowed dangle Limit sentence.
