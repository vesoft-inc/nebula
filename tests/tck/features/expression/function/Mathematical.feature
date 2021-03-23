# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Mathematical function Expression

  Scenario: bit functions
    When executing query:
      """
      return bit_and(5,bit_xor(4,bit_or(1,2))) as basic_test
      """
    Then the result should be, in any order:
      | basic_test |
      | 5          |
    When executing query:
      """
      return [bit_and(5,null),bit_or(5,null),bit_xor(5,null)] as null_test
      """
    Then the result should be, in any order:
      | null_test        |
      | [NULL,NULL,NULL] |
    When executing query:
      """
      return [bit_and(5,true),bit_or(2,1.3),bit_xor("5",1)] as error_test
      """
    Then the result should be, in any order:
      | error_test                     |
      | [BAD_TYPE, BAD_TYPE, BAD_TYPE] |
