# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: NULL related operations

  Scenario: NULL comparison
    Given any graph
    When executing query:
      """
      RETURN NULL IS NULL AS value1, NULL == NULL AS value2, NULL != NULL AS value3, NULL >= NULL AS value4,  NULL <= NULL AS value5
      """
    Then the result should be, in any order:
      | value1 | value2 | value3 | value4 | value5 |
      | true   | NULL   | NULL   | NULL   | NULL   |

  Scenario: NULL with math functions
    Given any graph
    When executing query:
      """
      RETURN abs(NULL) AS value1, floor(NULL) AS value2, ceil(NULL) AS value3, round(NULL) AS value4, sqrt(NULL) AS value5
      """
    Then the result should be, in any order:
      | value1 | value2 | value3 | value4 | value5 |
      | NULL   | NULL   | NULL   | NULL   | NULL   |
    When executing query:
      """
      RETURN cbrt(NULL) AS value1, hypot(NULL, NULL) AS value2, pow(NULL, NULL) AS value3, exp(NULL) AS value4, exp2(NULL) AS value5
      """
    Then the result should be, in any order:
      | value1 | value2   | value3   | value4 | value5 |
      | NULL   | BAD_TYPE | BAD_TYPE | NULL   | NULL   |
    When executing query:
      """
      RETURN log(NULL) AS value1, log2(NULL) AS value2, log10(NULL) AS value3, sin(NULL) AS value4, asin(NULL) AS value5
      """
    Then the result should be, in any order:
      | value1 | value2 | value3 | value4 | value5 |
      | NULL   | NULL   | NULL   | NULL   | NULL   |
    When executing query:
      """
      RETURN cos(NULL) AS value1, acos(NULL) AS value2, tan(NULL) AS value3, atan(NULL) AS value4, rand32(NULL) AS value5
      """
    Then the result should be, in any order:
      | value1 | value2 | value3 | value4 | value5   |
      | NULL   | NULL   | NULL   | NULL   | BAD_TYPE |
    When executing query:
      """
      RETURN collect(NULL) AS value1, avg(NULL) AS value2, count(NULL) AS value3, max(NULL) AS value4, rand64(NULL,NULL) AS value5
      """
    Then the result should be, in any order:
      | value1 | value2 | value3 | value4 | value5   |
      | []     | NULL   | 0      | NULL   | BAD_TYPE |
    When executing query:
      """
      RETURN min(NULL) AS value1, std(NULL) AS value2, sum(NULL) AS value3, bit_and(NULL) AS value4, bit_or(NULL,NULL) AS value5
      """
    Then the result should be, in any order:
      | value1 | value2 | value3 | value4 | value5 |
      | NULL   | NULL   | 0      | NULL   | NULL   |
    When executing query:
      """
      RETURN bit_xor(NULL) AS value1, size(NULL) AS value2, range(NULL,NULL) AS value3, sign(NULL) AS value4, radians(NULL) AS value5
      """
    Then the result should be, in any order:
      | value1 | value2 | value3   | value4 | value5 |
      | NULL   | NULL   | BAD_TYPE | NULL   | NULL   |
