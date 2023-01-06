# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Test lookup on ngdata data

  Background:
    Given a graph with space named "ngdata"

  Scenario: lookup on the tag property of double type
    When executing query:
      """
      LOOKUP ON Label_11 WHERE Label_11.Label_11_5_Double>0 YIELD id(vertex) AS vid
      """
    Then the result should be, in any order:
      | vid |
      | 88  |
      | 11  |
      | 19  |
      | 69  |
      | 104 |
      | 45  |
      | 79  |
      | 83  |
      | 383 |
      | 60  |
      | 75  |
      | 22  |
      | 108 |
      | 39  |
      | 68  |
      | 89  |
      | 12  |
      | 2   |
      | 33  |
      | 28  |
      | 41  |
      | 59  |
      | 16  |
      | 105 |
      | 71  |
      | 56  |
      | 31  |
      | 103 |
      | 4   |
      | 92  |
      | 24  |
      | 21  |
      | 107 |
      | 10  |
      | 82  |
      | 63  |
      | 7   |
      | 52  |
      | 99  |
      | 6   |
      | 85  |
      | 14  |
      | 74  |
      | 30  |
      | 26  |
      | 50  |
      | 38  |
      | 47  |
      | 3   |
      | 36  |
      | 96  |
      | 78  |
      | 80  |
      | 25  |
      | 97  |
      | 81  |
      | 15  |
      | 27  |
      | 98  |
      | 102 |
      | 101 |
      | 70  |
      | 1   |
      | 77  |
      | 53  |
    When executing query:
      """
      LOOKUP ON Label_11 WHERE Label_11.Label_11_5_Double>10 YIELD id(vertex) AS vid
      """
    Then the result should be, in any order:
      | vid |
