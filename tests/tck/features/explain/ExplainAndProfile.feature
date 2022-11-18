Feature: Explain and Profile

  Background:
    Given a graph with space named "nba"

  Scenario Outline: Different format
    When executing query:
      """
      <explain> FORMAT="<format>" YIELD 1
      """
    Then the execution should be successful
    When executing query:
      """
      <explain> FORMAT="<format>" {
        $var=YIELD 1 AS a;
        YIELD $var.*;
      }
      """
    Then the execution should be successful
    When executing query:
      """
      <explain> FORMAT="<format>" {
        YIELD 1 AS a;
      }
      """
    Then the execution should be successful

    Examples:
      | explain | format     |
      | EXPLAIN | row        |
      | EXPLAIN | dot        |
      | EXPLAIN | dot:struct |
      | PROFILE | row        |
      | PROFILE | dot        |
      | PROFILE | dot:struct |

  Scenario Outline: Error format
    When executing query:
      """
      <explain> FORMAT="unknown" YIELD 1
      """
    Then a SyntaxError should be raised at runtime.
    When executing query:
      """
      <explain> FORMAT="unknown" {
        $var=YIELD 1 AS a;
        YIELD $var.*;
      }
      """
    Then a SyntaxError should be raised at runtime.
    When executing query:
      """
      <explain> FORMAT="unknown" {
        YIELD 1 AS a;
      }
      """
    Then a SyntaxError should be raised at runtime.
    When executing query:
      """
      <explain> EXPLAIN YIELD 1
      """
    Then a SyntaxError should be raised at runtime.
    When executing query:
      """
      <explain> PROFILE YIELD 1
      """
    Then a SyntaxError should be raised at runtime.

    Examples:
      | explain |
      | EXPLAIN |
      | PROFILE |

  Scenario: Test profiling data format
    When profiling query:
      """
      GO 4 STEPS FROM 'Tim Duncan' OVER like YIELD like._dst AS dst | YIELD count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 6        |
    And the execution plan should be:
      | id | name         | dependencies | profiling data                                                                                                                                                  | operator info     |
      | 7  | Aggregate    | 6            | {"version":0, "rows": 1}                                                                                                                                        |                   |
      | 6  | Project      | 5            | {"version":0, "rows": 6}                                                                                                                                        |                   |
      | 5  | GetNeighbors | 4            | {"version":0, "rows": 6, "resp[0]": {"vertices": 3}}                                                                                                            |                   |
      | 4  | Loop         | 0            | [{"version":0, "rows": 1},{"version":1, "rows": 1},{"version":2, "rows": 1},{"version":3, "rows": 1}]                                                           | {"loopBody": "3"} |
      | 3  | Dedup        | 2            | [{"version":0, "rows": 2},{"version":1, "rows": 3},{"version":2, "rows": 3}]                                                                                    |                   |
      | 2  | GetDstBySrc  | 1            | [{"version":0, "rows": 2, "resp[0]": {"vertices": 2}},{"version":1, "rows": 3, "resp[0]":{"vertices": 3}}, {"version":2, "rows": 3, "resp[0]":{"vertices": 3}}] |                   |
      | 1  | Start        |              | [{"version":0, "rows": 0},{"version":1, "rows": 0},{"version":2, "rows": 0}]                                                                                    |                   |
      | 0  | Start        |              | {"version":0, "rows": 0}                                                                                                                                        |                   |
