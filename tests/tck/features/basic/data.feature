Feature: data

  Background: Prepare space
    Given a graph with space named "nba"

  Scenario: list, set, map
    When executing query:
      """
      RETURN size(LIST[]) AS a, size(SET{}) AS b, size(MAP{}) AS c
      """
    Then the result should be, in any order, with relax comparison:
      | a | b | c |
      | 0 | 0 | 0 |
    When executing query:
      """
      RETURN 1 IN LIST[] AS a, "Tony" IN SET{} AS b, "a" IN MAP{} AS c
      """
    Then the result should be, in any order, with relax comparison:
      | a     | b     | c     |
      | false | false | false |
    When executing query:
      """
      RETURN LIST[1, 2] AS a, SET{1, 2, 1} AS b, MAP{a:1, b:2} AS c, MAP{a: LIST[1,2], b: SET{1,2,1}, c: "hee"} AS d
      """
    Then the result should be, in any order, with relax comparison:
      | a      | b      | c          | d                                |
      | [1, 2] | {1, 2} | {a:1, b:2} | {a: [1, 2], b: {2, 1}, c: "hee"} |
    When executing query:
      """
      RETURN 1 IN LIST[1, 2] AS a, 2 IN SET{1, 2, 1} AS b, "a" IN MAP{a:1, b:2} AS c, MAP{a: LIST[1,2], b: SET{1,2,1}, c: "hee"}["b"] AS d
      """
    Then the result should be, in any order, with relax comparison:
      | a    | b    | c    | d      |
      | true | true | true | {2, 1} |
    When executing query:
      """
      RETURN [], {}, {}
      """
    Then a SyntaxError should be raised at runtime:
    When executing query:
      """
      RETURN [1, 2] AS a, {1, 2, 1} AS b, {a:1, b:2} AS c
      """
    Then the result should be, in any order, with relax comparison:
      | a      | b      | c          |
      | [1, 2] | {1, 2} | {a:1, b:2} |
    When executing query:
      """
      RETURN 2 IN [1, 2] AS a, 2 IN {1, 2, 1} AS b, "b" IN MAP{a:1, b:2} AS c
      """
    Then the result should be, in any order, with relax comparison:
      | a    | b    | c    |
      | true | true | true |
