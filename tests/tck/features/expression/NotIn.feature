# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Not In Expression

  Scenario: yield NOT IN list
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD 1 NOT IN [1, 2, 3] AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |
    When executing query:
      """
      YIELD 0 NOT IN [1, 2, 3] AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD 'hello' NOT IN ['hello', 'world', 3] AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |

  Scenario: yield NOT IN set
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD 1 NOT IN {1, 2, 3} AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |
    When executing query:
      """
      YIELD 0 NOT IN {1, 2, 3} AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    When executing query:
      """
      YIELD 'hello' NOT IN {'hello', 'world', 3} AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |

  Scenario: Match Not In Set
    Given a graph with space named "nba"
    When executing query:
      """
      match p0 =  (n0)<-[e0:`like`|`teammate`|`teammate`]->(n1)
      where id(n0) == "Suns"
      and  not (e0.like.likeness in  [e0.teammate.end_year, ( e0.teammate.start_year ) ] )
      or  not (( "" ) not ends with ( ""  +  ""  +  "" ))
      and ("" not in ( ""  +  ""  +  ""  +  "" ))
      or (e0.teammate.start_year > ( e0.teammate.end_year ))
      and (( ( ( e0.like.likeness ) ) ) / e0.teammate.start_year >
      e0.teammate.start_year)
      or (e0.like.likeness*e0.teammate.start_year%e0.teammate.end_year+
      ( ( e0.teammate.start_year ) ) > e0.teammate.end_year)
      or (( ( ( ( e0.teammate.end_year ) ) ) ) in  [9.8978784E7 ] )
      return e0.like.likeness, e0.teammate.start_year, e0.teammate.start_year,
      e0.teammate.end_year, e0.teammate.end_year
      limit 91
      """
    Then a SemanticError should be raised at runtime: Type error `("" NOT IN "")'

  @jmq
  Scenario: Using NOT IN list in GO
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT IN ['Danny Green']
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.player.name      |
      | 'LaMarcus Aldridge' |
      | 'Manu Ginobili'     |
      | 'Tim Duncan'        |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT IN ['Danny Green']
      YIELD like._dst
      """
    Then the result should be, in any order:
      | like._dst           |
      | 'LaMarcus Aldridge' |
      | 'Manu Ginobili'     |
      | 'Tim Duncan'        |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like.likeness NOT IN [95,56,21]
      YIELD $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $$.player.name      | like.likeness |
      | 'LaMarcus Aldridge' | 90            |
    When executing query:
      """
      $A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
      GO FROM $A.ID OVER like WHERE like.likeness NOT IN [95,56,21]
      YIELD $^.player.name, $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $^.player.name      | $$.player.name | like.likeness |
      | 'Manu Ginobili'     | 'Tim Duncan'   | 90            |
      | 'LaMarcus Aldridge' | 'Tim Duncan'   | 75            |
      | 'LaMarcus Aldridge' | 'Tony Parker'  | 75            |

  Scenario: Using NOT IN set in GO
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT IN {'Danny Green'}
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.player.name      |
      | 'LaMarcus Aldridge' |
      | 'Manu Ginobili'     |
      | 'Tim Duncan'        |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT IN {'Danny Green'}
      YIELD like._dst
      """
    Then the result should be, in any order:
      | like._dst           |
      | 'LaMarcus Aldridge' |
      | 'Manu Ginobili'     |
      | 'Tim Duncan'        |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like.likeness NOT IN {95,56,21}
      YIELD $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $$.player.name      | like.likeness |
      | 'LaMarcus Aldridge' | 90            |
